use std::{
    collections::{btree_map, BTreeMap}, fmt::format, fs::{File, OpenOptions}, io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write}, path::PathBuf, vec
};

use fs4::FileExt;
use serde::de::value;

use crate::{__function, pb, pg, ppb, ppg, pppb, pppg, pppr, pppy, ppr, ppy, pr, py};
use crate::{error::Result, pppp};
pub type KeyDir = BTreeMap<Vec<u8>, (u64, u32)>;
const LOG_HEADER_SIZE: u32 = 8;

// 磁盘存储引擎定义
pub struct DiskEngine {
    keydir: KeyDir,
   pub log: Log,
}

impl DiskEngine {
    pub fn new(file_path: PathBuf) -> Result<Self> {
        __function!("新建磁盘存储引擎中~~~");
        pppg!("开始构建日志文件系统~~~");
        let mut log = Log::new(file_path)?;
        pppg!("日志文件系统构建成功~~~");
        pppg!("开始构建内存索引系统~~~");

        // 从 log 中去恢复的 keydir
        let keydir = log.build_keydir()?;
        pppg!("内存索引系统构建成功~~");

        ppg!("现在的内存索引条目=>", keydir);
        pppg!("磁盘存储引擎构建成功，正式启动~~~");
        Ok(Self { keydir, log })
    }

    pub fn new_compact(file_path: PathBuf) -> Result<Self> {
        let mut eng = Self::new(file_path)?;
        eng.compact()?;
        Ok(eng)
    }

    fn compact(&mut self) -> Result<()> {
        // 新打开一个临时日志文件
        let mut new_path = self.log.file_path.clone();
        new_path.set_extension("compact");

        let mut new_log = Log::new(new_path)?;
        let mut new_keydir = KeyDir::new();

        // 重写数据到临时文件中
        for (key, (offset, val_size)) in self.keydir.iter() {
            // 读取 value
            let value = self.log.read_value(*offset, *val_size)?;
            let (new_offset, new_size) = new_log.write_entry(key, Some(&value))?;

            new_keydir.insert(
                key.clone(),
                (new_offset + new_size as u64 - *val_size as u64, *val_size),
            );
        }

        // 将临时文件更改为正式文件
        std::fs::rename(&new_log.file_path, &self.log.file_path)?;

        new_log.file_path = self.log.file_path.clone();
        self.keydir = new_keydir;
        self.log = new_log;

        Ok(())
    }
}

impl super::engine::Engine for DiskEngine {
    type EngineIterator<'a> = DiskEngineIterator<'a>;

    fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        // 先写日志
        let (offset, size) = self.log.write_entry(&key, Some(&value))?;
        // 更新内存索引
        // 100----------------|-----150
        //                   130
        // val size = 20
        let val_size = value.len() as u32;
        self.keydir
            .insert(key, (offset + size as u64 - val_size as u64, val_size));
        Ok(())
    }

    fn get(&mut self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        match self.keydir.get(&key) {
            Some((offset, val_size)) => {
                let val = self.log.read_value(*offset, *val_size)?;
                Ok(Some(val))
            }
            None => Ok(None),
        }
    }

    fn delete(&mut self, key: Vec<u8>) -> Result<()> {
        self.log.write_entry(&key, None)?;
        self.keydir.remove(&key);
        Ok(())
    }

    fn scan(&mut self, range: impl std::ops::RangeBounds<Vec<u8>>) -> Self::EngineIterator<'_> {
        DiskEngineIterator {
            inner: self.keydir.range(range),
            log: &mut self.log,
        }
    }
}

pub struct DiskEngineIterator<'a> {
    inner: btree_map::Range<'a, Vec<u8>, (u64, u32)>,
    log: &'a mut Log,
}

impl<'a> DiskEngineIterator<'a> {
    fn map(&mut self, item: (&Vec<u8>, &(u64, u32))) -> <Self as Iterator>::Item {
        let (k, (offset, val_size)) = item;
        let value = self.log.read_value(*offset, *val_size)?;
        Ok((k.clone(), value))
    }
}

impl<'a> super::engine::EngineIterator for DiskEngineIterator<'a> {}

impl<'a> Iterator for DiskEngineIterator<'a> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|item| self.map(item))
    }
}

impl<'a> DoubleEndedIterator for DiskEngineIterator<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner.next_back().map(|item| self.map(item))
    }
}

pub struct Log {
    file_path: PathBuf,
    file: std::fs::File,
}

impl Log {
    fn new(file_path: PathBuf) -> Result<Self> {
        // pppp!(40);
        __function!("启动日志文件系统~~");

        // 如果目录不存在的话则创建
        if let Some(dir) = file_path.parent() {
            if !dir.exists() {
                std::fs::create_dir_all(&dir)?;
            }
        }

        // 打开文件
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&file_path)?;

        // 加文件锁，保证同时只能有一个服务去使用这个文件
        file.try_lock_exclusive()?;

        Ok(Self { file, file_path })
    }

    // 遍历数据文件，构建内存索引

    fn build_keydir(&mut self) -> Result<KeyDir> {
        __function!("构建内存索引");
        let mut keydir = KeyDir::new();
        let file_size = self.file.metadata()?.len();
        let mut buf_reader = BufReader::new(&self.file);
        pppg!("初次启动引擎，循环读取日志条目，构建索引");
        let mut offset = 0;
        let mut count = 1;
        loop {
            if offset >= file_size {
                pppg!("日志为空，未有数据，直接退出");
                break;
            }
            let s = format!("第{}条数据:", count);
            count+=1;
            pppg!(s);
            let (key, val_size) = Self::read_entry(&mut buf_reader, offset)?;
            let key_size = key.len() as u32;
            // pppy!(key, val_size);
            pppy!(format!("key={:?},val_size={:?}",key,val_size));
            if val_size == -1 {
                pppg!("val_size=-1，这是删除的数据，不用他来构建索引");
                keydir.remove(&key);
                offset += key_size as u64 + LOG_HEADER_SIZE as u64;
            } else {
                pppg!("这是有效的数据，要用他来构建索引");
                keydir.insert(
                    key,
                    (
                        offset + LOG_HEADER_SIZE as u64 + key_size as u64,
                        val_size as u32,
                    ),
                );
                offset += key_size as u64 + val_size as u64 + LOG_HEADER_SIZE as u64;
            }
        }

        // pppy!("首次读取得到的索引内容为:", keydir);
        pppy!(format!("首次读到的索引内容为:{:?}",keydir));
        Ok(keydir)
    }

    // 遍历数据文件，构建内存索引

   pub  fn read_log(&mut self) -> Result<()> {
        __function!("读取日志文件");
        // let mut keydir = KeyDir::new();
        let file_size = self.file.metadata()?.len();
        let mut buf_reader = BufReader::new(&self.file);
        let mut offset = 0;
        let mut count =1;
        loop {
            if offset >= file_size {
                pppg!("日志读取完毕，直接退出");
                break;
            }
            let s = format!("第{}条数据:", count);
            count+=1;
            pppg!(s);
            let (key_size, val_size) = Self::read_log_entry(&mut buf_reader, offset)?;
            if val_size == -1 {
                offset += key_size as u64 + LOG_HEADER_SIZE as u64;
            } else {
                offset += key_size as u64 + val_size as u64 + LOG_HEADER_SIZE as u64;
            }
        }
        Ok(())
    }
    // +-------------+-------------+----------------+----------------+
    // | key len(4)    val len(4)     key(varint)       val(varint)  |
    // +-------------+-------------+----------------+----------------+
    fn write_entry(&mut self, key: &Vec<u8>, value: Option<&Vec<u8>>) -> Result<(u64, u32)> {
        // 首先将文件偏移移动到文件末尾
        let offset = self.file.seek(SeekFrom::End(0))?;
        let key_size = key.len() as u32;
        let val_size = value.map_or(0, |v| v.len() as u32);
        let total_size = key_size + val_size + LOG_HEADER_SIZE;

        // 写入 key size、value size、key、value
        let mut writer = BufWriter::with_capacity(total_size as usize, &self.file);
        writer.write_all(&key_size.to_be_bytes())?;
        writer.write_all(&value.map_or(-1, |v| v.len() as i32).to_be_bytes())?;
        writer.write_all(&key)?;
        if let Some(v) = value {
            writer.write_all(v)?;
        }
        writer.flush()?;

        Ok((offset, total_size)) //返回的是当前未追加日志的文件偏移和总的日志条目大小！
    }

    fn read_value(&mut self, offset: u64, val_size: u32) -> Result<Vec<u8>> {
        self.file.seek(SeekFrom::Start(offset))?;
        let mut buf = vec![0; val_size as usize];
        self.file.read_exact(&mut buf)?;
        Ok(buf)
    }

    fn read_entry(buf_reader: &mut BufReader<&File>, offset: u64) -> Result<(Vec<u8>, i32)> {
        buf_reader.seek(SeekFrom::Start(offset))?;
        let mut len_buf = [0; 4];

        // 读取 key size
        buf_reader.read_exact(&mut len_buf)?;
        let key_size = u32::from_be_bytes(len_buf);

        // 读取 value size
        buf_reader.read_exact(&mut len_buf)?;
        let val_size = i32::from_be_bytes(len_buf);

        // 读取 key
        let mut key = vec![0; key_size as usize];
        buf_reader.read_exact(&mut key)?;

        if val_size != -1 {
            // 读取 value
            let mut value = vec![0; val_size as usize];
            buf_reader.read_exact(&mut value)?;
            pppy!(format!("本次读取到的日志条目信息:key_size={},val_size={},key={:?},value={:?}",key_size,val_size,key,value));
            // pppy!(
            //     "",
            //     "key_size=",
            //     key_size,
            //     "val_size=",
            //     val_size,
            //     "key=",
            //     key,
            //     "value",
            //     value
            // );
        } else {
            pppy!(format!("本次读取到的日志条目信息:key_size={},val_size={},key={:?},value=None",key_size,val_size,key));

            // pppy!(
            //     "本次读取到的日志条目信息:",
            //     "key_size=",
            //     key_size,
            //     "val_size=",
            //     val_size,
            //     "key=",
            //     key,
            //     "value",
            //     "None"
            // );
        }

        Ok((key, val_size))
    }

    fn read_log_entry(buf_reader: &mut BufReader<&File>, offset: u64) -> Result<(u32, i32)> {
        buf_reader.seek(SeekFrom::Start(offset))?;
        let mut len_buf = [0; 4];

        // 读取 key size
        buf_reader.read_exact(&mut len_buf)?;
        let key_size = u32::from_be_bytes(len_buf);

        // 读取 value size
        buf_reader.read_exact(&mut len_buf)?;
        let val_size = i32::from_be_bytes(len_buf);

        // 读取 key
        let mut key = vec![0; key_size as usize];
        buf_reader.read_exact(&mut key)?;

        if val_size != -1 {
            // 读取 value
            let mut value = vec![0; val_size as usize];
            buf_reader.read_exact(&mut value)?;
            pppy!(
                "本次读取到的日志条目信息:",
                "key_size=",
                key_size,
                "val_size=",
                val_size,
                "key=",
                key,
                "value",
                value
            );
        } else {
            pppy!(
                "本次读取到的日志条目信息:",
                "key_size=",
                key_size,
                "val_size=",
                val_size,
                "key=",
                key,
                "value",
                "None"
            );
        }

        Ok((key_size, val_size))
    }
}

#[cfg(test)]
mod tets {
    // use futures::future::ok;
    use crate::{__function, pb, pg, ppb, ppg, pppb, pppg, pppr, pppy, ppr, ppy, pr, py};
    use crate::{
        error::Result,
        pppp,
        storage::{disk::DiskEngine, engine::Engine},
    };
    use std::path::PathBuf;

    use super::Log;
    use std::fs;

    #[test]
    fn delete_log_file() -> Result<()> {
        let log_dir = "/tmp/sqldb/sqldb-log";
        // 检查目录是否存在
        if !fs::metadata(log_dir).is_ok() {
            // 若目录不存在，直接返回 Ok(())
            pppg!("日志文件已被清空");
            return Ok(());
        }
        pppg!("正在清空日志文件");
        let res = std::fs::remove_dir_all("/tmp/sqldb")?;
        pppg!("清空日志文件完毕");
        Ok(res)
    }
    #[test]
    fn read_log_file() -> Result<()> {
        pppg!("开始读取日志文件~~~");
        let mut log = Log::new(PathBuf::from("/tmp/sqldb/sqldb-log"))?;
        log.read_log()?;
        pppg!("读取日志文件完毕~~~");
        Ok(())
    }
    #[test]
    fn read_log_file_compact() -> Result<()> {
        pppg!("开始读取日志文件~~~");
        pppg!("开启临时重写~~~");
        let mut eng = DiskEngine::new_compact(PathBuf::from("/tmp/sqldb/sqldb-log"))?;
        // let mut log = Log::new(PathBuf::from("/tmp/sqldb/sqldb-log"))?;
        eng.log.read_log()?;
        pppg!("读取日志文件完毕~~~"); 
        drop(eng);
        Ok(())
    }
    #[test]
    fn test_build_keydir_from_disk_init() -> Result<()> {
        pppp!(40);
        let mut eng: DiskEngine = DiskEngine::new(PathBuf::from("/tmp/sqldb/sqldb-log"))?;
        // 写一些数据
        eng.set(b"key1".to_vec(), b"value".to_vec())?;
        eng.set(b"key2".to_vec(), b"value".to_vec())?;
        eng.set(b"key3".to_vec(), b"value".to_vec())?;
        eng.delete(b"key1".to_vec())?;

        std::fs::remove_dir_all("/tmp/sqldb")?;
        Ok(())
    }
    #[test]
    fn test_build_keydir_from_disk_not_init() -> Result<()> {
        pppp!(40);
        let mut eng = DiskEngine::new(PathBuf::from("/tmp/sqldb/sqldb-log"))?;
        // 写一些数据
        eng.set(b"key1".to_vec(), b"value".to_vec())?;
        eng.set(b"key2".to_vec(), b"value".to_vec())?;
        eng.set(b"key3".to_vec(), b"value".to_vec())?;
        // 查看临时文件重写的情况
        eng.delete(b"key1".to_vec())?;
        eng.set(b"key3".to_vec(), b"value3".to_vec())?;
        Ok(())
    }
    #[test]
    fn test_disk_duplicate_date() -> Result<()> {
        pppp!(40);
        let mut eng = DiskEngine::new(PathBuf::from("/tmp/sqldb/sqldb-log"))?;
        // 写一些数据
        eng.set(b"key1".to_vec(), b"value".to_vec())?;
        eng.set(b"key2".to_vec(), b"value".to_vec())?;
        eng.set(b"key3".to_vec(), b"value".to_vec())?;
        eng.set(b"key1".to_vec(), b"value1".to_vec())?;
        // eng.set(b"key2".to_vec(), b"value1".to_vec())?;
        // std::fs::remove_dir_all("/tmp/sqldb")?;
        Ok(())
    }
    #[test]
    fn test_disk_engine_compact() -> Result<()> {
        pppp!(40);
        let mut eng = DiskEngine::new(PathBuf::from("/tmp/sqldb/sqldb-log"))?;
        // 写一些数据
        eng.set(b"key1".to_vec(), b"value".to_vec())?;
        eng.set(b"key2".to_vec(), b"value".to_vec())?;
        eng.set(b"key3".to_vec(), b"value".to_vec())?;
        eng.delete(b"key1".to_vec())?;
        eng.delete(b"key2".to_vec())?;

        // 重写
        eng.set(b"aa".to_vec(), b"value1".to_vec())?;
        eng.set(b"aa".to_vec(), b"value2".to_vec())?;
        eng.set(b"aa".to_vec(), b"value3".to_vec())?;
        eng.set(b"bb".to_vec(), b"value4".to_vec())?;
        eng.set(b"bb".to_vec(), b"value5".to_vec())?;

        let iter = eng.scan(..);
        let v = iter.collect::<Result<Vec<_>>>()?;
        assert_eq!(
            v,
            vec![
                (b"aa".to_vec(), b"value3".to_vec()),
                (b"bb".to_vec(), b"value5".to_vec()),
                (b"key3".to_vec(), b"value".to_vec()),
            ]
        );
        drop(eng);

        let mut eng2 = DiskEngine::new_compact(PathBuf::from("/tmp/sqldb/sqldb-log"))?;
        let iter2 = eng2.scan(..);
        let v2 = iter2.collect::<Result<Vec<_>>>()?;
        assert_eq!(
            v2,
            vec![
                (b"aa".to_vec(), b"value3".to_vec()),
                (b"bb".to_vec(), b"value5".to_vec()),
                (b"key3".to_vec(), b"value".to_vec()),
            ]
        );
        drop(eng2);

        std::fs::remove_dir_all("/tmp/sqldb")?;

        Ok(())
    }
}
