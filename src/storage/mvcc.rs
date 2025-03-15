use crate::{__function, pb, pg, ppb, ppg, pppb, pppg, pppr, pppy, ppr, ppy, pr, py};
use std::{
    collections::{BTreeMap, HashSet},
    sync::{Arc, Mutex, MutexGuard},
    u64,
};

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

use super::{
    engine::Engine,
    keycode::{deserialize_key, serialize_key},
};

pub type Version = u64; //Each transaction is assigned a uniquely increasing version number!

// 多版本并发读取的本质就是为执行引擎上锁！
// Arc=>多线程引用计数指针
pub struct Mvcc<E: Engine> {
    engine: Arc<Mutex<E>>,
}

impl<E: Engine> Clone for Mvcc<E> {
    fn clone(&self) -> Self {
        Self {
            engine: self.engine.clone(),
        }
    }
}

impl<E: Engine> Mvcc<E> {
    pub fn new(eng: E) -> Self {
        // __function!("在MVCC理念下实现Arc的新建引擎系统");
        Self {
            engine: Arc::new(Mutex::new(eng)),
        }
    }
    // 在mvcc下启动事务
    pub fn begin(&self) -> Result<MvccTransaction<E>> {
        // __function!("在MVCC理念下启动事务管理系统");
        MvccTransaction::begin(self.engine.clone())
    }
}

// this is MVCC concrete implement which contains the Engine and transactionState!
pub struct MvccTransaction<E: Engine> {
    engine: Arc<Mutex<E>>,
    state: TransactionState,
}

// this is the read_view which called transactive as well!
pub struct TransactionState {
    // the verison of current transaction
    pub version: Version,
    // the active transaction set in the moment of my transaction created!
    pub active_versions: HashSet<Version>,
}

// judge whether the data can be access or not by the data version!
// this is the 4 steps simply model implement!
impl TransactionState {
    fn is_visible(&self, version: Version) -> bool {
        if self.active_versions.contains(&version) {
            return false;
        } else {
            return version <= self.version; //the data I can access contains my own data as well as the data commited previously!
        }
    }
}

// this is all the mvcc info's key which need to store in disk
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum MvccKey {
    NextVersion,
    TxnAcvtive(Version),
    TxnWrite(Version, #[serde(with = "serde_bytes")] Vec<u8>),
    Version(#[serde(with = "serde_bytes")] Vec<u8>, Version),
}

impl MvccKey {
    pub fn encode(&self) -> Result<Vec<u8>> {
        serialize_key(&self)
    }

    pub fn decode(data: Vec<u8>) -> Result<Self> {
        deserialize_key(&data)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum MvccKeyPrefix {
    NextVersion,
    TxnAcvtive,
    TxnWrite(Version),
    Version(#[serde(with = "serde_bytes")] Vec<u8>),
}

impl MvccKeyPrefix {
    pub fn encode(&self) -> Result<Vec<u8>> {
        serialize_key(&self)
    }
}

//all the MVCC data was store in the memory! so it must implement the trait Engine ,
// which apply the mothed : get\set\scan\ we need!

impl<E: Engine> MvccTransaction<E> {
    // start the transaction!
    pub fn begin(eng: Arc<Mutex<E>>) -> Result<Self> {
        // __function!();
        // __function!("MvccTransaction::begin()=>");
        // pppg!("MVCC事务的开启=>");
        // get the storage engine
        let mut engine = eng.lock()?;
        // pppb!("先获取存储引擎里面存放的所有mvcc信息:下一个版本号、活跃事务列表集合、当前事务写入的信息、存储数据-版本号的枚举");
        pppb!("先获取最新的版本号~~=>");

        // get the next version in memory, as my verison
        // remember all the data of mvcc had stored in Memory！
        let next_version = match engine.get(MvccKey::NextVersion.encode()?)? {
            Some(value) => {
                // pppb!("获取存储引擎里面下一个版本号的值，从存储引擎里面拿出来~");
                bincode::deserialize(&value)?
            }
            None => {
                // pppb!("存储引擎里面没有下一个版本号的值，说明这是第一次操作该数据，赋初始版本值~1");
                1
            }
        };

        pppb!(
            // "获取到了(上一个操作数据留下的/首次赋值的~1)下一个版本号(也就是自己的对应的数据版本)=>",
            next_version
        );
        // pppb!("保存下一个 version::存储引擎将MVCCKey——NextVersion与对应Value存储进引擎中,让其他事务再开始的时候能够通过这个key在存储引擎中拿到相应的版本号(刚刚递增的版本号)");

        // we must save the next_version so that the next transaction can acquire it,which as k-v style ,and the key is the first field of the struct —— MVCCkey！
        // the value we need to save is the next_version we just get plus 1.
        engine.set(
            MvccKey::NextVersion.encode()?,
            bincode::serialize(&(next_version + 1))?,
        )?;

        // get the active transaction set in this moment!
        let active_versions = Self::scan_active(&mut engine)?;

        // put my own transaction version into the active transaction set!
        engine.set(MvccKey::TxnAcvtive(next_version).encode()?, vec![])?;
        // pppb!("成功完成Mvcc事务开启");
        // pppb!("现在我们才完成了开启事务前的准备工作，返回一个自身事务的版本号，和当前或活跃事务集合=>");

        Ok(Self {
            engine: eng.clone(),
            state: TransactionState {
                version: next_version,
                active_versions,
            },
        })
    }

    // get my own version
    pub fn version(&self) -> u64 {
        self.state.version
    }

    // commit the transaction
    pub fn commit(&self) -> Result<()> {
        __function!("MvccTransaction::commit()=>");
        pppg!("提交MVCC管理下的事务");

        // get the storge engine
        let mut engine = self.engine.lock()?;
        // pppb!("提交数据也要先获取存储引擎!获取成功！");
        // pppb!("txnWrite是解决我们当前事务记录了哪些信息，每个事务开启时都会先构造read_view，而且是私有的，所以这里直接在事务结束后消除记录的信息即可，因为事物都结束了，这个的信息也就没用了，要回滚早回滚了~~");
        // pppb!("找到这个当前事务的 TxnWrite 信息，并将之删除，那通过什么找呢？");
        let mut delete_keys = Vec::new();
        // pppb!("通过前缀找~，先找到所有写入该事务的key，存在集合里面，后面再统一删除~");
        // find current transaction TxnWrite info !
        let mut iter = engine.scan_prefix(MvccKeyPrefix::TxnWrite(self.state.version).encode()?);
        while let Some((key, _)) = iter.next().transpose()? {
            delete_keys.push(key);
        }
        pppb!("准备删除的当前事务的信息=>");
        ppy!(delete_keys);
        drop(iter);
        // iteratively delete the info of this transaction by the transaction prefix!
        for key in delete_keys.into_iter() {
            engine.delete(key)?;
        }

        // transaction over! remove from the active transaction naturally!
        engine.delete(MvccKey::TxnAcvtive(self.state.version).encode()?)
    }

    // rollback transaction!
    pub fn rollback(&self) -> Result<()> {
        // get the storage engine！
        let mut engine = self.engine.lock()?;

        let mut delete_keys = Vec::new();
        // find current transaction TxnWrite info !
        let mut iter = engine.scan_prefix(MvccKeyPrefix::TxnWrite(self.state.version).encode()?);
        while let Some((key, _)) = iter.next().transpose()? {
            match MvccKey::decode(key.clone())? {
                MvccKey::TxnWrite(_, raw_key) => {
                    delete_keys.push(MvccKey::Version(raw_key, self.state.version).encode()?);
                }
                _ => {
                    return Err(Error::Internal(format!(
                        "unexpected key: {:?}",
                        String::from_utf8(key)
                    )))
                }
            }
            delete_keys.push(key);
        }
        drop(iter);

        for key in delete_keys.into_iter() {
            engine.delete(key)?;
        }

        // transaction over! remove from the active transaction naturally!
        engine.delete(MvccKey::TxnAcvtive(self.state.version).encode()?)
    }

    pub fn set(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        // __function!("set开始存放数据=>");
        self.write_inner(key, Some(value))
    }

    pub fn delete(&self, key: Vec<u8>) -> Result<()> {
        // __function!("delete开始删除数据=>");
        self.write_inner(key, None)
    }
    // the method is to find the first visible data which key is specified as the param！
    pub fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        // get the engine!
        let mut engine = self.engine.lock()?;

        let from = MvccKey::Version(key.clone(), 0).encode()?;
        let to = MvccKey::Version(key.clone(), self.state.version).encode()?;
        let mut iter = engine.scan(from..=to).rev();
        // Iterator from the lastest version and return the data as soon as find the first visible version!
        while let Some((key, value)) = iter.next().transpose()? {
            match MvccKey::decode(key.clone())? {
                //serialize First
                MvccKey::Version(_, version) => {
                    if self.state.is_visible(version) {
                        return Ok(bincode::deserialize(&value)?); //deserialize then
                    }
                }
                _ => {
                    return Err(Error::Internal(format!(
                        "unexpected key: {:?}",
                        String::from_utf8(key)
                    )))
                }
            }
        }
        Ok(None)
    }

    // why not? cause the
    pub fn scan_prefix(&self, prefix: Vec<u8>) -> Result<Vec<ScanResult>> {
        let mut eng = self.engine.lock()?;
        let mut enc_prefix = MvccKeyPrefix::Version(prefix).encode()?;
        // 原始值           编码后
        // 97 98 99     -> 97 98 99 0 0
        // 前缀原始值        前缀编码后
        // 97 98        -> 97 98 0 0         -> 97 98
        // 去掉最后的 [0, 0] 后缀
        enc_prefix.truncate(enc_prefix.len() - 2);

        let mut iter = eng.scan_prefix(enc_prefix);
        let mut results = BTreeMap::new();
        while let Some((key, value)) = iter.next().transpose()? {
            match MvccKey::decode(key.clone())? {
                MvccKey::Version(raw_key, version) => {
                    if self.state.is_visible(version) {
                        match bincode::deserialize(&value)? {
                            Some(raw_value) => results.insert(raw_key, raw_value),
                            None => results.remove(&raw_key),
                        };
                    }
                }
                _ => {
                    return Err(Error::Internal(format!(
                        "Unexepected key {:?}",
                        String::from_utf8(key)
                    )))
                }
            }
        }

        Ok(results
            .into_iter()
            .map(|(key, value)| ScanResult { key, value })
            .collect())
    }

    // mothed of set/delete we see earliy all called the write_inner method as the core operation!
    // attention！write Operation is differ from other Operation, which must detect whether exist write conflict or not！
    // The conflict detection here is mainly to determine whether other transactions have modified the data
    // when the current transaction is writing or deleting data, and the modification result is invisible to the current transaction.
    // 这里的冲突检测主要是判断当前事务在写入或删除数据时，是否有其他事务已经对该数据进行了修改且修改结果对当前事务不可见。
    fn write_inner(&self, key: Vec<u8>, value: Option<Vec<u8>>) -> Result<()> {
        //get the storge engine because all data was set in its memory!
        let mut engine = self.engine.lock()?;

        // 检测冲突
        //  3 4 5
        //  6
        //  key1-3 key2-4 key3-5
        // from bound —— which is the left bound of the active transactions!
        // Find the smallest version number in an active transaction. If there are no active transactions, the current transaction version number plus 1 is used.
        let from = MvccKey::Version(
            key.clone(),
            self.state
                .active_versions
                .iter()
                .min()
                .copied()
                .unwrap_or(self.state.version + 1),
        )
        .encode()?;
        let to = MvccKey::Version(key.clone(), u64::MAX).encode()?;
        //  当前活跃事务列表 3 4 5
        //  当前事务 6
        // 只需要判断最后一个版本号
        // 1. key 按照顺序排列，扫描出的结果是从小到大的
        // 2. 假如有新的的事务修改了这个 key，比如 10，修改之后 10 提交了，那么 6 再修改这个 key 就是冲突的
        // 3. 如果是当前活跃事务修改了这个 key，比如 4，那么事务 5 就不可能修改这个 key，因为此时在活跃事务列表中
        // if current transaction version greater the date's version and the data not in the active transactions ,it is ok to write the data!
        // otherwise , can't ,on one hand,if the data's version greater than current transactinos's version, it's not visible
        // one the other hand ,if the data is in the active transaction ,it also can't write !
        // the core pof the judgement is the visibility!
        if let Some((k, _)) = engine.scan(from..=to).last().transpose()? {
            match MvccKey::decode(k.clone())? {
                MvccKey::Version(_, version) => {
                    // if it is not visible,directly return error of WriteConflict!
                    if !self.state.is_visible(version) {
                        return Err(Error::WriteConflict);
                    }
                }
                _ => {
                    return Err(Error::Internal(format!(
                        "unexpected key: {:?}",
                        String::from_utf8(k)
                    )))
                }
            }
        }

        // we must record the writeInfo of this key so that to rollback the transaction maybe laterly!
        engine.set(
            MvccKey::TxnWrite(self.state.version, key.clone()).encode()?,
            vec![],
        )?;

        //set the real key-value
        engine.set(
            MvccKey::Version(key.clone(), self.state.version).encode()?,
            bincode::serialize(&value)?,
        )?;
        Ok(())
    }

    // scan to acquire the active transaction list in this moment!
    fn scan_active(engine: &mut MutexGuard<E>) -> Result<HashSet<Version>> {
        let mut active_versions = HashSet::new();
        // get the iterator of data set which has the TxnActive prefix, this means the transaction is active in this moment!so ,put it into m_ids
        // this is aiming at all transactions in the engine!
        let mut iter = engine.scan_prefix(MvccKeyPrefix::TxnAcvtive.encode()?);
        while let Some((key, _)) = iter.next().transpose()? {
            match MvccKey::decode(key.clone())? {
                MvccKey::TxnAcvtive(version) => {
                    active_versions.insert(version); //as we say ,the version should be put into active_set!
                }
                _ => {
                    return Err(Error::Internal(format!(
                        "unexpected key: {:?}",
                        String::from_utf8(key)
                    )))
                }
            }
        }
        Ok(active_versions)
    }
}

#[derive(Debug, PartialEq)]
pub struct ScanResult {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use crate::{
        __function,
        error::Result,
        pppg, pppp, pppy,
        storage::{disk::DiskEngine,disk::Log, engine::Engine, memory::MemoryEngine},
    };
    use std::path::PathBuf;

    // use super::d;
    use std::fs;
    use super::Mvcc;

    // use std::{fmt::format, fs};
    //delete the data in disk
    #[test]
    fn delete_log_file() -> Result<()> {
        let log_dir = "/tmp/sqldb/sqldb-log";
        // 检查目录是否存在
        if !fs::metadata(log_dir).is_ok() {
            // 若目录不存在，直接返回 Ok(())
            pppg!("日志文件在之前已被清空");
            return Ok(());
        }
        pppg!("正在清空日志文件");
        let res = std::fs::remove_dir_all("/tmp/sqldb")?;
        pppg!("清空日志文件完毕");
        Ok(res)
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

    // 1. solve_dirty read
    fn solve_dirty_read(eng: impl Engine) -> Result<()> {
        delete_log_file()?;
        let mvcc = Mvcc::new(eng);
        pppg!("事务1开始");
        let tx1 = mvcc.begin()?;
        pppg!("事务1插入数据{xidian,211},但并未提交");
        tx1.set(b"xidian".to_vec(), b"211".to_vec())?;
        // tx1.commit()?;
        pppg!("事务2开始");
        let tx2 = mvcc.begin()?;
        let res = tx2.get(b"xidian".to_vec())?;
        pppy!(format!("事务2读取key='xidian'的数据结果={:?}", res));
        Ok(())
    }

    #[test]
    fn test_solve_dirty_read() -> Result<()> {
        solve_dirty_read(MemoryEngine::new())?;
        let p = tempfile::tempdir()?.into_path().join("/tmp/sqldb/sqldb-log");
        solve_dirty_read(DiskEngine::new(p.clone())?)?;
        Ok(())
    }

    // solve unrepeatable read problem
    // 2.1 solve unrepeatable read higher version
    fn solve_unrepeatable_read1_higher_version(eng: impl Engine) -> Result<()> {
        delete_log_file()?;
        let mvcc = Mvcc::new(eng);
        pppg!("事务1开始");
        let tx1 = mvcc.begin()?;
        pppg!("事务2开始");
        let tx2 = mvcc.begin()?;
        tx2.set(b"xidian".to_vec(), b"211".to_vec())?;
        //through the tx2 modified this date at this moment ,but it happened after tx1 created,
        //in other word , this moment the data's version which have the key of 'xidian' greater than the tx1,so the tx1 can't see this data!
        tx2.commit()?;
        let res = tx1.get(b"xidian".to_vec())?;
        pppy!(format!("事务1读取key='xidian'的数据结果={:?}", res));
        tx1.commit()?;
        Ok(())
    }

    #[test]
    fn test_unrepeatable_read1_higher_version() -> Result<()> {
        solve_unrepeatable_read1_higher_version(MemoryEngine::new())?;
        let p = tempfile::tempdir()?.into_path().join("/tmp/sqldb/sqldb-log");
        solve_unrepeatable_read1_higher_version(DiskEngine::new(p.clone())?)?;
        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    // 2.2. solve unrepeatable read _in_my_transaction_active_set
    fn solve_unrepeatable_read2_in_my_transaction_active_set(eng: impl Engine) -> Result<()> {
        delete_log_file()?;

        let mvcc = Mvcc::new(eng);
        pppg!("事务2开始");
        let tx2 = mvcc.begin()?;
        tx2.set(b"xidian".to_vec(), b"211".to_vec())?;
        //through the tx2 modified this date and it's version if lowwer than me,but in this moment ,it is in my active transaction set(That is to say it haven't commit) ,so the tx1 can't see this data!
        pppg!("事务1开始");
        let tx1 = mvcc.begin()?;
        let res = tx1.get(b"xidian".to_vec())?;
        pppy!(format!("事务1读取key='xidian'的数据结果={:?}", res));
        tx1.commit()?;
        tx2.commit()?;
        Ok(())
    }

    #[test]
    fn test_unrepeatable_read2_in_my_transaction_active_set() -> Result<()> {
        solve_unrepeatable_read2_in_my_transaction_active_set(MemoryEngine::new())?;
        let p = tempfile::tempdir()?.into_path().join("/tmp/sqldb/sqldb-log");
        solve_unrepeatable_read2_in_my_transaction_active_set(DiskEngine::new(p.clone())?)?;
        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    // 3. phantom read
    fn solve_phantom_read(eng: impl Engine) -> Result<()> {
        delete_log_file()?;

        let mvcc = Mvcc::new(eng);
        pppg!("事务1开始");
        let tx = mvcc.begin()?;
        tx.set(b"xidian-1".to_vec(), b"211-1".to_vec())?;
        tx.set(b"xidian-2".to_vec(), b"211-2".to_vec())?;
        tx.set(b"xidian-3".to_vec(), b"211-3".to_vec())?;
        tx.commit()?;
        pppg!("事务1提交了3条数据后，事务2、3开始");
        pppg!("事务2开始");
        let tx1 = mvcc.begin()?;
        pppg!("事务3开始");
        let tx2 = mvcc.begin()?;
        pppg!("事务1读取prefixkey='xidian'的数据集合");
        let iter1: Vec<super::ScanResult> = tx1.scan_prefix(b"xidian".to_vec())?;
        pppy!(format!("共有{}条数据，分别是{:?}", iter1.len(), iter1));
        pppg!("事务2新增了一条数据{xidian-4,211-4}");
        tx2.set(b"xidian-4".to_vec(), b"211-4".to_vec())?;
        tx2.commit()?;
        pppg!("事务1再次读取prefixkey='xidian'的数据集合");
        let iter2 = tx1.scan_prefix(b"xidian".to_vec())?;
        pppy!(format!("共有{}条数据，分别是{:?}", iter2.len(), iter2));

        Ok(())
    }

    #[test]
    fn test_phantom_read() -> Result<()> {
        solve_phantom_read(MemoryEngine::new())?;
        let p = tempfile::tempdir()?.into_path().join("/tmp/sqldb/sqldb-log");
        solve_phantom_read(DiskEngine::new(p.clone())?)?;
        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    // 4. rollback
    fn rollback(eng: impl Engine) -> Result<()> {
        delete_log_file()?;

        let mvcc = Mvcc::new(eng);
        pppg!("事务1开始");
        let tx = mvcc.begin()?;
        tx.set(b"xidian-1".to_vec(), b"211-1".to_vec())?;
        tx.set(b"xidian-2".to_vec(), b"211-2".to_vec())?;
        tx.set(b"xidian-3".to_vec(), b"211-3".to_vec())?;
        tx.commit()?;
        pppg!("事务1提交了3条数据后，事务2开始");
        pppg!("事务2欲存入3条数据");
        let tx1 = mvcc.begin()?;
        tx.set(b"xidian-1".to_vec(), b"985-1".to_vec())?;
        tx.set(b"xidian-2".to_vec(), b"985-2".to_vec())?;
        tx.set(b"xidian-3".to_vec(), b"985-3".to_vec())?;
        pppg!("事务2欲存入的3条数据被回滚了");
        let iter1: Vec<super::ScanResult> = tx1.scan_prefix(b"xidian".to_vec())?;
        tx1.rollback()?;
        pppg!("事务1读取prefixkey='xidian'的数据集合");
        pppy!(format!("共有{}条数据，分别是{:?}", iter1.len(), iter1));
        Ok(())
    }

    #[test]
    fn test_rollback() -> Result<()> {
        rollback(MemoryEngine::new())?;
        let p = tempfile::tempdir()?.into_path().join("/tmp/sqldb/sqldb-log");
        rollback(DiskEngine::new(p.clone())?)?;
        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    // 5.write problem 
    fn solve_write_conflict_not_commit(eng: impl Engine) -> Result<()> {
        delete_log_file()?;
        let mvcc = Mvcc::new(eng);
        pppg!("事务1开始");
        let tx = mvcc.begin()?;
        tx.set(b"xidian-1".to_vec(), b"211-1".to_vec())?;
        // tx.commit()?;
        pppg!("事务2开始");
        let tx1 = mvcc.begin()?;
        tx1.set(b"xidian-1".to_vec(), b"211-1".to_vec())?;
        Ok(())
    }

    #[test]
    fn test_write_conflict() -> Result<()> {
        // solve_write_conflict_not_commit(MemoryEngine::new())?;
        let p = tempfile::tempdir()?.into_path().join("/tmp/sqldb/sqldb-log");
        solve_write_conflict_not_commit(DiskEngine::new(p.clone())?)?;
        Ok(())
    }

    // 6.Durability problem
    fn solve_durability(eng: impl Engine) -> Result<()> {
        
        let mvcc = Mvcc::new(eng);
        pppg!("事务1开始");
        let tx = mvcc.begin()?;
        tx.set(b"xidian-1".to_vec(), b"211-1".to_vec())?;
        tx.commit()?;
        Ok(())
    }

    #[test]
    fn test_durability_write() -> Result<()> {
        delete_log_file()?;
        solve_durability(MemoryEngine::new())?;
        let p = tempfile::tempdir()?.into_path().join("/tmp/sqldb/sqldb-log");
        solve_durability(DiskEngine::new(p.clone())?)?;
        // std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    #[test]
    fn test_durability_read() -> Result<()> {
        let mvcc1 = Mvcc::new(MemoryEngine::new());

        pppg!("事务1开始");
        let tx = mvcc1.begin()?;
        let res = tx.get(b"xidian-1".to_vec())?;
        pppg!(res);
        tx.commit()?;


        let p = tempfile::tempdir()?.into_path().join("/tmp/sqldb/sqldb-log");
        let mvcc2 = Mvcc::new(DiskEngine::new(p.clone())?);
        pppg!("事务2开始");
        let tx1 = mvcc2.begin()?;
        let res1 = tx1.get(b"xidian-1".to_vec())?;
        pppg!(res1);
        tx1.commit()?;
        // std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }
}
