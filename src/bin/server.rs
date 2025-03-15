use futures::SinkExt;
use sqldb_rs::error::Result;
use sqldb_rs::sql;
use sqldb_rs::sql::engine::kv::KVEngine;
use sqldb_rs::storage::disk::DiskEngine;
use tokio::net::{TcpListener, TcpStream};
use tokio_stream::StreamExt;
use tokio_util::codec::{Framed, LinesCodec};

use sqldb_rs::{__function, pb, pg, ppb, ppg, pppb, pppg, pppr, pppy, ppr, ppy, pr, py};
use std::env;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};

// 临时目录
const DB_PATH: &str = "/tmp/sqldb-test/sqldb-log";
const RESPONSE_END: &str = "!!!end!!!";

/// Possible requests our clients can send us
enum SqlRequest {
    SQL(String),       //一个简单的sql语句
    ListTables,        //查看所有表
    TableInfo(String), //查看当前的表结构
}

impl SqlRequest {
    // 这里解析命令
    pub fn parse(cmd: &str) -> Self {
        
        let mut upper_cmd = cmd.to_uppercase(); //全部命令转大写
        // 展示所有表的命令
        // if upper_cmd == "SHOW TABLES;" {
            if upper_cmd.starts_with("SHOW TABLES")&&upper_cmd.ends_with(";") {
            return SqlRequest::ListTables;
        }
        // "SHOW TABLE table_name"
        // 展示某一个表的信息，这里应该是3个参数，不是的话就报错了！
        if upper_cmd.starts_with("SHOW TABLE")&&upper_cmd.ends_with(";") {
            upper_cmd=upper_cmd[0..upper_cmd.len()-1].to_string();
            let args = upper_cmd.split_ascii_whitespace().collect::<Vec<_>>();
            if args.len() == 3 {
                return SqlRequest::TableInfo(args[2].to_lowercase());
            }
        }
        // 以上两种命令都不是，那就是执行sql命令！
        SqlRequest::SQL(cmd.into())
    }
}

// 这里抽象一个服务端的sever回话
pub struct ServerSession<E: sql::engine::Engine> {
    session: sql::engine::Session<E>,
}

impl<E: sql::engine::Engine + 'static> ServerSession<E> {
    pub fn new(eng: MutexGuard<E>) -> Result<Self> {
        Ok(Self {
            session: eng.session()?,//新建一个执行引擎的会话
        })
    }

    // 处理请求的函数，调用Tokio的接口
    pub async fn handle_request(&mut self, socket: TcpStream) -> Result<()> {
        let mut lines = Framed::new(socket, LinesCodec::new());
        // 得到了客户端传来的字节流
        while let Some(result) = lines.next().await {
            // 注意是逐个解析的，这是字节流
            match result {
                //返回执行的结果
                // 这里返回的是string类型
                Ok(line) => {
                    // 解析并得到 SqlRequest
                    let req = SqlRequest::parse(&line);

                    // 执行请求
                    let response = match req {
                        SqlRequest::SQL(sql) => match self.session.execute(&sql) {
                            Ok(rs) => {
                                pppr!(rs);
                                rs.to_string()
                            }
                            Err(e) => e.to_string(),
                        },
                        SqlRequest::ListTables => match self.session.get_table_names() {
                            Ok(names) => names,
                            Err(e) => e.to_string(),
                        },
                        SqlRequest::TableInfo(table_name) => {
                            match self.session.get_table(table_name) {
                                Ok(tbinfo) => tbinfo,
                                Err(e) => e.to_string(),
                            }
                        }
                    };

                    // 发送执行结果
                    if let Err(e) = lines.send(response.as_str()).await {
                        println!("error on sending response; error = {e:?}");
                    }
                    if let Err(e) = lines.send(RESPONSE_END).await {
                        println!("error on sending response; error = {e:?}");
                    }

                }

                Err(e) => {
                    println!("error on decoding from socket; error = {e:?}");
                }
            }
        }

        Ok(())
    }
}

// tokio异步框架，高并发执行
#[tokio::main]
async fn main() -> Result<()> {
    // 启动 TCP 服务
    let addr = env::args()
        .nth(1) //监听 的端口
        .unwrap_or_else(|| "127.0.0.1:8080".to_string()); //启动的服务！

    let listener = TcpListener::bind(&addr).await?;
    // println!("\x1B[31msqldb server starts, listening on: {addr}\x1B[0m");
    println!("\x1B[31msqldb server starts, listening on: \x1B[0m");
    println!("\x1B[31m{:■^60}\x1B[0m", addr);

    // 服务器端~初始化 DB~和启动执行引擎是一样的代码
    let p = PathBuf::from(DB_PATH);
    let kvengine = KVEngine::new(DiskEngine::new(p.clone())?);
    //这里的引擎是 并发下的 访问引擎了~其实是共享所有权的
    let shared_engine = Arc::new(Mutex::new(kvengine));

    let pid = std::process::id();
    println!("\x1B[32mSever进程的ID是: {}\n\x1B[0m", pid);
    loop {
        match listener.accept().await {
            Ok((socket, _)) => {
                //字节流
                // 必须克隆引擎，因为
                let db = shared_engine.clone();
                // 新建服务端回话，将引擎包含进去！
                //必须并发上锁实现，不然回导致并发安全问题
                let mut ss = ServerSession::new(db.lock()?)?; 
                tokio::spawn(async move {
                    //这里来真正并发执行任务了
                    match ss.handle_request(socket).await {
                        Ok(_) => {}
                        Err(e) => {
                            println!("internal server error {:?}", e);
                        }
                    }
                });
            }
            Err(e) => println!("error accepting socket; error = {e:?}"),
        }
    }
}
