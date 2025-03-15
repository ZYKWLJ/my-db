use futures::{SinkExt, TryStreamExt};
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use std::env;
use std::{error::Error, net::SocketAddr};
use tokio::net::TcpStream;
use tokio_util::codec::{FramedRead, FramedWrite, LinesCodec};

const RESPONSE_END: &str = "!!!end!!!";
// 一句话：通过端口连接到 tcp 服务，并且发送请求即可
pub struct Client {
    stream: TcpStream,
    txn_version: Option<u64>,//在这里记录事务的版本！
}

impl Client {
    // 连接TCP的一个构造函数！
    pub async fn new(addr: SocketAddr) -> Result<Self, Box<dyn Error>> {
        let stream = TcpStream::connect(addr).await?;
        Ok(Self {
            stream,
            txn_version: None,
        })
    }

    // 执行SQL语句~
    pub async fn execute_sql(&mut self, sql_cmd: &str) -> Result<(), Box<dyn Error>> {
        let (r, w) = self.stream.split();//将TCP的通信流分割成读写流
        let mut sink = FramedWrite::new(w, LinesCodec::new());//包装了写流并应用了 LinesCodec 编解码器，意味着它会`·按行·`来处理发送的数据包装了写流并应用了 LinesCodec 编解码器，意味着它会按行来处理发送的数据
        let mut stream = FramedRead::new(r, LinesCodec::new());
        println!("客户端将命令传给服务器执行了~~\n");
        // 将命令行中的命令传给服务器中！一直等待执行
        sink.send(sql_cmd).await?;

        // 拿到结果并打印
        while let Some(res) = stream.try_next().await? {
            if res == RESPONSE_END {
                break;
            }
            // 这里是根据从服务端传来的命令显示当前事务的状况。
            // 如果已经提交或者回滚了，当前事务版本就没了
            if res.starts_with("TRANSACTION") {
                let args = res.split(" ").collect::<Vec<_>>(); //将命令按照" "分隔开
                if args[2] == "COMMIT" || args[2] == "ROLLBACK" {
                    self.txn_version = None;
                }
                // 否则当前事务版本就在此！
                if args[2] == "BEGIN" {
                    let version = args[1].parse::<u64>().unwrap();
                    self.txn_version = Some(version);
                }
            }

            // 打印从sever端传来的结果~
            println!("{}", res);
        }
        Ok(())
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        if self.txn_version.is_some() {
            futures::executor::block_on(self.execute_sql("ROLLBACK;")).expect("rollback failed");
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // 与指定的主机端口通信~
    let addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:8080".to_string());

    let addr = addr.parse::<SocketAddr>()?;
    //套接字是传输层到应用层的指引，套接字就像是一座桥梁，它让应用层的应用程序能够利用传输层提供的服务来进行网络通信。
    let mut client = Client::new(addr).await?; //返回一个与指定的ip建立连接的客户端！

    let mut editor = DefaultEditor::new()?;
    loop {
        // 这里需要好好的体会
        // !todo!();
        // 根据客户端的事务版本（txn_version）情况来生成不同的命令提示符字符串。
        // 如果 txn_version 是 Some 变体，意味着存在事务版本信息，就使用 format!("sqldb#{}> ", version) 将版本号嵌入到提示符中，
        // 例如可能显示 sqldb#1> 这样的提示符；如果 txn_version 是 None，就直接使用默认的提示符 "sqldb> "。
        // 这里可以做判断是在事务里面执行的还是普通执行的！两者的提示不一样！
        let prompt = match client.txn_version {
            Some(version) => format!("EYKDB#{}> ", version),
            None => "EYKDB> ".into(),
        };
        let pid = std::process::id();
        println!("\x1B[32mClient进程的ID是: {}\n\x1B[0m", pid);
        let readline = editor.readline(&prompt); //这里读取输入
        match readline {
            //匹配输入的命令
            Ok(sql_cmd) => {
                let sql_cmd = sql_cmd.trim(); //去掉前后空格！
                if sql_cmd.len() > 0 {
                    if sql_cmd == "quit" {
                        //如果是quit就直接退出循环
                        break;
                    }
                    // 将这个有效的命令添加到编辑器的历史记录中（方便用户后续可以通过上下键等方式查看历史输入）！
                    editor.add_history_entry(sql_cmd)?;
                    // 并且接下来执行SQL语句！异步执行，这里才开始执行
                    client.execute_sql(sql_cmd).await?;
                }
            }
            Err(ReadlineError::Interrupted) => break,
            Err(ReadlineError::Eof) => break,
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }
    #[cfg(feature = "with-file-history")]
    editor.save_history("history.txt");
    // Ok(())
    Ok(())
}
