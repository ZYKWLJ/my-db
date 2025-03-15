use std::collections::HashSet;

use crate::error::{Error, Result};
use crate::{__function, pb, pg, ppb, ppg, pppb, pppg, pppr, pppy, ppr, ppy, pr, py};

use super::{
    executor::ResultSet,
    parser::{
        ast::{self, Expression},
        Parser,
    },
    plan::Plan,
    schema::Table,
    types::{Row, Value},
};

pub mod kv;

// 抽象的 SQL 引擎层定义，目前只有一个 KVEngine
pub trait Engine: Clone {
    type Transaction: Transaction;

    fn begin(&self) -> Result<Self::Transaction>;

    fn session(&self) -> Result<Session<Self>> {
        __function!("客户端的事务session建立成功~");
        Ok(Session {
            engine: self.clone(),
            txn: None,
        })
    }
}

// 抽象的事务信息，包含了 DDL 和 DML 操作
// 底层可以接入普通的 KV 存储引擎，也可以接入分布式存储引擎
pub trait Transaction {
    // 提交事务
    fn commit(&self) -> Result<()>;
    // 回滚事务
    fn rollback(&self) -> Result<()>;
    // 版本号
    fn version(&self) -> u64;

    // 创建行
    fn create_row(&mut self, table_name: String, row: Row) -> Result<()>;
    // 更新行
    fn update_row(&mut self, table: &Table, id: &Value, row: Row) -> Result<()>;
    // 删除行
    fn delete_row(&mut self, table: &Table, id: &Value) -> Result<()>;
    // 扫描表
    fn scan_table(&self, table_name: String, filter: Option<Expression>) -> Result<Vec<Row>>;
    // 索引的操作！
    // 获取索引
    fn load_index(
        // 表名列名列值
        &self,
        table_name: &str,
        col_name: &str,
        col_value: &Value,//这里是主键值！
    ) -> Result<HashSet<Value>>;
    // 保存索引
    fn save_index(
        &self,
        table_name: &str,
        col_name: &str,
        col_value: &Value,
        index: HashSet<Value>,
    ) -> Result<()>;
    // 根据 索引id 获取行
    fn read_by_id(&self, table_name: &str, id: &Value) -> Result<Option<Row>>;

    // DDL 相关操作
    // 创建表
    fn create_table(&mut self, table: Table) -> Result<()>;
    // 删除表
    fn drop_table(&mut self, table_name: String) -> Result<()>;
    // 获取所有的表名
    fn get_table_names(&self) -> Result<Vec<String>>;
    // 获取表信息
    fn get_table(&self, table_name: String) -> Result<Option<Table>>;
    // 获取表信息，不存在则报错
    fn must_get_table(&self, table_name: String) -> Result<Table> {
        self.get_table(table_name.clone())?
            .ok_or(Error::Internal(format!(
                "table {} does not exist",
                table_name
            )))
    }
}

// 客户端 session 定义
pub struct Session<E: Engine> {
    engine: E,
    txn: Option<E::Transaction>,
}

// 每次执行一个语句就是一个会话~所以执行是在会话里面执行的
impl<E: Engine + 'static> Session<E> {
    // 执行客户端 SQL 语句
    pub fn execute(&mut self, sql: &str) -> Result<ResultSet> {

        match Parser::new(sql).parse()? {
            // 直接在这里对事务进行处理
            // 如果开启的事务本身就再一个事务里面，那么直接报错，因为当前事务还没有结束
            ast::Statement::Begin if self.txn.is_some() => {
                Err(Error::Internal("Already in transaction".into()))
            }
            // 如果事务不存在的话，提交与回滚操作就是不存在的！
            ast::Statement::Commit | ast::Statement::Rollback if self.txn.is_none() => {
                Err(Error::Internal("Not in transaction".into()))
            }
            // 接下来就是正式的执行事务的命令
            ast::Statement::Begin => {
                let txn = self.engine.begin()?;
                let version = txn.version();
                self.txn = Some(txn);
                Ok(ResultSet::Begin { version })
            }
            ast::Statement::Commit => {
                let txn = self.txn.take().unwrap();
                let version = txn.version();
                txn.commit()?;
                Ok(ResultSet::Commit { version })
            }
            ast::Statement::Rollback => {
                let txn = self.txn.take().unwrap();
                let version = txn.version();
                txn.rollback()?;
                Ok(ResultSet::Rollback { version })
            }
            ast::Statement::Explain { stmt } => {
                let plan = match self.txn.as_ref() {
                    Some(_) => Plan::build(*stmt, self.txn.as_mut().unwrap())?,
                    None => {
                        let mut txn = self.engine.begin()?;
                        let plan = Plan::build(*stmt, &mut txn)?;
                        txn.commit()?;
                        plan
                    }
                };
                Ok(ResultSet::Explain {
                    plan: plan.0.to_string(),
                })
            }
            stmt if self.txn.is_some() => {
                Plan::build(stmt, self.txn.as_mut().unwrap())?.execute(self.txn.as_mut().unwrap())
            }

            stmt => {
                let mut txn = self.engine.begin()?;
                // 构建 plan，执行 SQL 语句
                // 先build在execute的
                // 这里execute后，立马传到planner里面执行节点里面去执行节点
                match Plan::build(stmt, &mut txn)?.execute(&mut txn) {
                    Ok(result) => {
                        txn.commit()?;
                        Ok(result)
                    }
                    Err(err) => {
                        txn.rollback()?;
                        Err(err)
                    }
                }
            }
        }
    }

    pub fn get_table(&self, table_name: String) -> Result<String> {
        let table = match self.txn.as_ref() {
            Some(txn) => txn.must_get_table(table_name)?,
            None => {
                let txn = self.engine.begin()?;
                let table = txn.must_get_table(table_name)?;
                txn.commit()?;
                table
            }
        };

        Ok(table.to_string())
    }

    // 获取到所有的表名字
    pub fn get_table_names(&self) -> Result<String> {
        let names = match self.txn.as_ref() {
            Some(txn) => txn.get_table_names()?,
            None => {
                let txn = self.engine.begin()?;
                let names = txn.get_table_names()?;
                txn.commit()?;
                names
            }
        };

        Ok(names.join("\n"))
    }
}
