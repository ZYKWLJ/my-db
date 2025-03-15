use super::{engine::Transaction, plan::Node, types::Row};
use crate::error::Result;
use agg::Aggregate;
use join::{HashJoin, NestedLoopJoin};
use mutation::{Delete, Insert, Update};
use query::{Filter, IndexScan, Limit, Offset, Order, PrimaryKeyScan, Projection, Scan};
use schema::{CreateTable, DropTable};

mod agg;
mod join;
mod mutation;
mod query;
mod schema;

// 执行器定义
pub trait Executor<T: Transaction> {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet>;
}

// 在这里才是真真正正的统一执行，但是下面还有分支！，这里只是统一执行分类了
impl<T: Transaction + 'static> dyn Executor<T> {
    pub fn build(node: Node) -> Box<dyn Executor<T>> {
        match node {
            Node::CreateTable { schema } => CreateTable::new(schema),
            Node::DropTable { name } => DropTable::new(name),
            Node::Insert {
                table_name,
                columns,
                values,
            } => Insert::new(table_name, columns, values),
            Node::Scan { table_name, filter } => Scan::new(table_name, filter),
            Node::Update {
                table_name,
                source,
                columns,
            } => Update::new(table_name, Self::build(*source), columns),
            Node::Delete { table_name, source } => Delete::new(table_name, Self::build(*source)),
            Node::Order { source, order_by } => Order::new(Self::build(*source), order_by),
            Node::Limit { source, limit } => Limit::new(Self::build(*source), limit),
            Node::Offset { source, offset } => Offset::new(Self::build(*source), offset),
            Node::Projection { source, exprs } => Projection::new(Self::build(*source), exprs),
            // 执行连接语句
            Node::NestedLoopJoin {
                left,
                right,
                predicate,
                outer,
            } => NestedLoopJoin::new(Self::build(*left), Self::build(*right), predicate, outer),
            Node::Aggregate {
                source,
                exprs,
                group_by,
            } => Aggregate::new(Self::build(*source), exprs, group_by),
            Node::Filter { source, predicate } => Filter::new(Self::build(*source), predicate),
            Node::IndexScan {
                table_name,
                field,
                value,
            } => IndexScan::new(table_name, field, value),
            Node::PrimaryKeyScan { table_name, value } => PrimaryKeyScan::new(table_name, value),
            Node::HashJoin {
                left,
                right,
                predicate,
                outer,
            } => HashJoin::new(Self::build(*left), Self::build(*right), predicate, outer),
        }
    }
}

// 执行返回的结果集格式！
#[derive(Debug, PartialEq)]
pub enum ResultSet {
    CreateTable {
        table_name: String,
    },
    DropTable {
        table_name: String,
    },
    Insert {
        count: usize,
    },
    Scan {
        columns: Vec<String>,
        rows: Vec<Row>,
    },
    Update {
        count: usize,
    },
    Delete {
        count: usize,
    },
    Begin {
        version: u64,
    },
    Commit {
        version: u64,
    },
    Rollback {
        version: u64,
    },
    Explain {
        plan: String,
    },
}

// 这里就是对应实现的执行结果返回的呈现方式！
impl ResultSet {
    pub fn to_string(&self) -> String {
        match self {
            // 创建表
            ResultSet::CreateTable { table_name } => {
                format!("CREATE TABLE {} SUCCESSED!", table_name)
            }
            ResultSet::DropTable { table_name } => format!("DROP TABLE {} SUCCESSED!", table_name),
            ResultSet::Insert { count } => format!("INSERT {} rows", count),
            // 这里就是我们平时的查询表的输出内容~方方正正的框子
            // +-------------+---------------+---------------+
            // | employee_id | employee_name | department_id |
            // +-------------+---------------+---------------+
            // |           1 | Alice         |             1 |
            // |           2 | Bob           |             1 |
            // |           3 | Charlie       |             2 |
            // |           4 | David         |          NULL |
            // +-------------+---------------+---------------+
            // 4 rows in set (0.00 sec)
            ResultSet::Scan { columns, rows } => {
                //返回列名字及数据
                let rows_len = rows.len();
                // \x1B[32m绿色=>{:■^width$}\x1B[0m\n\
                // 找到每一列最大的长度
                // 以便全部容纳数据
                // 先获得每一列的列名的长度
                let mut max_len = columns.iter().map(|c| c.len()).collect::<Vec<_>>();
                for one_row in rows {
                    for (i, v) in one_row.iter().enumerate() {
                        if v.to_string().len() > max_len[i] {
                            max_len[i] = v.to_string().len();
                        }
                    }
                }
                // 展示分隔符0
                let mut sep0 = max_len
                    .iter()
                    .map(|v| format!("{}", "-".repeat(*v + 1)))
                    // .map(|v| format!("{}", "■".repeat(*v + 1)))
                    .collect::<Vec<_>>()
                    .join("+");
                sep0 = format!("\x1B[32m+{}+\x1B[0m", sep0);
                // 展示列
                let columns0 = columns
                    .iter()
                    .zip(max_len.iter())
                    .map(|(col, &len)| format!("{:width$}", col, width = len))
                    .collect::<Vec<_>>()
                    .join("\x1B[32m |\x1B[0m");
                // columns=" |"+columns+" |";
                let columns = format!("\x1B[32m|\x1B[0m{} \x1B[32m|\x1B[0m", columns0);

                // 展示分隔符1
                let mut sep1 = max_len
                    .iter()
                    .map(|v| format!("{}", "-".repeat(*v + 1)))
                    // .map(|v| format!("{}", "■".repeat(*v + 1)))
                    .collect::<Vec<_>>()
                    .join("+");
                sep1 = format!("\x1B[32m+{}+\x1B[0m", sep1);

                // 展示列数据
                let rows = rows
                .iter()
                .map(|row| {
                     let formatted_row = row.iter()
                        .zip(max_len.iter())
                        .map(|(v, &len)| format!("{:width$}", v.to_string(), width = len))
                        .collect::<Vec<_>>()
                        .join("\x1B[32m |\x1B[0m");
                     format!("\x1B[32m|\x1B[0m{} \x1B[32m|\x1B[0m", formatted_row)
                 })
                .collect::<Vec<_>>()
                .join("\n");

                // 展示分隔符3
                let mut  sep2 = max_len
                    .iter()
                    .map(|v| format!("{}", "-".repeat(*v + 1)))
                    .collect::<Vec<_>>()
                    .join("+");
                sep2 = format!("\x1B[32m+{}+\x1B[0m", sep2);

                format!(
                    "{}\n{}\n{}\n{}\n{}\n\x1B[32m({} rows in set)\x1B[0m",
                    sep0, columns, sep1, rows, sep2, rows_len
                )
            }
            // 事务的结果
            ResultSet::Update { count } => format!("UPDATE {} rows", count),
            ResultSet::Delete { count } => format!("DELETE {} rows", count),
            ResultSet::Begin { version } => format!("TRANSACTION {} BEGIN", version),
            ResultSet::Commit { version } => format!("TRANSACTION {} COMMIT", version),
            ResultSet::Rollback { version } => format!("TRANSACTION {} ROLLBACK", version),
            ResultSet::Explain { plan } => plan.to_string(),
        }
    }
}
