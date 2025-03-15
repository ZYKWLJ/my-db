use std::collections::HashMap;

use crate::{
    error::{Error, Result},
    sql::{
        engine::Transaction,
        parser::ast::{self, evaluate_expr, Expression},
        types::Value,
    },
};

use super::{Executor, ResultSet};
// join语句的执行语句显然的，使用 a left join b on a.id=b.id;类似语句，这里就是这样的
pub struct NestedLoopJoin<T: Transaction> {
    left: Box<dyn Executor<T>>,
    right: Box<dyn Executor<T>>,
    predicate: Option<Expression>,
    outer: bool,
}

impl<T: Transaction> NestedLoopJoin<T> {
    pub fn new(
        left: Box<dyn Executor<T>>,
        right: Box<dyn Executor<T>>,
        predicate: Option<Expression>,
        outer: bool,
    ) -> Box<Self> {
        Box::new(Self {
            left,
            right,
            predicate,
            outer,
        })
    }
}

impl<T: Transaction> Executor<T> for NestedLoopJoin<T> {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        // 先执行左边的
        // 获取左表的执行总的结果
        if let ResultSet::Scan {
            columns: lcols,
            rows: lrows,
        } = self.left.execute(txn)?

        {
            let mut new_rows = Vec::new();
            let mut new_cols = lcols.clone();//将左表的列赋值给新列
            // 再执行右边的
            // 获取右表的执行的总的结果
            if let ResultSet::Scan {
                columns: rcols,
                rows: rrows,
            } = self.right.execute(txn)?
            
            // 这个作用域内全部是匹配工作，做得是左右交互的匹配工作
            {
                new_cols.extend(rcols.clone());//将右表的列合并在新列上

                for lrow in &lrows {
                    let mut matched = false;

                    for rrow in &rrows {
                        let mut row = lrow.clone();//先把左表的数据复制过去

                        // 如果有条件，查看是否满足 Join 条件
                        // evaluate_expr 的结果是 Value::Null、Value::Boolean(false)、或 Value::Boolean(true)：
                        // Value::Null：跳过当前行。
                        // Value::Boolean(false)：跳过当前行。
                        // Value::Boolean(true)：如果条件为真，将右表的行 rrow 与左表的行 lrow 合并，并添加到 new_rows 中，同时设置 matched 为 true。
                        if let Some(expr) = &self.predicate {//然后评估表达式！
                            match evaluate_expr(expr, &lcols, lrow, &rcols, rrow)? {
                                Value::Null => {}
                                Value::Boolean(false) => {}
                                Value::Boolean(true) => {
                                    row.extend(rrow.clone());
                                    new_rows.push(row);
                                    matched = true;
                                }
                                _ => return Err(Error::Internal("Unexpected expression".into())),
                            }
                        } else {
                            // 如果没有条件（None），表示这是一个 Cross Join（笛卡尔积），即两个表的所有行都会组合在一起。
                            // 就是默认的两个for循环
                            row.extend(rrow.clone());
                            new_rows.push(row);
                        }
                    }
                    // 外连接（Outer Join）处理：(代表着一边没有数据~~)
                    // 如果是外连接（self.outer 为 true）且当前行没有匹配（!matched），
                    // 则将左表的行 lrow 添加到 new_rows 中，并在右表的列位置填充 NULL 值，表示右表没有数据。
                    if self.outer && !matched {
                        let mut row = lrow.clone();
                        for _ in 0..rrows[0].len() {
                            row.push(Value::Null);
                        }
                        new_rows.push(row);
                    }
                }
            }
            // 返回结果~
            return Ok(ResultSet::Scan {
                columns: new_cols,
                rows: new_rows,
            });
        }

        Err(Error::Internal("Unexpected result set".into()))
    }
}

// 哈希优化结构
pub struct HashJoin<T: Transaction> {
    left: Box<dyn Executor<T>>,
    right: Box<dyn Executor<T>>,
    predicate: Option<Expression>,
    outer: bool,//是不是外连接(设计是够丢弃数据)
}

impl<T: Transaction> HashJoin<T> {
    pub fn new(
        left: Box<dyn Executor<T>>,
        right: Box<dyn Executor<T>>,
        predicate: Option<Expression>,
        outer: bool,
    ) -> Box<Self> {
        Box::new(Self {
            left,
            right,
            predicate,
            outer,
        })
    }
}

impl<T: Transaction> Executor<T> for HashJoin<T> {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        // 先执行左边的，这里是获取左边的结果集合。self.left.execute(txn) 执行左侧表（self.left 是 NestedLoopJoin 中的左表），并获取结果集。
        if let ResultSet::Scan {
            columns: lcols,
            rows: lrows,
        } = self.left.execute(txn)?
        //这个语句到这里就结束了！至此顺利获得了左边的结果集合！
        //再执行右边的，这里是获取右边的结果集合。执行右侧表（self.right 是 NestedLoopJoin 中的右表），并获取结果集。
        {
            //这里 这个大括号完成总的匹配任务！
            let mut new_rows = Vec::new();
            let mut new_cols = lcols.clone(); //这里是新列，复制的是左表的列，下面还有合并右表的列！

            if let ResultSet::Scan {
                columns: rcols,
                rows: rrows,
            } = self.right.execute(txn)?
            //至此顺利获取到了右边的结果集合！
            {
                //将右表的列名（rcols）添加到左表的列名（lcols）中，new_cols 就是合并后的列名。
                new_cols.extend(rcols.clone());
                // 解析 HashJoin 条件   解析表达式得到两个相等的列名
                let (lfield, rfield) = match parse_join_filter(self.predicate) {
                    Some(filter) => filter,
                    None => return Err(Error::Internal("failed to parse join predicate".into())),
                };
                // 获取 join 列在表中列的位置
                let lpos = match lcols.iter().position(|c| *c == lfield) {
                    Some(pos) => pos,
                    None => {
                        return Err(Error::Internal(format!(
                            "column {} not exist in table",
                            lfield
                        )))
                    }
                };
                let rpos = match rcols.iter().position(|c| *c == rfield) {
                    Some(pos) => pos,
                    None => {
                        return Err(Error::Internal(format!(
                            "column {} not exist in table",
                            rfield
                        )))
                    }
                };

                // 构建哈希表   key为列的值，value为行数据
                let mut table = HashMap::new();
                for row in &rrows {
                    let rows = table.entry(row[rpos].clone()).or_insert(Vec::new());
                    rows.push(row.clone());
                }

                // 扫描左边获取记录
                // 嵌套循环遍历左右表的行
                // 外层循环遍历左表的每一行（lrow），内层循环遍历右表的每一行（rrow），通过嵌套循环实现连接操作。
                for lrow in lrows {
                    match table.get(&lrow[lpos]) {
                        Some(rows) => {
                            for r in rows {
                                let mut row = lrow.clone();
                                row.extend(r.clone());
                                new_rows.push(row);
                            }
                        }
                        None => {
                            if self.outer {
                                let mut row = lrow.clone();
                                for _ in 0..rrows[0].len() {
                                    row.push(Value::Null);
                                }
                                new_rows.push(row);
                            }
                        }
                    }
                }

                return Ok(ResultSet::Scan {
                    columns: new_cols,
                    rows: new_rows,
                });
            }
        }
        Err(Error::Internal("Unexpected result set".into()))
    }
}

fn parse_join_filter(predicate: Option<Expression>) -> Option<(String, String)> {
    match predicate {
        Some(expr) => match expr {
            Expression::Field(f) => Some((f, "".into())),
            Expression::Operation(operation) => match operation {
                ast::Operation::Equal(l, r) => {
                    let lv = parse_join_filter(Some(*l));
                    let rv = parse_join_filter(Some(*r));

                    Some((lv.unwrap().0, rv.unwrap().0))
                }
                _ => None,
            },
            _ => None,
        },
        None => None,
    }
}
