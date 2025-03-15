use crate::{
    error::{Error, Result}, pppg, sql::{
        engine::Transaction,
        parser::ast::{self, Expression},
        schema::{self, Table},
        types::Value,
    }
};

use super::{Node, Plan};

// 需要手动标注生命周期！
pub struct Planner<'a, T: Transaction> {
    txn: &'a mut T,
}

impl<'a, T: Transaction> Planner<'a, T> {
    pub fn new(txn: &'a mut T) -> Self {
        Self { txn }
    }

    pub fn build(&mut self, stmt: ast::Statement) -> Result<Plan> {
        Ok(Plan(self.build_statment(stmt)?))
    }

    fn build_statment(&self, stmt: ast::Statement) -> Result<Node> {
        Ok(match stmt {
            ast::Statement::CreateTable { name, columns } => Node::CreateTable {
                schema: Table {
                    name,
                    columns: columns
                        .into_iter()
                        .map(|c| {
                            let nullable = c.nullable.unwrap_or(!c.primary_key);
                            let default = match c.default {
                                Some(expr) => Some(Value::from_expression(expr)),
                                None if nullable => Some(Value::Null),
                                None => None,
                            };

                            schema::Column {
                                name: c.name,
                                datatype: c.datatype,
                                nullable,
                                default,
                                // 这里将抽象语法树里面的信息转化为具体的Node，然后送进去执行！
                                primary_key: c.primary_key,
                                //这里不是主键列，因为主键列不需要去建立索引
                                index: c.index && !c.primary_key,
                            }
                        })
                        .collect(),
                },
            },
            ast::Statement::DropTable { name } => Node::DropTable { name },
            ast::Statement::Insert {
                table_name,
                columns,
                values,
            } => Node::Insert {
                table_name,
                columns: columns.unwrap_or_default(),
                values,
            },
            // 选择语句的可用修饰！完美的诠释了什么叫
            // sfwgholo！这是select语句的全部实现，嘎了~~
            ast::Statement::Select {
                select,
                from,
                where_clause,
                group_by,
                having,
                order_by,
                limit,
                offset,
            } => {
                // from
                // 这里就会选择走索引还是走普通全表扫描
                let mut node = self.build_from_item(from, &where_clause)?;

                let mut has_agg = false;
                // aggregate、group by
                if !select.is_empty() {
                    for (expr, _) in select.iter() {
                        // 如果是 Function，说明是 agg
                        if let ast::Expression::Function(_, _) = expr {
                            has_agg = true;
                            break;
                        }
                    }
                    if group_by.is_some() {
                        has_agg = true;
                    }
                    if has_agg {
                        node = Node::Aggregate {
                            source: Box::new(node),
                            exprs: select.clone(),
                            group_by,
                        }
                    }
                }

                // having
                if let Some(expr) = having {
                    node = Node::Filter {
                        source: Box::new(node),
                        predicate: expr,
                    }
                }

                // order by
                if !order_by.is_empty() {
                    node = Node::Order {
                        source: Box::new(node),
                        order_by,
                    }
                }

                // offset
                if let Some(expr) = offset {
                    node = Node::Offset {
                        source: Box::new(node),
                        offset: match Value::from_expression(expr) {
                            Value::Integer(i) => i as usize,
                            _ => return Err(Error::Internal("invalid offset".into())),
                        },
                    }
                }

                // limit
                if let Some(expr) = limit {
                    node = Node::Limit {
                        source: Box::new(node),
                        limit: match Value::from_expression(expr) {
                            Value::Integer(i) => i as usize,
                            _ => return Err(Error::Internal("invalid limit".into())),
                        },
                    }
                }

                // projection
                if !select.is_empty() && !has_agg {
                    node = Node::Projection {
                        source: Box::new(node),
                        exprs: select,
                    }
                }

                node
            }
            ast::Statement::Update {
                table_name,
                columns,
                where_clause,
            } => Node::Update {
                table_name: table_name.clone(),
                source: Box::new(self.build_scan(table_name, where_clause)?),
                columns,
            },
            ast::Statement::Delete {
                table_name,
                where_clause,
            } => Node::Delete {
                table_name: table_name.clone(),
                source: Box::new(self.build_scan(table_name, where_clause)?),
            },
            // 处理事务的命令执行
            ast::Statement::Begin | ast::Statement::Commit | ast::Statement::Rollback => {
                return Err(Error::Internal("unexpected transaction command".into()));
            }
            ast::Statement::Explain { stmt: _ } => {
                return Err(Error::Internal("unexpected explain command".into()));
            }
        })
    }

    fn build_from_item(&self, item: ast::FromItem, filter: &Option<Expression>) -> Result<Node> {
        Ok(match item {
            // 这里总的查询表的逻辑
            // 这一句查表，回判断走索引还是走普通查询！！
            ast::FromItem::Table { name } => {
               let res= self.build_scan(name, filter.clone())?;
               pppg!("查表的节点：",res);
               res
            }
            ast::FromItem::Join {
                left,
                right,
                join_type,
                predicate,
            } => {
                // 如果是 right join，则交换位置
                let (left, right) = match join_type {
                    ast::JoinType::Right => (right, left),
                    _ => (left, right),
                };

                let outer = match join_type {
                    ast::JoinType::Cross | ast::JoinType::Inner => false,
                    _ => true,
                };
                // 实现两种合并，左合并还是右合并！
                if join_type == ast::JoinType::Cross {
                    Node::NestedLoopJoin {
                        left: Box::new(self.build_from_item(*left, filter)?),
                        right: Box::new(self.build_from_item(*right, filter)?),
                        predicate,
                        outer,
                    }
                } else {
                    Node::HashJoin {
                        left: Box::new(self.build_from_item(*left, filter)?),
                        right: Box::new(self.build_from_item(*right, filter)?),
                        predicate,
                        outer,
                    }
                }
            }
        })
    }

    fn build_scan(&self, table_name: String, filter: Option<Expression>) -> Result<Node> {
        Ok(match Self::parse_scan_filter(filter.clone()) {
            //匹配列的情况和他的值，但是这里不一定就是索引，所以这里需要做对比工作
            Some((field, value)) => {
                let table = self.txn.must_get_table(table_name.clone())?; //拿到txn里面的表结构的信息

                // 判断是否是主键，主键自动走索引！
                if table
                    .columns
                    .iter()
                    .position(|c| c.name == field && c.primary_key)
                    .is_some()
                {
                    return Ok(Node::PrimaryKeyScan { table_name, value });//主键索引
                }
                //找到列名和查询字段相同并且该列存在索引，那就直接走索引！返回所以信息
                match table
                    .columns
                    .iter()
                    .position(|c| c.name == field && c.index)
                {
                    Some(_) => Node::IndexScan {
                        //找到索引列，构造节点
                        table_name,
                        field,
                        value,
                    },
                    //表示没有匹配到我们想要的索引列的情况！进行普通扫描！
                    None => Node::Scan { table_name, filter },
                }
            }
            None => Node::Scan { table_name, filter },
        })
    }

    fn parse_scan_filter(filter: Option<Expression>) -> Option<(String, Value)> {
        match filter {
            Some(expr) => match expr {
                Expression::Field(f) => Some((f, Value::Null)),
                Expression::Consts(c) => {
                    Some(("".into(), Value::from_expression(Expression::Consts(c))))
                }
                Expression::Operation(operation) => match operation {
                    //这里就是在解析索引！
                    ast::Operation::Equal(l, r) => {
                        let lv = Self::parse_scan_filter(Some(*l));
                        let rv = Self::parse_scan_filter(Some(*r));

                        Some((lv.unwrap().0, rv.unwrap().1))
                    }
                    _ => None,
                },
                _ => None,
            },
            None => None,
        }
    }
}
