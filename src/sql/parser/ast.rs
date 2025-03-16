use std::{collections::BTreeMap, fmt::Display};

use crate::{
    error::{Error, Result},
    sql::types::{DataType, Value},
};

// Abstract Syntax Tree 抽象语法树定义
#[derive(Debug, PartialEq)]
pub enum Statement {
    CreateTable {
        name: String,
        columns: Vec<Column>,
    },
    DropTable {
        name: String,
    },
    Insert {
        table_name: String,
        columns: Option<Vec<String>>,
        values: Vec<Vec<Expression>>,
    },
    Select {
        select: Vec<(Expression, Option<String>)>,
        from: FromItem,
        where_clause: Option<Expression>,
        group_by: Option<Expression>,
        having: Option<Expression>,
        order_by: Vec<(String, OrderDirection)>,
        limit: Option<Expression>,
        offset: Option<Expression>,
    },
    // 更新操作的抽象语法树~~！！
    Update {
        table_name: String,
        columns: BTreeMap<String, Expression>,
        where_clause: Option<Expression>,
    },
    Delete {
        table_name: String,
        where_clause: Option<Expression>,
    },
    // 构建事务相关的抽象语法树
    Begin,
    Commit,
    Rollback,
    Explain {
        stmt: Box<Statement>,
    },
}
// 增长方向
#[derive(Debug, PartialEq)]
pub enum OrderDirection {
    Asc,
    Desc,
}

// 列定义
#[derive(Debug, PartialEq)]
pub struct Column {
    pub name: String,
    pub datatype: DataType,
    pub nullable: Option<bool>,
    pub default: Option<Expression>,
    //新增布尔值，表示是不是一个主键
    pub primary_key: bool,
    pub index: bool,
}

#[derive(Debug, PartialEq)]
// FromItem枚举 表示 FROM 子句可能包含的不同类型的元素
pub enum FromItem {
    Table {
        name: String,
    },

    Join {
        left: Box<FromItem>,
        right: Box<FromItem>,
        join_type: JoinType,
        predicate: Option<Expression>,
    },
}

#[derive(Debug, PartialEq)]
pub enum JoinType {
    Cross,
    Inner,
    Left,
    Right,
}

// 表达式定义，凡是1+1,a>1,等都是表达式
#[derive(Debug, PartialEq, Clone)]
pub enum Expression {
    Field(String),//字段
    Consts(Consts),//常量
    Operation(Operation),//=、>、<
    Function(String, String),//新增的聚集函数类型，表示聚集函数的相关操作！
}

impl From<Consts> for Expression {
    fn from(value: Consts) -> Self {
        Self::Consts(value)
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum Consts {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
}

#[derive(Debug, PartialEq, Clone)]
pub enum Operation {
    Equal(Box<Expression>, Box<Expression>),//这里就是索引的判断，如果查询的某一列上面有索引，那就直接走索引！这里的Equal左=字段名，右=索引名
    GreaterThan(Box<Expression>, Box<Expression>),
    LessThan(Box<Expression>, Box<Expression>),
}

impl Display for Expression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Expression::Field(v) => write!(f, "{}", v),
            Expression::Consts(c) => write!(
                f,
                "{}",
                Value::from_expression(Expression::Consts(c.clone()))
            ),
            Expression::Operation(operation) => match operation {
                Operation::Equal(l, r) => write!(f, "{} = {}", l, r),
                Operation::GreaterThan(l, r) => write!(f, "{} > {}", l, r),
                Operation::LessThan(l, r) => write!(f, "{} < {}", l, r),
            },
            Expression::Function(name, field) => write!(f, "{}({})", name, field),
        }
    }
}

// AST的用于评估表达式并返回结果的函数。它处理的表达式是基于一个自定义的数据类型 Expression，并通过不同的匹配来执行各种操作。
// expr 是待评估的表达式，类型为 &Expression，是一个引用。
// lcols 和 rcols 是左表和右表的列名向量 (Vec<String>)，分别表示两个表的字段。
// lrows 和 rrows 是左表和右表的行数据，类型为 Vec<Value>，其中 Value 是某个数据类型的封装，表示表格中的每一行。

// 整个函数的设计理念是基于`表达式树`的遍历与求值，通过`递归方式`逐层处理复杂的表达式逻辑。
pub fn evaluate_expr(
    expr: &Expression,
    lcols: &Vec<String>,
    lrows: &Vec<Value>,
    rcols: &Vec<String>,
    rrows: &Vec<Value>,
) -> Result<Value> {
    // 匹配表达式，返回结果集~
    match expr {
        // 如果表达式是字段（Field），我们在左表 (lcols) 中查找字段名 col_name，并返回对应位置的行数据 lrows[pos]。
        Expression::Field(col_name) => {
            let pos = match lcols.iter().position(|c| *c == *col_name) {
                Some(pos) => pos,
                None => {
                    return Err(Error::Internal(format!(
                        "column {} is not in table",
                        col_name
                    )))
                }
            };
            Ok(lrows[pos].clone())
        }

        // 如果表达式是常量（Consts），根据不同的常量类型（如 Null、Boolean、Integer、Float、String），返回对应的 Value 类型。
        Expression::Consts(consts) => Ok(match consts {
            Consts::Null => Value::Null,
            Consts::Boolean(b) => Value::Boolean(*b),
            Consts::Integer(i) => Value::Integer(*i),
            Consts::Float(f) => Value::Float(*f),
            Consts::String(s) => Value::String(s.clone()),
        }),
        // 如果表达式是 = 运算符。首先`递归`地评估左右表达式 lexpr 和 rexpr，并得到它们的值 lv 和 rv。
        Expression::Operation(operation) => match operation {
            Operation::Equal(lexpr, rexpr) => {
                let lv = evaluate_expr(&lexpr, lcols, lrows, rcols, rrows)?;
                let rv = evaluate_expr(&rexpr, rcols, rrows, lcols, lrows)?;
                Ok(match (lv, rv) {
                    (Value::Boolean(l), Value::Boolean(r)) => Value::Boolean(l == r),
                    (Value::Integer(l), Value::Integer(r)) => Value::Boolean(l == r),
                    (Value::Integer(l), Value::Float(r)) => Value::Boolean(l as f64 == r),
                    (Value::Float(l), Value::Integer(r)) => Value::Boolean(l == r as f64),
                    (Value::Float(l), Value::Float(r)) => Value::Boolean(l == r),
                    (Value::String(l), Value::String(r)) => Value::Boolean(l == r),
                    (Value::Null, _) => Value::Null,
                    (_, Value::Null) => Value::Null,
                    (l, r) => {
                        return Err(Error::Internal(format!(
                            "can not compare exression {} and {}",
                            l, r
                        )))
                    }
                })
            }
        // 如果表达式是>
            Operation::GreaterThan(lexpr, rexpr) => {
                let lv = evaluate_expr(&lexpr, lcols, lrows, rcols, rrows)?;
                let rv = evaluate_expr(&rexpr, rcols, rrows, lcols, lrows)?;
                Ok(match (lv, rv) {
                    (Value::Boolean(l), Value::Boolean(r)) => Value::Boolean(l > r),
                    (Value::Integer(l), Value::Integer(r)) => Value::Boolean(l > r),
                    (Value::Integer(l), Value::Float(r)) => Value::Boolean(l as f64 > r),
                    (Value::Float(l), Value::Integer(r)) => Value::Boolean(l > r as f64),
                    (Value::Float(l), Value::Float(r)) => Value::Boolean(l > r),
                    (Value::String(l), Value::String(r)) => Value::Boolean(l > r),
                    (Value::Null, _) => Value::Null,
                    (_, Value::Null) => Value::Null,
                    (l, r) => {
                        return Err(Error::Internal(format!(
                            "can not compare exression {} and {}",
                            l, r
                        )))
                    }
                })
            }
            // 如果表达式是<
            Operation::LessThan(lexpr, rexpr) => {
                let lv = evaluate_expr(&lexpr, lcols, lrows, rcols, rrows)?;
                let rv = evaluate_expr(&rexpr, rcols, rrows, lcols, lrows)?;
                Ok(match (lv, rv) {
                    (Value::Boolean(l), Value::Boolean(r)) => Value::Boolean(l < r),
                    (Value::Integer(l), Value::Integer(r)) => Value::Boolean(l < r),
                    (Value::Integer(l), Value::Float(r)) => Value::Boolean((l as f64) < r),
                    (Value::Float(l), Value::Integer(r)) => Value::Boolean(l < r as f64),
                    (Value::Float(l), Value::Float(r)) => Value::Boolean(l < r),
                    (Value::String(l), Value::String(r)) => Value::Boolean(l < r),
                    (Value::Null, _) => Value::Null,
                    (_, Value::Null) => Value::Null,
                    (l, r) => {
                        return Err(Error::Internal(format!(
                            "can not compare exression {} and {}",
                            l, r
                        )))
                    }
                })
            }
        },
        _ => return Err(Error::Internal("unexpected expression".into())),
    }
}
