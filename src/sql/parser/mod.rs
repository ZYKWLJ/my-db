use std::{collections::BTreeMap, iter::Peekable};

use ast::{Column, Expression, Operation, OrderDirection};
use lexer::{Keyword, Lexer, Token};

use crate::{
    error::{Error, Result},
    ppb, pppb, pppg, pppy,
};

use super::types::DataType;

pub mod ast;
mod lexer;

// 解析器定义
pub struct Parser<'a> {
    lexer: Peekable<Lexer<'a>>,
}

impl<'a> Parser<'a> {
    pub fn new(input: &'a str) -> Self {
        Parser {
            lexer: Lexer::new(input).peekable(),
        }
    }

    // 解析，获取到抽象语法树
    pub fn parse(&mut self) -> Result<ast::Statement> {
        pppb!("开始纯解析Token，并且组织成语法树");
        let stmt = self.parse_statement()?; //总分枝
        pppb!("期待sql语句的最后有一个分号");
        // 期望 sql 语句的最后有个分号
        self.next_expect(Token::Semicolon)?;
        // 分号之后不能有其他的符号
        pppb!("分号之后不能有其他的符号");

        if let Some(token) = self.peek()? {
            return Err(Error::Parse(format!("[Parser] Unexpected token {}", token)));
        }
        Ok(stmt)
    }

    fn parse_statement(&mut self) -> Result<ast::Statement> {
        // 查看第一个 Token 类型
        // 本质就是一个一个解析Token,分别查看第一个Token确定接下来的语句类型
        match self.peek()? {
            Some(Token::Keyword(Keyword::Create)) => self.parse_ddl(),
            Some(Token::Keyword(Keyword::Drop)) => self.parse_ddl(),
            Some(Token::Keyword(Keyword::Select)) => self.parse_select(),
            Some(Token::Keyword(Keyword::Insert)) => self.parse_insert(),
            Some(Token::Keyword(Keyword::Update)) => self.parse_update(),
            Some(Token::Keyword(Keyword::Delete)) => self.parse_delete(),
            Some(Token::Keyword(Keyword::Begin)) => self.parse_transaction(),
            Some(Token::Keyword(Keyword::Commit)) => self.parse_transaction(),
            Some(Token::Keyword(Keyword::Rollback)) => self.parse_transaction(),
            Some(Token::Keyword(Keyword::Explain)) => self.parse_explain(),
            Some(t) => Err(Error::Parse(format!("[Parser] Unexpected token {}", t))),
            None => Err(Error::Parse(format!("[Parser] Unexpected end of input"))),
        }
    }

    // 解析 DDL 类型
    // ddl统一指的是建表删表的过程~
    fn parse_ddl(&mut self) -> Result<ast::Statement> {
        match self.next()? {
            Token::Keyword(Keyword::Create) => self.parse_ddl_create_table(),
            Token::Keyword(Keyword::Drop) => self.parse_ddl_drop_table(),
            token => Err(Error::Parse(format!("[Parser] Unexpected token {}", token))),
        }
    }

    // 解析 Select 语句
    fn parse_select(&mut self) -> Result<ast::Statement> {
        pppy!("开始解析总的select语句......");
        // 返回的是AST
        let res = Ok(ast::Statement::Select {
            select: self.parse_select_clause()?,
            from: self.parse_from_clause()?,
            where_clause: self.parse_where_clause()?,
            group_by: self.parse_group_clause()?,
            having: self.parse_having_clause()?,
            order_by: self.parse_order_clause()?,
            limit: {
                pppb!(format!("开始解析limit子句......"));
                if self.next_if_token(Token::Keyword(Keyword::Limit)).is_some() {
                    let res = Some(self.parse_expression()?);
                    pppb!(format!("解析得到的limit子句为:{:?}", res));
                    res
                } else {
                    None
                }
            },
            offset: {
                pppb!(format!("开始解析offset子句......"));

                if self
                    .next_if_token(Token::Keyword(Keyword::Offset))
                    .is_some()
                {
                    // Some(self.parse_expression()?)
                    let res = Some(self.parse_expression()?);
                    pppb!(format!("解析得到的offset子句为:{:?}", res));
                    res
                } else {
                    None
                }
            },
        });
        pppy!(format!("解析得到的select总语法树为:{:?}", res));
        res
    }

    // 解析 Insert 语句
    fn parse_insert(&mut self) -> Result<ast::Statement> {
        self.next_expect(Token::Keyword(Keyword::Insert))?;
        self.next_expect(Token::Keyword(Keyword::Into))?;

        // 表名
        let table_name = self.next_ident()?;

        // 查看是否给指定的列进行 insert
        let columns = if self.next_if_token(Token::OpenParen).is_some() {
            let mut cols = Vec::new();
            loop {
                cols.push(self.next_ident()?.to_string());
                match self.next()? {
                    Token::CloseParen => break,
                    Token::Comma => {}
                    token => {
                        return Err(Error::Parse(format!("[Parser] Unexpected token {}", token)));
                    }
                }
            }
            Some(cols)
        } else {
            None
        };

        // 解析 value 信息
        self.next_expect(Token::Keyword(Keyword::Values))?;
        // insert into tbl(a, b, c) values (1, 2, 3),(4, 5, 6);
        let mut values = Vec::new();
        loop {
            self.next_expect(Token::OpenParen)?;
            let mut exprs = Vec::new();
            loop {
                exprs.push(self.parse_expression()?);
                match self.next()? {
                    Token::CloseParen => break,
                    Token::Comma => {}
                    token => {
                        return Err(Error::Parse(format!("[Parser] Unexpected token {}", token)));
                    }
                }
            }
            values.push(exprs);
            if self.next_if_token(Token::Comma).is_none() {
                break;
            }
        }

        Ok(ast::Statement::Insert {
            table_name,
            columns,
            values,
        })
    }

    // 解析 Create Table 语句
    fn parse_ddl_create_table(&mut self) -> Result<ast::Statement> {
        self.next_expect(Token::Keyword(Keyword::Table))?;
        // 期望是 Table 名
        let table_name = self.next_ident()?;
        // 表名之后应该是括号
        self.next_expect(Token::OpenParen)?;

        // 解析列信息
        let mut columns = Vec::new();
        loop {
            columns.push(self.parse_ddl_column()?);
            // 如果没有逗号，列解析完成，跳出
            if self.next_if_token(Token::Comma).is_none() {
                break;
            }
        }

        self.next_expect(Token::CloseParen)?;
        Ok(ast::Statement::CreateTable {
            name: table_name,
            columns,
        })
    }

    // 解析列信息
    fn parse_ddl_column(&mut self) -> Result<ast::Column> {
        let mut column = Column {
            name: self.next_ident()?,
            datatype: match self.next()? {
                Token::Keyword(Keyword::Int) | Token::Keyword(Keyword::Integer) => {
                    DataType::Integer
                }
                Token::Keyword(Keyword::Bool) | Token::Keyword(Keyword::Boolean) => {
                    DataType::Boolean
                }
                Token::Keyword(Keyword::Float) | Token::Keyword(Keyword::Double) => DataType::Float,
                Token::Keyword(Keyword::String)
                | Token::Keyword(Keyword::Text)
                | Token::Keyword(Keyword::Varchar) => DataType::String,
                token => return Err(Error::Parse(format!("[Parser] Unexpected token {}", token))),
            },
            nullable: None,
            default: None,
            primary_key: false,
            index: false,
        };

        // 解析列的默认值，以及是否可以为空
        while let Some(Token::Keyword(keyword)) = self.next_if_keyword() {
            match keyword {
                Keyword::Null => column.nullable = Some(true),
                Keyword::Not => {
                    self.next_expect(Token::Keyword(Keyword::Null))?;
                    column.nullable = Some(false);
                }
                Keyword::Default => column.default = Some(self.parse_expression()?),
                // 这里如果是Primary关键字开头的话，还要进一步判断，后面一定是一个key的关键字！表示这一列是我们的主键
                Keyword::Primary => {
                    self.next_expect(Token::Keyword(Keyword::Key))?;
                    column.primary_key = true; //主键确认！
                }
                Keyword::Index => column.index = true,
                k => return Err(Error::Parse(format!("[Parser] Unexpected keyword {}", k))),
            }
        }

        Ok(column)
    }

    // 解析 Drop Table 语句
    fn parse_ddl_drop_table(&mut self) -> Result<ast::Statement> {
        self.next_expect(Token::Keyword(Keyword::Table))?;
        Ok(ast::Statement::DropTable {
            name: self.next_ident()?,
        })
    }

    // 解析 Update 语句
    fn parse_update(&mut self) -> Result<ast::Statement> {
        self.next_expect(Token::Keyword(Keyword::Update))?; //期望第一个关键字是update！
                                                            // 表名
        let table_name = self.next_ident()?;
        self.next_expect(Token::Keyword(Keyword::Set))?;

        let mut columns = BTreeMap::new();
        loop {
            let col = self.next_ident()?;
            self.next_expect(Token::Equal)?;
            let value = self.parse_expression()?;
            if columns.contains_key(&col) {
                return Err(Error::Parse(format!(
                    "[parser] Duplicate column {} for update",
                    col
                )));
            }
            columns.insert(col, value);
            // 如果没有逗号，列解析完成，跳出
            if self.next_if_token(Token::Comma).is_none() {
                break;
            }
        }

        Ok(ast::Statement::Update {
            table_name,
            columns,
            where_clause: self.parse_where_clause()?,
        })
    }

    // 解析 Delete 语句
    fn parse_delete(&mut self) -> Result<ast::Statement> {
        self.next_expect(Token::Keyword(Keyword::Delete))?;
        self.next_expect(Token::Keyword(Keyword::From))?;
        // 表名
        let table_name = self.next_ident()?;

        Ok(ast::Statement::Delete {
            table_name,
            where_clause: self.parse_where_clause()?,
        })
    }

    // 解析事务语句
    fn parse_transaction(&mut self) -> Result<ast::Statement> {
        Ok(match self.next()? {
            // 匹配接下来的事务的类型~
            Token::Keyword(Keyword::Begin) => ast::Statement::Begin,
            Token::Keyword(Keyword::Commit) => ast::Statement::Commit,
            Token::Keyword(Keyword::Rollback) => ast::Statement::Rollback,
            _ => return Err(Error::Parse("unknown transaction command".into())),
        })
    }
    // 因为explain语句本身不需要去执行器中执行，直接在Engine里面处理就行了，就和前面的begin、commit类似，不需要在事务中处理！
    // 解析 explain 语句
    fn parse_explain(&mut self) -> Result<ast::Statement> {
        self.next_expect(Token::Keyword(Keyword::Explain))?;
        if let Some(Token::Keyword(Keyword::Explain)) = self.peek()? {
            //不能对explain语句进行嵌套展示！
            return Err(Error::Parse("canno nest explain statement".into()));
        }
        // 对build Statement进行重复解析！~
        let stmt = self.parse_statement()?;
        // 返回这课抽象语法树本身！
        Ok(ast::Statement::Explain {
            stmt: Box::new(stmt),
        })
    }
    // 解析where子句
    fn parse_where_clause(&mut self) -> Result<Option<Expression>> {
        pppb!("解析where子句");
        if self.next_if_token(Token::Keyword(Keyword::Where)).is_none() {
            return Ok(None);
        }
        pppy!(format!(
            "解析出来的where子句:{:?}",
            self.parse_opreation_expr()
        ));

        Ok(Some(self.parse_opreation_expr()?))
    }

    fn parse_having_clause(&mut self) -> Result<Option<Expression>> {
        if self
            .next_if_token(Token::Keyword(Keyword::Having))
            .is_none()
        {
            return Ok(None);
        }
        pppy!(format!(
            "解析出来的having子句:{:?}",
            self.parse_opreation_expr()
        ));
        Ok(Some(self.parse_opreation_expr()?))
    }
    // 解析 SQL 语句中的 ORDER BY 子句
    fn parse_order_clause(&mut self) -> Result<Vec<(String, OrderDirection)>> {
        pppy!("开始解析OrderBy子句......");
        let mut orders = Vec::new();
        if self.next_if_token(Token::Keyword(Keyword::Order)).is_none() {
            return Ok(orders);
        }
        self.next_expect(Token::Keyword(Keyword::By))?;

        loop {
            let col = self.next_ident()?;
            let ord = match self.next_if(|t| {
                matches!(
                    t,
                    Token::Keyword(Keyword::Asc) | Token::Keyword(Keyword::Desc)
                )
            }) {
                Some(Token::Keyword(Keyword::Asc)) => OrderDirection::Asc,
                Some(Token::Keyword(Keyword::Desc)) => OrderDirection::Desc,
                _ => OrderDirection::Asc,
            };
            orders.push((col, ord));

            if self.next_if_token(Token::Comma).is_none() {
                break;
            }
        }
        pppy!(format!("解析出来的OrderBy子句:{:?}", orders));
        Ok(orders)
    }

    fn parse_select_clause(&mut self) -> Result<Vec<(Expression, Option<String>)>> {
        pppy!("开始解析Select子句......");

        self.next_expect(Token::Keyword(Keyword::Select))?; //如果下一个类型不是select直接退出

        let mut select = Vec::new();
        // select *
        // 这是单独的分支，全表查询，说明已经解析完了！
        if self.next_if_token(Token::Asterisk).is_some() {
            pppy!(format!("解析出来的Select子句{:?}", select));
            return Ok(select);
        }
        // 具体查询某行某列，无限循环找Token
        loop {
            // 解析表达式，字段、函数、常量，解析完之后，会返回一个表达式！
            let expr = self.parse_expression()?;
            // 查看是否有别名 -检测关键词是否是as，没有就是null
            let alias = match self.next_if_token(Token::Keyword(Keyword::As)) {
                Some(_) => Some(self.next_ident()?),
                None => None,
            };

            select.push((expr, alias));
            // SELECT column1, column2 AS alias2, column3
            // 发现不是逗号说明解析完了，退出。如果是逗号，说明还没解析完，继续解析！
            if self.next_if_token(Token::Comma).is_none() {
                break;
            }
        }
        pppy!(format!("解析出来的Select子句{:?}", select));
        Ok(select)
    }

    fn parse_from_clause(&mut self) -> Result<ast::FromItem> {
        pppy!("开始解析From子句......");

        // From 关键字
        self.next_expect(Token::Keyword(Keyword::From))?;

        // 第一个表名
        let mut item = self.parse_from_table_clause()?;
        pppg!("下面检测是否有join连接");
        // 是否有 Join
        while let Some(join_type) = self.parse_from_clause_join()? {
            let left = Box::new(item);
            let right = Box::new(self.parse_from_table_clause()?);

            // 解析 Join 条件与类型
            pppg!("开始解析join条件与类型");
            let predicate = match join_type {
                ast::JoinType::Cross => None,
                _ => {
                    self.next_expect(Token::Keyword(Keyword::On))?;
                    let l = self.parse_expression()?;
                    pppy!(format!("解析出来的左连接:{:?}", l));
                    self.next_expect(Token::Equal)?;
                    let r = self.parse_expression()?;
                    pppy!(format!("解析出来的右连接:{:?}", r));
                    let (l, r) = match join_type {
                        ast::JoinType::Right => (r, l),
                        _ => (l, r),
                    };

                    let cond = Operation::Equal(Box::new(l), Box::new(r));
                    pppy!(format!("返回的连接条件:{:?}", cond));
                    Some(ast::Expression::Operation(cond))
                }
            };

            item = ast::FromItem::Join {
                left,
                right,
                join_type,
                predicate,
            };
            pppy!(format!("解析出来的From子句{:?}", item));
        }

        Ok(item)
    }

    fn parse_group_clause(&mut self) -> Result<Option<Expression>> {
        pppy!("开始解析Group子句......");

        if self.next_if_token(Token::Keyword(Keyword::Group)).is_none() {
            return Ok(None);
        }

        self.next_expect(Token::Keyword(Keyword::By))?;
        pppy!(format!("解析出来的Group子句{:?}", self.parse_expression()?));

        Ok(Some(self.parse_expression()?))
    }
    // 得到from后面的表名
    fn parse_from_table_clause(&mut self) -> Result<ast::FromItem> {
        pppy!("开始解析表名......");
        pppy!(format!(
            "解析出来的表名{:?}",
            ast::FromItem::Table {
                name: self.next_ident()?,
            }
        ));
        Ok(ast::FromItem::Table {
            name: self.next_ident()?,
        })
    }

    fn parse_from_clause_join(&mut self) -> Result<Option<ast::JoinType>> {
        pppy!("开始解析join条件......");
        // 是否是 Cross Join
        if self.next_if_token(Token::Keyword(Keyword::Cross)).is_some() {
            self.next_expect(Token::Keyword(Keyword::Join))?; //不是的话就报错！因为这是固定搭配！
            pppy!(format!(
                "解析出来的join条件:{:?}",
                Some(ast::JoinType::Cross)
            ));
            Ok(Some(ast::JoinType::Cross)) // Cross Join
        } else if self.next_if_token(Token::Keyword(Keyword::Join)).is_some() {
            pppy!(format!(
                "解析出来的join条件:{:?}",
                Some(ast::JoinType::Inner)
            ));
            Ok(Some(ast::JoinType::Inner)) // Inner Join
        } else if self.next_if_token(Token::Keyword(Keyword::Left)).is_some() {
            self.next_expect(Token::Keyword(Keyword::Join))?;
            pppy!(format!(
                "解析出来的join条件:{:?}",
                Some(ast::JoinType::Left)
            ));
            Ok(Some(ast::JoinType::Left)) // Left Join
        } else if self.next_if_token(Token::Keyword(Keyword::Right)).is_some() {
            self.next_expect(Token::Keyword(Keyword::Join))?;
            pppy!(format!(
                "解析出来的join条件:{:?}",
                Some(ast::JoinType::Right)
            ));
            Ok(Some(ast::JoinType::Right)) // Right Join
        } else {
            pppy!(format!("没有解析出join条件"));
            Ok(None)
        }
    }

    fn parse_opreation_expr(&mut self) -> Result<ast::Expression> {
        pppy!("开始解析运算符表达式......");
        // 类似于 a>1
        // 这里的left = a
        let left = self.parse_expression()?;
        // 下面继续解析right
        Ok(match self.next()? {
            // 优先级
            Token::Equal => {
                let res = ast::Expression::Operation(Operation::Equal(
                    Box::new(left),
                    Box::new(self.compute_math_operator(1)?),
                ));
                pppy!("解析出来的运算符表达式:{:?}", res);
                res
            }
            Token::GreaterThan => {
                let res = ast::Expression::Operation(Operation::GreaterThan(
                    Box::new(left),
                    Box::new(self.compute_math_operator(1)?),
                ));
                pppy!("解析出来的运算符表达式:{:?}", res);
                res
            }
            Token::LessThan => {
                let res = ast::Expression::Operation(Operation::LessThan(
                    Box::new(left),
                    Box::new(self.compute_math_operator(1)?),
                ));
                pppy!("解析出来的运算符表达式:{:?}", res);
                res
            }
            _ => return Err(Error::Internal("Unexpected token".into())),
        })
    }

    // 解析表达式
    fn parse_expression(&mut self) -> Result<ast::Expression> {
        pppb!("开始进行表达式解析......");
        Ok(match self.next()? {
            Token::Ident(ident) => {
                // 函数，检测(、)
                // count(col_name)
                if self.next_if_token(Token::OpenParen).is_some() {
                    let col_name = self.next_ident()?;
                    self.next_expect(Token::CloseParen)?;
                    pppy!(format!(
                        "解析得到的表达式,函数{:?}",
                        ast::Expression::Function(ident.clone(), col_name.clone())
                    ));
                    ast::Expression::Function(ident, col_name)
                } else {
                    // 列名
                    pppy!(format!(
                        "解析得到的表达式,列名{:?}",
                        ast::Expression::Field(ident.clone())
                    ));
                    ast::Expression::Field(ident)
                }
            }
            // Token行业的规定，数字只能是数字，不能混杂
            Token::Number(n) => {
                if n.chars().all(|c| c.is_ascii_digit()) {
                    // 整数
                    let res = ast::Consts::Integer(n.parse()?).into();
                    pppy!(format!("解析得到的表达式,整数{:?}", res));
                    res
                } else {
                    // 浮点数
                    let res = ast::Consts::Float(n.parse()?).into();
                    pppy!(format!("解析得到的表达式,附点数{:?}", res));
                    res
                }
            }
            Token::OpenParen => {
                let expr = self.compute_math_operator(1)?;
                self.next_expect(Token::CloseParen)?;
                let res = expr;
                pppy!(format!("解析得到的表达式,附点数{:?}", res));
                res
            }
            Token::String(s) => {
                let res = ast::Consts::String(s).into();
                pppy!(format!("解析得到的表达式,字符串{:?}", res));

                res
            }
            Token::Keyword(Keyword::True) => {
                let res = ast::Consts::Boolean(true).into();
                pppy!(format!("解析得到的表达式,True{:?}", res));

                res
            }
            Token::Keyword(Keyword::False) => {
                let res = ast::Consts::Boolean(false).into();
                pppy!(format!("解析得到的表达式,False{:?}", res));

                res
            }
            Token::Keyword(Keyword::Null) => {
                let res = ast::Consts::Null.into();
                pppy!(format!("解析得到的表达式,Null{:?}", res));

                res
            }
            t => {
                return Err(Error::Parse(format!(
                    "[Parser] Unexpected expression token {}",
                    t
                )))
            }
        })
    }

    // 计算数学表达式
    // 5 + 2 + 1
    // 5 + 2 * 1
    fn compute_math_operator(&mut self, min_prec: i32) -> Result<Expression> {
        pppb!("开始进行数学表达式运算解析,这里是Precedence Climbing算法......");
        let mut left = self.parse_expression()?;
        loop {
            // 当前 Token
            //查看下一个Token，因为可能是只有一个数字的情况
            let token = match self.peek()? {
                Some(t) => t,
                None => break,
            };
            // 1.如果不是运算符直接退出，因为是单个的5类似的
            // 2.如果运算符的优先级小于传入的优先级，那么可以直接退出，
            // 因为如果是+ *这种模式的话，*的优先级和+相等，不会退出，会一直计算，但是如果是+ +的话，那就是+(2)=1<min_prec=2直接退出，说明前面的运算部分可以直接计算
            if !token.is_operator() || token.precedence() < min_prec {
                break;
            }
            // 这里很重要的优先级递增，这是 Precedence Climbing 的基本实现
            let next_prec = token.precedence() + 1;
            self.next()?;

            // 递归计算右边的表达式
            let right = self.compute_math_operator(next_prec)?;
            // 计算左右两边的值
            left = token.compute_expr(left, right)?;
        }
        Ok(left)
    }

    fn peek(&mut self) -> Result<Option<Token>> {
        self.lexer.peek().cloned().transpose()
    }

    // 获取下一个 Token（词法单元）
    fn next(&mut self) -> Result<Token> {
        // self.lexer
        //     .next()
        //     .unwrap_or_else(|| Err(Error::Parse(format!("[Parser] Unexpected end of input"))))
        // 这是一个闭包，用于执行闭包中的代码并返回结果。
        match self.lexer.next() {
            Some(token) => token,
            None => Err(Error::Parse(format!("[Parser] Unexpected end of input"))),
        }
    }

    // 得到下一个Token
    fn next_ident(&mut self) -> Result<String> {
        pppb!("尝试读取下一个Token......");
        match self.next()? {
            Token::Ident(ident) =>{
              let res=  Ok(ident);
              pppy!(format!("下一个Token是{:?}",res));
              res
            } 
            token => Err(Error::Parse(format!(
                "[Parser] Expected ident, got token {}",
                token
            ))),
        }
    }
    // 获取下一个 Token 类型的元素，并检查该元素是否与期望的 Token 相匹配
    fn next_expect(&mut self, expect: Token) -> Result<()> {
        pppb!("尝试匹配下一个Token与当前Token......");
        let token = self.next()?;
        if token != expect {
            return Err(Error::Parse(format!(
                "[Parser] Expected token {}, got {}",
                expect, token
            )));
        }
        Ok(())
    }

    // 如果满足条件，则跳转到下一个 Token
    fn next_if<F: Fn(&Token) -> bool>(&mut self, predicate: F) -> Option<Token> {
        self.peek().unwrap_or(None).filter(|t| predicate(t))?;
        self.next().ok()
    }

    // 如果下一个 Token 是关键字，则跳转
    fn next_if_keyword(&mut self) -> Option<Token> {
        self.next_if(|t| matches!(t, Token::Keyword(_)))
    }

    fn next_if_token(&mut self, token: Token) -> Option<Token> {
        self.next_if(|t| t == &token)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        error::Result,
        pppr,
        sql::parser::ast::{self, Consts, Expression, OrderDirection},
    };

    use super::Parser;

    #[test]
    fn test_parser_select1() -> Result<()> {
        // 错误的词法会报错！就是在Parser阶段出现的！
        // let sql = "   sel ect   *  from t    ;";
        let sql = "   select   *  from t;";
        // let sql = "   select   col1 as a,col2  from t    ;";
        // let sql = "   select   col1 as a,col2 as b  from t1  join t2  on t1.a=t2.a ;";
        let stmt = Parser::new(sql).parse()?;
        Ok(())
    }
    #[test]
    fn test_parser_create_table() -> Result<()> {
        let sql1 = "
            create table tbl1 (
                a int default 100,
                b float not null,
                c varchar null,
                d bool default true
            );
        ";
        let stmt1 = Parser::new(sql1).parse()?;

        let sql2 = "
        create            table tbl1 (
            a int default     100,
            b float not null     ,
            c varchar      null,
            d       bool default        true
        );
        ";
        let stmt2 = Parser::new(sql2).parse()?;
        assert_eq!(stmt1, stmt2);

        let sql3 = "
            create            table tbl1 (
            a int default     100,
            b float not null     ,
            c varchar      null,
            d       bool default        true
        )
        ";

        let stmt3 = Parser::new(sql3).parse();
        assert!(stmt3.is_err());
        Ok(())
    }

    #[test]
    fn test_parser_insert() -> Result<()> {
        let sql1 = "insert into tbl1 values (1, 2, 3, 'a', true);";
        let stmt1 = Parser::new(sql1).parse()?;
        assert_eq!(
            stmt1,
            ast::Statement::Insert {
                table_name: "tbl1".to_string(),
                columns: None,
                values: vec![vec![
                    ast::Consts::Integer(1).into(),
                    ast::Consts::Integer(2).into(),
                    ast::Consts::Integer(3).into(),
                    ast::Consts::String("a".to_string()).into(),
                    ast::Consts::Boolean(true).into(),
                ]],
            }
        );

        let sql2 = "insert into tbl2 (c1, c2, c3) values (3, 'a', true),(4, 'b', false);";
        let stmt2 = Parser::new(sql2).parse()?;
        assert_eq!(
            stmt2,
            ast::Statement::Insert {
                table_name: "tbl2".to_string(),
                columns: Some(vec!["c1".to_string(), "c2".to_string(), "c3".to_string()]),
                values: vec![
                    vec![
                        ast::Consts::Integer(3).into(),
                        ast::Consts::String("a".to_string()).into(),
                        ast::Consts::Boolean(true).into(),
                    ],
                    vec![
                        ast::Consts::Integer(4).into(),
                        ast::Consts::String("b".to_string()).into(),
                        ast::Consts::Boolean(false).into(),
                    ],
                ],
            }
        );

        Ok(())
    }

    #[test]
    fn test_parser_select() -> Result<()> {
        let sql = "select * from tbl1;";
        pppr!("初始的sql文本:", sql);
        let stmt = Parser::new(sql).parse()?;
        pppr!("抽象语法树:", stmt);
        assert_eq!(
            stmt,
            ast::Statement::Select {
                select: vec![],
                from: ast::FromItem::Table {
                    name: "tbl1".into()
                },
                where_clause: Some(ast::Expression::Operation(ast::Operation::Equal(
                    Box::new(ast::Expression::Field("a".into())),
                    Box::new(ast::Expression::Consts(Consts::Integer(100)))
                ))),
                group_by: None,
                having: None,
                order_by: vec![],
                limit: Some(Expression::Consts(Consts::Integer(10))),
                offset: Some(Expression::Consts(Consts::Integer(20))),
            }
        );

        let sql = "select * from tbl1 order by a, b asc, c desc;";
        let stmt = Parser::new(sql).parse()?;
        assert_eq!(
            stmt,
            ast::Statement::Select {
                select: vec![],
                from: ast::FromItem::Table {
                    name: "tbl1".into()
                },
                where_clause: None,
                group_by: None,
                order_by: vec![
                    ("a".to_string(), OrderDirection::Asc),
                    ("b".to_string(), OrderDirection::Asc),
                    ("c".to_string(), OrderDirection::Desc),
                ],
                having: None,
                limit: None,
                offset: None,
            }
        );

        let sql = "select a as col1, b as col2, c from tbl1 order by a, b asc, c desc;";
        let stmt = Parser::new(sql).parse()?;
        assert_eq!(
            stmt,
            ast::Statement::Select {
                select: vec![
                    (Expression::Field("a".into()), Some("col1".into())),
                    (Expression::Field("b".into()), Some("col2".into())),
                    (Expression::Field("c".into()), None),
                ],
                from: ast::FromItem::Table {
                    name: "tbl1".into()
                },
                where_clause: None,
                group_by: None,
                having: None,
                order_by: vec![
                    ("a".to_string(), OrderDirection::Asc),
                    ("b".to_string(), OrderDirection::Asc),
                    ("c".to_string(), OrderDirection::Desc),
                ],
                limit: None,
                offset: None,
            }
        );

        let sql = "select * from tbl1 cross join tbl2 cross join tbl3;";
        let stmt = Parser::new(sql).parse()?;
        assert_eq!(
            stmt,
            ast::Statement::Select {
                select: vec![],
                from: ast::FromItem::Join {
                    left: Box::new(ast::FromItem::Join {
                        left: Box::new(ast::FromItem::Table {
                            name: "tbl1".into()
                        }),
                        right: Box::new(ast::FromItem::Table {
                            name: "tbl2".into()
                        }),
                        join_type: ast::JoinType::Cross,
                        predicate: None
                    }),
                    right: Box::new(ast::FromItem::Table {
                        name: "tbl3".into()
                    }),
                    join_type: ast::JoinType::Cross,
                    predicate: None
                },
                where_clause: None,
                group_by: None,
                having: None,
                order_by: vec![],
                limit: None,
                offset: None,
            }
        );

        let sql = "select count(a), min(b), max(c) from tbl1 group by a having min = 10;";
        let stmt = Parser::new(sql).parse()?;
        assert_eq!(
            stmt,
            ast::Statement::Select {
                select: vec![
                    (ast::Expression::Function("count".into(), "a".into()), None),
                    (ast::Expression::Function("min".into(), "b".into()), None),
                    (ast::Expression::Function("max".into(), "c".into()), None),
                ],
                from: ast::FromItem::Table {
                    name: "tbl1".into()
                },
                where_clause: None,
                group_by: Some(ast::Expression::Field("a".into())),
                having: Some(ast::Expression::Operation(ast::Operation::Equal(
                    Box::new(ast::Expression::Field("min".into())),
                    Box::new(ast::Expression::Consts(Consts::Integer(10)))
                ))),
                order_by: vec![],
                limit: None,
                offset: None,
            }
        );

        Ok(())
    }

    #[test]
    fn test_parser_update() -> Result<()> {
        let sql = "update tabl set a = 1, b = 2.0 where c = 'a';";
        let stmt = Parser::new(sql).parse()?;
        assert_eq!(
            stmt,
            ast::Statement::Update {
                table_name: "tabl".into(),
                columns: vec![
                    ("a".into(), ast::Consts::Integer(1).into()),
                    ("b".into(), ast::Consts::Float(2.0).into()),
                ]
                .into_iter()
                .collect(),
                where_clause: Some(ast::Expression::Operation(ast::Operation::Equal(
                    Box::new(ast::Expression::Field("c".into())),
                    Box::new(ast::Expression::Consts(Consts::String("a".into())))
                ))),
            }
        );

        Ok(())
    }
}
