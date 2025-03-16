use std::{fmt::Display, iter::Peekable, result, str::Chars};

use crate::{error::{Error, Result}, pppg};

use super::ast::{Consts, Expression};

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    // 关键字
    Keyword(Keyword),
    // 其他类型的字符串Token，比如表名、列名、函数等
    Ident(String),
    // 字符串类型的数据
    String(String),
    // 数值类型，比如整数和浮点数
    Number(String),
    // 左括号 (
    OpenParen,
    // 右括号 )
    CloseParen,
    // 逗号 ,
    Comma,
    // 分号 ;
    Semicolon,
    // 星号 & 乘号 *
    Asterisk,
    // 加号 +
    Plus,
    // 减号 -
    Minus,
    // 斜杠 & 除号 /
    Slash,
    // 等号 =
    Equal,
    // 大于
    GreaterThan,
    // 小于
    LessThan,
}

impl Token {
    // 判断是不是运算符
    pub fn is_operator(&self) -> bool {
        match self {
            Token::Plus | Token::Minus | Token::Asterisk | Token::Slash => true,
            _ => false,
        }
    }

    // 获取运算符的优先级
    pub fn precedence(&self) -> i32 {
        match self {
            Token::Plus | Token::Minus => 1,
            Token::Asterisk | Token::Slash => 2,
            _ => 0,
        }
    }

    // 根据运算符进行计算
    pub fn compute_expr(&self, l: Expression, r: Expression) -> Result<Expression> {
        let val = match (l, r) {
            (Expression::Consts(c1), Expression::Consts(c2)) => match (c1, c2) {
                (super::ast::Consts::Integer(l), super::ast::Consts::Integer(r)) => {
                    self.compute(l as f64, r as f64)?
                }
                (super::ast::Consts::Integer(l), super::ast::Consts::Float(r)) => {
                    self.compute(l as f64, r)?
                }
                (super::ast::Consts::Float(l), super::ast::Consts::Integer(r)) => {
                    self.compute(l, r as f64)?
                }
                (super::ast::Consts::Float(l), super::ast::Consts::Float(r)) => {
                    self.compute(l, r)?
                }
                _ => return Err(Error::Parse("cannot compute the expresssion".into())),
            },
            _ => return Err(Error::Parse("cannot compute the expresssion".into())),
        };
        Ok(Expression::Consts(Consts::Float(val)))
    }

    fn compute(&self, l: f64, r: f64) -> Result<f64> {
        Ok(match self {
            Token::Asterisk => l * r,
            Token::Plus => l + r,
            Token::Minus => l - r,
            Token::Slash => l / r,
            _ => return Err(Error::Parse("cannot compute the expresssion".into())),
        })
    }
}

impl Display for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            //write_str 接收一个 match 表达式的结果作为参数。这个 match 表达式根据 Token 的不同变体返回相应的字符串表示：
            // 使得 Token 类型可以方便地转换为人类可读的字符串形式，便于调试和显示。也即将SQL命令Token化！
            Token::Keyword(keyword) => keyword.to_str(),
            Token::Ident(ident) => ident,
            Token::String(v) => v,
            Token::Number(n) => n,
            Token::OpenParen => "(",
            Token::CloseParen => ")",
            Token::Comma => ",",
            Token::Semicolon => ";",
            Token::Asterisk => "*",
            Token::Plus => "+",
            Token::Minus => "-",
            Token::Slash => "/",
            Token::Equal => "=",
            Token::GreaterThan => ">",
            Token::LessThan => "<",
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Keyword {
    Create,
    Table,
    Int,
    Integer,
    Boolean,
    Bool,
    String,
    Text,
    Varchar,
    Float,
    Double,
    Select,
    From,
    Insert,
    Into,
    Values,
    True,
    False,
    Default,
    Not,
    Null,
    Primary,
    Key,
    Update,
    Set,
    Where,
    Delete,
    Order,
    By,
    Asc,
    Desc,
    Limit,
    Offset,
    As,
    Cross,
    Join,
    Left,
    Right,
    On,
    Group,
    Having,
    // 事务相关的命令关键字
    Begin,
    Commit,
    Rollback,
    // 索引关键字
    Index,
    Explain,
    // 删除表
    Drop,
}

impl Keyword {
    pub fn from_str(ident: &str) -> Option<Self> {
        Some(match ident.to_uppercase().as_ref() {
            //忽略了大小写
            "CREATE" => Keyword::Create,
            "TABLE" => Keyword::Table,
            "INT" => Keyword::Int,
            "INTEGER" => Keyword::Integer,
            "BOOLEAN" => Keyword::Boolean,
            "BOOL" => Keyword::Bool,
            "STRING" => Keyword::String,
            "TEXT" => Keyword::Text,
            "VARCHAR" => Keyword::Varchar,
            "FLOAT" => Keyword::Float,
            "DOUBLE" => Keyword::Double,
            "SELECT" => Keyword::Select,
            "FROM" => Keyword::From,
            "INSERT" => Keyword::Insert,
            "INTO" => Keyword::Into,
            "VALUES" => Keyword::Values,
            "TRUE" => Keyword::True,
            "FALSE" => Keyword::False,
            "DEFAULT" => Keyword::Default,
            "NOT" => Keyword::Not,
            "NULL" => Keyword::Null,
            "PRIMARY" => Keyword::Primary,
            "KEY" => Keyword::Key,
            "UPDATE" => Keyword::Update,
            "SET" => Keyword::Set,
            "WHERE" => Keyword::Where,
            "DELETE" => Keyword::Delete,
            "ORDER" => Keyword::Order,
            "BY" => Keyword::By,
            "ASC" => Keyword::Asc,
            "DESC" => Keyword::Desc,
            "LIMIT" => Keyword::Limit,
            "OFFSET" => Keyword::Offset,
            "AS" => Keyword::As,
            "CROSS" => Keyword::Cross,
            "JOIN" => Keyword::Join,
            "LEFT" => Keyword::Left,
            "RIGHT" => Keyword::Right,
            "ON" => Keyword::On,
            "GROUP" => Keyword::Group,
            "HAVING" => Keyword::Having,
            "BEGIN" => Keyword::Begin,
            "COMMIT" => Keyword::Commit,
            "ROLLBACK" => Keyword::Rollback,
            "INDEX" => Keyword::Index,
            "EXPLAIN" => Keyword::Explain,
            // 删除表实现
            "DROP" => Keyword::Drop,
            _ => return None,
        })
    }

    pub fn to_str(&self) -> &str {
        match self {
            Keyword::Create => "CREATE",
            Keyword::Table => "TABLE",
            Keyword::Int => "INT",
            Keyword::Integer => "INTEGER",
            Keyword::Boolean => "BOOLEAN",
            Keyword::Bool => "BOOL",
            Keyword::String => "STRING",
            Keyword::Text => "TEXT",
            Keyword::Varchar => "VARCHAR",
            Keyword::Float => "FLOAT",
            Keyword::Double => "DOUBLE",
            Keyword::Select => "SELECT",
            Keyword::From => "FROM",
            Keyword::Insert => "INSERT",
            Keyword::Into => "INTO",
            Keyword::Values => "VALUES",
            Keyword::True => "TRUE",
            Keyword::False => "FALSE",
            Keyword::Default => "DEFAULT",
            Keyword::Not => "NOT",
            Keyword::Null => "NULL",
            Keyword::Primary => "PRIMARY", //主键关键字
            Keyword::Key => "KEY",
            Keyword::Update => "UPDATE", //补充对应的额关键字！
            Keyword::Set => "SET",
            Keyword::Where => "WHERE",
            Keyword::Delete => "DELETE",
            Keyword::Order => "ORDER",
            Keyword::By => "BY",
            Keyword::Asc => "ASC",
            Keyword::Desc => "DESC",
            Keyword::Limit => "LIMIT",
            Keyword::Offset => "OFFSET",
            Keyword::As => "AS",
            Keyword::Cross => "CROSS",
            Keyword::Join => "JOIN",
            Keyword::Left => "LEFT",
            Keyword::Right => "RIGHT",
            Keyword::On => "ON",
            Keyword::Group => "GROUP",
            Keyword::Having => "HAVING",
            Keyword::Begin => "BEGIN",
            Keyword::Commit => "COMMIT",
            Keyword::Rollback => "ROLLBACK",
            Keyword::Index => "INDEX",
            Keyword::Explain => "EXPLAIN",
            Keyword::Drop => "DROP",
        }
    }
}

// 将 Keyword 枚举实例转换为字符串并写入到格式化器 f 中。方便输出，这使得带Token的Keyword::Drop方便的转化为Drop
impl Display for Keyword {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.to_str())
    }
}

// 词法分析 Lexer 定义
// 目前支持的 SQL 语法
// see README.md
#[derive(Debug)]
pub struct Lexer<'a> {
    // 就是一个迭代器，用于遍历 SQL 文本中的字符
    iter: Peekable<Chars<'a>>,
}

// 自定义迭代器，返回 Token
impl<'a> Iterator for Lexer<'a> {
    type Item = Result<Token>; //迭代的基本单元——包含token的Item

    fn next(&mut self) -> Option<Self::Item> {
        match self.scan() {
            Ok(Some(token)) => Some(Ok(token)), //如果还有下一个字符，并且下一个字符是Token的话，就继续，否则就报错！
            Ok(None) => self
                .iter
                .peek()
                .map(|c| Err(Error::Parse(format!("[Lexer] Unexpeted character {}", c)))),
            Err(err) => Some(Err(err)),
        }
    }
}
// 实现 Lexer 结构体，无非就是一直探测下一个Token，并组织成对应数据处理形式
impl<'a> Lexer<'a> {
    pub fn new(sql_text: &'a str) -> Self {
        Self {
            iter: sql_text.chars().peekable(),
        }
    }

    // 消除空白字符
    // eg. selct *       from        t;
    // 当 next_while 因为遇到不满足条件的字符而停止时，erase_whitespace 函数也会停止操作，此时迭代器的指针会停留在第一个非空白字符的位置，后续的字符扫描操作可以从该位置开始。
    fn erase_whitespace(&mut self) {
        self.next_while(|c| c.is_whitespace());
        //闭包本身就是一个迭代循环！这里非"" 会直接返回字符，但是是" "的话会全部过滤掉！
        // 实现数据洗净功能
    }

    // 如果满足条件，则跳转到下一个字符，并返回该字符
    fn next_if<F: Fn(char) -> bool>(&mut self, predicate: F) -> Option<char> {
        self.iter.peek().filter(|&c| predicate(*c))?;//不满足直接返回
        self.iter.next()//满足的话就跳转到下一个字符
    }

    // 判断当前字符是否满足条件，如果是的话就跳转到下一个字符
    // 这种设计使得 next_while 更加通用。它可以用于收集字符（当我们需要这些字符时），也可以用于跳过字符（当我们忽略返回值时）。
    //本质功能——使得空格块与数据分离！
    fn next_while<F: Fn(char) -> bool>(&mut self, predicate: F) -> Option<String> {
        let mut value = String::new(); //装载在一个字符串中
        while let Some(c) = self.next_if(&predicate) {//满足就一直添加，不满足直接返回None
            //满足的话就加入并返回当前值
            value.push(c);
        }
        let res = Some(value).filter(|v| !v.is_empty());
        // pppg!(format!("本次检测的空格为{:?}长度为{:?}", res.clone(),res.clone().unwrap().len()));
        if let Some(ref s) = res {
            pppg!(format!("本次消除的空格为{:?}长度为{:?}", s, s.len()));
        } else {
            pppg!(format!("空格消除完毕"));
        }
        res
    }

    // 只有是 Token 类型，才跳转到下一个，并返回 Token
    fn next_if_token<F: Fn(char) -> Option<Token>>(&mut self, predicate: F) -> Option<Token> {
        let token = self.iter.peek().and_then(|c| predicate(*c))?;
        self.iter.next();
        Some(token)
    }

    // 扫描拿到下一个 Token
    // 迭代器里面不断迭代词元！
    fn scan(&mut self) -> Result<Option<Token>> {
        // 确保指针移动到下一个非空白字符
        self.erase_whitespace();
        // 根据第一个字符判断
        match self.iter.peek() {
            
            Some('\'') => self.scan_string(), // 如果第一个字符是',说明是字符串，则扫描字符串
            Some(c) if c.is_ascii_digit() => Ok(self.scan_number()), // 扫描数字
            Some(c) if c.is_alphabetic() => Ok(self.scan_ident()), // 扫描 SQL 类型，可能是关键字，也有可能是表名、列名等
            Some(_) => Ok(self.scan_symbol()),                     // 扫描符号+-*、=（）等
            None => Ok(None),
        }
    }

    // 扫描字符串
    fn scan_string(&mut self) -> Result<Option<Token>> {
        // 判断是否是单引号开头
        if self.next_if(|c| c == '\'').is_none() {
            return Ok(None);
        }

        let mut val = String::new();
        loop {
            match self.iter.next() {
                Some('\'') => break,
                Some(c) => val.push(c),
                None => return Err(Error::Parse(format!("[Lexer] Unexpected end of string"))),
            }
        }

        Ok(Some(Token::String(val)))
    }

    // 扫描数字
    fn scan_number(&mut self) -> Option<Token> {
        // 先扫描一部分
        let mut num = self.next_while(|c| c.is_ascii_digit())?;
        // 如果中间有小数点，说明是浮点数
        if let Some(sep) = self.next_if(|c| c == '.') {
            num.push(sep);
            // 扫描小数点之后的部分
            while let Some(c) = self.next_if(|c| c.is_ascii_digit()) {
                num.push(c);
            }
        }

        Some(Token::Number(num))
    }

    // 扫描 Ident 类型，例如表名、列名等，也有可能是SQL词元
    fn scan_ident(&mut self) -> Option<Token> {
        // 确保了标识符总是以字母开头，否则就？返回错误。这是大多数编程语言和 SQL 中标识符的常见规则。
        let mut value = self.next_if(|c| c.is_alphabetic())?.to_string();
        // 接着扫描剩下的部分。直到遇到不是字母、数字或下划线的字符为止，从而构建完整的标识符。
        while let Some(c) = self.next_if(|c| c.is_alphanumeric() || c == '_') {
            value.push(c);
        }
        // 根据输入的 value 字符串尝试将其转换为 Keyword 枚举类型。如果转换成功，就将其包装成 Token::Keyword 类型；如果转换失败，就将 value 转换为小写后包装成 Token::Ident 类型，最后将结果用 Some 包裹。
        // 一举两得，因为有可能是SQL词元，也有可能是表名、列名等！这里直接全部判断解决了！
        Some(Keyword::from_str(&value).map_or(Token::Ident(value.to_lowercase()), Token::Keyword))
    }

    // 扫描符号
    fn scan_symbol(&mut self) -> Option<Token> {
        self.next_if_token(|c| match c {
            //只有是Token的元词，才会立即返回！
            '*' => Some(Token::Asterisk),
            '(' => Some(Token::OpenParen),
            ')' => Some(Token::CloseParen),
            ',' => Some(Token::Comma),
            ';' => Some(Token::Semicolon),
            '+' => Some(Token::Plus),
            '-' => Some(Token::Minus),
            '/' => Some(Token::Slash),
            '=' => Some(Token::Equal),
            '>' => Some(Token::GreaterThan),
            '<' => Some(Token::LessThan),
            _ => None,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{result, vec};

    use super::Lexer;
    use crate::{
        error::Result,
        pppb, pppr_no_ln, pppy,
        sql::parser::lexer::{Keyword, Token},
    };
 #[test]
    fn test_eliminate_white_space() -> Result<()> {
        let sql = "   sel ect   *  from t    ;";
        let mut lexer = Lexer::new(sql);
        let mut res = lexer.peekable()
        .collect::<Result<Vec<_>>>()?;
        pppy!(res);
        Ok(())
    }
}
