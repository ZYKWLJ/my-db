## 支持的 SQL 语法

### 1. Create/Drop Table
create table:
```sql
CREATE TABLE table_name (
    [ column_name data_type [index] [ column_constraint [...] ] ]
    [, ... ]
   );

   where data_type is:
    - BOOLEAN(BOOL): true | false
    - FLOAT(DOUBLE)
    - INTEGER(INT)
    - STRING(TEXT, VARCHAR)

   where column_constraint is:
   [ NOT NULL | NULL | DEFAULT expr ]
```
drop table:
```sql
DROP TABLE table_name;
```

### 2. Insert Into
```sql
INSERT INTO table_name
[ ( column_name [, ...] ) ]
values ( expr [, ...] );
```

### 3. Select
```sql
SELECT [* | col_name | function [ [ AS ] output_name [, ...] ]]
FROM from_item
[GROUP BY col_name]
[ORDER BY col_name [asc | desc] [, ...]]
[LIMIT count]
[OFFSET count]
```

where `function` is:
* count(col_name)
* min(col_name)
* max(col_name)
* sum(col_name)
* avg(col_name)

where `from_item` is:
* table_name
* table_name `join_type` table_name [`ON` predicate]

where `join_type` is:
* cross join
* join
* left join
* right join

where `on predicate` is:
* column_name = column_name

### 4. Update
```sql
UPDATE table_name
SET column_name = expr [, ...]
[WHERE condition];
```
where condition is: `column_name = expr`

### 5. Delete
```sql
DELETE FROM table_name
[WHERE condition];
```
where condition is: `column_name = expr`

### 5. Show Table
```sql
SHOW TABLES;
```

```sql
SHOW TABLE `table_name`;
```

### 6. Transaction

```
BEGIN;

COMMIT;

ROLLBACK;
```

## 7. Explain
```
explain sql;
```

## demo
```SQL
CREATE TABLE employees (
    employee_id INT PRIMARY KEY,
    employee_name VARCHAR(100),
    department_id INT
);

CREATE TABLE departments (
    department_id INT PRIMARY KEY,
    department_name VARCHAR(100)
);

CREATE TABLE projects (
    project_id INT PRIMARY KEY,
    project_name VARCHAR(100),
    department_id INT
);

INSERT INTO employees (employee_id, employee_name, department_id) VALUES
(1, 'Alice', 1),
(2, 'Bob', 1),
(3, 'Charlie', 2),
(4, 'David', NULL);

INSERT INTO departments (department_id, department_name) VALUES
(1, 'HR'),
(2, 'IT'),
(3, 'PR');

INSERT INTO projects (project_id, project_name, department_id) VALUES
(101, 'Project X', 1),
(102, 'Project Y', 2),
(103, 'Project Z', NULL);
```


## 笛卡尔积结构示例
### cross join
基础语法
```SQL
SELECT [* | col_name [ [ AS ] output_name [, ...] ]]
FROM from_item
[ORDER BY col_name [asc | desc] [, ...]]
[LIMIT count]
[OFFSET count]
```

测试用例
```SQL
select employee_name, department_name from employees cross join departments;
```

### inner join
```SQL
SELECT e.employee_name, d.department_name
FROM employees e
JOIN departments d ON e.department_id = d.department_id;
```

### left join
```SQL
SELECT e.employee_name, d.department_name
FROM employees e
LEFT JOIN departments d ON e.department_id = d.department_id;
```

### right join
```SQL
SELECT e.employee_name, d.department_name
FROM employees e
RIGHT JOIN departments d ON e.department_id = d.department_id;
```

### 多表join
```SQL
SELECT e.employee_name, d.department_name, p.project_name, e.department_id as '相同的部门号为'
FROM employees e
JOIN departments d ON e.department_id = d.department_id
JOIN projects p on p.department_id = d.department_id;
```

## 聚集函数
```SQL
SELECT [ * | col_name | function [ [ AS ] output_name [, ...] ]]
FROM from_item
[ORDER BY col_name [asc | desc] [, ...]]
[LIMIT count]
[OFFSET count]

where `function` is:
* count(col_name)
* min(col_name)
* max(col_name)
* sum(col_name)
* avg(col_name)
```

## Group by
GROUP BY 是 SQL 中用于对查询结果按照一个或多个列进行分组的子句，它常与`聚合函数`（如 `COUNT、SUM、AVG、MAX、MIN` 等）一起使用，来对每个分组进行相应的统计计算。GROUP BY 是 SQL 中用于对查询结果按照一个或多个列进行分组的子句，它常与聚合函数（如 COUNT、SUM、AVG、MAX、MIN 等）一起使用，来对每个分组进行相应的统计计算。

`注意事项`：当你使用 GROUP BY 对数据进行分组的时候，如果在 SELECT 子句中选择了非聚合函数作用的列（这里的 employee_name 就没用聚合函数处理），那么这个列必须同时出现在 GROUP BY 子句中。

### 使用实力
```SQL
select count(employee_id),department_id from employees group by department_id;
```

### group by语法定义
```SQL
SELECT [* | col_name | function [ [ AS ] output_name [, ...] ]]
FROM from_item
[GROUP BY col_name]
[ORDER BY col_name [asc | desc] [, ...]]
[LIMIT count]
[OFFSET count]
```

### having 语句
作用：
HAVING 子句主要用于在分组查询（通过 GROUP BY 对数据进行分组）的基础上，进一步筛选分组后的结果集。
也就是说，它允许你基于分组后的聚合函数结果（比如 SUM、COUNT、AVG 等）或者其他分组相关的条件来决定
哪些分组应该被包含在最终的查询结果中，相当于给分组数据添加了一个 **二次筛选**的机制。
现在完整的查询~

```SQL
SELECT [* | col_name | function [ [ AS ] output_name [, ...] ]]
FROM from_item
[WHERE expr]
[GROUP BY col_name]
[HAVING expr]
[ORDER BY col_name [asc | desc] [, ...]]
[LIMIT count]
[OFFSET count]
```

展示单个表信息
```SQL
SHOW TABLES;
```

展示所有表名
```SQL
SHOW TABLE `table_name`;
```

## 6.索引支持
### 存储结构
例如：假如我们的表 t 有 a、b、c 三个字段，其中 a 是主键，那么在插入数据进行存储的时候，实际的存储结构如下：

### 存储格式key
```SQL
#[derive(Debug, Serialize, Deserialize)]
enum Key {
    Table(String),
    Row(String, Value),
    Index(String, String, Value),
}
```

```SQL
insert into t values (1, 2, 'a');# 其中1-t-1的含义为`key枚举号+表名+列`
1-t-1 -> [1, 2, 'a']

insert into t values (2, 2, 'b');
1-t-2 -> [2, 2, 'b']

insert into t values (3, 3, 'c');
1-t-3 -> [3, 3, 'c']

insert into t values (4, 3, 'd');
1-t-4 -> [4, 3, 'd']
```
### 为b列添加索引

如果我们在 b 列建立了索引，那么可以`将 b 列对应的数据 id` 都`单独`存储起来，这样可以在扫描的时候`一次性加载`出来，并且通过数据 id 快速定位到对应的记录。

大致的存储结构如下：
可以看出，这里利用`b列`来查找`相应的数据列`,其中2-t-b-2的含义为`key枚举号+表名+索引+索引数据`
```SQL
2-t-b-2 -> 1-t-1
2-t-b-2 -> 1-t-2

2-t-b-3 -> 1-t-3
2-t-b-3 -> 1-t-4
```
由于我们的 KV 结构不支持重复的 key，所以这里使用一个 Set 去将所有的数据都存储到一个 key 里面。
有了这样一个索引结构之后，我们再执行 select * from t where b = 2 的时候，只需要加载 `2-t-b-2` 对应的`数据`，
然后直接根据 id 找到对应的数据行即可。

### 索引删除

除了在插入数据行维护索引之外，还需要在数据删除和更新的时候去更新索引内容。
如果是删除数据，那么索引列对应的数据也需要删除。

### 索引更新

行更新分为了两种情况，一种是主键被更新了，这类似于数据重新被删除然后添加。
另一种是索引列被更新了，这时候需要将旧的索引对应的 id 删除，并增加一条数据到新的索引列中。

```SQL
例如 SQL 是：update t set b = 3 where a = 2;

2-t-b-2 -> 1-t-1
2-t-b-2 -> 1-t-2

2-t-b-3 -> 1-t-3
2-t-b-3 -> 1-t-4

    |
    |
    |
    v

2-t-b-2 -> 1-t-1

2-t-b-3 -> 1-t-2 --注意这条数据被更新！
2-t-b-3 -> 1-t-3
2-t-b-3 -> 1-t-4
```


join哈希优化
```SQL
a1    b1 
1     2
2     3
3     5
4     6
5     7
```

```SQL
a1    b1 
1     2 -> 完整行数据
2     3 -> 完整行数据
3     5 -> 完整行数据
4     6 -> 完整行数据
5     7 -> 完整行数据
```

drop删除表实现
```SQL
DROP TABLE table_name;
```

### 优先级规定
```SQL
5 + 2 * 3 + 4

|-----------|   : prec 1
    |---|       : prec 2
```