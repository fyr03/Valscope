# 子集 Oracle技术文档


---

## 1. 水平切分 Oracle（Row-Cut Oracle）

### 1.1 核心不变式

Row-Cut Oracle 在同一张物理表的两个**时序状态**之间建立**数据子集关系**：状态 $S_1$ 是状态 $S_2$ 的真子多重集，即 $S_1 \subseteq S_2$（$S_2$ 由 $S_1$ 追加插入行得到）。由于两个状态具有完全相同的 schema 和辅助表，任何**行保留查询** $Q$ 必须满足以下单调性性质：

$$
\text{COUNT}(Q,\, S_1) \;\leq\; \text{COUNT}(Q,\, S_2)
$$
$$
\text{MAX}(c,\, Q,\, S_1) \;\leq\; \text{MAX}(c,\, Q,\, S_2) \qquad \forall\ \text{数值列}\ c
$$
$$
\text{MIN}(c,\, Q,\, S_1) \;\geq\; \text{MIN}(c,\, Q,\, S_2) \qquad \forall\ \text{数值列}\ c
$$
$$
\text{row\_digests}(Q,\, S_1) \;\subseteq\; \text{row\_digests}(Q,\, S_2)
$$

对任意查询 $Q$ 违反上述任一性质，即视为被测数据库系统存在逻辑 Bug。

---

### 1.2 变换规则（Transformation Rules）

#### 1.2.1 表初始化与索引预建

Oracle 首先调用 valscope 框架的 `create_sample_tables()` 获取一组虚拟 schema 对象（`VsTable`）。其中一张表被指定为**主表**（将从 $S_1$ 扩张到 $S_2$ 的表），其余为**辅助表**（全程内容固定不变）。所有表以 UUID 后缀命名（如 `subset_main_<uid>`、`subset_ref_<uid>_orders`），保证多轮测试间无状态冲突。

数据插入前，Oracle 在主表的非主键列中选定一列作为**谓词列**（predicate column），优先选取 `INT`、`VARCHAR`、`FLOAT`、`DOUBLE`、`DECIMAL` 类型的列，并为其创建二级索引：

```sql
CREATE INDEX i_s3_<uid> ON <main_table> (`<pred_col>`);
```

此外，以约 40% 的概率在另外两个类型兼容的列上创建**复合索引**。这些索引使优化器在 $S_1$ 阶段倾向于选择索引扫描路径，而在 $S_2$ 阶段（数据量大幅增长、分布偏移后）可能切换为全表扫描路径，从而制造**查询计划转换**（plan transition）——这是触发隐性优化器 Bug 的关键机制。

#### 1.2.2 偏斜分布配置（Skew Profile）

为最大化 $S_1 \to S_2$ 之间发生**查询计划切换**的概率，每轮测试均构建一个 `SkewProfile`。该配置为每一列定义一组"热点值"（hot values），使数据分布高度集中，模拟真实场景中的数据倾斜：

- **各列热点值**：`INT` 列取随机基值附近的 3 个连续整数；`VARCHAR` 列取一个 token 字符串及两个变体（如 `hv_NNNN`、`hv_NNNN_a`、`hv_NNNN_b`）；`FLOAT`/`DOUBLE`/`DECIMAL` 列取 3 个相邻浮点数。
- **谓词列热点值**：分为 `primary_hot`、`secondary_hot`、`tertiary_hot` 三档，分别用于基线阶段的不同插入层，确保生成的 WHERE 谓词在 $S_1$ 上有非空结果。
- **扩张热点值**（expansion hot value）：一个刻意位于基线热点范围之外的值（如 `max(existing) + 20`），在 $S_1 \to S_2$ 的扩张阶段被大量插入，使谓词列的值域分布发生显著漂移。`ANALYZE TABLE` 执行后，优化器感知到新的选择率估计，从而触发计划切换。

#### 1.2.3 S1 数据插入（三层策略）

$S_1$ 由主表上的三层插入序列构建，各层目的不同：

| 层次 | 行数 | 描述 |
|---|---|---|
| **热点种子行**（hot seed rows） | `BASELINE_HOT_ROWS` = 2 | 谓词列强制取 `primary_hot`，其余列以 50% 概率取热点值，保证谓词查询命中 |
| **偏斜随机行**（skewed random rows） | `BASELINE_RANDOM_ROWS` + random(0–3) ≈ 4–7 | 每列以 0.35 的概率取热点值，否则均匀随机生成，覆盖热点邻域 |
| **噪声行**（noise rows） | `BASELINE_NOISE_ROWS` = 4 | 随机选一个非 PK 列填入边界值，其余列设为 `NULL`，覆盖类型边界场景 |

各类型使用的边界值如下：
- `INT`：`{0, 1, -1, 2147483647, -2147483648, NULL}`
- `VARCHAR`：`{'', 'NULL', '0', '%', '_', NULL}`
- `FLOAT`/`DOUBLE`：`{0, 0.0, -0.0, 1.0, -1.0, 3.4028235E38, -3.4028235E38, NULL}`
- `DATE`/`DATETIME`/`TIMESTAMP`：`{'0000-00-00', '1000-01-01', '9999-12-31', '2023-02-29', ''}` 等
- `TIME`：`{'00:00:00', '23:59:59', '25:61:61', '', NULL}`
- `YEAR`：`{1901, 1970, 2038, 2155, NULL}`

辅助表使用 valscope 的 `generate_random_sql.py` 生成并固定，全程不再变动。所有插入使用 `INSERT IGNORE` 以静默跳过约束冲突（主键在 `[1, 10,000,000]` 范围内随机生成，存在小概率碰撞）。

#### 1.2.4 S2 扩张插入与 ANALYZE TABLE

在 $S_1$ 的所有基线查询快照收集完成后，Oracle 在**单个显式事务**内向主表追加大批行，完成向 $S_2$ 的扩张：

```sql
START TRANSACTION;
-- 插入 SKEWED_EXPANSION_ROWS + 64 × random(0..8) 行
-- SKEWED_EXPANSION_ROWS = 500（基础常量）
COMMIT;
```

扩张阶段每行的生成参数为 `hotspot_prob = 0.92`，即 92% 的值取自热点池——但谓词列使用的是 `expansion_hot`（基线热点范围之外的值）。这使谓词列的统计分布从原来集中于 `primary_hot` 附近，转移到 `expansion_hot` 附近，从而在 `ANALYZE TABLE` 后令优化器重新估算选择率。

事务提交后立即执行：

```sql
ANALYZE TABLE <main_table>;
```

这强制优化器重建索引基数和直方图统计信息，使 $S_2$ 上的查询计划很可能不同于 $S_1$（例如从索引范围扫描切换到全表扫描）。Oracle 会显式判断 EXPLAIN 计划是否发生变化：**计划未变化的查询**以 85% 的概率被跳过（`UNCHANGED_PLAN_VERIFY_PROB = 0.15`），将验证资源集中于计划切换场景。

#### 1.2.5 清理机制（无 Rollback）

Row-Cut Oracle **不使用 ROLLBACK** 进行状态管理。$S_1$ 和 $S_2$ 是同一张表的前后两个已提交状态，Oracle 在扩张事务提交前捕获 $S_1$ 的查询快照，在提交后重新执行同一查询获取 $S_2$ 的结果。由于扩张操作严格为追加（`INSERT IGNORE`），单调性不变式天然成立。

每轮测试结束后（无论是否发现 Bug），本轮创建的所有表均被 DROP：

```sql
DROP TABLE IF EXISTS <main_table>, <aux_table_1>, ..., <aux_table_n>;
```

这确保轮次间完全隔离，数据库始终以干净状态进入下一轮。

---

### 1.3 近似 Schema 观测（Approximate Schema）

Oracle 在查询生成时无需访问 `INFORMATION_SCHEMA`，而是从 valscope 的 `VsTable` 对象派生**近似 Schema**，每列映射为一个 `ColDef` 数据类：

```python
@dataclass
class ColDef:
    name:          str
    data_type:     str       # 规范化类型：'INT' | 'VARCHAR' | 'FLOAT' | 'DOUBLE'
                             #              | 'DECIMAL' | 'DATE' | 'DATETIME'
                             #              | 'TIMESTAMP' | 'TIME' | 'YEAR' | 'OPAQUE'
    is_primary_key: bool
    is_nullable:   bool
    varchar_len:   int       # VARCHAR 声明长度
    is_indexed:    bool      # 索引创建后更新
    declared_type: str       # 原始 DDL 类型字符串（如 "ENUM('a','b','c')"）
```

`declared_type` 字段保留完整的原始声明（含 ENUM 成员列表、DECIMAL 精度/标度），供查询生成器构造 CAST 表达式和范围谓词时使用。`is_indexed` 标志在 `_ensure_indexes()` 执行完成后更新，使查询生成器能够向索引列偏置 JOIN 条件和 WHERE 谓词，提升索引范围扫描计划出现的概率。

类型规范化按族归并：
- `TINYINT`、`SMALLINT`、`MEDIUMINT`、`INT`、`BIGINT` → INT 族
- `FLOAT`、`DOUBLE`、`DECIMAL` → 数值族
- `VARCHAR`、`TEXT`、`LONGTEXT`、`CHAR`、`TINYTEXT`、`MEDIUMTEXT`、`ENUM`、`SET` → 字符串族
- `DATE`、`DATETIME`、`TIMESTAMP`、`TIME`、`YEAR` → 时态族
- 其余类型 → `OPAQUE`（排除于 JOIN 条件和数值谓词之外）

---

### 1.4 查询生成

#### 1.4.1 生成器架构

`SubsetQueryGenerator` 接收近似 Schema（`(table_name, [ColDef])` 列表）和 `SkewProfile` 的热点值映射。它通过加权随机选择确定**查询形态**（query shape），再按该形态构造语法合法的 SQL 字符串。

**关键设计约束**：生成的查询必须是**行保留查询**（row-preserving query）——即 $Q$ 在 $S_1$ 上返回的每一行，在 $S_2$ 上也必须出现，以保证单调性不变式成立。这排除了 `LIMIT`、`GROUP BY ... HAVING`、`NOT IN (subquery)`、`EXCEPT`、`INTERSECT` 和反连接（anti-join）等构型。

生成完成后由 `_validate_monotone_sql()` 进行后验证，通过基于正则的黑名单扫描拒绝非单调构型。每个查询槽最多重试 `MAX_GENERATE_RETRIES = 16` 次。

### 1.4.2 支持的查询形态与 BNF

对应实现入口：

- [subset_query_gen.py](/oracle/subset_query_gen.py)

#### 记号

- 终结符用引号表示。
- 可选项用 `[ ... ]`。
- 重复项用 `{ ... }`。
- 选择项用 `|`。
- 本文档展开到语句结构层。

```bnf
<query> ::= <single_table>
          | <inner_join_2>
          | <inner_join_3>
          | <self_join>
          | <cross_join_filtered>
          | <cte_wrapper>
          | <implicit_conv_join>
          | <derived_table>
          | <in_subquery>
          | <exists_subquery>
          | <nested_derived>
          | <union_all>

<single_table> ::= "SELECT" [ "DISTINCT" ] <select_list>
                   "FROM" <table_name> <alias>
                   [ <where> ]
                   [ <order_by> ]

<inner_join_2> ::= "SELECT" [ "DISTINCT" ] <select_list>
                   "FROM" <table_name> <alias>
                   "INNER JOIN" <table_name> <alias> "ON" <join_cond>
                   [ <where> ]
                   [ <order_by> ]

<inner_join_3> ::= "SELECT" [ "DISTINCT" ] <select_list>
                   "FROM" <table_name> <alias>
                   "INNER JOIN" <table_name> <alias> "ON" <join_cond>
                   "INNER JOIN" <table_name> <alias> "ON" <join_cond>
                   [ <where> ]
                   [ <order_by> ]

<self_join> ::= "SELECT" <select_list>
                "FROM" <table_name> <alias>
                "INNER JOIN" <table_name> <alias> "ON" <equi_join_cond>
                [ <where> ]
                [ <order_by> ]

<cross_join_filtered> ::= "SELECT" <select_list>
                          "FROM" <table_name> <alias>
                          "CROSS JOIN" <table_name> <alias>
                          "WHERE" <col_col_pred> [ "AND" <pred> ]
                          [ <order_by> ]

<cte_wrapper> ::= "WITH" <cte_name> "AS" "(" <inner_select> ")"
                  "SELECT" <select_list>
                  "FROM" <cte_name> <alias>
                  [ "INNER JOIN" <table_name> <alias> "ON" <join_cond> ]
                  [ <where> ]
                  [ <order_by> ]

<implicit_conv_join> ::= "SELECT" <select_list>
                         "FROM" <table_name> <alias>
                         "INNER JOIN" <table_name> <alias>
                         "ON" <implicit_conv_cond>
                         [ <where> ]
                         [ <order_by> ]

<derived_table> ::= "SELECT" <select_list>
                    "FROM" "(" <inner_select> ")" "AS" <alias>
                    [ "INNER JOIN" <table_name> <alias> "ON" <join_cond> ]
                    [ <where> ]
                    [ <order_by> ]

<in_subquery> ::= "SELECT" <select_list>
                  "FROM" <table_name> <alias>
                  "WHERE" <col_ref> "IN"
                      "(" "SELECT" <col_ref>
                          "FROM" <table_name> <alias>
                          [ <where> ]
                      ")"
                  [ "AND" <pred> ]
                  [ <order_by> ]

<exists_subquery> ::= "SELECT" <select_list>
                      "FROM" <table_name> <alias>
                      "WHERE" "EXISTS"
                          "(" "SELECT" "1"
                              "FROM" <table_name> <alias>
                              "WHERE" <corr_pred> [ "AND" <pred> ]
                          ")"
                      [ "AND" <pred> ]
                      [ <order_by> ]

<nested_derived> ::= "SELECT" <select_list>
                     "FROM" "("
                         "SELECT" <select_list>
                         "FROM" "(" <inner_select> ")" <alias>
                         [ <where> ]
                     ")" <alias>
                     [ <where> ]
                     [ <order_by> ]

<union_all> ::= <union_branch> "UNION ALL" <union_branch>

<union_branch> ::= "SELECT" <union_select_list>
                   "FROM" <table_name> <alias>
                   [ <where> ]
                 | "SELECT" <union_select_list>
                   "FROM" <table_name> <alias>
                   "INNER JOIN" <table_name> <alias> "ON" <join_cond>
                   [ <where> ]

<inner_select> ::= "SELECT" <select_list>
                   "FROM" <table_name> <alias>
                   [ <where> ]

<where> ::= "WHERE" <pred_list>
<order_by> ::= "ORDER BY" <col_ref> ( "ASC" | "DESC" )
               { "," <col_ref> ( "ASC" | "DESC" ) }

<pred_list> ::= <pred>
              | <pred> "AND" <pred_list>
              | "(" <pred> "OR" <pred> ")"

<pred> ::= <comparison>
         | <null_pred>
         | <between_pred>
         | <like_pred>
         | <in_pred>

<comparison>   ::= <col_ref> <comp_op> <literal>
                 | <col_ref> <comp_op> <col_ref>

<null_pred>    ::= <col_ref> "IS" "NULL"
                 | <col_ref> "IS" "NOT" "NULL"

<between_pred> ::= <col_ref> "BETWEEN" <literal> "AND" <literal>

<like_pred>    ::= <col_ref> "LIKE" <prefix_pattern>

<in_pred>      ::= <col_ref> "IN" "(" <literal> { "," <literal> } ")"

<corr_pred>    ::= <col_ref> "=" <col_ref>

<col_col_pred> ::= <col_ref> <comp_op> <col_ref>

<join_cond>         ::= <equi_join_cond> | <implicit_conv_cond>
<equi_join_cond>    ::= <col_ref> "=" <col_ref>
<implicit_conv_cond>::= "CAST" "(" <col_ref> "AS" <type_name> ")" "=" <col_ref>

<select_list> ::= <col_expr> [ "AS" <alias> ] { "," <col_expr> [ "AS" <alias> ] }

<union_select_list> ::= <union_col_expr> [ "AS" <alias> ]
                        { "," <union_col_expr> [ "AS" <alias> ] }

<col_expr> ::= <col_ref>
             | <num_func> "(" <col_ref> ")"
             | <str_func> "(" <col_ref> ")"
             | "(" <col_ref> <arith_op> <integer> ")"
             | "CAST" "(" <col_ref> "AS" <type_name> ")"

<union_col_expr> ::= <col_ref>
                   | "CAST" "(" <col_ref> "AS" <type_name> ")"

<comp_op>  ::= "=" | "<>" | "<" | ">" | "<=" | ">="
<arith_op> ::= "+" | "-" | "*"

<num_func> ::= "ABS" | "ROUND" | "CEIL" | "FLOOR" | "SIGN"
             | "SQRT" | "SIN" | "COS" | "TAN" | "EXP" | "LOG"

<str_func> ::= "UPPER" | "LOWER" | "TRIM" | "LTRIM" | "RTRIM" | "REVERSE"

<type_name> ::= "SIGNED" | "UNSIGNED" | "CHAR" | "DATE" | "DATETIME"
              | "DECIMAL" "(" <integer> "," <integer> ")"

<literal>       ::= <integer_literal> | <float_literal>
                  | <string_literal> | <date_literal> | "NULL"
<prefix_pattern>::= <string_prefix> "'%'"
<col_ref>       ::= <alias> "." "`" <col_name> "`"
<table_name>    ::= "`" <identifier> "`"
```

#### 简要说明

- 所有查询形态均为**行保留查询**，排除 `LIMIT`、`GROUP BY ... HAVING`、`NOT IN`、`EXCEPT`、`INTERSECT` 及反连接，以保证单调性不变式成立。
- `<cte_wrapper>` 受方言开关控制，不支持 CTE 的方言中权重置为 0。
- `<equi_join_cond>` 仅在类型兼容的列之间生成；`FLOAT`、`DOUBLE`、`DECIMAL`、`OPAQUE` 类型的列被排除于等值 JOIN 之外。
- `<implicit_conv_join>` 刻意连接类型不匹配的列（如 `INT` 与 `VARCHAR`），通过 `CAST` 触发隐式类型转换路径；已知的 MySQL 日期索引/字符串等值问题由守卫函数屏蔽。
- `<union_select_list>` 与 `<select_list>` 分开定义，反映 `UNION ALL` 两侧分支仅允许裸列引用或 `CAST` 的约束。
- 数值标量函数受方言过滤：MariaDB 排除 `LOG`、`EXP`、`SIN`、`COS`、`TAN`。

---

### 1.5 验证机制：差分状态执行（Differential State Execution）

Row-Cut Oracle 采用**差分状态执行**而非回滚：同一张物理表先后扮演 $S_1$（扩张前）和 $S_2$（扩张后）两个角色，Oracle 在状态切换前后分别执行相同查询并对比结果。

#### 1.5.1 查询快照数据结构

```python
@dataclass
class QuerySnapshot:
    count:        Optional[int]               # COUNT(*) 结果
    max_values:   Dict[str, Optional[float]]  # MAX(col)，按数值列名索引
    min_values:   Dict[str, Optional[float]]  # MIN(col)，按数值列名索引
    row_digests:  Dict[str, int]              # SHA-256 摘要 → 出现次数
    explain_plan: List[str]                   # EXPLAIN 输出行（归一化后）
```

快照由 `_execute_snapshot(conn, sql, numeric_cols)` 收集，步骤如下：

1. **COUNT**：`SELECT COUNT(*) FROM (<sql>) AS _w`
2. **MAX/MIN（各数值列）**：先通过 `LIMIT 0` 内省获取查询实际暴露的列名集合，再对每个出现在结果列中的数值列执行 `SELECT MAX(`col`) FROM (<sql>) AS _w`
3. **行摘要**（仅当 `COUNT ≤ 10,000` 时）：全量拉取结果行，每行以 SHA-256 哈希规范化表示，计入频次字典
4. **EXPLAIN 计划**：优先尝试 `EXPLAIN FORMAT=TRADITIONAL`，失败则回退到普通 `EXPLAIN`，兼容不同方言

#### 1.5.2 行摘要规范化

为避免浮点表示差异导致的伪不等，所有值经过 `_canonicalize_digest_value()` 规范化：
- `None` → `"NULL"`
- `bool` → `"1"` 或 `"0"`
- `int` → `str(val)`
- `Decimal`（有限值）→ `format(val.normalize(), 'f')`
- `float`（零）→ `"0"`；（非零）→ `format(Decimal(str(val)).normalize(), 'f')`
- `str` → `str(val).rstrip()`（去除尾部空白）

各字段以 `|` 分隔后拼接，整体哈希为 SHA-256 十六进制串。

#### 1.5.3 EXPLAIN 计划归一化

比较前对每行 EXPLAIN 输出进行归一化，抑制随数据量变化的噪声字段：

```python
row = re.sub(r'rows=[^;]+',     'rows=?',     row)
row = re.sub(r'filtered=[^;]+', 'filtered=?', row)
row = re.sub(r'key_len=[^;]+',  'key_len=?',  row)
```

保留结构性字段（access type、使用的索引键、join 顺序），屏蔽随行数估计而变化的字段，使计划结构的等价性判断不受数据量影响。

#### 1.5.4 验证逻辑

对每对 `(QuerySpec, QuerySnapshot_S1)` 基线，在扩张到 $S_2$ 后执行如下验证流程：

```
plan_S2      = _capture_explain(conn, sql)
plan_changed = not _plans_equivalent(s1.explain_plan, plan_S2)

if not plan_changed and random() > UNCHANGED_PLAN_VERIFY_PROB:
    跳过  # 计划未变化的查询以 85% 概率跳过

else:
    snap_S2 = _execute_snapshot(conn, sql, numeric_cols)
    _verify(snap_S1, snap_S2)
```

`_verify()` 依次检查四项单调性断言：

1. **COUNT 单调**：`s1.count ≤ s2.count`
2. **MAX 单调（各数值列）**：`s1.max_values[c] ≤ s2.max_values[c]`，容差 `FLOAT_TOLERANCE = 1e-9`
3. **MIN 单调（各数值列）**：`s2.min_values[c] ≤ s1.min_values[c]`
4. **行集合子集**：重新执行查询取得 $S_2$ 的新鲜摘要；对 $S_1$ 中每个出现 $n$ 次的摘要 $d$，断言其在 $S_2$ 中出现次数 $\geq n$

任何 `AssertionError` 均被记录到 Bug 日志文件，内容包括：出错的 SQL、两份 EXPLAIN 计划、两份快照值，以及本轮完整的 SQL 回放日志（所有 DDL、DML、ANALYZE 语句），支持在全新数据库实例上确定性复现。

---

## 2. 垂直切分 Oracle（Column-Cut Oracle）

### 2.1 核心不变式

Column-Cut Oracle 构建两张**持有相同逻辑数据但 DDL Schema 不同**的表 $S_1$ 和 $S_2$，通过**Schema 变异算子**（mutation operator）定义两者的结构关系。根据变异类型的不同，两张表上相同查询的结果需满足不同的不变式：

- **等价类（ABCD）**：$S_1$ 与 $S_2$ 语义等价，相同查询 $Q$ 的结果必须**完全一致**：$Q(S_1) = Q(S_2)$。
- **单调类（E）**：$S_1$ 的信息量是 $S_2$ 的真子集（$S_1$ 中部分列含 NULL，$S_2$ 将其填充为具体值）。聚合结果须满足：

$$
\text{COUNT}(c,\, S_1) \leq \text{COUNT}(c,\, S_2), \quad
\text{MAX}(c,\, S_1) \leq \text{MAX}(c,\, S_2), \quad
\text{MIN}(c,\, S_1) \geq \text{MIN}(c,\, S_2)
$$
$$
\text{COUNT(DISTINCT } c,\, S_1) \leq \text{COUNT(DISTINCT } c,\, S_2)
$$

注意：变异 E 不改变行数，因此 $\text{COUNT}(*,\, S_1) = \text{COUNT}(*,\, S_2)$。

---

### 2.2 变异算子（Mutation Operators）

每轮按 60%/30%/10% 的概率随机选取 1/2/3 个变异算子的组合，且单调类变异（E）至多选取一个（选取概率为 20%）。

#### A1. `drop_not_null` — 约束放松

移除 $S_2$ DDL 中所有非主键、非 `AUTO_INCREMENT` 列的 `NOT NULL` 约束，将其替换为 `NULL`：

```sql
-- S1: col_price DECIMAL(10,2) NOT NULL
-- S2: col_price DECIMAL(10,2) NULL
```

**语义依据**：允许 NULL 的 Schema 是更宽松的超集：它接受原有的所有行，还接受该列为 NULL 的行。对于从 $S_1$ 直接复制的数据，查询结果应当完全相同。这一变异针对优化器利用 `NOT NULL` 知识的代码路径（如 IS NULL 消除、索引条件下推等）。

#### A2. `drop_unique` — 唯一约束删除

从 $S_2$ DDL 中删除 `UNIQUE KEY` / `UNIQUE INDEX` 行，使 $S_2$ 允许重复值。这一变异针对基于唯一约束的去重优化路径和唯一索引扫描逻辑。

#### B. `widen_types` — 类型宽化

对 $S_2$ DDL 中的列类型进行自动提升：

| $S_1$ 类型 | $S_2$ 类型 | 规则 |
|---|---|---|
| `INT` | `BIGINT` | 词边界替换，不影响 BIGINT/TINYINT 等 |
| `VARCHAR(n)` | `VARCHAR(min(2n, 1024))` | 声明长度加倍，上限 1024 |
| `DECIMAL(p,s)` | `DECIMAL(min(p+4, 38), s)` | 精度增加 4，上限 38 |

主键列（`AUTO_INCREMENT`）及约束行不参与类型宽化。这一变异针对优化器的类型推导逻辑（如 JOIN 时操作数声明宽度不同引起的范围键比较异常）以及存储引擎对更宽类型的处理。

#### C. `add_index` — 索引结构变化

在 $S_2$ 数据填充完成后，通过 `ALTER TABLE` 为 $S_2$ 新增二级索引：

```sql
ALTER TABLE s2 ADD INDEX idx_<uid>_s (`col_name`);
-- 以 50% 概率额外添加复合索引：
ALTER TABLE s2 ADD INDEX idx_<uid>_c (`col_a`, `col_b`);
```

索引列从非主键列中按评分选取（INT/VARCHAR/DATE 型优先，非 nullable 列加分），字符串类型使用前缀索引。这一变异是等价类中**触发查询计划分叉**的主要手段：$S_1$ 走全表扫描，$S_2$ 走索引扫描，相同 SQL 执行路径不同，若结果不一致则暴露 Bug。

#### D. `add_column` — 字段新增

在 $S_2$ DDL 的 `PRIMARY KEY` 约束行之前插入一个新的 nullable INT 列：

```sql
`col_extra_d` INT NULL,
```

$S_2$ 的数据从 $S_1$ 复制（仅复制共有列，`col_extra_d` 自动为 NULL）。这一变异针对额外 nullable 列场景下的代码路径，包括列偏移计算、投影裁剪等。

#### E. `null_to_value` — NULL 值填充（单调类）

这是唯一的单调类变异。Oracle 从主表的非主键 nullable 数值列中随机选取 1–2 列作为 **Φ 列集合**。

在 $S_1$ 填充阶段，Φ 列被 50% 概率地设为 NULL（噪声行中 Φ 列全部为 NULL），使 $S_1$ 的 Φ 列含有大量 NULL 值。

$S_2$ 的数据先从 $S_1$ 完整复制，再通过 UPDATE 将 Φ 列中的 NULL 替换为具体数值：

```sql
UPDATE s2 SET `phi_col` = <concrete_value> WHERE `phi_col` IS NULL;
```

填充值刻意选取在 $S_1$ 已有值域之外的极端值（如 `INT` 型取 `2147483647` 或 `-2147483648`，`DECIMAL` 型取 `99999999.99`），以确保 MAX/MIN 的变化方向明确，避免测试无意义地"碰巧通过"。

---

### 2.3 DDL 生成与优化序列

$S_2$ DDL 由 $S_1$ DDL 经过一条变换流水线生成：

```
s1_ddl
  │
  ├─ replace(s1_name → s2_name)
  ├─ [A1] _ddl_drop_not_null()
  ├─ [A2] _ddl_drop_unique()  →  _fix_trailing_commas()
  ├─ [B]  _ddl_widen_types()
  └─ [D]  _ddl_add_column()
          │
          └─► s2_ddl  →  CREATE TABLE s2 (...)
```

变异 C 和 E 不修改 DDL：C 在数据填充后通过 `ALTER TABLE ADD INDEX` 应用；E 在数据复制后通过 `UPDATE` 应用。

`_fix_trailing_commas()` 工具函数修复 `drop_unique` 删除约束行后产生的尾部多余逗号，确保 DDL 语法合法。

Column-Cut Oracle 每轮完整的**执行步骤序列**如下：

| 步骤 | 操作 | 说明 |
|---|---|---|
| 1 | 选择变异组合 | 随机选 1–3 个算子，至多 1 个 E |
| 2 | 创建 S1 及辅助表 | DDL 来自 valscope `create_sample_tables()` |
| 3 | 填充 S1 数据 | 三层插入（hot seed / skewed random / noise），E 变体 Φ 列含 NULL |
| 4 | 生成 S2 DDL，创建 S2 | DDL 变换流水线（A1/A2/B/D） |
| 5 | 复制 S1 数据到 S2 | `INSERT INTO s2 SELECT ... FROM s1`；E 变体追加 UPDATE |
| 6 | [C 变体] 新增索引 | `ALTER TABLE s2 ADD INDEX ...` |
| 7 | ANALYZE TABLE × 2 | 对 S1 和 S2 各执行一次，确保统计信息对称更新 |
| 8 | 生成查询，收集 S1 快照 | 同时预检 E 变体查询的 S2 COUNT(*) 不变性 |
| 9 | 收集 S2 快照并验证 | 等价类：结果全等；单调类：聚合单调 |
| 10 | DROP 所有表 | 轮次间完全隔离 |

步骤 7 的双 `ANALYZE TABLE` 至关重要：$S_2$ 刚创建时无统计信息，若不立即分析，优化器将基于缺省估计选择次优计划，导致等价类验证的"误报"（实为统计信息缺失而非 Bug）。对 $S_1$ 同步执行 ANALYZE 保证两张表在相同信息基础上被优化，使计划差异仅来源于 Schema 差异（即变异算子的语义效果）。

---

### 2.4 近似 Schema 观测（Column-Cut）

Column-Cut Oracle 使用与 Row-Cut Oracle 完全相同的 `ColDef` + `VsTable` 近似 Schema 机制（见 §1.3）。主要差异在于变异感知：

- **变异 D（`add_column`）**：`col_extra_d` 列仅存在于 $S_2$ 的 DDL 中。查询生成器使用 $S_1$ 的 Schema 生成共有查询（两张表必须响应相同的 SQL 文本），因此 `col_extra_d` 不参与查询生成。
- **变异 C（`add_index`）**：`ALTER TABLE` 完成后，受影响列的 `is_indexed` 标志更新为 `True`，供查询生成器在 WHERE 谓词和 JOIN 条件中优先引用，提升索引路径触发概率。

---

## 2.4 近似 Schema 观测（Column-Cut）

Column-Cut Oracle 使用与 Row-Cut Oracle 相同的 `ColDef` + `VsTable` 近似 Schema 机制（见 §1.3）。在此基础上有两处额外处理：

**变异 D（`add_column`）**：新增列 `col_extra_d` 仅存在于 $S_2$ 的 DDL 中。查询生成器以 $S_1$ 的 Schema 为基准生成查询（因为两张表必须响应同一条 SQL 文本，见 §2.5），`col_extra_d` 不参与查询生成。

**变异 C（`add_index`）**：`ALTER TABLE` 执行完成后，受影响列的 `is_indexed` 标志被更新为 `True`。这个标志在 Column-Cut Oracle 的查询生成器中有实质作用（见 §2.5），而 Row-Cut Oracle 的生成器仅将其用于轻量偏置，两者行为有所不同。

---

## 2.5 查询生成（Column-Cut）

### 2.5.1 生成器与查询映射机制

Column-Cut Oracle 使用专用的 `VerticalQueryGenerator`。该生成器以 $S_1$ 的表名生成查询，在需要对 $S_2$ 执行时，通过字符串替换 `sql.replace(s1_name, s2_name)` 得到 $S_2$ 版本——两条查询的逻辑结构完全相同，仅访问的表名不同。

**当前实现说明**：等价类（ABCD）按设计应使用 valscope 原生生成器，以覆盖 `GROUP BY`、`LEFT JOIN`、`HAVING` 等当前生成器暂未支持的查询模式；目前 ABCD 和 E 两类均统一使用 `VerticalQueryGenerator`，等价验证的正确性不受影响，但查询形态的覆盖面相对较窄，后续会改进。

---

### 2.5.2 查询形态（Column-Cut）

对应实现入口：

- [vertical_query_gen.py](/oracle/vertical_query_gen.py)

#### 记号

与 §1.4.2 相同。带 `†` 标注的产生式为本生成器在 §1.4.2 基础上的扩展。

```bnf
<query> ::= < §1.4.2 中全部形态 >
          | <rare_behavior_join>    †

<rare_behavior_join> ::= "SELECT" <select_list>                       †
                         "FROM" <table_name> <alias>
                         "INNER JOIN" <table_name> <alias>
                         "ON" <rare_join_cond>
                         [ <where> ]
                         [ <order_by> ]

<rare_join_cond> ::=
    <col_ref> "<=>" <col_ref>                                          †
  | "(" <col_ref> "," <col_ref> ")" "=" "(" <col_ref> "," <col_ref> ")"  †
  | "LEFT" "(" <col_ref> "," <integer> ")" "=" <col_ref>              †
  | <col_ref> "COLLATE" <collation_name> "=" <col_ref>                †
  | <col_ref> "=" <col_ref>                                            †  (* ENUM ↔ string *)
  | <col_ref> "+" "0" "=" <col_ref>                                   †  (* ENUM 算术转换 *)
  | "FIND_IN_SET" "(" <col_ref> "," <col_ref> ")" ">" "0"             †
  | <col_ref> "LIKE" "CONCAT" "(" <col_ref> "," "'%'" ")"             †
  | "CAST" "(" <col_ref> "AS" "DATE" ")" "=" <col_ref>                †
  | "DATE" "(" <col_ref> ")" "=" <col_ref>                            †
  | "STRCMP" "(" <col_ref> "," <col_ref> ")" "=" "0"                  †
  | <col_ref> "=" <col_ref>                                            †  (* CHAR 尾部空格填充 *)

<col_expr> ::= < §1.4.2 中全部形态 >
             | "COALESCE" "(" <col_ref> "," <literal> ")"             †
             | "(" "COALESCE" "(" <col_ref> "," "0" ")"
               "+" "COALESCE" "(" <col_ref> "," "0" ")" ")"           †
             | "CASE" "WHEN" <col_ref> ">=" <integer>
               "THEN" <col_ref> "ELSE" <integer> "END"                †
             | "CONCAT" "(" <col_ref> "," "''" ")"                    †
             | "CONCAT" "(" "COALESCE" "(" <col_ref> "," "''" ")"
               "," "COALESCE" "(" <col_ref> "," "''" ")" ")"          †
             | "CASE" "WHEN" <col_ref> "IS NULL"
               "THEN" "'missing'" "ELSE" <col_ref> "END"              †

<pred> ::= < §1.4.2 中全部形态 >
         | <num_func> "(" <col_ref> ")" ">=" <literal>                †
         | "COALESCE" "(" <col_ref> "," <literal> ")"
           <comp_op> <literal>                                         †
         | "LENGTH" "(" "COALESCE" "(" <col_ref> "," "''" ")" ")"
           ">=" <integer>                                              †

<collation_name> ::= "utf8mb4_bin" | "utf8mb4_general_ci" | "utf8mb4_unicode_ci"
```

#### 简要说明

- `<rare_behavior_join>` 包含 10 种子模式，均通过 `_build_rare_behavior_join()` 随机分发，针对 Schema 变异后可能触发的边缘优化器行为。
- `<col_expr>` 的扩展（`COALESCE`、`CASE`、`CONCAT`）针对变异 E（null_to_value）场景：$S_1$ 的 Φ 列含 NULL，这些表达式保证 $S_1$/$S_2$ 的 SELECT 结果在语义上可比较。
- `<pred>` 的扩展（`COALESCE` 谓词、`LENGTH` 谓词）同样服务于 Φ 列的 NULL 兼容性。
- 时态列（`DATE`、`DATETIME`、`TIMESTAMP`、`TIME`、`YEAR`）在本生成器中得到完整支持，可出现于 `<col_expr>`、`<pred>` 和 `<rare_join_cond>` 中；§1.4.2 的生成器对时态列仅做有限处理。

### 2.5.3 与 Row-Cut Oracle 生成器的实质差异

`VerticalQueryGenerator` 与 `SubsetQueryGenerator` 在以下几个方面有实质差异，均源于垂直 Oracle 的特殊需求：

**`is_indexed` 感知的谓词生成**：当列的 `is_indexed` 为 `True` 时（变异 C 新增索引后），谓词生成器大幅提升索引友好谓词的权重：`=`、`>=`、`<=`、`BETWEEN ... AND ...`、`col IN (...)`、`LIKE 'prefix%'` 等被优先选择，以此使 $S_2$（有索引）更容易触发索引范围扫描，而 $S_1$（无对应索引）走全表扫描，从而制造计划分叉。Row-Cut Oracle 中 `is_indexed` 仅用于轻量偏置，效果有限。

**时态类型支持**：`VerticalQueryGenerator` 完整处理 `DATE`、`DATETIME`、`TIMESTAMP`、`TIME`、`YEAR` 类型列，在 SELECT 表达式、WHERE 谓词、JOIN 条件中均可生成时态相关的表达式，如范围比较、`CAST(datetime AS DATE) = date_col` 等。

**COALESCE / CASE 富表达式**：`_col_expr()` 方法会为 SELECT 列表中的列生成 `COALESCE(col, default)`、`(CASE WHEN col >= threshold THEN col ELSE threshold END)`、`CONCAT(col, '')` 等形式。这一设计兼顾了 Φ 列在 $S_1$ 中含 NULL 的单调类场景，同时对等价类保证 $S_1$/$S_2$ 结果的一致性。

### 2.5.4 单调类（E）的基线筛选

对于 E 变异，基线收集阶段对每条候选查询增加**实证预检**：生成器产生 SQL 后，立即在 $S_2$ 上执行并检查 $\text{COUNT}(*)(S_2) = \text{COUNT}(*)(S_1)$（E 变异不改变行数，若不满足则说明查询引用了 Φ 列且语义不兼容）。不满足此条件的查询被丢弃，仅保留通过预检的查询进入验证流程。该机制无需静态分析，即可正确处理 `COALESCE`、`CASE`、`IS NOT NULL` 等所有含 Φ 列引用的复杂表达式形式。

此外，`_derive_monotone_metric_mask()` 会根据预检结果进一步筛选：对每个 Φ 列，只保留在基线阶段 $S_1$/$S_2$ 快照中方向已经正确的指标（如 MAX 确实更大、MIN 确实更小），作为最终验证时的断言掩码，避免对因数据巧合导致方向未发生变化的指标进行无意义的断言。

---

### 2.6 验证机制（Column-Cut）

与 Row-Cut Oracle 不同，Column-Cut Oracle 的 $S_1$ 和 $S_2$ 是**同一数据库中并存的两张独立表**，使用同一连接按顺序执行。不需要事务回滚，也不涉及单表的时序状态切换。

#### 2.6.1 快照结构

```python
@dataclass
class VertSnapshot:
    count:          Optional[int]
    max_values:     Dict[str, Optional[float]]   # 数值列 MAX
    min_values:     Dict[str, Optional[float]]   # 数值列 MIN
    col_counts:     Dict[str, Optional[int]]     # COUNT(col)，Φ 列专用
    count_distinct: Dict[str, Optional[int]]     # COUNT(DISTINCT col)，Φ 列专用
    row_digests:    Dict[str, int]               # 行摘要（等价类启用）
    explain_plan:   List[str]
```

#### 2.6.2 等价类验证（变异 A1/A2/B/C/D）

对等价类变异，两个快照必须完全一致：

- `COUNT(*)(S1) == COUNT(*)(S2)`（精确相等）
- `MAX(c)(S1) == MAX(c)(S2)`，所有数值列，容差 `FLOAT_TOLERANCE = 1e-9`
- `MIN(c)(S1) == MIN(c)(S2)`，所有数值列
- `row_digests(S1)` multiset `==` `row_digests(S2)`（通过 `Counter` 比较）

任何偏差均说明 Schema 变异导致语义等价查询产生不同结果——逻辑 Bug。

#### 2.6.3 单调类验证（变异 E）

对变异 E，针对各 Φ 列分别检查：

- `COUNT(*)(S1) == COUNT(*)(S2)`（行数不变，作为基本健全性检查）
- `COUNT(col)(S1) ≤ COUNT(col)(S2)`
- `MAX(col)(S1) ≤ MAX(col)(S2)`（数值类 Φ 列）
- `MIN(col)(S1) ≥ MIN(col)(S2)`（数值类 Φ 列）
- `COUNT(DISTINCT col)(S1) ≤ COUNT(DISTINCT col)(S2)`

注意：E 类**不执行行摘要（row digest）验证**，因为 Φ 列的 NULL → 具体值填充必然导致行内容发生变化，行摘要的等价性本就不成立，不应纳入断言。

#### 2.6.4 计划变化检测

Oracle 为 $S_1$ 和 $S_2$ 分别捕获 EXPLAIN 计划并进行结构比较。对等价类（尤其是变异 C），计划分叉是**设计预期**：$S_1$ 走全表扫描，$S_2$ 走索引扫描。所有查询无论计划是否变化均执行验证（等价类不依赖计划分叉触发）。对单调类，预检阶段已在基线收集时同步取得 $S_2$ 快照，最终验证阶段直接复用，避免重复执行。

---