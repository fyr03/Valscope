"""
oracle/subset_query_gen.py

SubsetQueryGenerator: 为 SubsetOracle 生成"行保留"SQL 查询的独立模块。

═══════════════════════════════════════════════════════════
语义保证（对任意 S1 ⊆ S2，以下属性对本模块生成的所有查询 Q 成立）
═══════════════════════════════════════════════════════════

  行保留（Row-Preserving）：
      ∀ row r ∈ Q(S1)  →  r ∈ Q(S2)
      即 Q(S1) ⊆ Q(S2)（多重集意义）

  聚合单调性（由行保留自然推导）：
      COUNT(Q(S1)) ≤ COUNT(Q(S2))
      MAX(col, Q(S1)) ≤ MAX(col, Q(S2))
      MIN(col, Q(S1)) ≥ MIN(col, Q(S2))

═══════════════════════════════════════════════════════════
实现方式
═══════════════════════════════════════════════════════════

  ✓ 允许：
      SELECT 列引用、标量函数（ABS/ROUND/SIN/COS/UPPER 等）、算术运算
      INNER JOIN（等值条件：单调，主表行增加时 JOIN 结果只增不减）
      派生表（FROM (SELECT ...) sub）
      WHERE IN (子查询)、WHERE EXISTS（均为单调谓词）
      WHERE 等值/范围谓词（=, >, <, >=, <=, BETWEEN, LIKE, IS NULL, IN 字面量）
      DISTINCT（子集关系对集合仍成立）
      ORDER BY（无 LIMIT，仅影响行序，不改变行集合）

  ✗ 禁止（已在生成层面排除，不依赖后期过滤）：
      聚合函数 COUNT / SUM / MIN / MAX / AVG 等（外层 oracle 手动做）
      NOT IN / NOT EXISTS / NOT BETWEEN（反单调谓词）
      LIMIT（截断行，破坏超集关系）
      GROUP BY / HAVING（折叠行，输出不再是原始行）
      WITH / CTE（语义复杂，含 RECURSIVE 时不单调）
      UNION / EXCEPT / INTERSECT（集合运算可能减少行）
      窗口函数 OVER()（RANK / LAG / LEAD 等依赖全局排序，不单调）
      LEFT JOIN / RIGHT JOIN（NULL 填充行在右表增长时摘要会改变）
      FOR UPDATE（锁语义，与测试无关）

═══════════════════════════════════════════════════════════
与 SubsetOracle 的集成
═══════════════════════════════════════════════════════════

    # 构造生成器（在 _build_baselines 里调用）
    tables = [(name_map[tbl.name], oracle._vs_table_to_coldefs(tbl))
              for tbl in vs_tables]
    hot_values = {name_map[vs_tables[0].name]: skew.hot_values_by_col}
    gen = SubsetQueryGenerator(tables, hot_values)

    for _ in range(MAX_QUERY_GEN_ATTEMPTS):
        sql = gen.generate()
        if sql:
            ...  # 执行快照、收集 baseline

═══════════════════════════════════════════════════════════
"""

import re
import random
from typing import Any, Dict, List, Optional, Tuple


# ─────────────────────────────────────────────────────────────
# 常量
# ─────────────────────────────────────────────────────────────

# 数值类型可用的标量函数（投影安全：不改变行的存在性）
_NUM_SCALAR_FUNS: List[str] = [
    'ABS', 'ROUND', 'CEIL', 'FLOOR', 'SIGN',
    'SQRT', 'SIN', 'COS', 'TAN', 'EXP', 'LOG',
]

# 字符串类型可用的标量函数
_STR_SCALAR_FUNS: List[str] = [
    'UPPER', 'LOWER', 'LENGTH', 'TRIM', 'LTRIM', 'RTRIM', 'REVERSE',
]

# 数值算术二元运算符（用于 SELECT 列表的表达式多样性）
_ARITH_OPS: List[str] = ['+', '-', '*']

# 不适合做 JOIN 等值键的类型（浮点等值比较语义不稳定）
_NO_JOIN_TYPES: frozenset = frozenset({'FLOAT', 'DOUBLE', 'DECIMAL'})

# 生成 SELECT 列数的范围
_SELECT_MIN_COLS = 1
_SELECT_MAX_COLS = 5

# WHERE 最多谓词数
_WHERE_MAX_PREDS = 3


# ─────────────────────────────────────────────────────────────
# SubsetQueryGenerator
# ─────────────────────────────────────────────────────────────

class SubsetQueryGenerator:
    """
    生成满足"行保留"语义的 SQL 查询。

    参数
    ----
    tables : List[Tuple[str, List[ColDef]]]
        表定义列表，每项为 (实际表名, 列定义列表)。
        至少 1 张表；提供 2-3 张时可生成多表 JOIN 查询，覆盖更多代码路径。

    skew_hot_values : Dict[str, Dict[str, List[str]]], optional
        偏斜热值字典，结构为 {table_name: {col_name: [sql_literal, ...]}}。
        用于生成更有区分度的 WHERE 谓词，与 SkewProfile.hot_values_by_col 对应。
    """

    def __init__(
        self,
        tables: List[Tuple[str, List[Any]]],
        skew_hot_values: Optional[Dict[str, Dict[str, List[str]]]] = None,
    ) -> None:
        if not tables:
            raise ValueError("SubsetQueryGenerator: 至少需要一张表")
        self.tables     = tables
        self.hot_values = skew_hot_values or {}
        self._ctr       = 0          # 别名计数器，保证每次生成的别名唯一

    # ─────────────────────────────────────────
    # 公共接口
    # ─────────────────────────────────────────

    def generate(self) -> Optional[str]:
        """
        生成一条行保留 SQL 查询字符串。
        内部随机选择 Shape，失败时返回 None（调用方应重试）。
        """
        self._ctr = 0   # 每次 generate 重置计数器，保证别名可读
        try:
            shape = self._choose_shape()
            builder = {
                'SINGLE':           self._build_single,
                'INNER_JOIN_2':     self._build_inner_join_2,
                'INNER_JOIN_3':     self._build_inner_join_3,
                'DERIVED_TABLE':    self._build_derived_table,
                'IN_SUBQUERY':      self._build_in_subquery,
                'EXISTS_SUBQUERY':  self._build_exists_subquery,
                'NESTED_DERIVED':   self._build_nested_derived,
            }
            return builder[shape]()
        except Exception as e:
            import traceback
            print(f"[gen ERROR] shape={shape}: {e}")
            traceback.print_exc()   # ← 临时加这两行
            return None     # 静默失败，调用方负责重试

    # ─────────────────────────────────────────
    # Shape 选择
    # ─────────────────────────────────────────

    def _choose_shape(self) -> str:
        n = len(self.tables)

        # (shape, weight) 列表
        if n == 1:
            pool = [
                ('SINGLE',          40),
                ('DERIVED_TABLE',   30),
                ('IN_SUBQUERY',     20),
                ('NESTED_DERIVED',  10),
            ]
        elif n == 2:
            pool = [
                ('SINGLE',          10),
                ('INNER_JOIN_2',    30),
                ('DERIVED_TABLE',   15),
                ('IN_SUBQUERY',     25),
                ('EXISTS_SUBQUERY', 15),
                ('NESTED_DERIVED',   5),
            ]
        else:   # n >= 3
            pool = [
                ('SINGLE',          8),
                ('INNER_JOIN_2',    25),
                ('INNER_JOIN_3',    15),
                ('DERIVED_TABLE',   12),
                ('IN_SUBQUERY',     20),
                ('EXISTS_SUBQUERY', 15),
                ('NESTED_DERIVED',   5),
            ]

        shapes, weights = zip(*pool)
        return random.choices(shapes, weights=weights, k=1)[0]

    # ─────────────────────────────────────────
    # Shape 实现：单表查询
    # ─────────────────────────────────────────

    def _build_single(self) -> str:
        """
        SELECT [DISTINCT] <exprs> FROM <t> [alias] [WHERE ...]
        最基础的形态，但 WHERE 谓词仍具备多样性。
        """
        tname, cols = random.choice(self.tables)
        alias = self._alias(tname)

        sel      = self._select_list([(alias, cols)])
        where    = self._where_clause([(alias, cols)])
        distinct = 'DISTINCT ' if random.random() < 0.2 else ''

        return f"SELECT {distinct}{sel} FROM `{tname}` {alias}{where}"

    # ─────────────────────────────────────────
    # Shape 实现：两表 INNER JOIN
    # ─────────────────────────────────────────

    def _build_inner_join_2(self) -> str:
        """
        SELECT ... FROM t1 a1
          INNER JOIN t2 a2 ON a1.k = a2.k
        [WHERE ...]

        JOIN 方向固定为 INNER JOIN：当两张表都增长时，结果单调递增（⊇ S1 的 JOIN 结果）。
        注意：LEFT JOIN 不满足此性质——右表增长后原本为 NULL 的列会得到实际值，
        导致行摘要改变，破坏子集关系。
        """
        t1n, t1c = self.tables[0]
        # 第二张表优先选不同的表，退而求其次用自身（自连接）
        other = [t for t in self.tables if t[0] != t1n]
        t2n, t2c = random.choice(other) if other else self.tables[0]

        a1 = self._alias(t1n)
        a2 = self._alias(t2n)

        on = self._join_on(a1, t1c, a2, t2c)
        if on is None:
            return self._build_single()     # 找不到兼容列时降级

        sel      = self._select_list([(a1, t1c), (a2, t2c)])
        where    = self._where_clause([(a1, t1c), (a2, t2c)])
        distinct = 'DISTINCT ' if random.random() < 0.15 else ''

        return (f"SELECT {distinct}{sel} FROM `{t1n}` {a1} "
                f"INNER JOIN `{t2n}` {a2} ON {on}{where}")

    # ─────────────────────────────────────────
    # Shape 实现：三表 INNER JOIN
    # ─────────────────────────────────────────

    def _build_inner_join_3(self) -> str:
        if len(self.tables) < 3:
            return self._build_inner_join_2()

        t1n, t1c = self.tables[0]
        t2n, t2c = self.tables[1]
        t3n, t3c = self.tables[2]
        a1 = self._alias(t1n)
        a2 = self._alias(t2n)
        a3 = self._alias(t3n)

        on12 = self._join_on(a1, t1c, a2, t2c) or '1=1'
        on23 = self._join_on(a2, t2c, a3, t3c) or '1=1'

        sel   = self._select_list([(a1, t1c), (a2, t2c), (a3, t3c)])
        where = self._where_clause([(a1, t1c)])     # WHERE 只加在主表，避免谓词过于复杂
        distinct = 'DISTINCT ' if random.random() < 0.1 else ''

        return (f"SELECT {distinct}{sel} FROM `{t1n}` {a1} "
                f"INNER JOIN `{t2n}` {a2} ON {on12} "
                f"INNER JOIN `{t3n}` {a3} ON {on23}{where}")

    # ─────────────────────────────────────────
    # Shape 实现：派生表
    # ─────────────────────────────────────────

    def _build_derived_table(self) -> Optional[str]:
        """
        SELECT ... FROM (SELECT <inner_cols> FROM t WHERE ...) sub
          [INNER JOIN t2 ... ]
          [WHERE ...]

        派生表是行保留的：内层只做投影+过滤，外层再做投影+过滤，
        两层都不含聚合，所以整体满足行保留语义。
        """
        tname, cols = random.choice(self.tables)
        inner_a = self._alias('i')

        # 内层：投影若干列 + WHERE
        inner_n   = min(random.randint(2, 4), len(cols))
        inner_cols = random.sample(cols, k=inner_n)
        inner_sel  = ', '.join(
            f"`{inner_a}`.`{c.name}` AS `{c.name}`" for c in inner_cols
        )
        inner_where = self._where_clause([(inner_a, cols)])
        inner_sql   = f"SELECT {inner_sel} FROM `{tname}` {inner_a}{inner_where}"

        sub_a = self._alias('s')

        # 外层：对 inner_cols 做投影（可加标量函数）
        outer_sel   = self._select_list_from_col_defs(sub_a, inner_cols)
        outer_where = self._where_clause([(sub_a, inner_cols)])

        # 偶尔外层再 INNER JOIN 一张真实表
        if random.random() < 0.35:
            other = [t for t in self.tables if t[0] != tname]
            if other:
                t2n, t2c = random.choice(other)
                a2  = self._alias(t2n)
                on  = self._join_on_cold(sub_a, inner_cols, a2, t2c)
                if on:
                    extra_n   = min(2, len(t2c))
                    extra_sel = ', '.join(
                        f"`{a2}`.`{c.name}`" for c in random.sample(t2c, k=extra_n)
                    )
                    return (f"SELECT {outer_sel}, {extra_sel} "
                            f"FROM ({inner_sql}) {sub_a} "
                            f"INNER JOIN `{t2n}` {a2} ON {on}{outer_where}")

        return f"SELECT {outer_sel} FROM ({inner_sql}) {sub_a}{outer_where}"

    # ─────────────────────────────────────────
    # Shape 实现：IN 子查询
    # ─────────────────────────────────────────

    def _build_in_subquery(self) -> str:
        """
        SELECT ... FROM t1 a1 WHERE a1.col IN (SELECT sq.col FROM t2 sq WHERE ...)

        IN 是单调谓词：子查询表增长 → 子查询结果集增大 → 主表满足 IN 的行只增不减。
        严格禁止 NOT IN（反单调）。
        """
        tname, cols = self.tables[0]
        alias = self._alias(tname)

        # 选适合做 IN 左侧的列（INT/VARCHAR，非主键）
        in_candidates = [c for c in cols
                         if c.data_type in ('INT', 'VARCHAR') and not c.is_primary_key]
        if not in_candidates:
            return self._build_single()
        main_col = random.choice(in_candidates)

        # 子查询的表和列（类型必须与 main_col 兼容）
        st_name, st_cols = random.choice(self.tables)
        compat = [c for c in st_cols if c.data_type == main_col.data_type]
        if not compat:
            return self._build_single()
        sub_col = random.choice(compat)

        sq_a      = self._alias('sq')
        sq_where  = self._where_clause([(sq_a, st_cols)])
        subquery  = (f"SELECT `{sq_a}`.`{sub_col.name}` "
                     f"FROM `{st_name}` {sq_a}{sq_where}")

        sel   = self._select_list([(alias, cols)])
        where = self._where_clause([(alias, cols)])
        in_pred = f"`{alias}`.`{main_col.name}` IN ({subquery})"

        if 'WHERE' in where.upper():
            where = f"{where} AND {in_pred}"
        else:
            where = f" WHERE {in_pred}"

        return f"SELECT {sel} FROM `{tname}` {alias}{where}"

    # ─────────────────────────────────────────
    # Shape 实现：EXISTS 相关子查询
    # ─────────────────────────────────────────

    def _build_exists_subquery(self) -> str:
        """
        SELECT ... FROM t1 a1 WHERE EXISTS (SELECT 1 FROM t2 sq WHERE sq.k = a1.k AND ...)

        EXISTS 是单调谓词：子查询表行增加 → 更多关联条件成立 → 主表满足 EXISTS 的行只增不减。
        严格禁止 NOT EXISTS（反单调）。
        """
        tname, cols = self.tables[0]
        alias = self._alias(tname)

        other = [t for t in self.tables if t[0] != tname]
        st_name, st_cols = random.choice(other) if other else self.tables[0]
        sq_a = self._alias('ex')

        # 关联条件列
        jc_main = self._pick_join_col(cols)
        jc_sub  = self._pick_compat_col(st_cols, jc_main)
        if jc_main is None or jc_sub is None:
            return self._build_single()

        corr = (f"`{alias}`.`{jc_main.name}` = `{sq_a}`.`{jc_sub.name}`")
        sq_extra = self._where_clause([(sq_a, st_cols)])
        if 'WHERE' in sq_extra.upper():
            sq_body = f"{sq_extra} AND {corr}"
        else:
            sq_body = f" WHERE {corr}"

        subquery = f"SELECT 1 FROM `{st_name}` {sq_a}{sq_body}"

        sel   = self._select_list([(alias, cols)])
        where = self._where_clause([(alias, cols)])
        exists_pred = f"EXISTS ({subquery})"

        if 'WHERE' in where.upper():
            where = f"{where} AND {exists_pred}"
        else:
            where = f" WHERE {exists_pred}"

        return f"SELECT {sel} FROM `{tname}` {alias}{where}"

    # ─────────────────────────────────────────
    # Shape 实现：嵌套派生表（两层子查询）
    # ─────────────────────────────────────────

    def _build_nested_derived(self) -> str:
        """
        SELECT ... FROM (
            SELECT ... FROM (SELECT ... FROM t WHERE ...) inner_sub
            [WHERE ...]
        ) outer_sub
        [WHERE ...]

        两层派生表，增加查询计划多样性，触发更多优化器路径。
        行保留性通过每层均不含聚合来保证。
        """
        tname, cols = random.choice(self.tables)

        # 最内层
        i1_a  = self._alias('i1')
        i1_n  = min(random.randint(2, 4), len(cols))
        i1_cs = random.sample(cols, k=i1_n)
        i1_sel = ', '.join(f"`{i1_a}`.`{c.name}` AS `{c.name}`" for c in i1_cs)
        i1_w   = self._where_clause([(i1_a, cols)])
        i1_sql = f"SELECT {i1_sel} FROM `{tname}` {i1_a}{i1_w}"

        # 中层
        m_a   = self._alias('m')
        m_n   = min(random.randint(1, len(i1_cs)), len(i1_cs))
        m_cs  = random.sample(i1_cs, k=m_n)
        m_sel = ', '.join(f"`{m_a}`.`{c.name}` AS `{c.name}`" for c in m_cs)
        m_w   = self._where_clause([(m_a, i1_cs)])
        m_sql = f"SELECT {m_sel} FROM ({i1_sql}) {m_a}{m_w}"

        # 外层
        o_a   = self._alias('o')
        o_sel = self._select_list_from_col_defs(o_a, m_cs)
        o_w   = self._where_clause([(o_a, m_cs)])

        return f"SELECT {o_sel} FROM ({m_sql}) {o_a}{o_w}"

    # ─────────────────────────────────────────
    # SELECT 列表构建
    # ─────────────────────────────────────────

    def _select_list(self, alias_cols: List[Tuple[str, List[Any]]],
                    allow_scalar_fn: bool = True) -> str:
        all_refs = [(a, c) for a, cols in alias_cols for c in cols]
        if not all_refs:
            return '*'
        k = min(random.randint(_SELECT_MIN_COLS, _SELECT_MAX_COLS), len(all_refs))
        chosen = random.sample(all_refs, k=k)

        # 按列名去重：同名列只保留第一个，彻底避免 "Duplicate column name"
        seen_names: set = set()
        deduped = []
        for a, c in chosen:
            if c.name not in seen_names:
                seen_names.add(c.name)
                deduped.append((a, c))

        return ', '.join(self._col_expr(a, c, allow_scalar_fn) for a, c in deduped)

    def _select_list_from_col_defs(self, alias: str, cols: List[Any]) -> str:
        """对已知 ColDef 列表构建 SELECT 列表（用于派生表外层）。"""
        k = min(random.randint(1, max(1, len(cols))), len(cols))
        chosen = random.sample(cols, k=k)
        exprs = [self._col_expr(alias, c) for c in chosen]
        return ', '.join(exprs) if exprs else f'`{alias}`.*'

    def _col_expr(self, alias: str, col: Any, allow_scalar_fn: bool = True) -> str:
        """
        为单列生成 SELECT 表达式。
        50% 概率裸列引用，50% 概率套一层标量函数或算术运算。
        标量函数不影响行的存在性，只影响输出列的值。
        """
        base = f"`{alias}`.`{col.name}`"
        dt   = col.data_type

        if not allow_scalar_fn or random.random() < 0.5:
            return base     # 裸引用，最常见

        r = random.random()
        if dt in ('INT', 'FLOAT', 'DOUBLE', 'DECIMAL'):
            if r < 0.35 and _NUM_SCALAR_FUNS:
                fn = random.choice(_NUM_SCALAR_FUNS)
                return f"{fn}({base})"
            elif r < 0.55 and _ARITH_OPS:
                op  = random.choice(_ARITH_OPS)
                lit = random.randint(1, 10)
                return f"({base} {op} {lit})"
            else:
                return base

        elif dt == 'VARCHAR':
            if r < 0.4 and _STR_SCALAR_FUNS:
                fn = random.choice(_STR_SCALAR_FUNS)
                return f"{fn}({base})"
            else:
                return base

        return base

    # ─────────────────────────────────────────
    # WHERE 子句构建
    # ─────────────────────────────────────────

    def _where_clause(
        self,
        alias_cols: List[Tuple[str, List[Any]]],
        max_preds: int = _WHERE_MAX_PREDS,
    ) -> str:
        """
        生成 WHERE 子句（可能为空字符串）。

        谓词类型：等值、范围、BETWEEN、LIKE、IS NULL、IS NOT NULL、IN 字面量。
        连接方式：AND 为主，偶尔对相邻两个谓词包成 OR（仍然满足单调性）。

        不含 NOT IN / NOT EXISTS / NOT BETWEEN（反单调禁止）。
        不含聚合函数（非 GROUP BY 上下文，也无意义）。
        """
        if random.random() < 0.12:  # 12% 概率无 WHERE（全表扫描路径）
            return ''

        all_refs = [
            (a, c) for a, cols in alias_cols
            for c in cols if not c.is_primary_key
        ]
        if not all_refs:
            all_refs = [(a, c) for a, cols in alias_cols for c in cols]
        if not all_refs:
            return ''

        n       = random.randint(1, min(max_preds, len(all_refs)))
        sampled = random.sample(all_refs, k=n)
        preds   = [p for a, c in sampled for p in [self._predicate(a, c)] if p]

        if not preds:
            return ''

        return ' WHERE ' + self._combine_preds(preds)

    def _combine_preds(self, preds: List[str]) -> str:
        """
        将多个谓词组合：以 AND 为主，20% 概率对相邻两个谓词包成 OR。
        OR 不破坏单调性（A OR B 依然是单调谓词，只要 A 和 B 各自单调）。
        """
        if len(preds) == 1:
            return preds[0]

        result = [preds[0]]
        for p in preds[1:]:
            if random.random() < 0.2 and result:
                prev = result.pop()
                result.append(f"({prev} OR {p})")
            else:
                result.append(p)
        return ' AND '.join(result)

    def _predicate(self, alias: str, col: Any) -> Optional[str]:
        """
        为单列生成一个单调谓词（=, >, <, >=, <=, BETWEEN, LIKE, IS NULL, IN 字面量）。
        使用 SkewProfile 热值提升谓词的鉴别力（更多行落在热值区间，使计划倾向走索引）。
        """
        ref  = f"`{alias}`.`{col.name}`"
        dt   = col.data_type
        r    = random.random()
        
        if dt == 'DATE':
                return f"{ref} IS NOT NULL" if r < 0.6 else f"{ref} IS NULL"
        
        hot  = self._hot(col)
        # ── INT ──────────────────────────────────────────
        if dt == 'INT':
            v1 = hot or str(random.randint(-100, 100))
            v2 = str(int(v1) + random.randint(1, 20))

            if r < 0.18:  return f"{ref} = {v1}"
            if r < 0.34:  return f"{ref} >= {v1}"
            if r < 0.50:  return f"{ref} <= {v1}"
            if r < 0.64:  return f"{ref} BETWEEN {v1} AND {v2}"
            if r < 0.74:  return f"{ref} IS NULL"
            if r < 0.84:  return f"{ref} IS NOT NULL"
            # IN 字面量
            lits = [str(random.randint(-200, 200)) for _ in range(random.randint(2, 5))]
            return f"{ref} IN ({', '.join(lits)})"

        # ── VARCHAR ──────────────────────────────────────
        elif dt == 'VARCHAR':
            v = hot or f"'val{random.randint(0, 99)}'"
            inner  = v.strip("'")
            prefix = (inner[:2] if len(inner) >= 2 else inner) or 'v'

            if r < 0.22:  return f"{ref} = {v}"
            if r < 0.40:  return f"{ref} LIKE '{prefix}%'"
            if r < 0.54:  return f"{ref} >= {v}"
            if r < 0.66:  return f"{ref} IS NULL"
            if r < 0.78:  return f"{ref} IS NOT NULL"
            lits = [f"'w{random.randint(0, 99)}'" for _ in range(random.randint(2, 4))]
            return f"{ref} IN ({', '.join(lits)})"

        # ── FLOAT / DOUBLE / DECIMAL ──────────────────────
        elif dt in ('FLOAT', 'DOUBLE', 'DECIMAL'):
            try:
                v1 = hot or f"{random.uniform(-100, 100):.3f}"
                v2 = f"{float(v1) + random.uniform(0.1, 10):.3f}"
            except (ValueError, TypeError):
                v1 = '0.0'
                v2 = '10.0'

            if r < 0.30:  return f"{ref} >= {v1}"
            if r < 0.55:  return f"{ref} <= {v1}"
            if r < 0.72:  return f"{ref} BETWEEN {v1} AND {v2}"
            if r < 0.87:  return f"{ref} IS NOT NULL"
            return f"{ref} IS NULL"
        
        # ── 其他类型（DATE, DATETIME 等）────────────────────
        else:
            if r < 0.5:   return f"{ref} IS NOT NULL"
            return f"{ref} IS NULL"

    # ─────────────────────────────────────────
    # JOIN ON 构建
    # ─────────────────────────────────────────

    def _join_on(
        self,
        a1: str, cols1: List[Any],
        a2: str, cols2: List[Any],
    ) -> Optional[str]:
        """
        寻找两张表中类型兼容的列对，生成等值 JOIN 条件。
        浮点类型不参与等值 JOIN（语义不稳定）。
        找不到兼容列时返回 None，调用方降级到单表或 ON 1=1。
        """
        c1 = self._pick_join_col(cols1)
        c2 = self._pick_compat_col(cols2, c1)
        if c1 is None or c2 is None:
            return None
        return f"`{a1}`.`{c1.name}` = `{a2}`.`{c2.name}`"

    def _join_on_cold(
        self,
        sub_alias: str, sub_cols: List[Any],
        a2: str,        cols2: List[Any],
    ) -> Optional[str]:
        """派生表与真实表的 JOIN ON（sub_cols 是派生表暴露的列）。"""
        return self._join_on(sub_alias, sub_cols, a2, cols2)

    def _pick_join_col(self, cols: List[Any]) -> Optional[Any]:
        """选适合做 JOIN 键的列（INT/VARCHAR/TINYINT，非浮点）。"""
        safe = [c for c in cols if c.data_type not in _NO_JOIN_TYPES]
        return random.choice(safe) if safe else None

    def _pick_compat_col(self, cols: List[Any], ref: Optional[Any]) -> Optional[Any]:
        """从 cols 中找与 ref 类型兼容的列。"""
        if ref is None:
            return None
        exact = [c for c in cols if c.data_type == ref.data_type]
        if exact:
            return random.choice(exact)
        # 宽松兼容：INT 族互通
        if ref.data_type == 'INT':
            loose = [c for c in cols if c.data_type in ('INT', 'TINYINT', 'BIGINT', 'SMALLINT')]
            if loose:
                return random.choice(loose)
        return None

    # ─────────────────────────────────────────
    # 热值 & 别名工具
    # ─────────────────────────────────────────

    def _hot(self, col: Any) -> Optional[str]:
        """
        尝试从 skew_hot_values 中取一个热值（SQL literal 形式）。
        遍历所有表的热值字典，按列名匹配（不要求表名精确对应）。
        """
        for _tname, col_map in self.hot_values.items():
            vals = col_map.get(col.name)
            if not vals:
                continue
            candidate = random.choice(vals)
            # 类型兼容检查：带引号的是字符串字面量，不能给数值列用
            is_quoted = candidate.startswith("'")
            if col.data_type in ('INT', 'FLOAT', 'DOUBLE', 'DECIMAL') and is_quoted:
                return None
            if col.data_type == 'VARCHAR' and not is_quoted and candidate != 'NULL':
                return None
            return candidate
        return None

    def _alias(self, base: str) -> str:
        """
        生成唯一的表别名：取 base 的前 3 个字母数字字符 + 自增计数器。
        避免与 SQL 关键字冲突（alias 本身不含关键字）。
        """
        self._ctr += 1
        safe = re.sub(r'[^a-z0-9]', '', base.lower())[:3] or 'tbl'
        return f"{safe}{self._ctr}"