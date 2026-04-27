"""
oracle/vertical_oracle.py

VerticalOracle: 基于 schema 垂直切分的 MySQL 逻辑 Bug 检测 Oracle

设计思路：
  S1 和 S2 持有相同数据，但 schema 结构不同（或部分数据填充方式不同）。
  对 S1 和 S2 执行相同的 SELECT 查询，验证结果是否符合预期关系。
  S1 / S2 是同一个 DB 里的两张独立表，共用同一个连接。

变体分类：
  等价类 (ABCD) — 断言 S1 结果 == S2 结果
    A1. drop_not_null  : 移除非 PK 列的 NOT NULL 约束
    A2. drop_unique    : 移除 UNIQUE KEY 约束
    B.  widen_types    : 列类型宽化（INT→BIGINT, VARCHAR(n)→VARCHAR(2n), DECIMAL(p,s)→DECIMAL(p+4,s)）
    C.  add_index      : S2 新增二级索引（触发 plan 变化）
    D.  add_column     : S2 新增一列 nullable INT

  单调类 (E) — 断言聚合结果满足单调性（S1 数据 ⊆ S2 数据，信息量 S1 < S2）
    E.  null_to_value  : S1 部分 Φ 列含 NULL，S2 将其填充为具体值
        COUNT(col) S1≤S2, MAX(col) S1≤S2, MIN(col) S1≥S2, COUNT(DISTINCT col) S1≤S2
        COUNT(*) S1==S2（行数不变）

Plan change 策略：
  - 等价类：C 变体新增索引是最直接的触发方式；A1 影响 NOT NULL 优化路径；B 影响类型推导
  - 单调类：E 变体 null_fraction 从高变低，ANALYZE 后优化器感知差异
  - 等价类对 plan change 不依赖，所有查询均验证；UNCHANGED_PLAN_VERIFY_PROB 设为 1.0
"""

import os
import re
import random
import hashlib
import uuid
import pymysql
import time
from decimal import Decimal
from collections import Counter
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Set, Tuple
from datetime import datetime

from oracle.vertical_query_gen import VerticalQueryGenerator
from data_structures.db_dialect import get_current_dialect, set_current_dialect


# ─────────────────────────────────────────────
# 常量配置
# ─────────────────────────────────────────────
TARGET_BASELINE_QUERIES = 6
MIN_BASELINE_QUERIES    = 3
MAX_QUERY_GEN_ATTEMPTS  = 120
BASELINE_ROWS           = 20
NOISE_ROWS              = 8
HOT_SEED_ROWS           = 4
FLOAT_TOLERANCE         = 1e-9
MONO_COUNT              = 'count'
MONO_MAX                = 'max'
MONO_MIN                = 'min'
MONO_COUNT_DISTINCT     = 'count_distinct'

# Mutation 类型常量
MUT_DROP_NOT_NULL = 'drop_not_null'   # A1
MUT_DROP_UNIQUE   = 'drop_unique'     # A2
MUT_WIDEN_TYPES   = 'widen_types'     # B
MUT_ADD_INDEX     = 'add_index'       # C
MUT_ADD_COLUMN    = 'add_column'      # D
MUT_NULL_TO_VALUE = 'null_to_value'   # E

EQUALITY_MUTATIONS = frozenset({
    MUT_DROP_NOT_NULL,
    # MUT_DROP_UNIQUE 暂时禁用：create_sample_tables() 只生成 PK/FK，不含 UNIQUE 约束，
    # A2 mutation 对现有 DDL 无效。待后续在 S1 DDL 中注入 UNIQUE 约束后重新启用。
    MUT_WIDEN_TYPES,
    MUT_ADD_INDEX,
    MUT_ADD_COLUMN,
})
MONOTONE_MUTATIONS = frozenset({MUT_NULL_TO_VALUE})

# 按方言区分的可忽略运行时错误
_RUNTIME_ERROR_CODES: Dict[str, set] = {
    'mysql':   {1365, 1690},
    'mariadb': {1365, 1690, 1292},
    'percona': {1365, 1690},
}
_RUNTIME_ERROR_PATTERNS: Dict[str, tuple] = {
    'mysql':   ('double value is out of range', 'bigint value is out of range',
                'decimal value is out of range', 'division by 0', 'division by zero'),
    'mariadb': ('double value is out of range', 'bigint value is out of range',
                'decimal value is out of range', 'division by 0', 'division by zero'),
    'percona': ('double value is out of range', 'bigint value is out of range',
                'decimal value is out of range', 'division by 0', 'division by zero'),
}

from pymysql.constants import FIELD_TYPE as _FT

# pymysql 数值字段类型集合（用于双重过滤：名字匹配 + 字段类型校验）
_PYMYSQL_NUMERIC_FIELD_TYPES = frozenset({
    _FT.TINY, _FT.SHORT, _FT.LONG, _FT.FLOAT, _FT.DOUBLE,
    _FT.LONGLONG, _FT.INT24, _FT.DECIMAL, _FT.NEWDECIMAL,
})

_STRING_LIKE_TYPES = frozenset({
    'VARCHAR', 'TEXT', 'LONGTEXT', 'CHAR', 'TINYTEXT', 'MEDIUMTEXT', 'ENUM', 'SET',
})
_TEMPORAL_TYPES = frozenset({'DATE', 'DATETIME', 'TIMESTAMP', 'TIME', 'YEAR'})


# ─────────────────────────────────────────────
# 数据类
# ─────────────────────────────────────────────
@dataclass
class ColDef:
    name:           str
    data_type:      str        # 'INT' / 'VARCHAR' / 'FLOAT' / 'DOUBLE' / 'DECIMAL' / 'DATE' / 'OPAQUE'
    declared_type:  str  = ''
    is_primary_key: bool = False
    is_nullable:    bool = True
    varchar_len:    int  = 128
    is_indexed:     bool = False


@dataclass
class VertQuerySpec:
    s1_sql:           str
    s2_sql:           str          # s1_sql.replace(s1_name, s2_name)
    mutations:        List[str]
    monotone_metrics: Dict[str, Set[str]] = field(default_factory=dict)


@dataclass
class VertSnapshot:
    count:          Optional[int]              = None   # COUNT(*)
    col_counts:     Dict[str, Optional[int]]   = field(default_factory=dict)  # COUNT(col) for Φ cols
    max_values:     Dict[str, Optional[float]] = field(default_factory=dict)
    min_values:     Dict[str, Optional[float]] = field(default_factory=dict)
    count_distinct: Dict[str, Optional[int]]   = field(default_factory=dict)  # COUNT(DISTINCT col)
    row_digests:    Dict[str, int]             = field(default_factory=dict)
    explain_plan:   List[str]                  = field(default_factory=list)


@dataclass
class VertSkewProfile:
    hot_values_by_col:    Dict[str, List[str]] = field(default_factory=dict)
    medium_values_by_col: Dict[str, List[str]] = field(default_factory=dict)


class IgnorableQueryRuntimeError(Exception):
    pass


# ═══════════════════════════════════════════════════════════════
class VerticalOracle:

    def __init__(self, db_config: dict, verbose: bool = True,
                 log_sql: bool = False, log_file: str = None,
                 enable_known_mysql_date_index_string_eq_workaround: Optional[bool] = None):
        self.db_config   = db_config
        self.verbose     = verbose
        self.log_sql     = log_sql
        self.log_file    = log_file
        self.enable_known_mysql_date_index_string_eq_workaround = (
            enable_known_mysql_date_index_string_eq_workaround
        )
        self._log_dir    = None
        self._sql_log:   List[str] = []
        self._dialect    = None
        self._ignorable_codes    = _RUNTIME_ERROR_CODES['mysql']
        self._ignorable_patterns = _RUNTIME_ERROR_PATTERNS['mysql']

        self.total_rounds  = 0
        self.total_queries = 0
        self.total_bugs    = 0

    # ──────────────────────────────────────────
    # 公共入口
    # ──────────────────────────────────────────
    def run(self) -> dict:
        uid = uuid.uuid4().hex[:8]
        self._sql_log = []

        db_type = self.db_config.get('db_type', 'MYSQL').upper()
        set_current_dialect(db_type)
        self._dialect = get_current_dialect()

        family = self._dialect.optimizer_family()
        self._ignorable_codes    = _RUNTIME_ERROR_CODES.get(family,    _RUNTIME_ERROR_CODES['mysql'])
        self._ignorable_patterns = _RUNTIME_ERROR_PATTERNS.get(family, _RUNTIME_ERROR_PATTERNS['mysql'])
        if self.enable_known_mysql_date_index_string_eq_workaround is None:
            self._workaround_date = (family == 'mysql')
        else:
            self._workaround_date = self.enable_known_mysql_date_index_string_eq_workaround

        round_stats = {
            'round_id': uid, 'mutations': [],
            'queries': 0, 'bugs': 0, 'skipped': False,
        }

        self._log_dir = os.path.join('invalid_mutation', db_type)
        os.makedirs(self._log_dir, exist_ok=True)

        self._log(f"\n{'='*60}")
        self._log(f" VERTICAL ORACLE round #{uid}")
        self._log(f"{'='*60}")

        conn = self._connect()
        if conn is None:
            self._log("  [SKIP] Cannot connect to database.")
            round_stats['skipped'] = True
            return round_stats

        from generate_random_sql import create_sample_tables, generate_create_table_sql
        vs_tables = create_sample_tables()
        main_vs   = vs_tables[0]

        s1_name   = f"vert_s1_{uid}"
        s2_name   = f"vert_s2_{uid}"
        aux_names = {tbl.name: f"vert_ref_{uid}_{tbl.name}" for tbl in vs_tables[1:]}
        all_names = [s1_name, s2_name] + list(aux_names.values())

        try:
            # ── Step 1a：选择 mutation 组合 ───────────────
            mutations = self._choose_mutations()
            round_stats['mutations'] = mutations
            has_monotone = any(m in MONOTONE_MUTATIONS for m in mutations)
            self._log(f"\n[Step 1] Mutations chosen: {mutations}")
            self._log(f"  Oracle type: {'MONOTONE (E)' if has_monotone else 'EQUALITY (ABCD)'}")

            # ── Step 2a：创建 S1 及辅助表 ─────────────────
            self._log(f"\n[Step 2] Creating S1 and auxiliary tables ...")
            main_cols    = self._vs_table_to_coldefs(main_vs)
            numeric_cols = [c for c in main_cols
                            if c.data_type in ('INT', 'FLOAT', 'DOUBLE', 'DECIMAL')]
            pk_col       = next((c for c in main_cols if c.is_primary_key), main_cols[0])

            s1_ddl = generate_create_table_sql(main_vs)
            s1_ddl = s1_ddl.replace(f"CREATE TABLE {main_vs.name}",
                                    f"CREATE TABLE IF NOT EXISTS {s1_name}")
            self._exec_ddl(conn, s1_ddl)

            for tbl in vs_tables[1:]:
                aux_ddl = generate_create_table_sql(tbl)
                aux_ddl = aux_ddl.replace(f"CREATE TABLE {tbl.name}",
                                          f"CREATE TABLE IF NOT EXISTS {aux_names[tbl.name]}")
                self._exec_ddl(conn, aux_ddl)

            # ── Step 2b：填充 S1 数据 ─────────────────────
            self._log(f"\n[Step 3] Populating S1 ...")
            phi_cols: List[ColDef] = []
            if has_monotone:
                phi_cols = self._choose_phi_cols(main_cols)
                if not phi_cols:
                    self._log("  [SKIP] No suitable phi cols for E mutation.")
                    round_stats['skipped'] = True
                    return round_stats
                self._log(f"  Phi cols (E mutation): {[c.name for c in phi_cols]}")

            skew = self._create_skew_profile(main_cols)
            self._insert_s1_data(conn, s1_name, main_cols, phi_cols, skew)
            self._insert_aux_data(conn, vs_tables, aux_names)

            s1_count = self._exec_single_int(conn, f"SELECT COUNT(*) FROM {s1_name}")
            if not s1_count:
                self._log("  [SKIP] S1 is empty after insertion.")
                round_stats['skipped'] = True
                return round_stats
            self._log(f"  S1 rows: {s1_count}")

            # ── Step 3a：生成 S2 DDL 并创建 S2 ───────────
            self._log(f"\n[Step 4] Creating S2 ...")
            s2_ddl = self._build_s2_ddl(s1_ddl, mutations, pk_col.name, s1_name, s2_name)
            self._exec_ddl(conn, s2_ddl)

            # ── Step 3b：填充 S2 数据 ─────────────────────
            self._populate_s2(conn, s1_name, s2_name, main_cols, phi_cols, mutations)

            # ── Step 3c：C 变体 — 新增索引 ────────────────
            indexed_col_names: Set[str] = set()
            if MUT_ADD_INDEX in mutations:
                indexed_col_names = self._add_s2_indexes(conn, s2_name, main_cols, uid)
                for c in main_cols:
                    c.is_indexed = c.name in indexed_col_names
                self._log(f"  C: secondary indexes added to S2 on {sorted(indexed_col_names)}")

            s2_count = self._exec_single_int(conn, f"SELECT COUNT(*) FROM {s2_name}")
            self._log(f"  S2 rows: {s2_count}")

            # ── Step 4a：ANALYZE ─────────────────────────
            self._log(f"\n[Step 5] ANALYZE TABLE ...")
            self._analyze_table(conn, s1_name)
            self._analyze_table(conn, s2_name)

            # ── Step 4b：生成查询，收集 S1 快照 ───────────
            self._log(f"\n[Step 6] Generating baseline queries on S1 ...")
            shared_tables = [(s1_name, main_cols)] + [
                (aux_names[tbl.name], self._vs_table_to_coldefs(tbl))
                for tbl in vs_tables[1:]
            ]
            # TODO (Medium): 等价类 (ABCD) 应使用 valscope 原生生成器以覆盖
            # GROUP BY / LEFT JOIN / HAVING 等 pattern 仍然被当前 vertical generator 禁止。
            # 目前统一使用 VerticalQueryGenerator，等价验证正确性不受影响，只是覆盖面仍偏窄。
            gen = VerticalQueryGenerator(
                tables=shared_tables,
                skew_hot_values={s1_name: skew.hot_values_by_col},
                dialect=self._dialect,
                enable_known_mysql_date_index_string_eq_workaround=self._workaround_date,
            )
            phi_col_names: Set[str] = {c.name for c in phi_cols}
            baselines: List[Tuple[VertQuerySpec, VertSnapshot, Optional[VertSnapshot]]] = []

            for _ in range(MAX_QUERY_GEN_ATTEMPTS):
                if len(baselines) >= TARGET_BASELINE_QUERIES:
                    break
                sql = gen.generate()
                if not sql:
                    continue

                # 收集 S1 快照
                snap = self._execute_snapshot(
                    conn, sql, numeric_cols,
                    include_digests=(not has_monotone),
                    phi_col_list=phi_cols,
                )
                if snap is None or not snap.count:
                    continue

                # E 变体：在 baseline 阶段直接跑一次 S2 快照。
                # 这样既能筛掉明显不满足单调前提的 query，也能把 S2 snapshot 复用到 Step 7。
                s2_snap_check: Optional[VertSnapshot] = None
                metric_mask: Dict[str, Set[str]] = {}
                if has_monotone:
                    s2_sql_check = sql.replace(s1_name, s2_name)
                    s2_snap_check = self._execute_snapshot(
                        conn, s2_sql_check, numeric_cols,
                        include_digests=False,
                        phi_col_list=phi_cols,
                    )
                    if s2_snap_check is None or s2_snap_check.count != snap.count:
                        continue
                    metric_mask = self._derive_monotone_metric_mask(
                        snap, s2_snap_check, phi_cols)
                    if not any(metric_mask.values()):
                        continue

                s2_sql = sql.replace(s1_name, s2_name)
                spec = VertQuerySpec(
                    s1_sql=sql,
                    s2_sql=s2_sql,
                    mutations=mutations,
                    monotone_metrics=metric_mask,
                )
                baselines.append((spec, snap, s2_snap_check))
                self._log(f"  [query] {sql[:80]}...")

            self._log(f"  Collected {len(baselines)}/{TARGET_BASELINE_QUERIES} queries.")
            if len(baselines) < MIN_BASELINE_QUERIES:
                self._log("  [SKIP] Not enough valid baseline queries.")
                round_stats['skipped'] = True
                return round_stats

            # ── Step 4c/4d：收集 S2 快照，执行验证 ────────
            self._log(f"\n[Step 7] Verifying S1 vs S2 ...")
            for i, (spec, s1_snap, cached_s2_snap) in enumerate(baselines):
                s2_snap = cached_s2_snap
                if s2_snap is None:
                    s2_snap = self._execute_snapshot(
                        conn, spec.s2_sql, numeric_cols,
                        include_digests=(not has_monotone),
                        phi_col_list=phi_cols,
                    )
                if s2_snap is None:
                    self._log(f"  Query[{i+1}] skipped (S2 runtime error)")
                    continue

                s2_plan = self._capture_explain(conn, spec.s2_sql)
                s2_snap.explain_plan = s2_plan
                plan_changed = not self._plans_equivalent(s1_snap.explain_plan, s2_plan)
                self._log(f"  Query[{i+1}] plan_changed={plan_changed}")
                self._log(f"  Query[{i+1}] S1 SQL: {spec.s1_sql[:70]}...")
                self._log(f"  Query[{i+1}] Plan S1: {self._fmt_plan(s1_snap.explain_plan)}")
                self._log(f"  Query[{i+1}] Plan S2: {self._fmt_plan(s2_plan)}")

                try:
                    if has_monotone:
                        self._verify_monotonicity(spec, s1_snap, s2_snap, phi_cols)
                    else:
                        self._verify_equality(conn, spec, s1_snap, s2_snap, numeric_cols)
                    round_stats['queries'] += 1
                except IgnorableQueryRuntimeError as e:
                    self._log(f"  Query[{i+1}] skipped during verification: {e}")
                except AssertionError as e:
                    round_stats['queries'] += 1
                    round_stats['bugs'] += 1
                    self._log_bug(str(e), spec, s1_snap, s2_snap, uid)

            self._log(f"\n  All checks PASSED for round #{uid}")

        except Exception as e:
            self._log(f"  [ERROR] round #{uid}: {e}")
            import traceback; traceback.print_exc()
        finally:
            for name in all_names:
                self._drop_if_exists(conn, name)
            conn.close()
            self._sql_log = []
            self._log(f"{'='*60}\n")

        if not round_stats['skipped']:
            self.total_rounds  += 1
            self.total_queries += round_stats['queries']
            self.total_bugs    += round_stats['bugs']

        return round_stats

    # ──────────────────────────────────────────
    # Step 1a：选择 mutation 组合
    # ──────────────────────────────────────────
    def _choose_mutations(self) -> List[str]:
        """随机选 1-3 个 mutation，至多一个单调类（E）。"""
        n = random.choices([1, 2, 3], weights=[0.60, 0.30, 0.10])[0]
        include_e = random.random() < 0.20   # 20% 概率包含 E 变体

        mutations: List[str] = []
        if include_e:
            mutations.append(MUT_NULL_TO_VALUE)
            n -= 1

        eq_pool = list(EQUALITY_MUTATIONS)
        random.shuffle(eq_pool)
        mutations.extend(eq_pool[:max(n, 0)])
        return mutations

    # ──────────────────────────────────────────
    # Step 3a：DDL 变异（A / B / D 系列）
    # ──────────────────────────────────────────
    def _build_s2_ddl(self, s1_ddl: str, mutations: List[str],
                      pk_col_name: str, s1_name: str, s2_name: str) -> str:
        """基于 S1 DDL 生成 S2 DDL，依次应用 A/B/D 系列 mutations。
        C 系列（add_index）在数据填充后单独执行 ALTER TABLE。
        E 系列（null_to_value）不改变 DDL。
        """
        ddl = s1_ddl.replace(s1_name, s2_name)

        if MUT_DROP_NOT_NULL in mutations:
            ddl = self._ddl_drop_not_null(ddl)
        if MUT_DROP_UNIQUE in mutations:
            ddl = self._ddl_drop_unique(ddl)
        if MUT_WIDEN_TYPES in mutations:
            ddl = self._ddl_widen_types(ddl, pk_col_name)
        if MUT_ADD_COLUMN in mutations:
            ddl = self._ddl_add_column(ddl)

        return ddl

    def _ddl_drop_not_null(self, ddl: str) -> str:
        """移除所有非 PK（非 AUTO_INCREMENT）列的 NOT NULL 约束。"""
        lines = ddl.split('\n')
        result = []
        for line in lines:
            stripped_upper = line.strip().upper()
            # 保留 PRIMARY KEY 行、UNIQUE 行不变
            if (stripped_upper.startswith('PRIMARY KEY') or
                    stripped_upper.startswith('UNIQUE')):
                result.append(line)
                continue
            # AUTO_INCREMENT 列（通常是 PK）不改，MySQL 要求其必须是 NOT NULL
            if 'NOT NULL' in line.upper() and 'AUTO_INCREMENT' not in line.upper():
                line = re.sub(r'\bNOT\s+NULL\b', 'NULL', line, flags=re.IGNORECASE)
            result.append(line)
        return '\n'.join(result)

    def _ddl_drop_unique(self, ddl: str) -> str:
        """移除 UNIQUE KEY / UNIQUE INDEX 行。"""
        lines = ddl.split('\n')
        result = []
        for line in lines:
            if re.match(r'\s*UNIQUE\s+(KEY|INDEX)\b', line, re.IGNORECASE):
                continue   # 跳过该行
            result.append(line)
        return '\n'.join(self._fix_trailing_commas(result))

    def _fix_trailing_commas(self, lines: List[str]) -> List[str]:
        """修复因删行产生的尾部多余逗号。"""
        result = []
        for i, line in enumerate(lines):
            # 找到下一个非空行的内容
            next_content = ''
            for j in range(i + 1, len(lines)):
                if lines[j].strip():
                    next_content = lines[j].strip()
                    break
            # 如果当前行末有逗号，而下一有效行是结束符 ')'，删除逗号
            if line.rstrip().endswith(',') and next_content.startswith(')'):
                line = line.rstrip()[:-1]
            result.append(line)
        return result

    def _ddl_widen_types(self, ddl: str, pk_col_name: str) -> str:
        """宽化列类型：INT→BIGINT, VARCHAR(n)→VARCHAR(2n cap 1024), DECIMAL(p,s)→DECIMAL(p+4,s)。"""
        lines = ddl.split('\n')
        result = []
        for line in lines:
            stripped_upper = line.strip().upper()
            # 跳过 PK 列（有 AUTO_INCREMENT）和约束行
            if ('AUTO_INCREMENT' in line.upper() or
                    stripped_upper.startswith('PRIMARY') or
                    stripped_upper.startswith('UNIQUE') or
                    stripped_upper.startswith('KEY') or
                    stripped_upper.startswith(')')):
                result.append(line)
                continue
            # INT → BIGINT（word boundary 保证不匹配 BIGINT/TINYINT 中的 INT）
            line = re.sub(r'\bINT\b', 'BIGINT', line, flags=re.IGNORECASE)
            # VARCHAR(n) → VARCHAR(min(n*2, 1024))
            def widen_varchar(m):
                return f"VARCHAR({min(int(m.group(1)) * 2, 1024)})"
            line = re.sub(r'\bVARCHAR\((\d+)\)', widen_varchar, line, flags=re.IGNORECASE)
            # DECIMAL(p,s) → DECIMAL(min(p+4, 38), s)
            def widen_decimal(m):
                p, s = int(m.group(1)), int(m.group(2))
                return f"DECIMAL({min(p + 4, 38)},{s})"
            line = re.sub(r'\bDECIMAL\((\d+),\s*(\d+)\)', widen_decimal, line, flags=re.IGNORECASE)
            result.append(line)
        return '\n'.join(result)

    def _ddl_add_column(self, ddl: str) -> str:
        """在 PRIMARY KEY 行之前插入一个额外的 nullable INT 列（D 变体）。"""
        lines = ddl.split('\n')
        result = []
        inserted = False
        for line in lines:
            if not inserted and line.strip().upper().startswith('PRIMARY KEY'):
                result.append('  `col_extra_d` INT NULL,')
                inserted = True
            result.append(line)
        if not inserted:
            # 退化情况：在结束括号前插入
            for i in range(len(result) - 1, -1, -1):
                if result[i].strip().startswith(')'):
                    result.insert(i, '  `col_extra_d` INT NULL')
                    # 给前一行加逗号
                    if i > 0 and not result[i - 1].rstrip().endswith(','):
                        result[i - 1] = result[i - 1].rstrip() + ','
                    break
        return '\n'.join(result)

    # ──────────────────────────────────────────
    # Step 2b：S1 数据插入
    # ──────────────────────────────────────────
    def _insert_s1_data(self, conn, s1_name: str, main_cols: List[ColDef],
                        phi_cols: List[ColDef], skew: VertSkewProfile):
        """填充 S1：使用偏斜分布 + 边界噪声，让优化器更容易感知热点值。"""
        phi_names = {c.name for c in phi_cols}

        for _ in range(HOT_SEED_ROWS):
            vals = []
            for c in main_cols:
                if c.is_primary_key:
                    vals.append(str(random.randint(1, 10_000_000)))
                elif c.name in phi_names and random.random() < 0.50:
                    vals.append('NULL')
                else:
                    hot_vals = skew.hot_values_by_col.get(c.name, [])
                    vals.append(random.choice(hot_vals) if hot_vals else self._random_val(c))
            self._try_insert(conn, s1_name, main_cols, vals)

        for _ in range(BASELINE_ROWS):
            vals = []
            for c in main_cols:
                if c.is_primary_key:
                    vals.append(str(random.randint(1, 10_000_000)))
                elif c.name in phi_names and random.random() < 0.50:
                    vals.append('NULL')
                else:
                    vals.append(self._skewed_random_val(c, skew))
            self._try_insert(conn, s1_name, main_cols, vals)

        # noise rows：边界值 + 更高 NULL 比例
        for _ in range(NOISE_ROWS):
            vals = []
            for c in main_cols:
                if c.is_primary_key:
                    vals.append(str(random.randint(1, 10_000_000)))
                elif c.name in phi_names:
                    vals.append('NULL')   # noise rows 中 Φ 列全为 NULL
                else:
                    vals.append(self._noise_val(c))
            self._try_insert(conn, s1_name, main_cols, vals)

    def _create_skew_profile(self, cols: List[ColDef]) -> VertSkewProfile:
        return VertSkewProfile(
            hot_values_by_col={c.name: self._create_hot_values(c) for c in cols},
            medium_values_by_col={c.name: self._create_medium_values(c) for c in cols},
        )

    def _create_hot_values(self, col: ColDef) -> List[str]:
        dt = col.data_type
        if dt == 'INT':
            base = random.randint(-16, 16)
            return [str(base), str(base + 1 + random.randint(0, 2))]
        if dt == 'VARCHAR':
            token = f"hv_{random.randint(100, 9999)}"
            return [f"'{token}'", f"'{token}_x'"]
        if dt in ('FLOAT', 'DOUBLE'):
            base = random.randint(-200, 200) / 10.0
            return [f"{base:.3f}", f"{base + 1.0:.3f}"]
        if dt == 'DECIMAL':
            base = random.randint(-1000, 1000) / 100.0
            return [f"{base:.2f}", f"{base + 1.0:.2f}"]
        if dt == 'DATE':
            return ["'1970-01-01'", "'2023-01-01'"]
        return ['NULL']

    def _create_medium_values(self, col: ColDef) -> List[str]:
        dt = col.data_type
        if dt == 'INT':
            base = random.randint(-64, 64)
            return [str(base), str(base + 7), str(base - 7)]
        if dt == 'VARCHAR':
            token = f"mv_{random.randint(100, 9999)}"
            return [f"'{token}'", f"'{token}_tail'", "'2023-01-01'"]
        if dt in ('FLOAT', 'DOUBLE'):
            base = random.randint(-500, 500) / 10.0
            return [f"{base:.3f}", f"{base + 5.0:.3f}", f"{base - 5.0:.3f}"]
        if dt == 'DECIMAL':
            base = random.randint(-5000, 5000) / 100.0
            return [f"{base:.2f}", f"{base + 5.0:.2f}", f"{base - 5.0:.2f}"]
        if dt == 'DATE':
            return ["'2000-01-01'", "'2010-06-15'", "'2024-02-29'"]
        return ['NULL']

    def _skewed_random_val(self, col: ColDef, skew: VertSkewProfile) -> str:
        if col.is_primary_key:
            return str(random.randint(1, 10_000_000))

        roll = random.random()
        if roll < 0.48:
            hot_vals = skew.hot_values_by_col.get(col.name, [])
            if hot_vals:
                return random.choice(hot_vals)
        if roll < 0.82:
            medium_vals = skew.medium_values_by_col.get(col.name, [])
            if medium_vals:
                return random.choice(medium_vals)
        return self._random_val(col)

    def _random_val(self, col: ColDef) -> str:
        dt = col.data_type
        if dt == 'INT':
            if random.random() < 0.15:
                return random.choice(['2147483647', '-2147483648', '0', '1', '-1'])
            return str(random.randint(-1000, 1000))
        if dt == 'VARCHAR':
            n = random.randint(1, min(12, col.varchar_len))
            return f"'{''.join(random.choices('abcdefghijklmnopqrstuvwxyz0123456789', k=n))}'"
        if dt in ('FLOAT', 'DOUBLE'):
            if random.random() < 0.12:
                boundary = '3.4028235E38' if dt == 'FLOAT' else '1.7976931348623157E308'
                return random.choice([boundary, f"-{boundary}", '0.0', '1.0', '-1.0'])
            return f"{random.uniform(-1000, 1000):.3f}"
        if dt == 'DECIMAL':
            if random.random() < 0.12:
                return random.choice(['99999999.99', '-99999999.99', '0.00', '1.00', '-1.00'])
            return f"{random.uniform(-1000, 1000):.2f}"
        if dt == 'DATE':
            if random.random() < 0.15:
                return random.choice(["'1000-01-01'", "'1970-01-01'", "'2024-02-29'", "'9999-12-31'"])
            return (f"'{random.randint(2000, 2023)}-"
                    f"{random.randint(1, 12):02d}-{random.randint(1, 28):02d}'")
        return 'NULL'

    def _noise_val(self, col: ColDef) -> str:
        """生成边界值或 NULL，用于 noise rows。"""
        dt = col.data_type
        boundary = {
            'INT':     ['0', '1', '-1', '2147483647', '-2147483648', 'NULL'],
            'VARCHAR': ["''", "'NULL'", "'0'", "'1'", "'%'", "'_'", "'2023-01-01'", 'NULL'],
            'FLOAT':   ['0', '0.0', '-0.0', '1.0', '-1.0', '3.4028235E38', '-3.4028235E38', 'NULL'],
            'DOUBLE':  ['0', '0.0', '-0.0', '1.0', '-1.0', '1.7976931348623157E308', '-1.7976931348623157E308', 'NULL'],
            'DECIMAL': ['0.00', '1.00', '-1.00', '99999999.99', '-99999999.99', 'NULL'],
            'DATE':    ["'1000-01-01'", "'1970-01-01'", "'2024-02-29'", "'9999-12-31'", 'NULL'],
        }
        candidates = list(boundary.get(dt, ['NULL']))
        if not col.is_nullable:
            candidates = [v for v in candidates if v != 'NULL']
        return random.choice(candidates or ['NULL'])

    def _insert_aux_data(self, conn, vs_tables, aux_names: Dict[str, str]):
        from generate_random_sql import generate_insert_sql
        primary_keys_dict = {tbl.name: list(range(1, 21)) for tbl in vs_tables}
        for tbl in vs_tables[1:]:
            actual = aux_names[tbl.name]
            try:
                sql = generate_insert_sql(
                    tbl, num_rows=10,
                    existing_primary_keys=primary_keys_dict,
                    primary_key_values=list(range(1, 11)),
                )
                for line in sql.strip().split('\n'):
                    line = line.strip()
                    if not line:
                        continue
                    line = (line
                            .replace(f"INSERT INTO {tbl.name}", f"INSERT IGNORE INTO {actual}")
                            .replace(f"INSERT  INTO {tbl.name}", f"INSERT IGNORE INTO {actual}"))
                    try:
                        self._exec_dml(conn, line.rstrip(';'))
                    except Exception as e:
                        self._log(f"  aux insert skipped: {e}")
            except Exception as e:
                self._log(f"  aux data gen failed for {actual}: {e}")

    # ──────────────────────────────────────────
    # Step 3b：S2 数据填充
    # ──────────────────────────────────────────
    def _populate_s2(self, conn, s1_name: str, s2_name: str,
                     main_cols: List[ColDef], phi_cols: List[ColDef],
                     mutations: List[str]):
        """将 S1 数据复制到 S2（仅共有列）。若含 E 变体，将 Φ 列的 NULL 填充为具体值。"""
        # D 变体 S2 多一列 col_extra_d，只复制共有列，col_extra_d 自动为 NULL
        shared_col_names = ', '.join(f'`{c.name}`' for c in main_cols)
        self._exec_dml(conn,
            f"INSERT INTO {s2_name} ({shared_col_names}) "
            f"SELECT {shared_col_names} FROM {s1_name}")
        self._log(f"  S2 data copied from S1 ({shared_col_names[:60]}...)")

        if MUT_NULL_TO_VALUE in mutations:
            for col in phi_cols:
                concrete = self._concrete_fill_value(col)
                self._exec_dml(conn,
                    f"UPDATE {s2_name} SET `{col.name}` = {concrete} "
                    f"WHERE `{col.name}` IS NULL")
            self._log(f"  E: filled Φ cols {[c.name for c in phi_cols]} with concrete values")

    def _concrete_fill_value(self, col: ColDef) -> str:
        """为 E 变体生成填充 NULL 的具体值（取值要在 S1 已有值域之外以避免 trivial 通过）。"""
        dt = col.data_type
        if dt == 'INT':
            if random.random() < 0.5:
                return random.choice(['2147483647', '2147483646', '999999999'])
            return random.choice(['-2147483648', '-2147483647', '-999999999'])
        if dt == 'VARCHAR': return f"'fill_{random.randint(100, 999)}'"
        if dt == 'FLOAT':
            return random.choice(['3.4028235E38', '-3.4028235E38', '99999.000', '-99999.000'])
        if dt == 'DOUBLE':
            return random.choice(['1.7976931348623157E308', '-1.7976931348623157E308', '999999.000', '-999999.000'])
        if dt == 'DECIMAL':
            return random.choice(['99999999.99', '-99999999.99', '9999.99', '-9999.99'])
        return '1'

    # ──────────────────────────────────────────
    # Step 3c：C 变体 — 新增索引
    # ──────────────────────────────────────────
    def _add_s2_indexes(self, conn, s2_name: str, main_cols: List[ColDef], uid: str) -> Set[str]:
        """为 S2 新增单列索引（必选）和复合索引（可选），触发 plan change。"""
        candidates = [
            c for c in main_cols
            if not c.is_primary_key
            and c.data_type in ('INT', 'FLOAT', 'DOUBLE', 'DECIMAL', 'VARCHAR', 'DATE')
        ]
        if not candidates:
            return set()

        # 单列索引（必选）
        def score(col: ColDef) -> int:
            base = 0
            if col.data_type in ('INT', 'VARCHAR', 'DATE'):
                base += 5
            elif col.data_type in ('DECIMAL', 'FLOAT', 'DOUBLE'):
                base += 3
            if not col.is_nullable:
                base += 1
            return base

        candidates = sorted(candidates, key=score, reverse=True)
        top_k = candidates[:max(2, min(4, len(candidates)))]
        col = random.choice(top_k)
        chosen = {col.name}
        idx_expr = (f"`{col.name}`(32)" if col.data_type == 'VARCHAR'
                    else f"`{col.name}`")
        self._exec_ddl(conn,
            f"CREATE INDEX i_vert_{uid}_s ON {s2_name} ({idx_expr})",
            ignore_error=True)

        # 复合索引（选两列，50% 概率）
        secondary_pool = [c for c in top_k if c.name != col.name]
        if secondary_pool and random.random() < 0.50:
            c1, c2 = col, random.choice(secondary_pool)
            chosen.add(c2.name)
            e1 = f"`{c1.name}`(32)" if c1.data_type == 'VARCHAR' else f"`{c1.name}`"
            e2 = f"`{c2.name}`(32)" if c2.data_type == 'VARCHAR' else f"`{c2.name}`"
            self._exec_ddl(conn,
                f"CREATE INDEX i_vert_{uid}_c ON {s2_name} ({e1}, {e2})",
                ignore_error=True)
        return chosen

    # ──────────────────────────────────────────
    # E 变体辅助
    # ──────────────────────────────────────────
    def _choose_phi_cols(self, main_cols: List[ColDef]) -> List[ColDef]:
        """选择 E 变体要 NULL→值填充的列集合 Φ。只选 nullable 的数值/字符串列。"""
        candidates = [
            c for c in main_cols
            if not c.is_primary_key
            and c.is_nullable
            and c.data_type in ('INT', 'FLOAT', 'DOUBLE', 'DECIMAL')
        ]
        if not candidates:
            return []
        k = min(random.randint(1, 2), len(candidates))
        return random.sample(candidates, k)
    # 注：E 变体的 query 兼容性不再用正则静态过滤（_is_compatible_with_e 已移除），
    # 改为在 baseline 收集阶段对每条查询实证检验 COUNT(*) 的 S1==S2 不变性。
    # 这能正确处理限定引用、COALESCE、CASE、IS NOT NULL 等所有情形。

    # ──────────────────────────────────────────
    # 快照收集
    # ──────────────────────────────────────────
    def _execute_snapshot(self, conn, sql: str, numeric_cols: List[ColDef],
                          include_digests: bool = True,
                          phi_col_list: List['ColDef'] = None) -> Optional['VertSnapshot']:
        """执行 sql，收集 COUNT(*)、MAX/MIN、phi col 聚合、行摘要、EXPLAIN。

        phi_col_list: E 变体的 Φ 列完整 ColDef 列表。
          - COUNT(col) 和 COUNT(DISTINCT col) 对所有类型（含 VARCHAR）收集
          - MAX/MIN 仅对数值类型收集
          - 非数值类型的 MAX/MIN 不收集，_verify_monotonicity 会安全跳过
        """
        if phi_col_list is None:
            phi_col_list = []
        snap = VertSnapshot()
        try:
            wrap = f"({sql}) AS _w"

            snap.count = self._exec_single_int(conn, f"SELECT COUNT(*) FROM {wrap}")
            if snap.count is None:
                return None

            result_numeric = self._result_numeric_cols(conn, sql, numeric_cols)
            for c in result_numeric:
                snap.max_values[c.name] = self._exec_single_float(
                    conn, f"SELECT MAX(`{c.name}`) FROM {wrap}")
                snap.min_values[c.name] = self._exec_single_float(
                    conn, f"SELECT MIN(`{c.name}`) FROM {wrap}")

            # E 变体：对 phi cols 单独收集聚合，独立于 numeric_cols 路径
            # 先拿结果列名集合（不依赖 FIELD_TYPE，phi cols 可能是 VARCHAR）
            if phi_col_list:
                try:
                    with conn.cursor() as cur:
                        cur.execute(f"SELECT * FROM ({sql}) AS _phi_sc LIMIT 0")
                        result_names = {desc[0] for desc in (cur.description or [])}
                except Exception:
                    result_names = set()

                for c in phi_col_list:
                    if c.name not in result_names:
                        continue
                    # COUNT(col) 和 COUNT DISTINCT：所有类型通用
                    snap.col_counts[c.name] = self._exec_single_int(
                        conn, f"SELECT COUNT(`{c.name}`) FROM {wrap}")
                    snap.count_distinct[c.name] = self._exec_single_int(
                        conn, f"SELECT COUNT(DISTINCT `{c.name}`) FROM {wrap}")
                    # MAX/MIN：只对数值类型有意义
                    if c.data_type in ('INT', 'FLOAT', 'DOUBLE', 'DECIMAL'):
                        if c.name not in snap.max_values:   # 避免与 numeric_cols 重复
                            snap.max_values[c.name] = self._exec_single_float(
                                conn, f"SELECT MAX(`{c.name}`) FROM {wrap}")
                        if c.name not in snap.min_values:
                            snap.min_values[c.name] = self._exec_single_float(
                                conn, f"SELECT MIN(`{c.name}`) FROM {wrap}")

            if include_digests and snap.count <= 10000:
                snap.row_digests = self._capture_row_digests(conn, sql)

            snap.explain_plan = self._capture_explain(conn, sql)

        except IgnorableQueryRuntimeError:
            return None
        except Exception as e:
            self._log(f"  snapshot failed: {e}")
            return None
        return snap

    def _derive_monotone_metric_mask(self, s1: VertSnapshot, s2: VertSnapshot,
                                     phi_cols: List[ColDef]) -> Dict[str, Set[str]]:
        """Only keep E metrics whose direction already matches in baseline S1/S2 results."""
        metric_mask: Dict[str, Set[str]] = {}
        for col in phi_cols:
            name = col.name
            allowed: Set[str] = set()

            c1, c2 = s1.col_counts.get(name), s2.col_counts.get(name)
            if c1 is not None and c2 is not None and c1 <= c2:
                allowed.add(MONO_COUNT)

            mx1, mx2 = s1.max_values.get(name), s2.max_values.get(name)
            if mx1 is not None and mx2 is not None and mx1 <= mx2 + FLOAT_TOLERANCE:
                allowed.add(MONO_MAX)

            mn1, mn2 = s1.min_values.get(name), s2.min_values.get(name)
            if mn1 is not None and mn2 is not None and mn2 <= mn1 + FLOAT_TOLERANCE:
                allowed.add(MONO_MIN)

            cd1, cd2 = s1.count_distinct.get(name), s2.count_distinct.get(name)
            if cd1 is not None and cd2 is not None and cd1 <= cd2:
                allowed.add(MONO_COUNT_DISTINCT)

            metric_mask[name] = allowed
        return metric_mask

    def _result_numeric_cols(self, conn, sql: str,
                              numeric_cols: List[ColDef]) -> List[ColDef]:
        """过滤出实际出现在查询结果列中的 numeric_cols，使用字段类型双重校验。
        名字匹配防止同名非数值列误入，FIELD_TYPE 校验防止派生表别名列误判。
        """
        try:
            with conn.cursor() as cur:
                cur.execute(f"SELECT * FROM ({sql}) AS _sc LIMIT 0")
                if not cur.description:
                    return []
                # desc: (name, type_code, ...) — 取首次出现的名字，避免 JOIN 产生的重名
                result_col_map: Dict[str, int] = {}
                for desc in cur.description:
                    col_name, type_code = desc[0], desc[1]
                    if col_name not in result_col_map:
                        result_col_map[col_name] = type_code
            return [
                c for c in numeric_cols
                if c.name in result_col_map
                and result_col_map[c.name] in _PYMYSQL_NUMERIC_FIELD_TYPES
            ]
        except Exception:
            return []

    # ──────────────────────────────────────────
    # 验证：等价类（ABCD）
    # ──────────────────────────────────────────
    def _verify_equality(self, conn, spec: VertQuerySpec,
                         s1: VertSnapshot, s2: VertSnapshot,
                         numeric_cols: List[ColDef]):
        """ABCD 变体：S1 和 S2 的所有查询结果必须完全相等。"""
        # COUNT(*)
        if s1.count is not None and s2.count is not None:
            if s1.count != s2.count:
                raise AssertionError(
                    f"COUNT(*) equality violation: S1={s1.count} != S2={s2.count}\n"
                    f"  S1: {spec.s1_sql}\n  S2: {spec.s2_sql}\n"
                    f"  Plan S1: {self._fmt_plan(s1.explain_plan)}\n"
                    f"  Plan S2: {self._fmt_plan(s2.explain_plan)}")
            self._log(f"  COUNT(*) {s1.count} == {s2.count}  [PASS]")

        # MAX / MIN per numeric col（浮点容差）
        for c in numeric_cols:
            v1, v2 = s1.max_values.get(c.name), s2.max_values.get(c.name)
            if v1 is not None and v2 is not None:
                if abs(v1 - v2) > FLOAT_TOLERANCE:
                    raise AssertionError(
                        f"MAX({c.name}) equality violation: S1={v1} != S2={v2}\n"
                        f"  S1: {spec.s1_sql}\n  S2: {spec.s2_sql}\n"
                        f"  Plan S1: {self._fmt_plan(s1.explain_plan)}\n"
                        f"  Plan S2: {self._fmt_plan(s2.explain_plan)}")
                self._log(f"  MAX({c.name}) {v1} == {v2}  [PASS]")

            v1, v2 = s1.min_values.get(c.name), s2.min_values.get(c.name)
            if v1 is not None and v2 is not None:
                if abs(v1 - v2) > FLOAT_TOLERANCE:
                    raise AssertionError(
                        f"MIN({c.name}) equality violation: S1={v1} != S2={v2}\n"
                        f"  S1: {spec.s1_sql}\n  S2: {spec.s2_sql}\n"
                        f"  Plan S1: {self._fmt_plan(s1.explain_plan)}\n"
                        f"  Plan S2: {self._fmt_plan(s2.explain_plan)}")
                self._log(f"  MIN({c.name}) {v1} == {v2}  [PASS]")

        # 行摘要 multiset 完全相等
        if s1.row_digests or s2.row_digests:
            if Counter(s1.row_digests) != Counter(s2.row_digests):
                missing_in_s2 = {k: v for k, v in s1.row_digests.items()
                                 if Counter(s1.row_digests)[k] > Counter(s2.row_digests).get(k, 0)}
                raise AssertionError(
                    f"Row digest equality violation: "
                    f"|S1|={sum(s1.row_digests.values())} "
                    f"|S2|={sum(s2.row_digests.values())} "
                    f"missing_digests={len(missing_in_s2)}\n"
                    f"  S1: {spec.s1_sql}\n  S2: {spec.s2_sql}\n"
                    f"  Plan S1: {self._fmt_plan(s1.explain_plan)}\n"
                    f"  Plan S2: {self._fmt_plan(s2.explain_plan)}")
            self._log(f"  Row digest multiset  [PASS]")

    # ──────────────────────────────────────────
    # 验证：单调类（E）
    # ──────────────────────────────────────────
    def _verify_monotonicity(self, spec: VertQuerySpec,
                              s1: VertSnapshot, s2: VertSnapshot,
                              phi_cols: List[ColDef]):
        """E 变体：COUNT(*) 等价，Φ 列上的 COUNT/MAX/MIN/COUNT DISTINCT 满足单调性。"""
        # COUNT(*) 等价（行数不变）
        if s1.count is not None and s2.count is not None:
            if s1.count != s2.count:
                raise AssertionError(
                    f"COUNT(*) equality violation (E): S1={s1.count} != S2={s2.count}\n"
                    f"  S1: {spec.s1_sql}\n  S2: {spec.s2_sql}\n"
                    f"  Plan S1: {self._fmt_plan(s1.explain_plan)}\n"
                    f"  Plan S2: {self._fmt_plan(s2.explain_plan)}")
            self._log(f"  COUNT(*) {s1.count} == {s2.count}  [PASS]")

        for col in phi_cols:
            n = col.name
            allowed = spec.monotone_metrics.get(n, set())

            # COUNT(col) S1 ≤ S2（NULL 不计入，S2 填充后计数增加）
            c1, c2 = s1.col_counts.get(n), s2.col_counts.get(n)
            if MONO_COUNT in allowed and c1 is not None and c2 is not None:
                if c1 > c2:
                    raise AssertionError(
                        f"COUNT({n}) monotonicity violation: S1={c1} > S2={c2}\n"
                        f"  S1: {spec.s1_sql}\n  S2: {spec.s2_sql}\n"
                        f"  Plan S1: {self._fmt_plan(s1.explain_plan)}\n"
                        f"  Plan S2: {self._fmt_plan(s2.explain_plan)}")
                self._log(f"  COUNT({n}) S1={c1} <= S2={c2}  [PASS]")

            # MAX(col) S1 ≤ S2（S2 的值域包含 S1 的非 NULL 值，再加填充值）
            mx1, mx2 = s1.max_values.get(n), s2.max_values.get(n)
            if MONO_MAX in allowed and mx1 is not None and mx2 is not None:
                if mx1 > mx2 + FLOAT_TOLERANCE:
                    raise AssertionError(
                        f"MAX({n}) monotonicity violation: S1={mx1} > S2={mx2}\n"
                        f"  S1: {spec.s1_sql}\n  S2: {spec.s2_sql}\n"
                        f"  Plan S1: {self._fmt_plan(s1.explain_plan)}\n"
                        f"  Plan S2: {self._fmt_plan(s2.explain_plan)}")
                self._log(f"  MAX({n}) S1={mx1} <= S2={mx2}  [PASS]")

            # MIN(col) S1 ≥ S2（填充值可能小于 S1 的最小非 NULL 值）
            mn1, mn2 = s1.min_values.get(n), s2.min_values.get(n)
            if MONO_MIN in allowed and mn1 is not None and mn2 is not None:
                if mn2 > mn1 + FLOAT_TOLERANCE:
                    raise AssertionError(
                        f"MIN({n}) monotonicity violation: S2_min={mn2} > S1_min={mn1}\n"
                        f"  S1: {spec.s1_sql}\n  S2: {spec.s2_sql}\n"
                        f"  Plan S1: {self._fmt_plan(s1.explain_plan)}\n"
                        f"  Plan S2: {self._fmt_plan(s2.explain_plan)}")
                self._log(f"  MIN({n}) S1={mn1} >= S2={mn2}  [PASS]")

            # COUNT(DISTINCT col) S1 ≤ S2
            cd1, cd2 = s1.count_distinct.get(n), s2.count_distinct.get(n)
            if MONO_COUNT_DISTINCT in allowed and cd1 is not None and cd2 is not None:
                if cd1 > cd2:
                    raise AssertionError(
                        f"COUNT(DISTINCT {n}) monotonicity violation: S1={cd1} > S2={cd2}\n"
                        f"  S1: {spec.s1_sql}\n  S2: {spec.s2_sql}\n"
                        f"  Plan S1: {self._fmt_plan(s1.explain_plan)}\n"
                        f"  Plan S2: {self._fmt_plan(s2.explain_plan)}")
                self._log(f"  COUNT(DISTINCT {n}) S1={cd1} <= S2={cd2}  [PASS]")

    # ──────────────────────────────────────────
    # valscope 集成：ColDef 转换
    # ──────────────────────────────────────────
    def _vs_table_to_coldefs(self, vs_table) -> List[ColDef]:
        cols = []
        for c in vs_table.columns:
            dt = c.data_type.upper().split('(')[0].strip()
            if dt in ('TINYINT', 'SMALLINT', 'MEDIUMINT', 'BIGINT', 'INT'):
                base_dt, vlen = 'INT', 128
            elif dt in ('FLOAT', 'DOUBLE'):
                base_dt, vlen = dt, 128
            elif dt in ('DECIMAL', 'NUMERIC'):
                base_dt, vlen = 'DECIMAL', 128
            elif dt in _STRING_LIKE_TYPES:
                m = re.search(r'\((\d+)\)', c.data_type)
                vlen = int(m.group(1)) if m else 128
                base_dt = 'VARCHAR'
            elif dt in _TEMPORAL_TYPES:
                base_dt, vlen = 'DATE', 32
            else:
                base_dt, vlen = 'OPAQUE', 128

            cols.append(ColDef(
                name=c.name,
                data_type=base_dt,
                declared_type=c.data_type,
                is_primary_key=(c.name == vs_table.primary_key),
                is_nullable=c.is_nullable,
                varchar_len=vlen,
            ))
        return cols

    # ──────────────────────────────────────────
    # 行摘要
    # ──────────────────────────────────────────
    def _capture_row_digests(self, conn, select_sql: str) -> Dict[str, int]:
        digests: Dict[str, int] = {}
        try:
            with conn.cursor() as cur:
                cur.execute(select_sql)
                for row in cur.fetchall():
                    d = self._row_digest(row)
                    digests[d] = digests.get(d, 0) + 1
        except Exception as e:
            if self._is_expected_runtime_error(e):
                raise IgnorableQueryRuntimeError(str(e)) from e
            self._log(f"  row digest capture failed: {e}")
        return digests

    def _row_digest(self, row: tuple) -> str:
        h = hashlib.sha256()
        for i, val in enumerate(row):
            if i > 0:
                h.update(b'|')
            if val is None:
                s = 'NULL'
            elif isinstance(val, bool):
                s = '1' if val else '0'
            elif isinstance(val, int):
                s = str(val)
            elif isinstance(val, Decimal):
                s = format(val.normalize(), 'f') if val.is_finite() else str(val)
            elif isinstance(val, float):
                s = format(Decimal(str(val)).normalize(), 'f') if val != 0.0 else '0'
            else:
                s = str(val).rstrip()
            h.update(s.encode('utf-8'))
        return h.hexdigest()

    # ──────────────────────────────────────────
    # EXPLAIN
    # ──────────────────────────────────────────
    def _capture_explain(self, conn, select_sql: str) -> List[str]:
        explain_sqls = [
            f"EXPLAIN FORMAT=TRADITIONAL {select_sql}",
            f"EXPLAIN {select_sql}",
        ]
        last_error = None
        for explain_sql in explain_sqls:
            try:
                with conn.cursor() as cur:
                    cur.execute(explain_sql)
                    if not cur.description:
                        continue
                    rows = self._format_explain_rows(cur.description, cur.fetchall())
                    if rows:
                        return rows
            except Exception as e:
                last_error = e
        if last_error is not None:
            self._log(f"  EXPLAIN failed: {last_error}")
        return []

    def _format_explain_rows(self, description, fetched_rows) -> List[str]:
        if not description:
            return []

        col_names = [str(d[0]).lower() for d in description]
        canonical_cols = [
            'id', 'select_type', 'table', 'type',
            'possible_keys', 'key', 'key_len', 'rows',
            'filtered', 'extra',
        ]

        def sval(v):
            return 'null' if v is None else str(v)

        if any(name in col_names for name in canonical_cols):
            def gcol(row, name):
                try:
                    v = row[col_names.index(name)]
                    return sval(v)
                except (ValueError, IndexError):
                    return 'null'

            return [
                f"id={gcol(row,'id')};select_type={gcol(row,'select_type')};"
                f"table={gcol(row,'table')};type={gcol(row,'type')};"
                f"possible_keys={gcol(row,'possible_keys')};key={gcol(row,'key')};"
                f"key_len={gcol(row,'key_len')};rows={gcol(row,'rows')};"
                f"filtered={gcol(row,'filtered')};extra={gcol(row,'extra')}"
                for row in fetched_rows
            ]

        formatted = []
        for row in fetched_rows:
            parts = []
            for idx, name in enumerate(col_names):
                try:
                    val = row[idx]
                except IndexError:
                    val = None
                parts.append(f"{name}={sval(val)}")
            if parts:
                formatted.append(';'.join(parts))
        return formatted

    def _plans_equivalent(self, p1: List[str], p2: List[str]) -> bool:
        if len(p1) != len(p2):
            return False
        return all(
            self._normalize_plan_row(r1) == self._normalize_plan_row(r2)
            for r1, r2 in zip(p1, p2)
        )

    def _normalize_plan_row(self, row: str) -> str:
        row = re.sub(r'rows=[^;]+',     'rows=?',     row)
        row = re.sub(r'filtered=[^;]+', 'filtered=?', row)
        row = re.sub(r'key_len=[^;]+',  'key_len=?',  row)
        return row.strip()

    # ──────────────────────────────────────────
    # Bug 日志
    # ──────────────────────────────────────────
    def _log_bug(self, error_msg: str, spec: VertQuerySpec,
                 s1: VertSnapshot, s2: VertSnapshot, uid: str):
        log_path = os.path.join(
            self._log_dir,
            f'VerticalOracle_bugs_{time.strftime("%Y%m%d_%H%M%S")}.log'
        )
        ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        with open(log_path, 'a', encoding='utf-8') as f:
            f.write(f"\n{'='*60}\n")
            f.write(f"[{ts}] Round #{uid} BUG DETECTED\n")
            f.write(f"Mutations    : {spec.mutations}\n")
            f.write(f"S1 SQL       : {spec.s1_sql}\n")
            f.write(f"S2 SQL       : {spec.s2_sql}\n")
            f.write(f"Plan S1      : {self._fmt_plan(s1.explain_plan)}\n")
            f.write(f"Plan S2      : {self._fmt_plan(s2.explain_plan)}\n")
            f.write(f"S1 count     : {s1.count}\n")
            f.write(f"S2 count     : {s2.count}\n")
            f.write(f"Error        : {error_msg}\n")
            f.write(f"\n-- 完整复现序列 ({len(self._sql_log)} statements) --\n")
            for sql in self._sql_log:
                f.write(sql + '\n')
            f.write(f"\n-- 验证查询 --\n")
            f.write(f"S1: {spec.s1_sql};\n")
            f.write(f"S2: {spec.s2_sql};\n")
        self._log(f"  [BUG] Logged to {log_path}")
        print(f"[VerticalOracle] BUG DETECTED: {error_msg[:120]}")

    # ──────────────────────────────────────────
    # ANALYZE TABLE
    # ──────────────────────────────────────────
    def _analyze_table(self, conn, table_name: str):
        self._sql_log.append(f"ANALYZE TABLE {table_name};")
        self._log(f"  ANALYZE TABLE {table_name}")
        try:
            with conn.cursor() as cur:
                cur.execute(f"ANALYZE TABLE {table_name}")
        except Exception as e:
            self._log(f"  ANALYZE failed: {e}")

    # ──────────────────────────────────────────
    # 数据库工具方法
    # ──────────────────────────────────────────
    def _connect(self):
        try:
            conn = pymysql.connect(
                host=self.db_config.get('host', '127.0.0.1'),
                port=self.db_config.get('port', 3306),
                user=self.db_config.get('user', 'root'),
                password=self.db_config.get('password', ''),
                charset='utf8mb4',
                autocommit=True,
            )
            db_name = self.db_config.get('database', 'test')
            with conn.cursor() as cur:
                cur.execute(f"CREATE DATABASE IF NOT EXISTS `{db_name}` "
                            f"CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
                cur.execute(f"USE `{db_name}`")
            return conn
        except Exception as e:
            self._log(f"  DB connect failed: {e}")
            return None

    def _exec_ddl(self, conn, sql: str, ignore_error: bool = False):
        self._sql_log.append(sql + ';')
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
        except Exception as e:
            if not ignore_error:
                raise
            self._log(f"  DDL ignored error: {e}")

    def _exec_dml(self, conn, sql: str):
        self._sql_log.append(sql + ';')
        with conn.cursor() as cur:
            cur.execute(sql)

    def _exec_single_int(self, conn, sql: str) -> Optional[int]:
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                row = cur.fetchone()
                return int(row[0]) if row and row[0] is not None else None
        except Exception as e:
            if self._is_expected_runtime_error(e):
                raise IgnorableQueryRuntimeError(str(e)) from e
            self._log(f"  exec_single_int failed: {e}")
            return None

    def _exec_single_float(self, conn, sql: str) -> Optional[float]:
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                row = cur.fetchone()
                return float(row[0]) if row and row[0] is not None else None
        except Exception as e:
            if self._is_expected_runtime_error(e):
                raise IgnorableQueryRuntimeError(str(e)) from e
            self._log(f"  exec_single_float failed: {e}")
            return None

    def _drop_if_exists(self, conn, table_name: str):
        try:
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {table_name}")
        except Exception:
            pass

    def _try_insert(self, conn, table_name: str, cols: List[ColDef], vals: List[str]):
        col_names = ', '.join(f'`{c.name}`' for c in cols)
        sql = f"INSERT IGNORE INTO {table_name} ({col_names}) VALUES ({', '.join(vals)})"
        try:
            self._exec_dml(conn, sql)
        except Exception as e:
            self._log(f"  INSERT skipped: {e}")

    def _is_expected_runtime_error(self, err: Exception) -> bool:
        code = None
        if getattr(err, 'args', None):
            first = err.args[0]
            if isinstance(first, int):
                code = first
        if code in self._ignorable_codes:
            return True
        msg = str(err).lower()
        return any(pat in msg for pat in self._ignorable_patterns)

    def _log(self, msg: str):
        if self.verbose:
            print(msg)
        if self.log_file:
            with open(self.log_file, 'a', encoding='utf-8') as f:
                f.write(msg + '\n')

    def _fmt_plan(self, plan: List[str]) -> str:
        return ' | '.join(plan) if plan else '[]'
