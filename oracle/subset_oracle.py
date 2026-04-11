"""
SubsetOracle: 基于数据子集关系 S1 ⊆ S2 的 MySQL 逻辑 Bug 检测 Oracle

核心思路：
  1. 复用 valscope 的 create_sample_tables() 获取 t1/t2/t3 的 schema，
     在 MySQL 里建对应临时表（主表+辅助表）
  2. 主表插入少量偏斜数据 → S1；辅助表插入固定数据（全程不变）
  3. 生成查询：优先用 valscope 的 generate_random_sql() 生成多表 JOIN 等复杂查询，
     过滤不满足单调性的查询（NOT IN / HAVING / LIMIT 等），再用简单查询兜底
  4. 主表再大量插入偏斜数据 + ANALYZE TABLE → S2（S1 ⊆ S2）
  5. 比较 S1/S2 的 EXPLAIN 计划是否变化（计划切换时更容易暴露 bug）
  6. 验证单调性：COUNT/MAX/MIN/行集合

单调性保证：
  辅助表固定不变 + 主表只增不减 → JOIN 结果只增不减 → 单调性依然成立
"""

import os
import re
import random
import math
import hashlib
import uuid
import pymysql
import time
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Tuple, Any
from datetime import datetime


# ─────────────────────────────────────────────
# 常量配置
# ─────────────────────────────────────────────
TARGET_BASELINE_QUERIES      = 6
MIN_BASELINE_QUERIES         = 3
MAX_QUERY_GEN_ATTEMPTS       = 72
BASELINE_HOT_ROWS            = 2
BASELINE_RANDOM_ROWS         = 4
BASELINE_NOISE_ROWS          = 4
SKEWED_EXPANSION_ROWS        = 500
UNCHANGED_PLAN_VERIFY_PROB   = 0.15
FLOAT_TOLERANCE              = 1e-9

# 不满足单调性的关键词，出现则过滤该查询
_UNSAFE_RE = re.compile(
    r'\bNOT\s+IN\b'
    r'|\bNOT\s+EXISTS\b'
    r'|\bEXCEPT\b'
    r'|\bMINUS\b'
    r'|\bHAVING\b'
    r'|\bLIMIT\b'
    # GROUP BY + 聚合函数组合会破坏行子集关系
    # 单独的 GROUP BY COUNT/MAX/MIN 是安全的，但含 STDDEV/VAR/GROUP_CONCAT 等不是
    r'|\bSTDDEV\b'
    r'|\bSTD\b'
    r'|\bVARIANCE\b'
    r'|\bVAR_POP\b'
    r'|\bVAR_SAMP\b'
    r'|\bSTDDEV_POP\b'
    r'|\bSTDDEV_SAMP\b'
    r'|\bGROUP_CONCAT\b'
    r'|\bAVG\b'        # AVG 也不单调（增加行会改变平均值）
    r'|\bRANK\s*\(\s*\)'
    r'|\bDENSE_RANK\s*\(\s*\)'
    r'|\bROW_NUMBER\s*\(\s*\)'
    r'|\bNTILE\s*\('
    r'|\bLAG\s*\('
    r'|\bLEAD\s*\('
    r'|\bFIRST_VALUE\s*\('
    r'|\bLAST_VALUE\s*\('
    r'|\bNTH_VALUE\s*\('
    r'|\bOVER\s*\('
    r'|\bNOT\s+BETWEEN\b'           # 反单调条件
    r'|\bUNION\b(?!\s+ALL\b)'
    r'|\bBIT_XOR\b'
    r'|\bBIT_AND\b'
    r'|\bSUM\s'
    r'|\bCOUNT\s*\(\s*DISTINCT\b'
    r'|\bBIT_OR\b'
    r'|\bCOS\s*\('      # 浮点函数做 GROUP BY 键，分组不稳定
    r'|\bSIN\s*\('
    r'|\bTAN\s*\('
    r'|\bLOG\s*\('
    r'|\bSQRT\s*\('
    r'|\bROUND\s*\('    # ROUND 作为 GROUP BY 键时也有类似问题
    r'|\bFLOOR\s*\('
    r'|\bCEIL\s*\('
    r'|\bRIGHT\s+JOIN\b',
    re.IGNORECASE
)

# ─────────────────────────────────────────────
# 数据类
# ─────────────────────────────────────────────
@dataclass
class ColDef:
    name: str
    data_type: str          # 'INT' / 'VARCHAR' / 'FLOAT' / 'DOUBLE' / 'DECIMAL'
    is_primary_key: bool = False
    is_nullable: bool = True
    varchar_len: int = 128


@dataclass
class SkewProfile:
    predicate_col: ColDef
    primary_hot:   str
    secondary_hot: str
    tertiary_hot:  str
    expansion_hot: str
    hot_values_by_col: Dict[str, List[str]] = field(default_factory=dict)


@dataclass
class QuerySpec:
    table_name:   str        # 主表名（用于 verify 里的 COUNT 等）
    shape:        str
    where_clause: str
    select_sql:   str
    result_cols:  List[ColDef] = field(default_factory=list)
    grouping_col: Optional[ColDef] = None
    agg_col:      Optional[ColDef] = None
    offset:       int = 0
    agg_src_sql:  Optional[str] = None
    is_valscope:  bool = False   # True = 来自 valscope 生成，verify 走通用路径


SIMPLE_QUERY_SHAPES = [
    'PLAIN_SELECT', 'DERIVED_TABLE', 'IN_SUBQUERY',
    'COUNT_STAR', 'GROUP_BY_COUNT', 'GROUP_BY_MAX',
    'COUNT_DISTINCT', 'ORDER_BY_LIMIT_ASC', 'ORDER_BY_LIMIT_DESC',
]


@dataclass
class QuerySnapshot:
    count:          Optional[int]                    = None
    max_values:     Dict[str, Optional[float]]       = field(default_factory=dict)
    min_values:     Dict[str, Optional[float]]       = field(default_factory=dict)
    row_digests:    Dict[str, int]                   = field(default_factory=dict)
    grouped_counts: Dict[str, int]                   = field(default_factory=dict)
    grouped_max:    Dict[str, Optional[float]]       = field(default_factory=dict)
    distinct_count: Optional[int]                    = None
    ordered_value:  Optional[float]                  = None
    explain_plan:   List[str]                        = field(default_factory=list)


# ═══════════════════════════════════════════════════════════════
class SubsetOracle:

    def __init__(self, db_config: dict, verbose: bool = True,
                 log_sql: bool = False, log_file: str = None):
        self.db_config = db_config
        self.verbose   = verbose
        self.log_sql   = log_sql
        self.log_file  = log_file
        self._log_dir  = None
        self._sql_log  = []

        self.total_rounds       = 0
        self.total_queries      = 0
        self.total_plan_changes = 0
        self.total_bugs         = 0

    # ──────────────────────────────────────────
    # 公共入口
    # ──────────────────────────────────────────
    def run(self) -> dict:
        uid          = uuid.uuid4().hex[:8]
        self._sql_log = []

        round_stats = {
            'round_id': uid, 'queries': 0,
            'plan_changes': 0, 'bugs': 0, 'skipped': False,
        }

        db_type = self.db_config.get('db_type', 'MYSQL').upper()
        self._log_dir = os.path.join('invalid_mutation', db_type)
        os.makedirs(self._log_dir, exist_ok=True)

        self._log(f"\n{'='*60}")
        self._log(f" SUBSET ORACLE round #{uid}")
        self._log(f"{'='*60}")

        conn = self._connect()
        if conn is None:
            self._log("  [SKIP] Cannot connect to database.")
            round_stats['skipped'] = True
            return round_stats

        # ── 从 valscope 获取表 schema ──────────────────────────
        name_map, vs_tables, main_vs_table = self._build_name_map(uid)
        main_name       = name_map[main_vs_table.name]
        all_actual_names = list(name_map.values())

        try:
            # ── Step 1：建所有表 ──────────────────────────────
            self._log(f"\n[Step 1] Creating tables ...")
            self._create_all_tables(conn, vs_tables, name_map)

            main_cols    = self._vs_table_to_coldefs(main_vs_table)
            numeric_cols = [c for c in main_cols
                            if c.data_type in ('INT', 'FLOAT', 'DOUBLE', 'DECIMAL')]

            # ── Step 1.5：谓词列、索引、偏斜配置 ─────────────
            pred_col = self._choose_predicate_col(main_cols)
            self._ensure_indexes(conn, main_name, pred_col, uid)
            skew = self._create_skew_profile(main_cols, pred_col)
            self._log(f"  Main table: {main_name}, predicate col: {pred_col.name}, "
                      f"primary_hot={skew.primary_hot}")

            # ── Step 2：插入 S1 数据 ──────────────────────────
            self._log(f"\n[Step 2] Building S1 ...")
            # 主表：偏斜分布
            self._insert_hot_seed_rows(conn, main_name, main_cols, skew, BASELINE_HOT_ROWS)
            self._insert_skewed_rows(conn, main_name, main_cols, skew,
                                     BASELINE_RANDOM_ROWS + random.randint(0, 3),
                                     0.35, stage='baseline')
            self._insert_noise_rows(conn, main_name, main_cols, BASELINE_NOISE_ROWS)
            # 辅助表：固定数据（全程不变）
            self._insert_aux_data(conn, vs_tables, name_map)

            s1_count = self._exec_single_int(conn, f"SELECT COUNT(*) FROM {main_name}")
            if not s1_count:
                self._log("  [SKIP] S1 is empty.")
                round_stats['skipped'] = True
                return round_stats
            self._log(f"  S1 row count: {s1_count}")

            # ── Step 3：生成查询并收集 S1 快照 ───────────────
            self._log(f"\n[Step 3] Building baseline queries on S1 ...")
            baselines = self._build_baselines(
                conn, vs_tables, name_map, main_name, main_cols, numeric_cols, skew)
            self._log(f"  Validated baselines: {len(baselines)}")
            if len(baselines) < MIN_BASELINE_QUERIES:
                self._log("  [SKIP] Not enough valid baseline queries.")
                round_stats['skipped'] = True
                return round_stats

            # ── Step 4：插入 S2 数据 + ANALYZE ───────────────
            self._log(f"\n[Step 4] Expanding to S2 ...")
            self._sql_log.append('START TRANSACTION;')
            conn.begin()
            self._insert_skewed_rows(conn, main_name, main_cols, skew,
                                     SKEWED_EXPANSION_ROWS + 64 * random.randint(0, 8),
                                     0.92, stage='expansion')
            conn.commit()
            self._sql_log.append('COMMIT;')

            s2_count = self._exec_single_int(conn, f"SELECT COUNT(*) FROM {main_name}")
            if s2_count is None or s2_count <= s1_count:
                self._log("  [SKIP] Table did not grow enough.")
                return round_stats
            self._log(f"  S2 row count: {s2_count}, growth ratio: {s2_count/s1_count:.2f}x")

            self._analyze_table(conn, main_name)

            # ── Step 5+6：验证单调性 ──────────────────────────
            self._log(f"\n[Step 5+6] Verifying monotonicity on S2 ...")
            for i, (spec, s1_snap) in enumerate(baselines):
                s2_plan      = self._capture_explain(conn, spec.select_sql)
                plan_changed = not self._plans_equivalent(s1_snap.explain_plan, s2_plan)
                self._log(f"  Query[{i+1}] plan_changed={plan_changed} "
                          f"({'valscope' if spec.is_valscope else 'simple'})")

                if not plan_changed and random.random() > UNCHANGED_PLAN_VERIFY_PROB:
                    self._log(f"  Query[{i+1}] plan unchanged, skipping.")
                    continue

                round_stats['queries'] += 1
                if plan_changed:
                    round_stats['plan_changes'] += 1

                s2_snap = self._execute_snapshot(conn, spec, numeric_cols, capture_rows=False)
                s2_snap.explain_plan = s2_plan

                try:
                    self._verify(conn, spec, s1_snap, s2_snap, numeric_cols)
                except AssertionError as e:
                    round_stats['bugs'] += 1
                    self._log_bug(str(e), spec, s1_snap, s2_snap, uid)

            self._log(f"\n  All checks PASSED for round #{uid}")

        except Exception as e:
            self._log(f"  [ERROR] round #{uid}: {e}")
        finally:
            for n in all_actual_names:
                self._drop_if_exists(conn, n)
            conn.close()
            self._sql_log = []
            self._log(f"{'='*60}\n")

        if not round_stats['skipped']:
            self.total_rounds       += 1
            self.total_queries      += round_stats['queries']
            self.total_plan_changes += round_stats['plan_changes']
            self.total_bugs         += round_stats['bugs']

        return round_stats

    # ──────────────────────────────────────────
    # valscope 集成：表结构
    # ──────────────────────────────────────────
    def _build_name_map(self, uid: str):
        """
        调用 valscope 的 create_sample_tables() 拿到 t1/t2/t3 schema，
        生成 {原始表名: 临时表名} 映射。
        返回 (name_map, vs_tables, main_vs_table)
        """
        from generate_random_sql import create_sample_tables
        vs_tables = create_sample_tables()   # [t1, t2, t3, ...]

        name_map = {}
        for i, tbl in enumerate(vs_tables):
            if i == 0:
                name_map[tbl.name] = f"subset3_{uid}"
            else:
                name_map[tbl.name] = f"subset3_ref_{uid}_{tbl.name}"

        main_vs_table = vs_tables[0]
        return name_map, vs_tables, main_vs_table

    def _create_all_tables(self, conn, vs_tables, name_map: dict):
        """
        用 valscope 的 generate_create_table_sql() 建表，
        替换表名，去掉外键约束（避免插入顺序依赖）
        """
        from generate_random_sql import generate_create_table_sql
        for tbl in vs_tables:
            ddl = generate_create_table_sql(tbl)
            actual = name_map[tbl.name]
            # 替换表名
            ddl = ddl.replace(f"CREATE TABLE {tbl.name}",
                              f"CREATE TABLE {actual}")
            # 去掉外键约束
            ddl = self._strip_foreign_keys(ddl)
            self._exec_ddl(conn, ddl)
            self._log(f"  Created: {actual}")

    def _strip_foreign_keys(self, ddl: str) -> str:
        """从 DDL 里删除所有 FOREIGN KEY 行"""
        lines = ddl.split('\n')
        cleaned = [l for l in lines
                   if 'FOREIGN KEY' not in l.upper() and 'REFERENCES' not in l.upper()]
        # 修复最后一个有效列定义后面可能多余的逗号
        result = '\n'.join(cleaned)
        result = re.sub(r',\s*\n(\s*\))', r'\n\1', result)
        return result

    def _vs_table_to_coldefs(self, vs_table) -> List[ColDef]:
        """把 valscope Table 对象转成 SubsetOracle 的 ColDef 列表"""
        cols = []
        for c in vs_table.columns:
            dt = c.data_type.upper()
            if dt.startswith('VARCHAR') or dt.startswith('CHAR') \
                    or 'TEXT' in dt or 'ENUM' in dt or 'SET(' in dt:
                base_dt = 'VARCHAR'
                vlen = 255
            elif dt.startswith('DECIMAL') or dt.startswith('NUMERIC'):
                base_dt = 'DECIMAL'
                vlen = 128
            elif 'FLOAT' in dt:
                base_dt = 'FLOAT'
                vlen = 128
            elif 'DOUBLE' in dt:
                base_dt = 'DOUBLE'
                vlen = 128
            elif dt in ('DATE', 'DATETIME', 'TIMESTAMP', 'TIME', 'YEAR'):
                base_dt = 'VARCHAR'   # 日期类型当字符串处理，避免热值生成复杂
                vlen = 32
            else:
                base_dt = 'INT'
                vlen = 128
            cols.append(ColDef(
                name=c.name,
                data_type=base_dt,
                is_primary_key=(c.name == vs_table.primary_key),
                is_nullable=c.is_nullable,
                varchar_len=vlen,
            ))
        return cols

    def _insert_aux_data(self, conn, vs_tables, name_map: dict):
        """
        向辅助表（t2, t3 等）插入固定数据。
        复用 valscope 的 generate_insert_sql()，替换表名后执行。
        辅助表全程不参与 S2 扩展。
        """
        from generate_random_sql import generate_insert_sql
        # 先收集主表主键，供辅助表外键参考
        primary_keys_dict = {}
        for tbl in vs_tables:
            primary_keys_dict[tbl.name] = list(range(1, 21))  # 简单用 1-20

        for tbl in vs_tables[1:]:   # 跳过 t1（主表）
            actual = name_map[tbl.name]
            try:
                insert_sql = generate_insert_sql(
                    tbl, num_rows=10,
                    existing_primary_keys=primary_keys_dict,
                    primary_key_values=list(range(1, 11))
                )
                for line in insert_sql.strip().split('\n'):
                    line = line.strip()
                    if not line:
                        continue
                    # 替换表名，改为 INSERT IGNORE
                    line = line.replace(f"INSERT INTO {tbl.name}",
                                        f"INSERT IGNORE INTO {actual}")
                    line = line.replace(f"INSERT  INTO {tbl.name}",
                                        f"INSERT IGNORE INTO {actual}")
                    try:
                        self._exec_dml(conn, line.rstrip(';'))
                    except Exception as e:
                        self._log(f"  aux insert skipped: {e}")
            except Exception as e:
                self._log(f"  aux data gen failed for {actual}: {e}")

    # ──────────────────────────────────────────
    # valscope 集成：查询生成
    # ──────────────────────────────────────────
    def _is_monotone_query(self, sql: str) -> bool:
        """过滤不满足单调性的查询"""
        return not bool(_UNSAFE_RE.search(sql))

    def _remap_sql(self, sql: str, name_map: dict) -> str:
        """
        把 valscope 生成的 SQL 里的原始表名（t1/t2/t3）
        替换成对应的临时表名，用词边界匹配防止误替换列名。
        按名字长度倒序，避免 t1 替换影响 t10 等。
        """
        result = sql
        for orig, actual in sorted(name_map.items(),
                                   key=lambda x: len(x[0]), reverse=True):
            result = re.sub(rf'\b{re.escape(orig)}\b', actual, result)
        return result

    def _build_baselines(self, conn, vs_tables, name_map: dict,
                          main_name: str, main_cols: List[ColDef],
                          numeric_cols: List[ColDef],
                          skew: SkewProfile) -> List[Tuple[QuerySpec, QuerySnapshot]]:
        """
        两层查询生成：
          A. valscope generate_random_sql() → 复杂查询（多表 JOIN 等），过滤不安全
          B. 自己的 _build_query_spec()     → 简单查询，保证数量达标
        """
        from generate_random_sql import generate_random_sql, create_sample_functions, set_tables

        results: Dict[str, Tuple[QuerySpec, QuerySnapshot]] = {}

        # ── A：valscope 生成复杂查询 ──────────────────────────
        try:
            set_tables(vs_tables)   # 设置全局表状态
            functions = create_sample_functions()

            for attempt in range(MAX_QUERY_GEN_ATTEMPTS // 2):
                if len(results) >= TARGET_BASELINE_QUERIES:
                    break
                try:
                    raw_sql = generate_random_sql(vs_tables, functions)
                    if not raw_sql:
                        continue
                    if not self._is_monotone_query(raw_sql):
                        continue
                    sql = self._remap_sql(raw_sql, name_map)
                    if sql in results:
                        continue

                    # 通用快照：COUNT(*) 包装 + 行摘要
                    snap = self._execute_generic_snapshot(conn, sql, numeric_cols)
                    if snap is None or snap.count is None or snap.count == 0:
                        continue

                    spec = QuerySpec(
                        table_name=main_name,
                        shape='VALSCOPE',
                        where_clause='',
                        select_sql=sql,
                        is_valscope=True,
                    )
                    results[sql] = (spec, snap)
                    self._log(f"  [valscope] collected: {sql[:80]}...")
                except Exception as e:
                    pass   # 生成失败静默跳过
        except Exception as e:
            self._log(f"  valscope query gen failed, falling back: {e}")

        # ── B：简单查询兜底 ───────────────────────────────────
        aux_name = list(name_map.values())[1] if len(name_map) > 1 else None
        for attempt in range(MAX_QUERY_GEN_ATTEMPTS):
            if len(results) >= TARGET_BASELINE_QUERIES:
                break
            try:
                spec = self._build_query_spec(
                    main_name, main_cols, numeric_cols, skew, aux_name)
                if spec.select_sql in results:
                    continue
                snap = self._execute_snapshot(conn, spec, numeric_cols, capture_rows=True)
                if self._has_useful_result(spec, snap):
                    results[spec.select_sql] = (spec, snap)
            except Exception:
                pass

        return list(results.values())

    def _execute_generic_snapshot(self, conn, sql: str,
                                   numeric_cols: List[ColDef]) -> Optional[QuerySnapshot]:
        """
        对 valscope 生成的任意 SQL 取通用快照：
          - COUNT(*) 通过包装子查询获得
          - 行摘要用于子集验证
        """
        snap = QuerySnapshot()
        try:
            # COUNT via wrapping
            count_sql = f"SELECT COUNT(*) FROM ({sql}) AS _oracle_wrap"
            snap.count = self._exec_single_int(conn, count_sql)

            # 行摘要（只在 S1 阶段 capture，数量不超过 10000 行时才做）
            if snap.count and snap.count <= 10000:
                snap.row_digests = self._capture_row_digests(conn, sql)

            snap.explain_plan = self._capture_explain(conn, sql)
        except Exception as e:
            self._log(f"  generic snapshot failed: {e}")
            return None
        return snap

    # ──────────────────────────────────────────
    # Step 1.5：谓词列 / 索引 / 偏斜配置
    # ──────────────────────────────────────────
    def _choose_predicate_col(self, cols: List[ColDef]) -> ColDef:
        preferred = [c for c in cols if not c.is_primary_key
                     and c.data_type in ('INT', 'VARCHAR', 'FLOAT', 'DOUBLE', 'DECIMAL')]
        if preferred:
            return random.choice(preferred)
        non_pk = [c for c in cols if not c.is_primary_key]
        return random.choice(non_pk) if non_pk else cols[0]

    def _ensure_indexes(self, conn, table_name: str, pred_col: ColDef, uid: str):
        idx = f"i_s3_{uid}"
        self._exec_ddl(conn, f"CREATE INDEX {idx} ON {table_name} (`{pred_col.name}`)",
                       ignore_error=True)

    def _create_skew_profile(self, cols: List[ColDef], pred_col: ColDef) -> SkewProfile:
        hot_by_col: Dict[str, List[str]] = {}
        for c in cols:
            hot_by_col[c.name] = self._create_hot_values(c)

        pred_hots = hot_by_col[pred_col.name]
        primary   = pred_hots[0]
        secondary = pred_hots[1] if len(pred_hots) > 1 else primary
        tertiary  = pred_hots[2] if len(pred_hots) > 2 else secondary
        expansion = self._create_expansion_hot_value(pred_col, pred_hots)

        return SkewProfile(
            predicate_col=pred_col,
            primary_hot=primary, secondary_hot=secondary,
            tertiary_hot=tertiary, expansion_hot=expansion,
            hot_values_by_col=hot_by_col,
        )

    def _create_hot_values(self, col: ColDef) -> List[str]:
        dt = col.data_type
        if dt == 'INT':
            a = random.randint(-16, 16)
            return [str(a), str(a+1+random.randint(0,3)), str(a-1-random.randint(0,3))]
        if dt == 'VARCHAR':
            s = f"hv_{random.randint(100,9999)}"
            return [f"'{s}'", f"'{s}_a'", f"'{s}_b'"]
        if dt in ('FLOAT', 'DOUBLE'):
            a = random.randint(-200,200)/10.0
            return [f"{a:.3f}", f"{a+1.0:.3f}", f"{a-1.0:.3f}"]
        if dt == 'DECIMAL':
            a = random.randint(-1000,1000)/100.0
            return [f"{a:.2f}", f"{a+1.0:.2f}", f"{a-1.0:.2f}"]
        return ['NULL']

    def _create_expansion_hot_value(self, col: ColDef, existing: List[str]) -> str:
        dt = col.data_type
        if dt == 'INT':
            nums = [int(v) for v in existing if v != 'NULL']
            base = max(nums) if nums else 0
            for i in range(8):
                c = str(base+20+i)
                if c not in existing: return c
            return str(base+40)
        if dt == 'VARCHAR':
            for i in range(16):
                c = f"'exp_{random.randint(1000,9999)}_{i}'"
                if c not in existing: return c
            return f"'exp_final_{len(existing)}'"
        if dt in ('FLOAT','DOUBLE'):
            nums = [float(v) for v in existing if v != 'NULL']
            base = max(nums) if nums else 0.0
            for i in range(8):
                c = f"{base+20.0+i:.3f}"
                if c not in existing: return c
            return f"{base+40.0:.3f}"
        if dt == 'DECIMAL':
            nums = [float(v) for v in existing if v != 'NULL']
            base = max(nums) if nums else 0.0
            for i in range(8):
                c = f"{base+20.0+i:.2f}"
                if c not in existing: return c
            return f"{base+40.0:.2f}"
        return 'NULL'

    # ──────────────────────────────────────────
    # Step 2 & 4：数据插入（主表）
    # ──────────────────────────────────────────
    def _insert_hot_seed_rows(self, conn, table_name: str,
                               cols: List[ColDef], skew: SkewProfile, n: int):
        for _ in range(n):
            vals = []
            for c in cols:
                if c.name == skew.predicate_col.name:
                    vals.append(skew.primary_hot)
                else:
                    vals.append(self._generate_value(c, skew, 0.5, 'baseline'))
            self._try_insert(conn, table_name, cols, vals)

    def _insert_skewed_rows(self, conn, table_name: str, cols: List[ColDef],
                             skew: SkewProfile, n: int, hotspot_prob: float, stage: str):
        for _ in range(n):
            vals = [self._generate_value(c, skew, hotspot_prob, stage) for c in cols]
            self._try_insert(conn, table_name, cols, vals)

    def _insert_noise_rows(self, conn, table_name: str, cols: List[ColDef], n: int):
        boundary_map: Dict[str, List[str]] = {}
        for c in cols:
            dt = c.data_type
            if dt == 'INT':
                boundary_map[c.name] = ['0','1','-1','2147483647','-2147483648','NULL']
            elif dt == 'VARCHAR':
                boundary_map[c.name] = ["''","'NULL'","'0'","'%'","'_'","NULL"]
            elif dt in ('FLOAT','DOUBLE'):
                boundary_map[c.name] = ['0','0.0','-0.0','1.0','-1.0',
                                         '3.4028235E38','-3.4028235E38','NULL']
            elif dt == 'DECIMAL':
                boundary_map[c.name] = ['0','0.00','1.00','-1.00',
                                         '99999999.99','-99999999.99','NULL']
            else:
                boundary_map[c.name] = ['NULL']

        for _ in range(n):
            target = random.choice([c for c in cols if not c.is_primary_key] or cols)
            bval   = random.choice(boundary_map[target.name])
            vals = []
            for c in cols:
                if c.is_primary_key:
                    vals.append(str(random.randint(1, 10_000_000)))
                elif c.name == target.name:
                    vals.append(bval)
                else:
                    vals.append('NULL')
            self._try_insert(conn, table_name, cols, vals)

    def _generate_value(self, col: ColDef, skew: SkewProfile,
                         hotspot_prob: float, stage: str) -> str:
        if col.is_primary_key:
            return str(random.randint(1, 10_000_000))

        use_hot   = random.random() < hotspot_prob
        hot_vals  = skew.hot_values_by_col.get(col.name, [])
        exp_hot   = skew.expansion_hot if col.name == skew.predicate_col.name else None
        dt        = col.data_type

        if use_hot and hot_vals:
            base = random.choice(hot_vals)
            if stage == 'expansion' and exp_hot and random.random() < 0.4:
                return exp_hot
            return base

        if dt == 'INT':    return str(random.randint(-1000, 1000))
        if dt == 'VARCHAR':
            n = random.randint(1, 20)
            return f"'{''.join(random.choices('abcdefghijklmnopqrstuvwxyz0123456789_', k=n))}'"
        if dt in ('FLOAT','DOUBLE'): return f"{random.uniform(-1000,1000):.3f}"
        if dt == 'DECIMAL':          return f"{random.uniform(-1000,1000):.2f}"
        return 'NULL'

    def _try_insert(self, conn, table_name: str, cols: List[ColDef], vals: List[str]):
        col_names = ', '.join(f'`{c.name}`' for c in cols)
        val_str   = ', '.join(vals)
        sql = f"INSERT IGNORE INTO {table_name} ({col_names}) VALUES ({val_str})"
        try:
            self._exec_dml(conn, sql)
        except Exception as e:
            self._log(f"  INSERT skipped: {e}")

    # ──────────────────────────────────────────
    # 简单查询生成（兜底）
    # ──────────────────────────────────────────
    def _build_query_spec(self, table_name: str, cols: List[ColDef],
                           numeric_cols: List[ColDef], skew: SkewProfile,
                           aux_name: Optional[str]) -> QuerySpec:
        shape = random.choice(SIMPLE_QUERY_SHAPES)
        where = self._build_where_clause(skew)

        if shape in ('ORDER_BY_LIMIT_ASC','ORDER_BY_LIMIT_DESC') and not numeric_cols:
            shape = 'COUNT_STAR'
        if shape == 'GROUP_BY_MAX' and not numeric_cols:
            shape = 'GROUP_BY_COUNT'

        result_cols  = self._choose_projection_cols(cols, skew.predicate_col)
        grouping_col = self._choose_grouping_col(cols)
        agg_col      = random.choice(numeric_cols) if numeric_cols else None
        offset       = random.randint(0, 4)
        col_list     = ', '.join(f'`{c.name}`' for c in result_cols)
        select_sql   = ''
        agg_src_sql  = None

        if shape == 'PLAIN_SELECT':
            select_sql = f"SELECT {col_list} FROM {table_name}{where}"

        elif shape == 'DERIVED_TABLE':
            inner = f"SELECT {col_list} FROM {table_name}{where}"
            outer = ', '.join(f'sub.`{c.name}`' for c in result_cols)
            select_sql  = f"SELECT {outer} FROM ({inner}) AS sub"
            agg_src_sql = select_sql

        elif shape == 'IN_SUBQUERY':
            pred = skew.predicate_col.name
            inner_where = where.replace(f'`{pred}`', f'inner_t.`{pred}`')
            inner_where = self._append_conjunct(inner_where,
                                                f"inner_t.`{pred}` IS NOT NULL")
            sub = (f"SELECT inner_t.`{pred}` FROM {table_name} AS inner_t{inner_where}")
            outer_cols = ', '.join(f'outer_t.`{c.name}`' for c in result_cols)
            select_sql = (f"SELECT {outer_cols} FROM {table_name} AS outer_t"
                          f" WHERE outer_t.`{pred}` IN ({sub})")
            agg_src_sql = select_sql

        elif shape == 'COUNT_STAR':
            select_sql = f"SELECT COUNT(*) FROM {table_name}{where}"

        elif shape == 'GROUP_BY_COUNT':
            if grouping_col is None:
                shape, select_sql = 'COUNT_STAR', f"SELECT COUNT(*) FROM {table_name}{where}"
            else:
                select_sql = (f"SELECT `{grouping_col.name}`, COUNT(*) "
                              f"FROM {table_name}{where} GROUP BY `{grouping_col.name}`")

        elif shape == 'GROUP_BY_MAX':
            if grouping_col is None or agg_col is None:
                shape, select_sql = 'COUNT_STAR', f"SELECT COUNT(*) FROM {table_name}{where}"
            else:
                select_sql = (f"SELECT `{grouping_col.name}`, MAX(`{agg_col.name}`) "
                              f"FROM {table_name}{where} GROUP BY `{grouping_col.name}`")

        elif shape == 'COUNT_DISTINCT':
            dist_col = random.choice(cols)
            select_sql = f"SELECT COUNT(DISTINCT `{dist_col.name}`) FROM {table_name}{where}"

        elif shape == 'ORDER_BY_LIMIT_ASC':
            if agg_col is None:
                shape, select_sql = 'COUNT_STAR', f"SELECT COUNT(*) FROM {table_name}{where}"
            else:
                w2 = self._append_conjunct(where, f"`{agg_col.name}` IS NOT NULL")
                select_sql = (f"SELECT `{agg_col.name}` FROM {table_name}{w2}"
                              f" ORDER BY `{agg_col.name}` ASC LIMIT 1 OFFSET {offset}")

        elif shape == 'ORDER_BY_LIMIT_DESC':
            if agg_col is None:
                shape, select_sql = 'COUNT_STAR', f"SELECT COUNT(*) FROM {table_name}{where}"
            else:
                w2 = self._append_conjunct(where, f"`{agg_col.name}` IS NOT NULL")
                select_sql = (f"SELECT `{agg_col.name}` FROM {table_name}{w2}"
                              f" ORDER BY `{agg_col.name}` DESC LIMIT 1 OFFSET {offset}")

        return QuerySpec(
            table_name=table_name, shape=shape, where_clause=where,
            select_sql=select_sql, result_cols=result_cols,
            grouping_col=grouping_col, agg_col=agg_col,
            offset=offset, agg_src_sql=agg_src_sql, is_valscope=False,
        )

    def _choose_projection_cols(self, cols, pred_col):
        style = random.choice(['ALL','PRED_ONLY','PRED_PLUS_ONE','RANDOM_SUBSET'])
        if style == 'ALL':   return list(cols)
        if style == 'PRED_ONLY': return [pred_col]
        if style == 'PRED_PLUS_ONE':
            others = [c for c in cols if c.name != pred_col.name]
            return [pred_col] + ([random.choice(others)] if others else [])
        subset = random.sample(cols, k=random.randint(1, max(1, len(cols)//2)))
        if pred_col not in subset: subset.insert(0, pred_col)
        return subset

    def _choose_grouping_col(self, cols):
        safe = [c for c in cols if not c.is_primary_key
                and c.data_type not in ('FLOAT','DOUBLE')]
        return random.choice(safe) if safe else None

    def _build_where_clause(self, skew: SkewProfile) -> str:
        col  = skew.predicate_col
        name = f"`{col.name}`"
        dt   = col.data_type

        if dt == 'INT':
            c = random.randint(0, 5)
            if c == 0: return f" WHERE {name} = {skew.primary_hot}"
            if c == 1: return f" WHERE {name} IN ({skew.primary_hot}, {skew.secondary_hot})"
            if c == 2: return f" WHERE {name} BETWEEN {skew.primary_hot} AND {skew.secondary_hot}"
            if c == 3: return f" WHERE {name} <= {skew.secondary_hot}"
            if c == 4: return f" WHERE {name} >= {skew.primary_hot}"
            return f" WHERE {name} IS NULL"

        if dt == 'VARCHAR':
            inner  = skew.primary_hot.strip("'")
            prefix = inner[0] if inner else 'h'
            c = random.randint(0, 4)
            if c == 0: return f" WHERE {name} = {skew.primary_hot}"
            if c == 1: return f" WHERE {name} IN ({skew.primary_hot}, {skew.secondary_hot})"
            if c == 2: return f" WHERE {name} LIKE '{prefix}%'"
            if c == 3: return f" WHERE {name} >= {skew.primary_hot}"
            return f" WHERE {name} IS NULL"

        if dt in ('FLOAT','DOUBLE','DECIMAL'):
            c = random.randint(0, 3)
            if c == 0: return f" WHERE {name} = {skew.primary_hot}"
            if c == 1: return f" WHERE {name} >= {skew.primary_hot}"
            if c == 2: return f" WHERE {name} <= {skew.secondary_hot}"
            if c == 3: return f" WHERE {name} BETWEEN {skew.primary_hot} AND {skew.secondary_hot}"
            return f" WHERE {name} IS NULL"

        return f" WHERE {name} IS NULL"

    def _append_conjunct(self, where: str, predicate: str) -> str:
        if 'WHERE' in where.upper():
            return f"{where} AND {predicate}"
        return f" WHERE {predicate}"

    # ──────────────────────────────────────────
    # 快照执行
    # ──────────────────────────────────────────
    def _execute_snapshot(self, conn, spec: QuerySpec,
                           numeric_cols: List[ColDef],
                           capture_rows: bool) -> QuerySnapshot:
        """简单 QuerySpec 的快照（非 valscope 来源）"""
        if spec.is_valscope:
            return self._execute_generic_snapshot(conn, spec.select_sql, numeric_cols)

        snap  = QuerySnapshot()
        shape = spec.shape

        def count_sql():
            if spec.agg_src_sql:
                return f"SELECT COUNT(*) FROM ({spec.agg_src_sql}) AS _w"
            return f"SELECT COUNT(*) FROM {spec.table_name}{spec.where_clause}"
        def max_sql(col):
            if spec.agg_src_sql:
                return f"SELECT MAX(`{col}`) FROM ({spec.agg_src_sql}) AS _w"
            return f"SELECT MAX(`{col}`) FROM {spec.table_name}{spec.where_clause}"
        def min_sql(col):
            if spec.agg_src_sql:
                return f"SELECT MIN(`{col}`) FROM ({spec.agg_src_sql}) AS _w"
            return f"SELECT MIN(`{col}`) FROM {spec.table_name}{spec.where_clause}"

        if shape == 'PLAIN_SELECT':
            snap.count = self._exec_single_int(conn, count_sql())
            for c in numeric_cols:
                snap.max_values[c.name] = self._exec_single_float(conn, max_sql(c.name))
                snap.min_values[c.name] = self._exec_single_float(conn, min_sql(c.name))
            if capture_rows:
                snap.row_digests = self._capture_row_digests(conn, spec.select_sql)

        elif shape in ('DERIVED_TABLE','IN_SUBQUERY'):
            snap.count = self._exec_single_int(conn, count_sql())
            if capture_rows:
                snap.row_digests = self._capture_row_digests(conn, spec.select_sql)

        elif shape == 'COUNT_STAR':
            snap.count = self._exec_single_int(conn, spec.select_sql)

        elif shape == 'GROUP_BY_COUNT':
            snap.grouped_counts = self._exec_grouped_int_map(conn, spec.select_sql)

        elif shape == 'GROUP_BY_MAX':
            snap.grouped_max = self._exec_grouped_float_map(conn, spec.select_sql)

        elif shape == 'COUNT_DISTINCT':
            snap.distinct_count = self._exec_single_int(conn, spec.select_sql)

        elif shape in ('ORDER_BY_LIMIT_ASC','ORDER_BY_LIMIT_DESC'):
            snap.ordered_value = self._exec_single_float(conn, spec.select_sql)

        snap.explain_plan = self._capture_explain(conn, spec.select_sql)
        return snap

    def _has_useful_result(self, spec: QuerySpec, snap: QuerySnapshot) -> bool:
        shape = spec.shape
        if shape in ('PLAIN_SELECT','DERIVED_TABLE','IN_SUBQUERY','COUNT_STAR'):
            return snap.count is not None and snap.count > 0
        if shape == 'GROUP_BY_COUNT':   return bool(snap.grouped_counts)
        if shape == 'GROUP_BY_MAX':     return bool(snap.grouped_max)
        if shape == 'COUNT_DISTINCT':   return snap.distinct_count is not None and snap.distinct_count > 0
        if shape in ('ORDER_BY_LIMIT_ASC','ORDER_BY_LIMIT_DESC'):
            return snap.ordered_value is not None
        return False

    # ──────────────────────────────────────────
    # Step 4：ANALYZE TABLE
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
    # Step 6：验证单调性
    # ──────────────────────────────────────────
    def _verify(self, conn, spec: QuerySpec, s1: QuerySnapshot,
                 s2: QuerySnapshot, numeric_cols: List[ColDef]):
        if spec.is_valscope:
            # valscope 查询走通用验证：COUNT + 行子集
            self._verify_count(spec, s1, s2)
            if s1.row_digests:
                self._verify_row_subset(conn, spec, s1, s2)
            return

        shape = spec.shape
        if shape == 'PLAIN_SELECT':
            self._verify_count(spec, s1, s2)
            for c in numeric_cols:
                self._verify_max(spec, c.name, s1, s2)
                self._verify_min(spec, c.name, s1, s2)
            self._verify_row_subset(conn, spec, s1, s2)
        elif shape in ('DERIVED_TABLE','IN_SUBQUERY'):
            self._verify_count(spec, s1, s2)
            self._verify_row_subset(conn, spec, s1, s2)
        elif shape == 'COUNT_STAR':
            self._verify_count(spec, s1, s2)
        elif shape == 'GROUP_BY_COUNT':
            self._verify_grouped_count(spec, s1, s2)
        elif shape == 'GROUP_BY_MAX':
            self._verify_grouped_max(spec, s1, s2)
        elif shape == 'COUNT_DISTINCT':
            self._verify_count_distinct(spec, s1, s2)
        elif shape == 'ORDER_BY_LIMIT_ASC':
            self._verify_order_asc(spec, s1, s2)
        elif shape == 'ORDER_BY_LIMIT_DESC':
            self._verify_order_desc(spec, s1, s2)

    def _verify_count(self, spec, s1, s2):
        if s1.count is None or s2.count is None: return
        if s1.count > s2.count:
            raise AssertionError(
                f"COUNT violation: S1={s1.count} > S2={s2.count}\n"
                f"  Query: {spec.select_sql}\n"
                f"  Plan1: {self._fmt_plan(s1.explain_plan)}\n"
                f"  Plan2: {self._fmt_plan(s2.explain_plan)}")
        self._log(f"  COUNT  S1={s1.count} <= S2={s2.count}  [PASS]")

    def _verify_max(self, spec, col, s1, s2):
        v1, v2 = s1.max_values.get(col), s2.max_values.get(col)
        if v1 is None: return
        if v2 is None or v1 > v2 + FLOAT_TOLERANCE:
            raise AssertionError(
                f"MAX({col}) violation: S1={v1} > S2={v2}\n"
                f"  Query: {spec.select_sql}\n"
                f"  Plan1: {self._fmt_plan(s1.explain_plan)}\n"
                f"  Plan2: {self._fmt_plan(s2.explain_plan)}")
        self._log(f"  MAX({col}) S1={v1} <= S2={v2}  [PASS]")

    def _verify_min(self, spec, col, s1, s2):
        v1, v2 = s1.min_values.get(col), s2.min_values.get(col)
        if v1 is None: return
        if v2 is None or v2 > v1 + FLOAT_TOLERANCE:
            raise AssertionError(
                f"MIN({col}) violation: S2_min={v2} > S1_min={v1} "
                f"(expected S2_min <= S1_min)\n"
                f"  Query: {spec.select_sql}\n"
                f"  Plan1: {self._fmt_plan(s1.explain_plan)}\n"
                f"  Plan2: {self._fmt_plan(s2.explain_plan)}")
        self._log(f"  MIN({col}) S1={v1} >= S2={v2}  [PASS]")

    def _verify_row_subset(self, conn, spec, s1, s2):
        if not s1.row_digests: return
        s2_digests = self._capture_row_digests(conn, spec.select_sql)
        missing    = {}
        remaining  = dict(s2_digests)
        for digest, cnt in s1.row_digests.items():
            avail = remaining.get(digest, 0)
            if avail < cnt:
                missing[digest] = cnt - avail
            else:
                remaining[digest] = avail - cnt
        if missing:
            raise AssertionError(
                f"ROW-SET subset violation: {len(missing)} digest(s) missing\n"
                f"  Query: {spec.select_sql}\n"
                f"  Plan1: {self._fmt_plan(s1.explain_plan)}\n"
                f"  Plan2: {self._fmt_plan(s2.explain_plan)}")
        s1t = sum(s1.row_digests.values())
        s2t = sum(s2_digests.values())
        self._log(f"  ROW-SET |S1|={s1t} ⊆ |S2|={s2t}  [PASS]")

    def _verify_grouped_count(self, spec, s1, s2):
        for key, cnt1 in s1.grouped_counts.items():
            cnt2 = s2.grouped_counts.get(key)
            if cnt2 is None or cnt1 > cnt2:
                raise AssertionError(
                    f"GROUP COUNT violation key={key}: S1={cnt1} > S2={cnt2}\n"
                    f"  Query: {spec.select_sql}")
        self._log(f"  GROUP COUNT  [PASS] ({len(s1.grouped_counts)} groups)")

    def _verify_grouped_max(self, spec, s1, s2):
        for key, v1 in s1.grouped_max.items():
            if v1 is None: continue
            v2 = s2.grouped_max.get(key)
            if v2 is None or v1 > v2 + FLOAT_TOLERANCE:
                raise AssertionError(
                    f"GROUP MAX violation key={key}: S1={v1} > S2={v2}\n"
                    f"  Query: {spec.select_sql}")
        self._log(f"  GROUP MAX  [PASS] ({len(s1.grouped_max)} groups)")

    def _verify_count_distinct(self, spec, s1, s2):
        if s1.distinct_count is None or s2.distinct_count is None: return
        if s1.distinct_count > s2.distinct_count:
            raise AssertionError(
                f"COUNT DISTINCT violation: S1={s1.distinct_count} > S2={s2.distinct_count}\n"
                f"  Query: {spec.select_sql}")
        self._log(f"  COUNT DISTINCT S1={s1.distinct_count} <= S2={s2.distinct_count}  [PASS]")

    def _verify_order_asc(self, spec, s1, s2):
        if s1.ordered_value is None or s2.ordered_value is None: return
        if s2.ordered_value > s1.ordered_value + FLOAT_TOLERANCE:
            raise AssertionError(
                f"ORDER ASC LIMIT violation: S2_min={s2.ordered_value} > S1_min={s1.ordered_value}\n"
                f"  Query: {spec.select_sql}")
        self._log(f"  ORDER ASC  S2={s2.ordered_value} <= S1={s1.ordered_value}  [PASS]")

    def _verify_order_desc(self, spec, s1, s2):
        if s1.ordered_value is None or s2.ordered_value is None: return
        if s1.ordered_value > s2.ordered_value + FLOAT_TOLERANCE:
            raise AssertionError(
                f"ORDER DESC LIMIT violation: S1_max={s1.ordered_value} > S2_max={s2.ordered_value}\n"
                f"  Query: {spec.select_sql}")
        self._log(f"  ORDER DESC  S1={s1.ordered_value} <= S2={s2.ordered_value}  [PASS]")

    # ──────────────────────────────────────────
    # EXPLAIN 计划
    # ──────────────────────────────────────────
    def _capture_explain(self, conn, select_sql: str) -> List[str]:
        rows = []
        try:
            with conn.cursor() as cur:
                cur.execute(f"EXPLAIN FORMAT=TRADITIONAL {select_sql}")
                for row in cur.fetchall():
                    def g(i): return str(row[i]) if row[i] is not None else 'null'
                    rows.append(
                        f"id={g(0)};select_type={g(1)};table={g(2)};"
                        f"type={g(4)};possible_keys={g(5)};key={g(6)};"
                        f"key_len={g(7)};rows={g(9)};filtered={g(10)};extra={g(11)}"
                    )
        except Exception as e:
            self._log(f"  EXPLAIN failed: {e}")
        return rows

    def _plans_equivalent(self, p1: List[str], p2: List[str]) -> bool:
        if len(p1) != len(p2): return False
        for r1, r2 in zip(p1, p2):
            if self._normalize_plan_row(r1) != self._normalize_plan_row(r2):
                return False
        return True

    def _normalize_plan_row(self, row: str) -> str:
        row = re.sub(r'rows=[^;]+', 'rows=?', row)
        row = re.sub(r'filtered=[^;]+', 'filtered=?', row)
        row = re.sub(r'key_len=[^;]+', 'key_len=?', row)
        return row.strip()

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
            self._log(f"  row digest capture failed: {e}")
        return digests

    def _row_digest(self, row: tuple) -> str:
        h = hashlib.sha256()
        for i, val in enumerate(row):
            if i > 0: h.update(b'|')
            s = 'NULL' if val is None else str(val).rstrip()
            try:
                f = float(s)
                if f == 0.0: s = '0.0'
            except (ValueError, TypeError):
                pass
            h.update(s.encode('utf-8'))
        return h.hexdigest()

    # ──────────────────────────────────────────
    # Bug 日志
    # ──────────────────────────────────────────
    def _log_bug(self, error_msg: str, spec: QuerySpec,
                  s1: QuerySnapshot, s2: QuerySnapshot, uid: str):
        log_path = os.path.join(self._log_dir, f'SubsetOracle_bugs_{time.strftime("%Y%m%d_%H%M%S")}.log')
        ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        with open(log_path, 'a', encoding='utf-8') as f:
            f.write(f"\n{'='*60}\n")
            f.write(f"[{ts}] Round #{uid} BUG DETECTED\n")
            f.write(f"Query Shape  : {spec.shape}"
                    f"{'  (valscope-generated)' if spec.is_valscope else ''}\n")
            f.write(f"SELECT SQL   : {spec.select_sql}\n")
            f.write(f"Plan S1      : {self._fmt_plan(s1.explain_plan)}\n")
            f.write(f"Plan S2      : {self._fmt_plan(s2.explain_plan)}\n")
            f.write(f"S1 count     : {s1.count}\n")
            f.write(f"S2 count     : {s2.count}\n")
            f.write(f"Error        : {error_msg}\n")
            f.write(f"\n-- 完整复现序列 ({len(self._sql_log)} statements) --\n")
            for sql in self._sql_log:
                f.write(sql + '\n')
            f.write(f"\n-- 验证查询 --\n")
            f.write(f"{spec.select_sql};\n")
        self._log(f"  [BUG] Logged to {log_path}")
        print(f"[SubsetOracle] BUG DETECTED: {error_msg[:120]}")

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
            self._log(f"  exec_single_int failed: {e}")
            return None

    def _exec_single_float(self, conn, sql: str) -> Optional[float]:
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                row = cur.fetchone()
                return float(row[0]) if row and row[0] is not None else None
        except Exception as e:
            self._log(f"  exec_single_float failed: {e}")
            return None

    def _exec_grouped_int_map(self, conn, sql: str) -> Dict[str, int]:
        result = {}
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                for row in cur.fetchall():
                    key = str(row[0]) if row[0] is not None else 'NULL'
                    result[key] = int(row[1])
        except Exception as e:
            self._log(f"  exec_grouped_int_map failed: {e}")
        return result

    def _exec_grouped_float_map(self, conn, sql: str) -> Dict[str, Optional[float]]:
        result = {}
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                for row in cur.fetchall():
                    key = str(row[0]) if row[0] is not None else 'NULL'
                    result[key] = float(row[1]) if row[1] is not None else None
        except Exception as e:
            self._log(f"  exec_grouped_float_map failed: {e}")
        return result

    def _drop_if_exists(self, conn, table_name: str):
        try:
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {table_name}")
        except Exception:
            pass

    # ──────────────────────────────────────────
    # 日志
    # ──────────────────────────────────────────
    def _log(self, msg: str):
        if self.verbose:
            print(msg)
        if self.log_file:
            with open(self.log_file, 'a', encoding='utf-8') as f:
                f.write(msg + '\n')

    def _fmt_plan(self, plan: List[str]) -> str:
        if not plan: return '[]'
        return ' | '.join(plan)