"""
SubsetOracle: 基于数据子集关系 S1 ⊆ S2 的 MySQL 逻辑 Bug 检测 Oracle

核心思路：
  1. 复用 valscope 的 create_sample_tables() 获取 t1/t2/t3 的 schema，
     在 MySQL 里建对应临时表（主表+辅助表）
  2. 主表插入少量偏斜数据 → S1；辅助表插入固定数据（全程不变）
  3. 使用 SubsetQueryGenerator 生成行保留查询，在 S1 上收集快照
  4. 主表再大量插入偏斜数据 + ANALYZE TABLE → S2（S1 ⊆ S2）
  5. 比较 S1/S2 的 EXPLAIN 计划是否变化（计划切换时更容易暴露 bug）
  6. 验证单调性：COUNT / MAX / MIN / 行集合子集

单调性保证：
  辅助表固定不变 + 主表只增不减 → JOIN 结果只增不减 → 单调性依然成立
"""

import os
import re
import random
import hashlib
import uuid
import pymysql
import time
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Tuple
from datetime import datetime
from oracle.subset_query_gen import SubsetQueryGenerator


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
EXPECTED_MYSQL_RUNTIME_ERROR_CODES = {1365, 1690}
EXPECTED_MYSQL_RUNTIME_ERROR_PATTERNS = (
    'double value is out of range',
    'bigint value is out of range',
    'decimal value is out of range',
    'division by 0',
    'division by zero',
)


# ─────────────────────────────────────────────
# 数据类
# ─────────────────────────────────────────────
@dataclass
class ColDef:
    name: str
    data_type: str       # 'INT' / 'VARCHAR' / 'FLOAT' / 'DOUBLE' / 'DECIMAL'
    is_primary_key: bool = False
    is_nullable: bool    = True
    varchar_len: int     = 128


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
    table_name: str   # 主表名（用于日志和错误信息）
    select_sql: str   # 完整 SELECT 语句


@dataclass
class QuerySnapshot:
    count:        Optional[int]                  = None
    max_values:   Dict[str, Optional[float]]     = field(default_factory=dict)
    min_values:   Dict[str, Optional[float]]     = field(default_factory=dict)
    row_digests:  Dict[str, int]                 = field(default_factory=dict)
    explain_plan: List[str]                      = field(default_factory=list)


class IgnorableQueryRuntimeError(Exception):
    pass


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
        uid             = uuid.uuid4().hex[:8]
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
            self._ensure_indexes(conn, main_name, main_cols, pred_col, uid)
            skew = self._create_skew_profile(main_cols, pred_col)
            self._log(f"  Main table: {main_name}, predicate col: {pred_col.name}, "
                      f"primary_hot={skew.primary_hot}")

            # ── Step 2：插入 S1 数据 ──────────────────────────
            self._log(f"\n[Step 2] Building S1 ...")
            self._insert_hot_seed_rows(conn, main_name, main_cols, skew, BASELINE_HOT_ROWS)
            self._insert_skewed_rows(conn, main_name, main_cols, skew,
                                     BASELINE_RANDOM_ROWS + random.randint(0, 3),
                                     0.35, stage='baseline')
            self._insert_noise_rows(conn, main_name, main_cols, BASELINE_NOISE_ROWS)
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
                self._log(f"  Query[{i+1}] plan_changed={plan_changed}")
                self._log(f"  Query[{i+1}] SQL: {spec.select_sql}")
                self._log(f"  Query[{i+1}] Plan S1: {self._fmt_plan(s1_snap.explain_plan)}")
                self._log(f"  Query[{i+1}] Plan S2: {self._fmt_plan(s2_plan)}")

                if not plan_changed and random.random() > UNCHANGED_PLAN_VERIFY_PROB:
                    self._log(f"  Query[{i+1}] plan unchanged, skipping.")
                    continue

                # S2 快照：与 S1 使用完全相同的方法，保证可比性
                s2_snap = self._execute_snapshot(conn, spec.select_sql, numeric_cols)
                if s2_snap is None:
                    self._log(f"  Query[{i+1}] skipped due to expected query runtime error.")
                    continue
                s2_snap.explain_plan = s2_plan

                try:
                    self._verify(conn, spec, s1_snap, s2_snap, numeric_cols)
                    round_stats['queries'] += 1
                    if plan_changed:
                        round_stats['plan_changes'] += 1
                except IgnorableQueryRuntimeError as e:
                    self._log(f"  Query[{i+1}] skipped during verification: {e}")
                except AssertionError as e:
                    round_stats['queries'] += 1
                    if plan_changed:
                        round_stats['plan_changes'] += 1
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
        from generate_random_sql import create_sample_tables
        vs_tables = create_sample_tables()
        name_map  = {}
        for i, tbl in enumerate(vs_tables):
            if i == 0:
                name_map[tbl.name] = f"subset3_{uid}"
            else:
                name_map[tbl.name] = f"subset3_ref_{uid}_{tbl.name}"
        return name_map, vs_tables, vs_tables[0]

    def _create_all_tables(self, conn, vs_tables, name_map: dict):
        from generate_random_sql import generate_create_table_sql
        for tbl in vs_tables:
            ddl = generate_create_table_sql(tbl)
            ddl = ddl.replace(f"CREATE TABLE {tbl.name}",
                              f"CREATE TABLE {name_map[tbl.name]}")
            ddl = self._strip_foreign_keys(ddl)
            self._exec_ddl(conn, ddl)
            self._log(f"  Created: {name_map[tbl.name]}")

    def _strip_foreign_keys(self, ddl: str) -> str:
        lines   = ddl.split('\n')
        cleaned = [l for l in lines
                   if 'FOREIGN KEY' not in l.upper() and 'REFERENCES' not in l.upper()]
        result  = '\n'.join(cleaned)
        return re.sub(r',\s*\n(\s*\))', r'\n\1', result)

    def _vs_table_to_coldefs(self, vs_table) -> List[ColDef]:
        cols = []
        for c in vs_table.columns:
            dt = c.data_type.upper()
            if dt.startswith('VARCHAR') or dt.startswith('CHAR') \
                    or 'TEXT' in dt or 'ENUM' in dt or 'SET(' in dt:
                base_dt, vlen = 'VARCHAR', 255
            elif dt.startswith('DECIMAL') or dt.startswith('NUMERIC'):
                base_dt, vlen = 'DECIMAL', 128
            elif 'FLOAT' in dt:
                base_dt, vlen = 'FLOAT', 128
            elif 'DOUBLE' in dt:
                base_dt, vlen = 'DOUBLE', 128
            elif dt in ('DATE', 'DATETIME', 'TIMESTAMP', 'TIME', 'YEAR'):
                base_dt, vlen = 'DATE', 32
            elif 'BLOB' in dt or 'BINARY' in dt or 'BIT' in dt \
                    or 'JSON' in dt or 'GEOMETRY' in dt or 'POINT' in dt \
                    or 'POLYGON' in dt or 'LINESTRING' in dt:
                base_dt = 'OPAQUE'  # 独立类型：禁止参与 JOIN / 列间比较，只生成 IS NULL / IS NOT NULL
                vlen = 128
            else:
                base_dt, vlen = 'INT', 128
            cols.append(ColDef(
                name=c.name,
                data_type=base_dt,
                is_primary_key=(c.name == vs_table.primary_key),
                is_nullable=c.is_nullable,
                varchar_len=vlen,
            ))
        return cols

    def _insert_aux_data(self, conn, vs_tables, name_map: dict):
        from generate_random_sql import generate_insert_sql
        primary_keys_dict = {tbl.name: list(range(1, 21)) for tbl in vs_tables}
        for tbl in vs_tables[1:]:
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
    # Step 3：查询生成 + S1 快照
    # ──────────────────────────────────────────
    def _build_baselines(self, conn, vs_tables, name_map: dict,
                          main_name: str, main_cols: List[ColDef],
                          numeric_cols: List[ColDef],
                          skew: SkewProfile) -> List[Tuple[QuerySpec, QuerySnapshot]]:
        tables = [
            (name_map[tbl.name], self._vs_table_to_coldefs(tbl))
            for tbl in vs_tables
        ]
        gen = SubsetQueryGenerator(
            tables=tables,
            skew_hot_values={name_map[vs_tables[0].name]: skew.hot_values_by_col},
        )

        results: Dict[str, Tuple[QuerySpec, QuerySnapshot]] = {}
        for _ in range(MAX_QUERY_GEN_ATTEMPTS):
            if len(results) >= TARGET_BASELINE_QUERIES:
                break
            sql = gen.generate()
            if not sql or sql in results:
                continue
            snap = self._execute_snapshot(conn, sql, numeric_cols)
            if snap is None or not snap.count:
                continue
            results[sql] = (QuerySpec(table_name=main_name, select_sql=sql), snap)
            self._log(f"  [query_gen] {sql[:80]}...")

        self._log(f"  Collected {len(results)}/{TARGET_BASELINE_QUERIES} baseline queries.")
        return list(results.values())

    # ──────────────────────────────────────────
    # 快照执行（S1 和 S2 共用同一方法）
    # ──────────────────────────────────────────
    def _execute_snapshot(self, conn, sql: str,
                        numeric_cols: List[ColDef]) -> Optional[QuerySnapshot]:
        snap = QuerySnapshot()
        try:
            wrap = f"({sql}) AS _w"

            snap.count = self._exec_single_int(conn, f"SELECT COUNT(*) FROM {wrap}")
            if snap.count is None:
                return None   # COUNT 失败才早退出，不影响其他查询

            result_numeric = self._result_numeric_cols(conn, sql, numeric_cols)
            for c in result_numeric:
                snap.max_values[c.name] = self._exec_single_float(
                    conn, f"SELECT MAX(`{c.name}`) FROM {wrap}")
                snap.min_values[c.name] = self._exec_single_float(
                    conn, f"SELECT MIN(`{c.name}`) FROM {wrap}")

            if snap.count <= 10000:
                snap.row_digests = self._capture_row_digests(conn, sql)

            snap.explain_plan = self._capture_explain(conn, sql)
        except IgnorableQueryRuntimeError as e:
            self._log(f"  snapshot skipped: {e}")
            return None
        except Exception as e:
            self._log(f"  snapshot failed: {e}")
            return None
        return snap

    def _result_numeric_cols(self, conn, sql: str,
                            numeric_cols: List[ColDef]) -> List[ColDef]:
        """查询实际暴露的列名，过滤掉不在 SELECT 列表里的 numeric_cols。"""
        try:
            with conn.cursor() as cur:
                cur.execute(f"SELECT * FROM ({sql}) AS _sc LIMIT 0")
                result_names = {desc[0] for desc in (cur.description or [])}
            return [c for c in numeric_cols if c.name in result_names]
        except Exception:
            return []

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

    def _ensure_indexes(self, conn, table_name: str, cols: List[ColDef],
                        pred_col: ColDef, uid: str):
        pred_idx_col = self._index_expr(pred_col)
        self._exec_ddl(conn, f"CREATE INDEX i_s3_{uid} ON {table_name} ({pred_idx_col})",
                       ignore_error=True)

        candidates = [
            c for c in cols
            if c.name != pred_col.name
            and not c.is_primary_key
            and c.data_type in ('INT', 'VARCHAR', 'DATE')
        ]

        if candidates:
            extra_cols = random.sample(candidates, k=min(random.randint(1, 2), len(candidates)))
            for c in extra_cols:
                self._exec_ddl(
                    conn,
                    f"CREATE INDEX i_s3_{uid}_{c.name} ON {table_name} ({self._index_expr(c)})",
                    ignore_error=True,
                )

        comp_candidates = [
            c for c in cols
            if not c.is_primary_key and c.data_type in ('INT', 'VARCHAR', 'DATE')
        ]
        pred_eligible = pred_col.data_type in ('INT', 'VARCHAR', 'DATE') and not pred_col.is_primary_key
        if pred_eligible and len(comp_candidates) >= 2 and random.random() < 0.8:
            others = [c for c in comp_candidates if c.name != pred_col.name]
            if others:
                c2 = random.choice(others)
                self._exec_ddl(
                    conn,
                    f"CREATE INDEX i_s3_{uid}_comp ON {table_name} "
                    f"({self._index_expr(pred_col)}, {self._index_expr(c2)})",
                    ignore_error=True,
                )
        elif len(comp_candidates) >= 2 and random.random() < 0.4:
            c1, c2 = random.sample(comp_candidates, k=2)
            self._exec_ddl(
                conn,
                f"CREATE INDEX i_s3_{uid}_comp ON {table_name} "
                f"({self._index_expr(c1)}, {self._index_expr(c2)})",
                ignore_error=True,
            )

    def _index_expr(self, col: ColDef) -> str:
        if col.data_type == 'VARCHAR':
            prefix_len = min(max(1, col.varchar_len), 64)
            return f"`{col.name}`({prefix_len})"
        return f"`{col.name}`"

    def _create_skew_profile(self, cols: List[ColDef], pred_col: ColDef) -> SkewProfile:
        hot_by_col: Dict[str, List[str]] = {c.name: self._create_hot_values(c) for c in cols}
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
        if dt == 'DATE':
            return ['NULL']
        if dt == 'INT':
            a = random.randint(-16, 16)
            return [str(a), str(a+1+random.randint(0,3)), str(a-1-random.randint(0,3))]
        if dt == 'VARCHAR':
            s = f"hv_{random.randint(100,9999)}"
            return [f"'{s}'", f"'{s}_a'", f"'{s}_b'"]
        if dt in ('FLOAT', 'DOUBLE'):
            a = random.randint(-200, 200) / 10.0
            return [f"{a:.3f}", f"{a+1.0:.3f}", f"{a-1.0:.3f}"]
        if dt == 'DECIMAL':
            a = random.randint(-1000, 1000) / 100.0
            return [f"{a:.2f}", f"{a+1.0:.2f}", f"{a-1.0:.2f}"]
        return ['NULL']

    def _create_expansion_hot_value(self, col: ColDef, existing: List[str]) -> str:
        dt = col.data_type
        if dt == 'INT':
            nums = [int(v) for v in existing if v != 'NULL']
            base = max(nums) if nums else 0
            for i in range(8):
                c = str(base + 20 + i)
                if c not in existing: return c
            return str(base + 40)
        if dt == 'VARCHAR':
            for i in range(16):
                c = f"'exp_{random.randint(1000,9999)}_{i}'"
                if c not in existing: return c
            return f"'exp_final_{len(existing)}'"
        if dt in ('FLOAT', 'DOUBLE'):
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
            vals = [
                skew.primary_hot if c.name == skew.predicate_col.name
                else self._generate_value(c, skew, 0.5, 'baseline')
                for c in cols
            ]
            self._try_insert(conn, table_name, cols, vals)

    def _insert_skewed_rows(self, conn, table_name: str, cols: List[ColDef],
                             skew: SkewProfile, n: int, hotspot_prob: float, stage: str):
        for _ in range(n):
            vals = [self._generate_value(c, skew, hotspot_prob, stage) for c in cols]
            self._try_insert(conn, table_name, cols, vals)

    def _insert_noise_rows(self, conn, table_name: str, cols: List[ColDef], n: int):
        boundary_map = {}
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
            non_pk = [c for c in cols if not c.is_primary_key]
            target = random.choice(non_pk if non_pk else cols)
            bval   = random.choice(boundary_map[target.name])
            vals = [
                str(random.randint(1, 10_000_000)) if c.is_primary_key
                else (bval if c.name == target.name else 'NULL')
                for c in cols
            ]
            self._try_insert(conn, table_name, cols, vals)

    def _generate_value(self, col: ColDef, skew: SkewProfile,
                         hotspot_prob: float, stage: str) -> str:
        if col.is_primary_key:
            return str(random.randint(1, 10_000_000))
        use_hot  = random.random() < hotspot_prob
        hot_vals = skew.hot_values_by_col.get(col.name, [])
        exp_hot  = skew.expansion_hot if col.name == skew.predicate_col.name else None
        dt       = col.data_type
        if use_hot and hot_vals:
            if stage == 'expansion' and exp_hot and random.random() < 0.4:
                return exp_hot
            return random.choice(hot_vals)
        if dt == 'INT':    return str(random.randint(-1000, 1000))
        if dt == 'VARCHAR':
            n = random.randint(1, 20)
            return f"'{''.join(random.choices('abcdefghijklmnopqrstuvwxyz0123456789_', k=n))}'"
        if dt in ('FLOAT','DOUBLE'): return f"{random.uniform(-1000, 1000):.3f}"
        if dt == 'DECIMAL':          return f"{random.uniform(-1000, 1000):.2f}"
        return 'NULL'

    def _try_insert(self, conn, table_name: str, cols: List[ColDef], vals: List[str]):
        col_names = ', '.join(f'`{c.name}`' for c in cols)
        sql = f"INSERT IGNORE INTO {table_name} ({col_names}) VALUES ({', '.join(vals)})"
        try:
            self._exec_dml(conn, sql)
        except Exception as e:
            self._log(f"  INSERT skipped: {e}")

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
        """
        统一验证路径（所有查询均为行保留查询）：
          1. COUNT 单调：COUNT(S1) ≤ COUNT(S2)
          2. MAX  单调：MAX(col, S1) ≤ MAX(col, S2)，对所有 numeric 列
          3. MIN  单调：MIN(col, S1) ≥ MIN(col, S2)，对所有 numeric 列
          4. 行集合子集：row_digests(S1) ⊆ row_digests(S2)
        """
        self._verify_count(spec, s1, s2)
        for c in numeric_cols:
            self._verify_max(spec, c.name, s1, s2)
            self._verify_min(spec, c.name, s1, s2)
        if s1.row_digests:
            self._verify_row_subset(conn, spec, s1, s2)

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
            if self._is_expected_query_runtime_error(e):
                raise IgnorableQueryRuntimeError(str(e)) from e
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
        log_path = os.path.join(
            self._log_dir,
            f'SubsetOracle_bugs_{time.strftime("%Y%m%d_%H%M%S")}.log'
        )
        ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        with open(log_path, 'a', encoding='utf-8') as f:
            f.write(f"\n{'='*60}\n")
            f.write(f"[{ts}] Round #{uid} BUG DETECTED\n")
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
            if self._is_expected_query_runtime_error(e):
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
            if self._is_expected_query_runtime_error(e):
                raise IgnorableQueryRuntimeError(str(e)) from e
            self._log(f"  exec_single_float failed: {e}")
            return None

    def _drop_if_exists(self, conn, table_name: str):
        try:
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {table_name}")
        except Exception:
            pass

    def _is_expected_query_runtime_error(self, err: Exception) -> bool:
        code = None
        if getattr(err, 'args', None):
            first = err.args[0]
            if isinstance(first, int):
                code = first

        if code in EXPECTED_MYSQL_RUNTIME_ERROR_CODES:
            return True

        msg = str(err).lower()
        return any(pat in msg for pat in EXPECTED_MYSQL_RUNTIME_ERROR_PATTERNS)

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
        return ' | '.join(plan) if plan else '[]'
