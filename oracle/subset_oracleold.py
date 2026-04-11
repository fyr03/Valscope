"""
SubsetOracle: 基于数据子集关系 S1 ⊆ S2 的 MySQL 逻辑 Bug 检测 Oracle

核心思路：
  1. 建临时表，插入少量带偏斜分布的数据 → S1
  2. 生成多条查询，在 S1 上执行并保存快照（COUNT / MAX / MIN / 行摘要 / 分组统计）
  3. 在同一张表上大量插入更多偏斜数据 + ANALYZE TABLE → S2（S1 ⊆ S2）
  4. 用相同查询在 S2 上执行并保存快照
  5. 比较 EXPLAIN 计划是否发生变化（计划切换时更容易暴露 bug）
  6. 验证单调性：COUNT(S1)≤COUNT(S2), MAX(S1)≤MAX(S2), MIN(S1)≥MIN(S2), rowset(S1)⊆rowset(S2)
"""

import os
import re
import random
import math
import hashlib
import uuid
import pymysql
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Tuple, Any
from datetime import datetime


# ─────────────────────────────────────────────
# 常量配置（对应 Java 里的 static final）
# ─────────────────────────────────────────────
TARGET_BASELINE_QUERIES      = 6      # 目标收集的基线查询数
MIN_BASELINE_QUERIES         = 3      # 低于此数则跳过本轮
MAX_QUERY_GEN_ATTEMPTS       = 72     # 生成查询的最大尝试次数
BASELINE_HOT_ROWS            = 2      # S1 热值行数
BASELINE_RANDOM_ROWS         = 4      # S1 随机行数
BASELINE_NOISE_ROWS          = 4      # S1 边界噪声行数
SKEWED_EXPANSION_ROWS        = 2000   # S2 扩展插入行数（基准值，实际会加随机抖动）
UNCHANGED_PLAN_VERIFY_PROB   = 0.15   # 计划未变时仍然验证的概率
FLOAT_TOLERANCE              = 1e-9   # 浮点比较容差


# ─────────────────────────────────────────────
# 数据类：列定义（轻量，不依赖 valscope Table 对象）
# ─────────────────────────────────────────────
@dataclass
class ColDef:
    name: str
    data_type: str          # 'INT' / 'VARCHAR' / 'FLOAT' / 'DOUBLE' / 'DECIMAL'
    is_primary_key: bool = False
    is_nullable: bool = True
    varchar_len: int = 128  # 仅 VARCHAR 使用


# ─────────────────────────────────────────────
# 数据类：偏斜分布配置
# ─────────────────────────────────────────────
@dataclass
class SkewProfile:
    predicate_col: ColDef
    primary_hot:   str      # 主热值（SQL literal，如 '42' 或 "'hv_123'"）
    secondary_hot: str
    tertiary_hot:  str
    expansion_hot: str      # S2 阶段新增热值
    hot_values_by_col: Dict[str, List[str]] = field(default_factory=dict)


# ─────────────────────────────────────────────
# 数据类：查询规格
# ─────────────────────────────────────────────
@dataclass
class QuerySpec:
    table_name:   str
    shape:        str       # 见 QUERY_SHAPES
    where_clause: str       # 含 WHERE 关键字，如 " WHERE `c1` = 42"
    select_sql:   str       # 完整 SELECT 语句
    result_cols:  List[ColDef] = field(default_factory=list)
    grouping_col: Optional[ColDef] = None
    agg_col:      Optional[ColDef] = None
    offset:       int = 0
    agg_src_sql:  Optional[str] = None  # DERIVED_TABLE / IN_SUBQUERY 用


QUERY_SHAPES = [
    'PLAIN_SELECT',
    'DERIVED_TABLE',
    'IN_SUBQUERY',
    'COUNT_STAR',
    'GROUP_BY_COUNT',
    'GROUP_BY_MAX',
    'COUNT_DISTINCT',
    'ORDER_BY_LIMIT_ASC',
    'ORDER_BY_LIMIT_DESC',
]


# ─────────────────────────────────────────────
# 数据类：查询快照（S1 或 S2 的执行结果）
# ─────────────────────────────────────────────
@dataclass
class QuerySnapshot:
    count:           Optional[int]           = None
    max_values:      Dict[str, Optional[float]] = field(default_factory=dict)
    min_values:      Dict[str, Optional[float]] = field(default_factory=dict)
    row_digests:     Dict[str, int]          = field(default_factory=dict)  # digest -> 出现次数
    grouped_counts:  Dict[str, int]          = field(default_factory=dict)
    grouped_max:     Dict[str, Optional[float]] = field(default_factory=dict)
    distinct_count:  Optional[int]           = None
    ordered_value:   Optional[float]         = None
    explain_plan:    List[str]               = field(default_factory=list)


# ═══════════════════════════════════════════════════════════════
#  主类
# ═══════════════════════════════════════════════════════════════
class SubsetOracle:

    def __init__(self, db_config: dict, verbose: bool = True, log_sql: bool = False,
             log_file: str = None):
        self.db_config  = db_config
        self.verbose    = verbose
        self.log_sql    = log_sql
        self.log_file   = log_file
        self._log_dir   = None

        # 累计统计（跨 run() 调用持续累加）
        self.total_rounds        = 0   # 总共完成的轮次
        self.total_queries       = 0   # 总共验证的查询数
        self.total_plan_changes  = 0   # 触发计划切换的查询数
        self.total_bugs          = 0   # 发现的 bug 数

    # ──────────────────────────────────────────
    # 公共入口
    # ──────────────────────────────────────────
    def run(self) -> dict:
        uid         = uuid.uuid4().hex[:8]
        table_name  = f"subset3_{uid}"
        aux_name    = f"subset3_aux_{uid}"
        self._sql_log = []

        # 本轮统计
        round_stats = {
            'round_id':     uid,
            'queries':      0,   # 本轮实际验证的查询数
            'plan_changes': 0,   # 本轮触发计划切换的查询数
            'bugs':         0,   # 本轮发现的 bug 数
            'skipped':      False,
        }

        # 日志目录
        db_type = self.db_config.get('db_type', 'MYSQL').upper()
        self._log_dir = os.path.join('invalid_mutation', db_type)
        os.makedirs(self._log_dir, exist_ok=True)

        self._log(f"\n{'='*60}")
        self._log(f" SUBSET ORACLE round #{uid}")
        self._log(f"{'='*60}")

        conn = self._connect()
        if conn is None:
            self._log("  [SKIP] Cannot connect to database.")
            return

        try:
            # ── Step 1：建表 ──────────────────────────────
            self._log(f"\n[Step 1] Creating table: {table_name}")
            cols = self._generate_random_columns()
            self._create_table(conn, table_name, cols)

            numeric_cols = [c for c in cols if c.data_type in ('INT', 'FLOAT', 'DOUBLE', 'DECIMAL')]

            # ── Step 1.5：选谓词列、建索引、构造偏斜配置 ──
            pred_col  = self._choose_predicate_col(cols)
            self._ensure_indexes(conn, table_name, pred_col, uid)
            skew      = self._create_skew_profile(cols, pred_col)
            self._log(f"  Predicate col: {pred_col.name}, primary_hot={skew.primary_hot}")

            # 辅助 JOIN 表
            self._create_aux_table(conn, aux_name, pred_col, skew, uid)

            # ── Step 2：插入 S1 数据 ──────────────────────
            self._log(f"\n[Step 2] Building S1 ...")
            self._insert_hot_seed_rows(conn, table_name, cols, skew, BASELINE_HOT_ROWS)
            self._insert_skewed_rows(conn, table_name, cols, skew,
                                     BASELINE_RANDOM_ROWS + random.randint(0, 3), 0.35, stage='baseline')
            self._insert_noise_rows(conn, table_name, cols, BASELINE_NOISE_ROWS)

            s1_count = self._exec_single_int(conn, f"SELECT COUNT(*) FROM {table_name}")
            if not s1_count:
                self._log("  [SKIP] S1 is empty.")
                return
            self._log(f"  S1 row count: {s1_count}")

            # ── Step 3：生成查询并收集 S1 快照 ───────────
            self._log(f"\n[Step 3] Building baseline queries on S1 ...")
            baselines = self._build_baselines(conn, table_name, cols, numeric_cols, skew, aux_name)
            self._log(f"  Validated baselines: {len(baselines)}")
            if len(baselines) < MIN_BASELINE_QUERIES:
                self._log("  [SKIP] Not enough valid baseline queries.")
                round_stats['skipped'] = True
                return round_stats

            # ── Step 4：插入 S2 数据 + ANALYZE TABLE ─────
            self._log(f"\n[Step 4] Expanding to S2 ...")
            self._sql_log.append('START TRANSACTION;')
            conn.begin()
            self._insert_skewed_rows(conn, table_name, cols, skew,
                                     SKEWED_EXPANSION_ROWS + 64 * random.randint(0, 8),
                                     0.92, stage='expansion')
            conn.commit()
            self._sql_log.append('COMMIT;')

            s2_count = self._exec_single_int(conn, f"SELECT COUNT(*) FROM {table_name}")
            if s2_count is None or s2_count <= s1_count:
                self._log("  [SKIP] Table did not grow enough.")
                return
            self._log(f"  S2 row count: {s2_count}, growth ratio: {s2_count/s1_count:.2f}x")

            self._analyze_table(conn, table_name)

            # ── Step 5 & 6：在 S2 上重跑查询并验证 ───────
            self._log(f"\n[Step 5+6] Verifying monotonicity on S2 ...")
            for i, (spec, s1_snap) in enumerate(baselines):
                s2_plan      = self._capture_explain(conn, spec.select_sql)
                plan_changed = not self._plans_equivalent(s1_snap.explain_plan, s2_plan)
                self._log(f"  Query[{i+1}] plan_changed={plan_changed}")

                if not plan_changed and random.random() > UNCHANGED_PLAN_VERIFY_PROB:
                    self._log(f"  Query[{i+1}] plan unchanged, skipping verification.")
                    continue

                # 走到这里才算真正验证了一条查询
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
            self._drop_if_exists(conn, table_name)
            self._drop_if_exists(conn, aux_name)
            conn.close()
            self._sql_log = []
            self._log(f"{'='*60}\n")

        # 累加全局统计
        if not round_stats['skipped']:
            self.total_rounds       += 1
            self.total_queries      += round_stats['queries']
            self.total_plan_changes += round_stats['plan_changes']
            self.total_bugs         += round_stats['bugs']

        return round_stats

    # ──────────────────────────────────────────
    # Step 1：建表
    # ──────────────────────────────────────────
    def _generate_random_columns(self) -> List[ColDef]:
        """随机生成 3~6 列，类型覆盖 INT / VARCHAR / FLOAT / DECIMAL"""
        type_pool = ['INT', 'INT', 'INT', 'VARCHAR', 'VARCHAR', 'FLOAT', 'DOUBLE', 'DECIMAL']
        n_cols = random.randint(3, 6)
        cols: List[ColDef] = []

        # 主键列（INT，不可 NULL）
        cols.append(ColDef(name='pk', data_type='INT',
                           is_primary_key=True, is_nullable=False))

        for i in range(1, n_cols):
            dtype = random.choice(type_pool)
            vlen  = random.choice([32, 64, 128, 255]) if dtype == 'VARCHAR' else 128
            cols.append(ColDef(
                name=f'c{i}',
                data_type=dtype,
                is_primary_key=False,
                is_nullable=True,
                varchar_len=vlen,
            ))
        return cols

    def _col_type_sql(self, col: ColDef) -> str:
        if col.data_type == 'VARCHAR':
            return f'VARCHAR({col.varchar_len})'
        if col.data_type == 'DECIMAL':
            return 'DECIMAL(30, 10)'
        return col.data_type

    def _create_table(self, conn, table_name: str, cols: List[ColDef]):
        col_defs = []
        pk_name  = None
        for c in cols:
            notnull  = ' NOT NULL' if not c.is_nullable else ''
            col_defs.append(f"  `{c.name}` {self._col_type_sql(c)}{notnull}")
            if c.is_primary_key:
                pk_name = c.name
        if pk_name:
            col_defs.append(f"  PRIMARY KEY (`{pk_name}`)")
        ddl = f"CREATE TABLE {table_name} (\n" + ",\n".join(col_defs) + "\n) ENGINE=InnoDB"
        self._exec_ddl(conn, ddl)
        self._log(f"  DDL: {ddl[:120]}...")

    # ──────────────────────────────────────────
    # Step 1.5：选谓词列 / 建索引 / 构造偏斜配置
    # ──────────────────────────────────────────
    def _choose_predicate_col(self, cols: List[ColDef]) -> ColDef:
        """优先选非主键的数值/字符串列"""
        preferred = [c for c in cols if not c.is_primary_key
                     and c.data_type in ('INT', 'VARCHAR', 'FLOAT', 'DOUBLE', 'DECIMAL')]
        if preferred:
            return random.choice(preferred)
        non_pk = [c for c in cols if not c.is_primary_key]
        return random.choice(non_pk) if non_pk else cols[0]

    def _ensure_indexes(self, conn, table_name: str, pred_col: ColDef, round_id: int):
        """在谓词列（及可选的复合列）上建索引"""
        idx_name = f"i_s3_{round_id}"
        self._exec_ddl(conn, f"CREATE INDEX {idx_name} ON {table_name} (`{pred_col.name}`)",
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
            primary_hot=primary,
            secondary_hot=secondary,
            tertiary_hot=tertiary,
            expansion_hot=expansion,
            hot_values_by_col=hot_by_col,
        )

    def _create_hot_values(self, col: ColDef) -> List[str]:
        """为列生成 3 个热值（SQL literal 形式）"""
        dt = col.data_type
        if dt == 'INT':
            anchor = random.randint(-16, 16)
            return [str(anchor), str(anchor + 1 + random.randint(0, 3)),
                    str(anchor - 1 - random.randint(0, 3))]
        if dt == 'VARCHAR':
            stem = f"hv_{random.randint(100, 9999)}"
            return [f"'{stem}'", f"'{stem}_a'", f"'{stem}_b'"]
        if dt in ('FLOAT', 'DOUBLE'):
            anchor = random.randint(-200, 200) / 10.0
            return [f"{anchor:.3f}", f"{anchor+1.0:.3f}", f"{anchor-1.0:.3f}"]
        if dt == 'DECIMAL':
            anchor = random.randint(-1000, 1000) / 100.0
            return [f"{anchor:.2f}", f"{anchor+1.0:.2f}", f"{anchor-1.0:.2f}"]
        return ['NULL']

    def _create_expansion_hot_value(self, col: ColDef, existing: List[str]) -> str:
        """生成一个与已有热值不重复的 expansion 热值"""
        dt = col.data_type
        if dt == 'INT':
            nums = [int(v) for v in existing if v != 'NULL']
            base = max(nums) if nums else 0
            for i in range(8):
                cand = str(base + 20 + i)
                if cand not in existing:
                    return cand
            return str(base + 40)
        if dt == 'VARCHAR':
            for i in range(16):
                cand = f"'exp_{random.randint(1000,9999)}_{i}'"
                if cand not in existing:
                    return cand
            return f"'exp_final_{len(existing)}'"
        if dt in ('FLOAT', 'DOUBLE'):
            nums = [float(v) for v in existing if v != 'NULL']
            base = max(nums) if nums else 0.0
            for i in range(8):
                cand = f"{base+20.0+i:.3f}"
                if cand not in existing:
                    return cand
            return f"{base+40.0:.3f}"
        if dt == 'DECIMAL':
            nums = [float(v) for v in existing if v != 'NULL']
            base = max(nums) if nums else 0.0
            for i in range(8):
                cand = f"{base+20.0+i:.2f}"
                if cand not in existing:
                    return cand
            return f"{base+40.0:.2f}"
        return 'NULL'

    def _create_aux_table(self, conn, aux_name: str,
                          pred_col: ColDef, skew: SkewProfile, round_id: int):
        """创建辅助 JOIN 表（用于 INNER_JOIN 查询 shape）"""
        type_sql = self._col_type_sql(pred_col)
        self._exec_ddl(conn,
            f"CREATE TABLE {aux_name} (`j0` {type_sql}) ENGINE=InnoDB",
            ignore_error=True)
        self._exec_ddl(conn,
            f"CREATE INDEX i_s3_aux_{round_id} ON {aux_name} (`j0`)",
            ignore_error=True)
        for val in [skew.primary_hot, skew.primary_hot,
                    skew.secondary_hot, skew.tertiary_hot, skew.expansion_hot]:
            try:
                self._exec_dml(conn,
                    f"INSERT IGNORE INTO {aux_name} (`j0`) VALUES ({val})")
            except Exception:
                pass

    # ──────────────────────────────────────────
    # Step 2 & 4：数据插入
    # ──────────────────────────────────────────
    def _insert_hot_seed_rows(self, conn, table_name: str,
                               cols: List[ColDef], skew: SkewProfile, n: int):
        """插入主热值行：谓词列固定为 primary_hot，其余列随机"""
        for _ in range(n):
            vals = []
            for c in cols:
                if c.name == skew.predicate_col.name:
                    vals.append(skew.primary_hot)
                else:
                    vals.append(self._generate_value(c, skew, 0.5, 'baseline'))
            self._try_insert(conn, table_name, cols, vals)

    def _insert_skewed_rows(self, conn, table_name: str, cols: List[ColDef],
                             skew: SkewProfile, n: int,
                             hotspot_prob: float, stage: str):
        """插入偏斜分布数据行"""
        for _ in range(n):
            vals = [self._generate_value(c, skew, hotspot_prob, stage) for c in cols]
            self._try_insert(conn, table_name, cols, vals)

    def _insert_noise_rows(self, conn, table_name: str,
                            cols: List[ColDef], n: int):
        """插入边界值行（NULL / INT_MAX / 空字符串 / -0.0 等）"""
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
            target_col = random.choice([c for c in cols if not c.is_primary_key] or cols)
            boundary_val = random.choice(boundary_map[target_col.name])
            vals = []
            for c in cols:
                if c.is_primary_key:
                    # pk 列必须给合法非 NULL 值，否则整行被 INSERT IGNORE 静默丢弃
                    vals.append(str(random.randint(1, 10_000_000)))
                elif c.name == target_col.name:
                    vals.append(boundary_val)
                else:
                    vals.append('NULL')
            self._try_insert(conn, table_name, cols, vals)

    def _generate_value(self, col: ColDef, skew: SkewProfile,
                         hotspot_prob: float, stage: str) -> str:
        """为某列生成一个值（SQL literal 形式）"""
        # 主键列不能重复，直接随机
        if col.is_primary_key:
            return str(random.randint(1, 10_000_000))

        use_hot = random.random() < hotspot_prob
        hot_vals = skew.hot_values_by_col.get(col.name, [])
        expansion_hot = skew.expansion_hot if col.name == skew.predicate_col.name else None

        dt = col.data_type

        if use_hot and hot_vals:
            base = random.choice(hot_vals)
            # expansion 阶段有概率使用 expansion_hot
            if stage == 'expansion' and expansion_hot and random.random() < 0.4:
                return expansion_hot
            return base

        # 随机值
        if dt == 'INT':
            return str(random.randint(-1000, 1000))
        if dt == 'VARCHAR':
            n = random.randint(1, 20)
            chars = 'abcdefghijklmnopqrstuvwxyz0123456789_'
            return f"'{''.join(random.choices(chars, k=n))}'"
        if dt in ('FLOAT', 'DOUBLE'):
            return f"{random.uniform(-1000, 1000):.3f}"
        if dt == 'DECIMAL':
            return f"{random.uniform(-1000, 1000):.2f}"
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
    # Step 3：生成查询并收集 S1 快照
    # ──────────────────────────────────────────
    def _build_baselines(self, conn, table_name: str, cols: List[ColDef],
                          numeric_cols: List[ColDef], skew: SkewProfile,
                          aux_name: str) -> List[Tuple['QuerySpec', 'QuerySnapshot']]:
        results: Dict[str, Tuple[QuerySpec, QuerySnapshot]] = {}
        for attempt in range(MAX_QUERY_GEN_ATTEMPTS):
            if len(results) >= TARGET_BASELINE_QUERIES:
                break
            try:
                spec = self._build_query_spec(table_name, cols, numeric_cols, skew, aux_name)
                if spec.select_sql in results:
                    continue
                snap = self._execute_snapshot(conn, spec, numeric_cols, capture_rows=True)
                if not self._has_useful_result(spec, snap):
                    continue
                results[spec.select_sql] = (spec, snap)
            except Exception as e:
                self._log(f"  Query gen attempt #{attempt+1} failed: {e}")
        return list(results.values())

    def _build_query_spec(self, table_name: str, cols: List[ColDef],
                           numeric_cols: List[ColDef], skew: SkewProfile,
                           aux_name: str) -> QuerySpec:
        """构造一条查询规格，对应 Java 的 buildQuerySpec()"""
        shape = random.choice(QUERY_SHAPES)
        where = self._build_where_clause(skew)

        # 排除无法使用的 shape
        if shape in ('ORDER_BY_LIMIT_ASC', 'ORDER_BY_LIMIT_DESC') and not numeric_cols:
            shape = 'COUNT_STAR'
        if shape == 'GROUP_BY_MAX' and not numeric_cols:
            shape = 'GROUP_BY_COUNT'

        result_cols  = self._choose_projection_cols(cols, skew.predicate_col)
        grouping_col = self._choose_grouping_col(cols)
        agg_col      = random.choice(numeric_cols) if numeric_cols else None
        offset       = random.randint(0, 4)

        col_list = ', '.join(f'`{c.name}`' for c in result_cols)
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
            # 把 WHERE 子句应用到内层子查询，保证热值谓词生效，触发计划切换
            inner_where = where.replace(f'`{pred}`', f'inner_t.`{pred}`')
            inner_where = self._append_conjunct(inner_where,
                                                f"inner_t.`{pred}` IS NOT NULL")
            sub = (f"SELECT inner_t.`{pred}` FROM {table_name} AS inner_t"
                   f"{inner_where}")
            outer_cols = ', '.join(f'outer_t.`{c.name}`' for c in result_cols)
            select_sql = (f"SELECT {outer_cols} FROM {table_name} AS outer_t"
                          f" WHERE outer_t.`{pred}` IN ({sub})")
            agg_src_sql = select_sql

        elif shape == 'COUNT_STAR':
            select_sql = f"SELECT COUNT(*) FROM {table_name}{where}"

        elif shape == 'GROUP_BY_COUNT':
            if grouping_col is None:
                shape = 'COUNT_STAR'
                select_sql = f"SELECT COUNT(*) FROM {table_name}{where}"
            else:
                select_sql = (f"SELECT `{grouping_col.name}`, COUNT(*) FROM {table_name}"
                              f"{where} GROUP BY `{grouping_col.name}`")

        elif shape == 'GROUP_BY_MAX':
            if grouping_col is None or agg_col is None:
                shape = 'COUNT_STAR'
                select_sql = f"SELECT COUNT(*) FROM {table_name}{where}"
            else:
                select_sql = (f"SELECT `{grouping_col.name}`, MAX(`{agg_col.name}`) FROM {table_name}"
                              f"{where} GROUP BY `{grouping_col.name}`")

        elif shape == 'COUNT_DISTINCT':
            dist_col = random.choice(cols)
            select_sql = f"SELECT COUNT(DISTINCT `{dist_col.name}`) FROM {table_name}{where}"

        elif shape == 'ORDER_BY_LIMIT_ASC':
            if agg_col is None:
                shape = 'COUNT_STAR'
                select_sql = f"SELECT COUNT(*) FROM {table_name}{where}"
            else:
                w2 = self._append_conjunct(where, f"`{agg_col.name}` IS NOT NULL")
                select_sql = (f"SELECT `{agg_col.name}` FROM {table_name}{w2}"
                              f" ORDER BY `{agg_col.name}` ASC LIMIT 1 OFFSET {offset}")

        elif shape == 'ORDER_BY_LIMIT_DESC':
            if agg_col is None:
                shape = 'COUNT_STAR'
                select_sql = f"SELECT COUNT(*) FROM {table_name}{where}"
            else:
                w2 = self._append_conjunct(where, f"`{agg_col.name}` IS NOT NULL")
                select_sql = (f"SELECT `{agg_col.name}` FROM {table_name}{w2}"
                              f" ORDER BY `{agg_col.name}` DESC LIMIT 1 OFFSET {offset}")

        return QuerySpec(
            table_name=table_name, shape=shape, where_clause=where,
            select_sql=select_sql, result_cols=result_cols,
            grouping_col=grouping_col, agg_col=agg_col,
            offset=offset, agg_src_sql=agg_src_sql,
        )

    def _choose_projection_cols(self, cols: List[ColDef], pred_col: ColDef) -> List[ColDef]:
        style = random.choice(['ALL', 'PRED_ONLY', 'PRED_PLUS_ONE', 'RANDOM_SUBSET'])
        if style == 'ALL':
            return list(cols)
        if style == 'PRED_ONLY':
            return [pred_col]
        if style == 'PRED_PLUS_ONE':
            others = [c for c in cols if c.name != pred_col.name]
            result = [pred_col]
            if others:
                result.append(random.choice(others))
            return result
        # RANDOM_SUBSET
        subset = random.sample(cols, k=random.randint(1, max(1, len(cols)//2)))
        if pred_col not in subset:
            subset.insert(0, pred_col)
        return subset

    def _choose_grouping_col(self, cols: List[ColDef]) -> Optional[ColDef]:
        safe = [c for c in cols
                if not c.is_primary_key
                and c.data_type not in ('FLOAT', 'DOUBLE')]
        return random.choice(safe) if safe else None

    def _build_where_clause(self, skew: SkewProfile) -> str:
        """根据谓词列类型生成 WHERE 子句"""
        col  = skew.predicate_col
        name = f"`{col.name}`"
        dt   = col.data_type

        if dt == 'INT':
            choice = random.randint(0, 5)   # 去掉 choice 5/6（expansion_hot 在 S1 阶段必然为空）
            if choice == 0: return f" WHERE {name} = {skew.primary_hot}"
            if choice == 1: return f" WHERE {name} IN ({skew.primary_hot}, {skew.secondary_hot})"
            if choice == 2: return f" WHERE {name} BETWEEN {skew.primary_hot} AND {skew.secondary_hot}"
            if choice == 3: return f" WHERE {name} <= {skew.secondary_hot}"
            if choice == 4: return f" WHERE {name} >= {skew.primary_hot}"
            return f" WHERE {name} IS NULL"

        if dt == 'VARCHAR':
            primary_inner = skew.primary_hot.strip("'")
            prefix = primary_inner[0] if primary_inner else 'h'
            choice = random.randint(0, 4)   # 去掉 choice 4/5（expansion_hot）
            if choice == 0: return f" WHERE {name} = {skew.primary_hot}"
            if choice == 1: return f" WHERE {name} IN ({skew.primary_hot}, {skew.secondary_hot})"
            if choice == 2: return f" WHERE {name} LIKE '{prefix}%'"
            if choice == 3: return f" WHERE {name} >= {skew.primary_hot}"
            return f" WHERE {name} IS NULL"

        if dt in ('FLOAT', 'DOUBLE', 'DECIMAL'):
            choice = random.randint(0, 3)   # 去掉 choice 4（expansion_hot）
            if choice == 0: return f" WHERE {name} = {skew.primary_hot}"
            if choice == 1: return f" WHERE {name} >= {skew.primary_hot}"
            if choice == 2: return f" WHERE {name} <= {skew.secondary_hot}"
            if choice == 3: return f" WHERE {name} BETWEEN {skew.primary_hot} AND {skew.secondary_hot}"
            return f" WHERE {name} IS NULL"

        return f" WHERE {name} IS NULL"

    def _append_conjunct(self, where: str, predicate: str) -> str:
        if 'WHERE' in where.upper():
            return f"{where} AND {predicate}"
        return f" WHERE {predicate}"

    # ──────────────────────────────────────────
    # 执行快照（S1 / S2）
    # ──────────────────────────────────────────
    def _execute_snapshot(self, conn, spec: QuerySpec,
                           numeric_cols: List[ColDef],
                           capture_rows: bool) -> QuerySnapshot:
        snap = QuerySnapshot()

        def count_sql():
            src = spec.agg_src_sql or spec.select_sql
            if spec.agg_src_sql:
                return f"SELECT COUNT(*) FROM ({spec.agg_src_sql}) AS _oracle_view"
            return f"SELECT COUNT(*) FROM {spec.table_name}{spec.where_clause}"

        def max_sql(col_name):
            if spec.agg_src_sql:
                return f"SELECT MAX(`{col_name}`) FROM ({spec.agg_src_sql}) AS _oracle_view"
            return f"SELECT MAX(`{col_name}`) FROM {spec.table_name}{spec.where_clause}"

        def min_sql(col_name):
            if spec.agg_src_sql:
                return f"SELECT MIN(`{col_name}`) FROM ({spec.agg_src_sql}) AS _oracle_view"
            return f"SELECT MIN(`{col_name}`) FROM {spec.table_name}{spec.where_clause}"

        shape = spec.shape

        if shape in ('PLAIN_SELECT',):
            snap.count = self._exec_single_int(conn, count_sql())
            for c in numeric_cols:
                snap.max_values[c.name] = self._exec_single_float(conn, max_sql(c.name))
                snap.min_values[c.name] = self._exec_single_float(conn, min_sql(c.name))
            if capture_rows:
                snap.row_digests = self._capture_row_digests(conn, spec.select_sql)

        elif shape in ('DERIVED_TABLE', 'IN_SUBQUERY'):
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

        elif shape in ('ORDER_BY_LIMIT_ASC', 'ORDER_BY_LIMIT_DESC'):
            snap.ordered_value = self._exec_single_float(conn, spec.select_sql)

        # 捕获 EXPLAIN 计划
        snap.explain_plan = self._capture_explain(conn, spec.select_sql)
        return snap

    def _has_useful_result(self, spec: QuerySpec, snap: QuerySnapshot) -> bool:
        shape = spec.shape
        if shape in ('PLAIN_SELECT', 'DERIVED_TABLE', 'IN_SUBQUERY', 'COUNT_STAR'):
            return snap.count is not None and snap.count > 0
        if shape in ('GROUP_BY_COUNT',):
            return bool(snap.grouped_counts)
        if shape == 'GROUP_BY_MAX':
            return bool(snap.grouped_max)
        if shape == 'COUNT_DISTINCT':
            return snap.distinct_count is not None and snap.distinct_count > 0
        if shape in ('ORDER_BY_LIMIT_ASC', 'ORDER_BY_LIMIT_DESC'):
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
    def _verify(self, conn, spec: QuerySpec,
                 s1: QuerySnapshot, s2: QuerySnapshot,
                 numeric_cols: List[ColDef]):
        shape = spec.shape
        if shape in ('PLAIN_SELECT',):
            self._verify_count(spec, s1, s2)
            for c in numeric_cols:
                self._verify_max(spec, c.name, s1, s2)
                self._verify_min(spec, c.name, s1, s2)
            self._verify_row_subset(conn, spec, s1, s2)

        elif shape in ('DERIVED_TABLE', 'IN_SUBQUERY'):
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

    def _verify_count(self, spec: QuerySpec, s1: QuerySnapshot, s2: QuerySnapshot):
        if s1.count is None or s2.count is None:
            return
        if s1.count > s2.count:
            raise AssertionError(
                f"COUNT violation: S1={s1.count} > S2={s2.count}\n"
                f"  Query: {spec.select_sql}\n"
                f"  Plan1: {self._fmt_plan(s1.explain_plan)}\n"
                f"  Plan2: {self._fmt_plan(s2.explain_plan)}")
        self._log(f"  COUNT  S1={s1.count} <= S2={s2.count}  [PASS]")

    def _verify_max(self, spec: QuerySpec, col: str,
                     s1: QuerySnapshot, s2: QuerySnapshot):
        v1 = s1.max_values.get(col)
        v2 = s2.max_values.get(col)
        if v1 is None:
            return
        if v2 is None or v1 > v2 + FLOAT_TOLERANCE:
            raise AssertionError(
                f"MAX({col}) violation: S1={v1} > S2={v2}\n"
                f"  Query: {spec.select_sql}\n"
                f"  Plan1: {self._fmt_plan(s1.explain_plan)}\n"
                f"  Plan2: {self._fmt_plan(s2.explain_plan)}")
        self._log(f"  MAX({col}) S1={v1} <= S2={v2}  [PASS]")

    def _verify_min(self, spec: QuerySpec, col: str,
                     s1: QuerySnapshot, s2: QuerySnapshot):
        v1 = s1.min_values.get(col)
        v2 = s2.min_values.get(col)
        if v1 is None:
            return
        if v2 is None or v2 > v1 + FLOAT_TOLERANCE:
            raise AssertionError(
                f"MIN({col}) violation: S2_min={v2} > S1_min={v1} (expected S2_min <= S1_min)\n"
                f"  Query: {spec.select_sql}\n"
                f"  Plan1: {self._fmt_plan(s1.explain_plan)}\n"
                f"  Plan2: {self._fmt_plan(s2.explain_plan)}")
        self._log(f"  MIN({col}) S1={v1} >= S2={v2}  [PASS]")

    def _verify_row_subset(self, conn, spec: QuerySpec,
                            s1: QuerySnapshot, s2: QuerySnapshot):
        """验证 S1 的行集合 ⊆ S2 的行集合（基于行摘要多重集合）"""
        if not s1.row_digests:
            return
        # 重新在 S2 上执行并收集摘要
        s2_digests = self._capture_row_digests(conn, spec.select_sql)
        missing = {}
        remaining = dict(s2_digests)
        for digest, cnt in s1.row_digests.items():
            available = remaining.get(digest, 0)
            if available < cnt:
                missing[digest] = cnt - available
            else:
                remaining[digest] = available - cnt
        if missing:
            raise AssertionError(
                f"ROW-SET subset violation: {len(missing)} digest(s) missing\n"
                f"  Query: {spec.select_sql}\n"
                f"  Plan1: {self._fmt_plan(s1.explain_plan)}\n"
                f"  Plan2: {self._fmt_plan(s2.explain_plan)}")
        s1_total = sum(s1.row_digests.values())
        s2_total = sum(s2_digests.values())
        self._log(f"  ROW-SET |S1|={s1_total} ⊆ |S2|={s2_total}  [PASS]")

    def _verify_grouped_count(self, spec: QuerySpec,
                               s1: QuerySnapshot, s2: QuerySnapshot):
        for key, cnt1 in s1.grouped_counts.items():
            cnt2 = s2.grouped_counts.get(key)
            if cnt2 is None or cnt1 > cnt2:
                raise AssertionError(
                    f"GROUP COUNT violation key={key}: S1={cnt1} > S2={cnt2}\n"
                    f"  Query: {spec.select_sql}")
        self._log(f"  GROUP COUNT  [PASS] ({len(s1.grouped_counts)} groups)")

    def _verify_grouped_max(self, spec: QuerySpec,
                             s1: QuerySnapshot, s2: QuerySnapshot):
        for key, v1 in s1.grouped_max.items():
            if v1 is None:
                continue
            v2 = s2.grouped_max.get(key)
            if v2 is None or v1 > v2 + FLOAT_TOLERANCE:
                raise AssertionError(
                    f"GROUP MAX violation key={key}: S1={v1} > S2={v2}\n"
                    f"  Query: {spec.select_sql}")
        self._log(f"  GROUP MAX  [PASS] ({len(s1.grouped_max)} groups)")

    def _verify_count_distinct(self, spec: QuerySpec,
                                s1: QuerySnapshot, s2: QuerySnapshot):
        if s1.distinct_count is None or s2.distinct_count is None:
            return
        if s1.distinct_count > s2.distinct_count:
            raise AssertionError(
                f"COUNT DISTINCT violation: S1={s1.distinct_count} > S2={s2.distinct_count}\n"
                f"  Query: {spec.select_sql}")
        self._log(f"  COUNT DISTINCT S1={s1.distinct_count} <= S2={s2.distinct_count}  [PASS]")

    def _verify_order_asc(self, spec: QuerySpec,
                           s1: QuerySnapshot, s2: QuerySnapshot):
        """ASC LIMIT 1 → S2 的最小值 ≤ S1 的最小值"""
        if s1.ordered_value is None or s2.ordered_value is None:
            return
        if s2.ordered_value > s1.ordered_value + FLOAT_TOLERANCE:
            raise AssertionError(
                f"ORDER ASC LIMIT violation: S2_min={s2.ordered_value} > S1_min={s1.ordered_value}\n"
                f"  Query: {spec.select_sql}")
        self._log(f"  ORDER ASC  S2={s2.ordered_value} <= S1={s1.ordered_value}  [PASS]")

    def _verify_order_desc(self, spec: QuerySpec,
                            s1: QuerySnapshot, s2: QuerySnapshot):
        """DESC LIMIT 1 → S2 的最大值 ≥ S1 的最大值"""
        if s1.ordered_value is None or s2.ordered_value is None:
            return
        if s1.ordered_value > s2.ordered_value + FLOAT_TOLERANCE:
            raise AssertionError(
                f"ORDER DESC LIMIT violation: S1_max={s1.ordered_value} > S2_max={s2.ordered_value}\n"
                f"  Query: {spec.select_sql}")
        self._log(f"  ORDER DESC  S1={s1.ordered_value} <= S2={s2.ordered_value}  [PASS]")

    # ──────────────────────────────────────────
    # EXPLAIN 计划捕获与比较
    # ──────────────────────────────────────────
    def _capture_explain(self, conn, select_sql: str) -> List[str]:
        rows = []
        try:
            with conn.cursor() as cur:
                cur.execute(f"EXPLAIN FORMAT=TRADITIONAL {select_sql}")
                for row in cur.fetchall():
                    # 列顺序：id, select_type, table, partitions, type,
                    #         possible_keys, key, key_len, ref, rows, filtered, Extra
                    def g(i):
                        return str(row[i]) if row[i] is not None else 'null'
                    rows.append(
                        f"id={g(0)};select_type={g(1)};table={g(2)};"
                        f"type={g(4)};possible_keys={g(5)};key={g(6)};"
                        f"key_len={g(7)};rows={g(9)};filtered={g(10)};extra={g(11)}"
                    )
        except Exception as e:
            self._log(f"  EXPLAIN failed: {e}")
        return rows

    def _plans_equivalent(self, p1: List[str], p2: List[str]) -> bool:
        if len(p1) != len(p2):
            return False
        for r1, r2 in zip(p1, p2):
            if self._normalize_plan_row(r1) != self._normalize_plan_row(r2):
                return False
        return True

    def _normalize_plan_row(self, row: str) -> str:
        """移除 rows= / filtered= / key_len= 的具体数值（随数据量变化，不代表路径切换）"""
        row = re.sub(r'rows=[^;]+', 'rows=?', row)
        row = re.sub(r'filtered=[^;]+', 'filtered=?', row)
        row = re.sub(r'key_len=[^;]+', 'key_len=?', row)
        return row.strip()

    # ──────────────────────────────────────────
    # 行摘要（SHA-256）
    # ──────────────────────────────────────────
    def _capture_row_digests(self, conn, select_sql: str) -> Dict[str, int]:
        """执行查询，返回 {行摘要: 出现次数} 字典"""
        digests: Dict[str, int] = {}
        try:
            with conn.cursor() as cur:
                cur.execute(select_sql)
                for row in cur.fetchall():
                    digest = self._row_digest(row)
                    digests[digest] = digests.get(digest, 0) + 1
        except Exception as e:
            self._log(f"  row digest capture failed: {e}")
        return digests

    def _row_digest(self, row: tuple) -> str:
        h = hashlib.sha256()
        for i, val in enumerate(row):
            if i > 0:
                h.update(b'|')
            s = 'NULL' if val is None else str(val).rstrip()
            # 浮点标准化：-0.0 → 0.0
            try:
                f = float(s)
                if f == 0.0:
                    s = '0.0'
            except (ValueError, TypeError):
                pass
            h.update(s.encode('utf-8'))
        return h.hexdigest()

    # ──────────────────────────────────────────
    # Bug 日志
    # ──────────────────────────────────────────
    def _log_bug(self, error_msg: str, spec: QuerySpec,
                s1: QuerySnapshot, s2: QuerySnapshot, uid: str):
        log_path = os.path.join(self._log_dir, 'SubsetOracle_bugs.log')
        ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        with open(log_path, 'a', encoding='utf-8') as f:
            f.write(f"\n{'='*60}\n")
            f.write(f"[{ts}] Round #{uid} BUG DETECTED\n")
            f.write(f"Query Shape : {spec.shape}\n")
            f.write(f"SELECT SQL  : {spec.select_sql}\n")
            f.write(f"WHERE       : {spec.where_clause}\n")
            f.write(f"Plan S1     : {self._fmt_plan(s1.explain_plan)}\n")
            f.write(f"Plan S2     : {self._fmt_plan(s2.explain_plan)}\n")
            f.write(f"S1 count    : {s1.count}\n")
            f.write(f"S2 count    : {s2.count}\n")
            f.write(f"Error       : {error_msg}\n")
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

    def _fmt_plan(self, plan: List[str]) -> str:
        if not plan:
            return '[]'
        return ' | '.join(plan)