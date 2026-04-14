#!/usr/bin/env python3
"""
replay_sql_percona.py
读取包含 SQL 语句序列的 txt 文件，在 Percona Server 中逐条执行。

针对 Percona 的调整：
1. 默认端口改为 3310。
2. 默认用户和密码改为 sqlancer。
3. 强化了对加密插件 (caching_sha2_password) 的依赖检查。

依赖：
    pip install mysql-connector-python cryptography
"""

import argparse
import sys
import traceback
from pathlib import Path

try:
    import mysql.connector
    from mysql.connector import Error as MySQLError
except ImportError:
    print("❌ 缺少基础依赖，请执行：pip install mysql-connector-python")
    sys.exit(1)

# ──────────────────────────────────────────────
# SQL 解析与日志类 (保持原逻辑，确保稳定)
# ──────────────────────────────────────────────

def split_statements(text: str) -> list[str]:
    statements = []
    current = []
    in_single = in_double = False
    i = 0
    while i < len(text):
        ch = text[i]
        if not in_single and not in_double and ch == '-' and text[i:i+2] == '--':
            end = text.find('\n', i)
            if end == -1: break
            current.append(text[i:end+1])
            i = end + 1
            continue
        if not in_single and not in_double and ch == '/' and text[i:i+2] == '/*':
            end = text.find('*/', i+2)
            if end == -1: break
            current.append(text[i:end+2])
            i = end + 2
            continue
        if ch == "'" and not in_double: in_single = not in_single
        elif ch == '"' and not in_single: in_double = not in_double
        if ch == ';' and not in_single and not in_double:
            current.append(ch)
            stmt = ''.join(current).strip()
            if stmt and stmt != ';': statements.append(stmt)
            current = []
            i += 1
            continue
        current.append(ch)
        i += 1
    last = ''.join(current).strip()
    if last: statements.append(last)
    return statements

class Logger:
    def __init__(self, log_path: str):
        self.path = Path(log_path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = open(self.path, 'w', encoding='utf-8')

    def write(self, text: str):
        print(text, end='')
        self._fh.write(text)
        self._fh.flush()

    def close(self):
        self._fh.close()

    def log_select(self, sql: str, rows: list, cols: list):
        self.write(sql.rstrip() + '\n')
        if not rows:
            self.write("(empty)\n")
            return
        widths = {c: len(str(c)) for c in cols}
        for row in rows:
            for c in cols:
                val = 'NULL' if row[c] is None else str(row[c])
                widths[c] = max(widths[c], len(val))
        sep = '+' + '+'.join('-' * (widths[c] + 2) for c in cols) + '+'
        header = '|' + '|'.join(f" {str(c):<{widths[c]}} " for c in cols) + '|'
        self.write(sep + '\n' + header + '\n' + sep + '\n')
        for row in rows:
            line = '|' + '|'.join(f" {'NULL' if row[c] is None else str(row[c]):<{widths[c]}} " for c in cols) + '|'
            self.write(line + '\n')
        self.write(sep + '\n')

    def log_error(self, sql: str, exc: Exception):
        self.write(sql.rstrip() + '\n')
        self.write(f"ERROR: {exc}\n")

# ──────────────────────────────────────────────
# 执行引擎 (针对 Percona/MySQL 8.0 优化)
# ──────────────────────────────────────────────

def connect(args):
    try:
        return mysql.connector.connect(
            host=args.host,
            port=args.port,
            user=args.user,
            password=args.password,
            consume_results=True,
            connection_timeout=30,
            # 显式允许使用更安全的验证插件
            auth_plugin='caching_sha2_password' if args.user != 'yaoruifei' else None 
        )
    except MySQLError as e:
        if "cryptography" in str(e):
            print("\n❌ 错误：Percona 8.0 要求加密库。")
            print("请执行：pip install cryptography\n")
        raise e

def run(args):
    input_path = Path(args.input)
    if not input_path.exists():
        print(f"❌ 找不到输入文件：{input_path}")
        sys.exit(1)

    statements = split_statements(input_path.read_text(encoding='utf-8', errors='replace'))
    if not statements:
        print("⚠️ 未发现 SQL 语句。")
        sys.exit(0)

    logger = Logger(args.output)

    try:
        conn = connect(args)
        cursor = conn.cursor(dictionary=True)
        
        # 初始化数据库
        db = args.database
        for isql in [f"DROP DATABASE IF EXISTS `{db}`", f"CREATE DATABASE `{db}`", f"USE `{db}`"]:
            cursor.execute(isql)
        conn.commit()
    except Exception as e:
        logger.write(f"[FATAL] 连接或初始化失败: {e}\n")
        logger.close()
        sys.exit(1)

    for sql in statements:
        try:
            cursor.execute(sql)
            if cursor.description:
                rows = cursor.fetchall()
                cols = list(rows[0].keys()) if rows else []
                logger.log_select(sql, rows, cols)
            else:
                conn.commit()
                logger.write(sql.rstrip() + '\n')
        except Exception as e:
            logger.log_error(sql, e)
            try: conn.rollback()
            except: pass

    cursor.close()
    conn.close()
    logger.close()
    print(f"\n✅ 回放完成，日志：{Path(args.output).resolve()}")

def parse_args():
    p = argparse.ArgumentParser(description="Percona SQL 回放工具")
    p.add_argument('--input',    required=True)
    p.add_argument('--output',   required=True)
    p.add_argument('--host',     default='127.0.0.1')
    p.add_argument('--port',     default=3310, type=int) # Percona 默认端口
    p.add_argument('--user',     default='root')     # 默认用户
    p.add_argument('--password', default='123456')     # 默认密码
    p.add_argument('--database', default='percona_replay')
    return p.parse_args()

if __name__ == '__main__':
    run(parse_args())