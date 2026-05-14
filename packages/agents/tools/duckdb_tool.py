"""
duckdb_tool.py — DuckDB SQL 查询工具（两边共用）
"""
import json


def duckdb_query(sql: str) -> str:
    """在内存 DuckDB 上执行只读 SQL，返回 JSON"""
    import duckdb
    con = duckdb.connect(":memory:")
    try:
        result = con.execute(sql).fetchdf()
        return json.dumps(json.loads(result.to_json(orient="records")), ensure_ascii=False)
    finally:
        con.close()


def duckdb_register_parquet(table_name: str, parquet_path: str) -> str:
    """注册 parquet 文件为 DuckDB 表"""
    import duckdb
    con = duckdb.connect(":memory:")
    try:
        con.execute(f"CREATE VIEW {table_name} AS SELECT * FROM read_parquet('{parquet_path}')")
        row_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        return f"表 {table_name} 已注册, {row_count} 行"
    finally:
        con.close()
