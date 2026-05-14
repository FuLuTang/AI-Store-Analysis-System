"""底层纯函数：DuckDB 持久化连接，统一使用 analysis.duckdb"""
import json
from ...workspace import Workspace


def _get_connection(ws: Workspace):
    import duckdb
    return duckdb.connect(ws.duckdb_path)


def duckdb_query_impl(ws: Workspace, sql: str) -> str:
    con = _get_connection(ws)
    try:
        result = con.execute(sql).fetchdf()
        return json.dumps(json.loads(result.to_json(orient="records")), ensure_ascii=False)
    finally:
        con.close()


def duckdb_register_parquet_impl(ws: Workspace, table_name: str, parquet_path: str) -> str:
    con = _get_connection(ws)
    try:
        con.execute(f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM read_parquet('{parquet_path}')")
        row_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        return f"表 {table_name} 已注册, {row_count} 行"
    finally:
        con.close()
