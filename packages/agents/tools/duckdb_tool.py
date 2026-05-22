"""DuckDB 查询（委托到 impl）"""
from .impl.duckdb_impl import duckdb_query_impl, duckdb_register_parquet_impl
from ..workspace import Workspace


def duckdb_query(ws: Workspace, sql: str) -> str:
    return duckdb_query_impl(ws, sql)


def duckdb_register_parquet(ws: Workspace, table_name: str, parquet_path: str) -> str:
    return duckdb_register_parquet_impl(ws, table_name, parquet_path)
