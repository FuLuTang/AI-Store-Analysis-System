"""DuckDB 工具：建临时库、注册 parquet、执行 SQL。"""

from packages.agents.workspace import Workspace


async def create_duckdb(ws: Workspace) -> str:
    """在当前 workspace 创建 DuckDB 内存库并返回连接标识。"""
    ...


async def register_parquet(ws: Workspace, table_name: str, parquet_path: str) -> None:
    """将 parquet 文件注册为 DuckDB 表/视图。"""
    ...


async def execute_sql(ws: Workspace, sql: str) -> list[dict]:
    """在 workspace 的 DuckDB 上执行 SELECT SQL，返回行列表。"""
    ...
