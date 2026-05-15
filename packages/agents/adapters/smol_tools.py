"""Smolagents adapter：@tool + 闭包捕获 workspace"""
import json
from ..workspace import Workspace
from ..tools.impl.file_impl import read_file_impl, write_file_impl, list_files_impl
from ..tools.impl.python_impl import run_python_impl
from ..tools.impl.duckdb_impl import duckdb_query_impl, duckdb_register_parquet_impl
from ..tools.impl.context_impl import read_context_impl
from ..tools.impl.profile_impl import profile_table_impl
from ..tools.impl.validate_impl import validate_result_impl
from ..tools.impl.setup_impl import setup_workspace_impl, cleanup_workspace_impl, list_tables_impl


def build_smol_tools(ws: Workspace):
    try:
        from smolagents import tool
    except ImportError:
        raise ImportError("smolagents 未安装，请执行: pip install smolagents")

    @tool
    def read_file(path: str) -> str:
        return read_file_impl(ws, path)

    @tool
    def write_file(path: str, content: str) -> str:
        return write_file_impl(ws, path, content)

    @tool
    def list_files(subdir: str = "") -> list[str]:
        return list_files_impl(ws, subdir)

    @tool
    def run_python(script_path: str, timeout: int = 300) -> str:
        return run_python_impl(ws, script_path, timeout)

    @tool
    def duckdb_query(sql: str) -> str:
        return duckdb_query_impl(ws, sql)

    @tool
    def duckdb_register_parquet(table_name: str, parquet_path: str) -> str:
        return duckdb_register_parquet_impl(ws, table_name, parquet_path)

    @tool
    def read_context(doc_name: str) -> str:
        return read_context_impl(ws, doc_name)

    @tool
    def profile_table(parquet_path: str) -> str:
        return profile_table_impl(ws, parquet_path)

    @tool
    def validate_result(raw: str) -> str:
        data = json.loads(raw) if isinstance(raw, str) else raw
        return str(validate_result_impl(data))

    @tool
    def setup_workspace() -> str:
        return setup_workspace_impl(ws)

    @tool
    def cleanup_workspace(mode: str = "large") -> str:
        return cleanup_workspace_impl(ws, mode)

    @tool
    def list_tables() -> str:
        return list_tables_impl(ws)

    return [
        read_file, write_file, list_files,
        run_python,
        duckdb_query, duckdb_register_parquet,
        read_context,
        profile_table,
        validate_result,
        setup_workspace, cleanup_workspace, list_tables,
    ]
