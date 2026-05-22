"""共享工具注册表。底层委托到 impl，适配器见 adapters/。"""
from .file_tool import read_workspace_file, write_workspace_file, list_workspace_files
from .duckdb_tool import duckdb_query, duckdb_register_parquet
from .python_tool import run_python_script
from .context_tool import read_context
from .validate_tool import validate_result
from .profile_tool import profile_table
from .setup_tool import setup_workspace, cleanup_workspace, list_tables

__all__ = [
    "read_workspace_file", "write_workspace_file", "list_workspace_files",
    "duckdb_query", "duckdb_register_parquet",
    "run_python_script",
    "read_context",
    "validate_result",
    "profile_table",
    "setup_workspace", "cleanup_workspace", "list_tables",
]
