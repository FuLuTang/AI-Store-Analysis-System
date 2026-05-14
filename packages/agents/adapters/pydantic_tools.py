"""Pydantic AI adapter：@agent.tool + ctx.deps.workspace 模式"""
from dataclasses import dataclass, field

from ..workspace import Workspace
from ..tools.impl.file_impl import read_file_impl, write_file_impl, list_files_impl
from ..tools.impl.python_impl import run_python_impl
from ..tools.impl.duckdb_impl import duckdb_query_impl, duckdb_register_parquet_impl
from ..tools.impl.context_impl import read_context_impl
from ..tools.impl.profile_impl import profile_table_impl
from ..tools.impl.validate_impl import validate_result_impl


@dataclass
class AgentDeps:
    workspace: Workspace
    context_docs: dict = field(default_factory=dict)


def register_pydantic_tools(agent):
    ws = None

    @agent.tool
    async def read_file(ctx: RunContext[AgentDeps], path: str) -> str:
        return read_file_impl(ctx.deps.workspace, path)

    @agent.tool
    async def write_file(ctx: RunContext[AgentDeps], path: str, content: str) -> str:
        return write_file_impl(ctx.deps.workspace, path, content)

    @agent.tool
    async def list_files(ctx: RunContext[AgentDeps], subdir: str = "") -> list[str]:
        return list_files_impl(ctx.deps.workspace, subdir)

    @agent.tool
    async def run_python(ctx: RunContext[AgentDeps], script_path: str, timeout: int = 300) -> str:
        return run_python_impl(ctx.deps.workspace, script_path, timeout)

    @agent.tool
    async def duckdb_query(ctx: RunContext[AgentDeps], sql: str) -> str:
        return duckdb_query_impl(ctx.deps.workspace, sql)

    @agent.tool
    async def duckdb_register_parquet(ctx: RunContext[AgentDeps], table_name: str, parquet_path: str) -> str:
        return duckdb_register_parquet_impl(ctx.deps.workspace, table_name, parquet_path)

    @agent.tool
    async def read_context(ctx: RunContext[AgentDeps], doc_name: str) -> str:
        return read_context_impl(ctx.deps.workspace, doc_name)

    @agent.tool
    async def profile_table(ctx: RunContext[AgentDeps], parquet_path: str) -> str:
        return profile_table_impl(ctx.deps.workspace, parquet_path)

    @agent.tool
    async def validate_result(ctx: RunContext[AgentDeps], raw: dict) -> dict:
        return validate_result_impl(raw)

    return {
        "read_file": read_file,
        "write_file": write_file,
        "list_files": list_files,
        "run_python": run_python,
        "duckdb_query": duckdb_query,
        "duckdb_register_parquet": duckdb_register_parquet,
        "read_context": read_context,
        "profile_table": profile_table,
        "validate_result": validate_result,
    }
