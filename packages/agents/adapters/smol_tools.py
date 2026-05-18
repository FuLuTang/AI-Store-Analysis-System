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

    @tool
    def read_plan() -> str:
        """Read the task plan from output/plan.json. Returns JSON array of steps with status."""
        plan_path = ws.resolve("output/plan.json")
        if not plan_path.exists():
            return json.dumps({"error": "plan.json not found"})
        return plan_path.read_text(encoding="utf-8")

    @tool
    def check_plan(success: bool, step_index: int) -> str:
        """Mark a plan step as success or failed.

        Args:
            success: True = success, False = failed
            step_index: Index of the step in the plan (0-based)
        """
        plan_path = ws.resolve("output/plan.json")
        if not plan_path.exists():
            return json.dumps({"error": "plan.json not found"})
        plan = json.loads(plan_path.read_text(encoding="utf-8"))
        if step_index < 0 or step_index >= len(plan):
            return json.dumps({"error": f"step_index {step_index} out of range (0-{len(plan)-1})"})
        plan[step_index]["status"] = "success" if success else "failed"
        plan_path.write_text(json.dumps(plan, ensure_ascii=False, indent=2), encoding="utf-8")
        return f"Step {step_index} marked as {'success' if success else 'failed'}"

    return [
        read_file, write_file, list_files,
        run_python,
        duckdb_query, duckdb_register_parquet,
        read_context,
        profile_table,
        validate_result,
        setup_workspace, cleanup_workspace, list_tables,
        read_plan, check_plan,
    ]
