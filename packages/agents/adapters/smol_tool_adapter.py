"""
smol_tool_adapter.py — 将共享 tools/ 包装为 smolagents @tool 函数

职责：注入 workspace 引用，为每个共享工具创建 smolagents-compatible 包装。
"""
from ..workspace import AgentWorkspace


def create_smol_tools(ws: AgentWorkspace) -> list:
    """注入 workspace 并返回 smolagents @tool 列表"""
    from smolagents import tool

    _inject_workspace(ws)

    from ..tools import (
        read_workspace_file,
        write_workspace_file,
        list_workspace_files,
        duckdb_query,
        duckdb_register_parquet,
        run_python_script,
        read_context,
        validate_result,
        profile_table,
    )

    @tool
    def t_read_workspace_file(path: str) -> str:
        """Read a file from the workspace.

        Args:
            path: Relative path within the workspace
        """
        return read_workspace_file(path)

    @tool
    def t_write_workspace_file(path: str, content: str) -> str:
        """Write content to a file in the workspace.

        Args:
            path: Relative path within the workspace
            content: Text content to write
        """
        return write_workspace_file(path, content)

    @tool
    def t_list_workspace_files(subdir: str = "") -> str:
        """List all files in the workspace.

        Args:
            subdir: Optional subdirectory to list (default: root)
        """
        files = list_workspace_files(subdir)
        return "\n".join(files) if files else "(empty)"

    @tool
    def t_duckdb_query(sql: str) -> str:
        """Execute a read-only SQL query on DuckDB. Returns JSON.

        Args:
            sql: SQL query string (SELECT only, no DROP/DELETE/INSERT)
        """
        return duckdb_query(sql)

    @tool
    def t_duckdb_register_parquet(table_name: str, parquet_path: str) -> str:
        """Register a parquet file as a DuckDB table.

        Args:
            table_name: Name for the registered table
            parquet_path: Path to the parquet file in workspace
        """
        return duckdb_register_parquet(table_name, parquet_path)

    @tool
    def t_run_python_script(script_path: str) -> str:
        """Execute a Python script inside the workspace sandbox.

        Args:
            script_path: Relative path to the Python script in workspace
        """
        return run_python_script(script_path)

    @tool
    def t_read_context(doc_name: str) -> str:
        """Read a context document (field definitions, metric formulas, industry rules).

        Args:
            doc_name: Document file name (e.g. '指标计算文档.md')
        """
        return read_context(doc_name)

    @tool
    def t_validate_result(result_json: str) -> str:
        """Validate output against AgentResult schema before submission.

        Args:
            result_json: JSON string of the AgentResult
        """
        import json as _json
        try:
            data = _json.loads(result_json) if isinstance(result_json, str) else result_json
        except _json.JSONDecodeError:
            return '{"valid": false, "errors": "Invalid JSON"}'
        return _json.dumps(validate_result(data), ensure_ascii=False)

    @tool
    def t_profile_table(parquet_path: str) -> str:
        """Profile a parquet file: returns column names, dtypes, samples, null rates.

        Args:
            parquet_path: Path to the parquet file in workspace
        """
        return profile_table(parquet_path)

    @tool
    def t_submit_final_result(result_json: str) -> str:
        """Submit the final AgentResult JSON. Writes to output/result.json.
        Must call this as the LAST step after validate_result passes.

        Args:
            result_json: Validated AgentResult JSON string
        """
        import json as _json
        try:
            data = _json.loads(result_json) if isinstance(result_json, str) else result_json
        except _json.JSONDecodeError:
            return "Error: invalid JSON, cannot submit"
        ws.resolve("output/result.json").write_text(
            _json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        return "Final result submitted to output/result.json"

    return [
        t_read_workspace_file,
        t_write_workspace_file,
        t_list_workspace_files,
        t_duckdb_query,
        t_duckdb_register_parquet,
        t_run_python_script,
        t_read_context,
        t_validate_result,
        t_profile_table,
        t_submit_final_result,
    ]


def _inject_workspace(ws: AgentWorkspace):
    """Monkey-patch get_workspace() in shared tools so they find this workspace."""
    import packages.agents.tools.file_tool as file_tool
    file_tool.get_workspace = lambda: ws
