"""
smol_tool_adapter.py — 精简版：只保留 CodeAgent 原生 Python 做不了的

CodeAgent 自己写 Python 能做的事：
  - 读文件: open("input/xxx").read()
  - 写文件: open("output/xxx", "w").write()
  - 列文件: os.listdir() / glob.glob()
  - 数据画像: pandas.read_parquet().info()

保留为 tool 的事：
  - duckdb_query         — 只读 SQL 查询（防 Agent 写错 DuckDB API）
  - duckdb_register      — 固定注册步骤，减少犯蠢
  - read_context         — 稳定读指标文档
  - list_tables          — 快速看 DuckDB 里有什么
  - validate_result      — 必须过 Pydantic 校验
  - submit_final_result  — smol 独有的提交动作
"""
from ..workspace import AgentWorkspace


def create_smol_tools(ws: AgentWorkspace) -> list:
    """注入 workspace 并返回 smolagents @tool 列表"""
    from smolagents import tool

    _inject_workspace(ws)

    from ..tools import (
        duckdb_query,
        duckdb_register_parquet,
        read_context,
        validate_result,
    )

    @tool
    def t_duckdb_query(sql: str) -> str:
        """Execute read-only SQL on DuckDB. Returns JSON.
        The DuckDB instance already has registered tables.

        Args:
            sql: SELECT-only SQL query
        """
        return duckdb_query(sql)

    @tool
    def t_duckdb_register(table_name: str, parquet_path: str) -> str:
        """Register a parquet file as a DuckDB table.

        Args:
            table_name: Table name to register
            parquet_path: Path to parquet in workspace (e.g. 'output/flat.parquet')
        """
        return duckdb_register_parquet(table_name, parquet_path)

    @tool
    def t_read_context(doc_name: str) -> str:
        """Read a context document from workspace context/ directory.

        Args:
            doc_name: Document file name (e.g. '指标计算文档.md')
        """
        return read_context(doc_name)

    @tool
    def t_list_tables() -> str:
        """List all registered DuckDB tables and their row counts."""
        import duckdb, json
        con = duckdb.connect(":memory:")
        try:
            tables = con.execute(
                "SELECT table_name, estimated_size FROM duckdb_tables()"
            ).fetchall()
            result = {}
            for name, _ in tables:
                count = con.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
                result[name] = count
            return json.dumps(result, ensure_ascii=False)
        finally:
            con.close()

    @tool
    def t_validate_result(result_json: str) -> str:
        """Validate output JSON against AgentResult schema.
        Call this before submit_final_result.

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
    def t_submit_final_result(result_json: str) -> str:
        """Submit the final AgentResult. Writes to output/result.json.
        MUST call this as the LAST step after validate_result passes.

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
        t_duckdb_query,
        t_duckdb_register,
        t_read_context,
        t_list_tables,
        t_validate_result,
        t_submit_final_result,
    ]


def _inject_workspace(ws: AgentWorkspace):
    import packages.agents.tools.file_tool as file_tool
    file_tool.get_workspace = lambda: ws
