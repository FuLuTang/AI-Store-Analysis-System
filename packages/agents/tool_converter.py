"""tool_converter.py — 将 _impl 工具函数转成 OpenAI function-calling tools schema。

新增/删除工具只改这个文件，prompt 和 agent loop 都不需要动。
"""

import json
from .workspace import Workspace


def available_tool_call_for_agent(ws: Workspace) -> list[dict]:
    """返回 OpenAI tools 参数列表，由已有的 _impl 函数自动拼合。"""
    return [
        # ── 文档解析 ──
        _make_tool(
            name="read_document",
            description="读取任意文档内容（xlsx/csv/pdf/docx/txt/md/json），返回文本摘要（含 sheet 名、列名、行数和样本行）。",
            parameters={
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "workspace 内的相对路径，如 'input/report.xlsx'",
                    }
                },
                "required": ["path"],
            },
        ),
        # ── 文件读写 ──
        _make_tool(
            name="read_file",
            description="读取 workspace 内的文本文件内容。",
            parameters={
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "workspace 内的相对路径"},
                },
                "required": ["path"],
            },
        ),
        _make_tool(
            name="write_file",
            description="将文本内容写入 workspace 文件。父目录自动创建。",
            parameters={
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "workspace 内的相对路径"},
                    "content": {"type": "string", "description": "要写入的文本内容"},
                },
                "required": ["path", "content"],
            },
        ),
        _make_tool(
            name="list_files",
            description="列出 workspace 子目录下的文件。",
            parameters={
                "type": "object",
                "properties": {
                    "subdir": {
                        "type": "string",
                        "description": "子目录名，如 'input', 'tables', 'output'。空字符串表示根目录",
                    }
                },
            },
        ),
        # ── Python 脚本执行 ──
        _make_tool(
            name="run_python",
            description="在 workspace 沙箱内执行一个 Python 脚本，返回 stdout。脚本须先 write_file 写入 scripts/ 目录。",
            parameters={
                "type": "object",
                "properties": {
                    "script_path": {
                        "type": "string",
                        "description": "脚本路径，如 'scripts/flatten.py'",
                    }
                },
                "required": ["script_path"],
            },
        ),
        # ── DuckDB ──
        _make_tool(
            name="duckdb_query",
            description="在 workspace 的 DuckDB 上执行只读 SELECT 查询，返回 JSON 结果。禁止 INSERT/DROP/DELETE/ALTER。注意 DuckDB 标识符需用双引号，如 SELECT \"毛利(元)\" FROM 表名，不要用反引号。",
            parameters={
                "type": "object",
                "properties": {
                    "sql": {"type": "string", "description": "要执行的只读 SELECT SQL 语句"},
                },
                "required": ["sql"],
            },
        ),
        _make_tool(
            name="duckdb_register_parquet",
            description="将 workspace 中的 parquet 文件注册为 DuckDB 视图。",
            parameters={
                "type": "object",
                "properties": {
                    "table_name": {"type": "string", "description": "注册后的表名"},
                    "parquet_path": {
                        "type": "string",
                        "description": "parquet 文件的相对路径，如 'tables/sales.parquet'",
                    },
                },
                "required": ["table_name", "parquet_path"],
            },
        ),
        _make_tool(
            name="list_tables",
            description="列出 DuckDB 中所有已注册的表及其列名和行数。",
            parameters={"type": "object", "properties": {}},
        ),
        # ── 上下文 / 计划 ──
        _make_tool(
            name="read_context",
            description="读取上下文文档。topic 可选 '指标计算文档' 等。",
            parameters={
                "type": "object",
                "properties": {
                    "topic": {"type": "string", "description": "文档主题名称"},
                },
                "required": ["topic"],
            },
        ),
        _make_tool(
            name="read_plan",
            description="读取当前任务计划（plan.json），了解步骤、状态和验收条件。",
            parameters={"type": "object", "properties": {}},
        ),
        _make_tool(
            name="check_plan",
            description="验证当前步骤是否已完成并推进到下一步。每完成一个步骤后必须调用！",
            parameters={
                "type": "object",
                "properties": {
                    "step_index": {
                        "type": "integer",
                        "description": "plan 中步骤的 0-based 索引",
                    }
                },
                "required": ["step_index"],
            },
        ),
    ]


def build_tool_map(ws: Workspace) -> dict:
    """构建 {tool_name: callable} 映射，供 agent loop 执行工具调用。"""
    from ..tools.impl.doc_impl import read_document_impl
    from ..tools.impl.file_impl import read_file_impl, write_file_impl, list_files_impl
    from ..tools.impl.python_impl import run_python_impl
    from ..tools.impl.duckdb_impl import duckdb_query_impl, duckdb_register_parquet_impl
    from ..tools.impl.context_impl import read_context_impl
    from ..tools.impl.setup_impl import list_tables_impl, read_plan_short_impl
    from ..tools.impl.plan_check_impl import run_step_check
    def _read_document(path: str) -> str:
        return read_document_impl(ws, path)

    def _read_file(path: str) -> str:
        return read_file_impl(ws, path)

    def _write_file(path: str, content: str) -> str:
        return write_file_impl(ws, path, content)

    def _list_files(subdir: str = "") -> str:
        files = list_files_impl(ws, subdir)
        return json.dumps(files, ensure_ascii=False)

    def _run_python(script_path: str) -> str:
        return run_python_impl(ws, script_path)

    def _duckdb_query(sql: str) -> str:
        return duckdb_query_impl(ws, sql)

    def _duckdb_register_parquet(table_name: str, parquet_path: str) -> str:
        return duckdb_register_parquet_impl(ws, table_name, parquet_path)

    def _list_tables() -> str:
        return list_tables_impl(ws)

    def _read_context(topic: str) -> str:
        from pathlib import Path
        root = Path(__file__).resolve().parent.parent.parent
        doc_map = {
            "指标计算文档": root / "docs" / "指标计算文档.md",
        }
        path = doc_map.get(topic.replace(".md", ""))
        if path and path.is_file():
            return path.read_text(encoding="utf-8")
        return f"未找到文档: topic={topic}"

    def _read_plan() -> str:
        plan_path = ws.resolve("output/plan.json")
        if not plan_path.exists():
            return "(plan 尚未初始化)"
        plan = json.loads(plan_path.read_text(encoding="utf-8"))
        for step in plan:
            step.pop("check", None)
        return json.dumps(plan, ensure_ascii=False, indent=2)

    def _check_plan(step_index: int) -> str:
        plan_path = ws.resolve("output/plan.json")
        if not plan_path.exists():
            return json.dumps({"error": "plan.json not found"})
        plan = json.loads(plan_path.read_text(encoding="utf-8"))
        if step_index < 0 or step_index >= len(plan):
            return json.dumps({"error": f"step_index {step_index} out of range (0-{len(plan)-1})"})
        step = plan[step_index]
        ok, errors = run_step_check(ws, step)
        step["errors"] = errors
        step["status"] = "success" if ok else "failed"
        # 推进下一步
        if ok:
            for i in range(step_index + 1, len(plan)):
                if plan[i]["status"] == "pending":
                    plan[i]["status"] = "in_progress"
                    break
        plan_path.write_text(json.dumps(plan, ensure_ascii=False, indent=2), encoding="utf-8")
        result = {"step_index": step_index, "ok": ok, "errors": errors}
        if ok:
            result["advanced"] = True
        return json.dumps(result, ensure_ascii=False)

    return {
        "read_document": _read_document,
        "read_file": _read_file,
        "write_file": _write_file,
        "list_files": _list_files,
        "run_python": _run_python,
        "duckdb_query": _duckdb_query,
        "duckdb_register_parquet": _duckdb_register_parquet,
        "list_tables": _list_tables,
        "read_context": _read_context,
        "read_plan": _read_plan,
        "check_plan": _check_plan,
    }


def _make_tool(name: str, description: str, parameters: dict) -> dict:
    return {
        "type": "function",
        "function": {
            "name": name,
            "description": description,
            "parameters": parameters,
        },
    }
