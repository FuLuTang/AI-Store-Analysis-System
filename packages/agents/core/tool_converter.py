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
            name="read_document_structure",
            description="对各类文档（Excel, CSV, Word, PDF, JSON, Markdown, Zip, SQLite）进行结构化结构侦察，不返回全文内容以保障上下文安全。返回包含大纲、列名、行数或表结构的 JSON 元数据。",
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
            description="从 workspace 读取文本文件内容，支持分页(offset/limit)以及首尾快捷读取(head/tail)。对于大文件或二进制文件，请勿全文读取。",
            parameters={
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "workspace 内的相对路径"},
                    "offset": {"type": "integer", "description": "从第几行开始读取（0-based，使用 head/tail 时忽略）"},
                    "limit": {"type": "integer", "description": "最多读取多少行，默认且最大 2000 行"},
                    "head": {"type": "integer", "description": "从文件开头读取的行数"},
                    "tail": {"type": "integer", "description": "从文件结尾读取的行数"},
                },
                "required": ["path"],
            },
        ),
        _make_tool(
            name="write_file",
            description="将文本内容写入 workspace 文件。父目录自动创建；默认原子覆盖，可用 mode='append' 追加。",
            parameters={
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "workspace 内的相对路径"},
                    "content": {"type": "string", "description": "要写入的文本内容"},
                    "mode": {"type": "string", "enum": ["overwrite", "append"], "description": "写入模式，默认 overwrite；追加写入用 append"},
                },
                "required": ["path", "content"],
            },
        ),
        _make_tool(
            name="list_files",
            description="列出 workspace 子目录下的文件列表，返回包含路径、文件名、字节大小、可读大小、后缀、文件类型和推荐工具的 JSON 数组。",
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
        _make_tool(
            name="search_files",
            description="在指定文件或整个工作区内快速检索关键字（支持正则），返回匹配的行号及文本行，避免读取大文件全文。",
            parameters={
                "type": "object",
                "properties": {
                    "pattern": {"type": "string", "description": "检索的文本子串或正则表达式"},
                    "path": {"type": "string", "description": "指定检索的单个文件相对路径。若为空，全局检索所有文本文件"},
                    "regex": {"type": "boolean", "description": "是否启用正则表达式匹配。默认 false"},
                    "max_matches": {"type": "integer", "description": "返回的最大匹配项数量。默认 50，最大 200"},
                },
                "required": ["pattern"],
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
        # ── DuckDB 与 SQLite ──
        _make_tool(
            name="duckdb_query",
            description="在 workspace 的 DuckDB 上执行只读 SELECT 查询，返回 JSON 结果。禁止写操作。DuckDB 标识符需用双引号包裹，如 SELECT \"毛利\" FROM 表名。",
            parameters={
                "type": "object",
                "properties": {
                    "sql": {"type": "string", "description": "要执行的只读 SELECT SQL 语句"},
                },
                "required": ["sql"],
            },
        ),
        _make_tool(
            name="query_sqlite",
            description="在工作区内的指定 SQLite 数据库文件（.sqlite, .db）上执行只读 SELECT 查询，返回数据行的 JSON 数组。",
            parameters={
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "SQLite 数据库文件在工作区的相对路径"},
                    "sql": {"type": "string", "description": "待执行的只读 SELECT SQL 语句"},
                },
                "required": ["path", "sql"],
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
            description="列出 DuckDB 中所有已注册的表及其列名 and 行数。",
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


def get_step_milestone(step_idx: int, total_steps: int) -> int:
    if total_steps <= 0:
        return 15
    return min(15 + int((step_idx + 1) * 75 / total_steps), 90)


def get_plan_progress_info(ws) -> tuple[int, int]:
    """读取 plan.json，返回 (当前进行中/待执行步骤的索引, 总步骤数)。"""
    try:
        plan_path = ws.resolve("output/plan.json")
        if not plan_path.exists():
            return -1, 0
        plan = json.loads(plan_path.read_text(encoding="utf-8"))
        total = len(plan)
        for idx, step in enumerate(plan):
            if step.get("status") in ("in_progress", "pending"):
                return idx, total
        return total - 1, total
    except Exception:
        return -1, 0


def build_tool_map(ws: Workspace, emit_log=None, emit_status=None) -> dict:
    """构建 {tool_name: callable} 映射，供 agent loop 执行工具调用。

    emit_log(node_id, message)  —— 日志回调，message 可为 str 或 dict
    emit_status(node_id, status) —— 节点状态回调
    """
    _emit_log = emit_log or (lambda nid, msg: None)
    _emit_status = emit_status or (lambda nid, st: None)

    from .tools.impl.doc_impl import read_document_impl, read_document_structure_impl
    from .tools.impl.file_impl import read_file_impl, write_file_impl, list_files_impl
    from .tools.impl.python_impl import run_python_impl
    from .tools.impl.duckdb_impl import duckdb_query_impl, duckdb_register_parquet_impl
    from .tools.impl.context_impl import read_context_impl
    from .tools.impl.setup_impl import list_tables_impl, read_plan_short_impl
    from .tools.impl.plan_check_impl import run_step_check
    from .tools.impl.search_impl import search_files_impl
    from .tools.impl.sqlite_impl import query_sqlite_impl

    def _read_document(path: str) -> str:
        return read_document_structure_impl(ws, path)

    def _read_file(path: str, offset: int = 0, limit: int = 2000, head: int = None, tail: int = None) -> str:
        return read_file_impl(ws, path, offset=offset, limit=limit, head=head, tail=tail)

    def _write_file(path: str, content: str, mode: str = "overwrite") -> str:
        return write_file_impl(ws, path, content, mode=mode)

    def _list_files(subdir: str = "") -> str:
        files = list_files_impl(ws, subdir)
        return json.dumps(files, ensure_ascii=False)

    def _search_files(pattern: str, path: str = None, regex: bool = False, max_matches: int = 50) -> str:
        return search_files_impl(ws, pattern, path=path, regex=regex, max_matches=max_matches)

    def _query_sqlite(path: str, sql: str) -> str:
        return query_sqlite_impl(ws, path, sql)

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
        root = Path(__file__).resolve().parent.parent.parent.parent
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

        # 节点 ID 映射: step_index 0→custom_step0, 1→custom_step1 ...
        node_id = f"custom_step{step_index}"
        total_steps = len(plan)

        if ok:
            done_pct = get_step_milestone(step_index, total_steps)
            _emit_status(node_id, "success")
            # 推进下一步
            next_idx = None
            for i in range(step_index + 1, total_steps):
                if plan[i]["status"] == "pending":
                    plan[i]["status"] = "in_progress"
                    next_idx = i
                    break
            if next_idx is not None:
                next_node = f"custom_step{next_idx}"
                _emit_status(next_node, "active")
                _emit_log(next_node, {
                    "level": "status",
                    "message": f"[步骤 {next_idx + 1}/{total_steps}] {plan[next_idx].get('title', '')} 开始执行...",
                    "step": {"index": next_idx, "title": plan[next_idx].get("title", "")},
                    "progress": done_pct
                })
            else:
                # 所有步骤完成
                _emit_log(node_id, {
                    "level": "status",
                    "message": f"[步骤 {step_index + 1}/{total_steps}] 全部步骤已完成 ✅",
                    "progress": done_pct
                })
        else:
            _emit_status(node_id, "error")
            _emit_log(node_id, {
                "level": "error",
                "message": f"[步骤 {step_index + 1}/{total_steps}] {step.get('title', '')} 验证失败",
                "error_details": "; ".join(errors) if errors else "未知错误"
            })

        plan_path.write_text(json.dumps(plan, ensure_ascii=False, indent=2), encoding="utf-8")
        result = {"step_index": step_index, "ok": ok, "errors": errors}
        if ok:
            result["advanced"] = True
        return json.dumps(result, ensure_ascii=False)

    return {
        "read_document": _read_document,
        "read_document_structure": _read_document,
        "read_file": _read_file,
        "write_file": _write_file,
        "list_files": _list_files,
        "search_files": _search_files,
        "run_python": _run_python,
        "duckdb_query": _duckdb_query,
        "query_sqlite": _query_sqlite,
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
