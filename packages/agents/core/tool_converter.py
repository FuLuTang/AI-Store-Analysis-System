"""tool_converter.py — 将 _impl 工具函数转成 OpenAI function-calling tools schema。

新增/删除工具只改这个文件，prompt 和 agent loop 都不需要动。
"""

import json
from ..workspace import Workspace


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
        _make_tool(
            name="extract_document_tables",
            description="从文档中提取结构化表格数据，返回 JSON 行数组。支持 xlsx, csv, pdf, docx。",
            parameters={
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "workspace 内的相对路径，如 'input/sales.xlsx'",
                    },
                    "sheet": {
                        "type": "string",
                        "description": "Sheet 名称（可选），不区分大小写。留空则使用第一个 sheet",
                    },
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
            description="在 workspace 的 DuckDB 上执行只读 SELECT 查询，返回 JSON 结果。禁止 INSERT/DROP/DELETE/ALTER。",
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
        _make_tool(
            name="validate_result",
            description="校验最终 AgentResult JSON 的完整性和格式正确性。",
            parameters={
                "type": "object",
                "properties": {
                    "result_json": {"type": "string", "description": "AgentResult 的 JSON 字符串"},
                },
                "required": ["result_json"],
            },
        ),
        _make_tool(
            name="finish_task",
            description="结束或中止当前分析任务。如果任务成功完成，传入 success=true 和 text 总结；如果发生不可恢复的严重错误需要报错停止，传入 success=false 并提供详细的 text 原因。在传入 success=true 时，系统会校验是否所有计划步骤都已成功验证，否则会返回错误提示。",
            parameters={
                "type": "object",
                "properties": {
                    "success": {
                        "type": "boolean",
                        "description": "是否成功完成任务。true 表示成功结束，false 表示报错终止任务。"
                    },
                    "text": {
                        "type": "string",
                        "description": "如果 success=true，写最终总结（包含健康状态、核心结论和交付物清单）；如果 success=false，写具体的错误原因说明。"
                    }
                },
                "required": ["success", "text"],
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
        plan_path = ws.resolve("plan.json")
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


def build_tool_map(ws: Workspace, emit_log=None, emit_status=None, on_finish=None) -> dict:
    """构建 {tool_name: callable} 映射，供 agent loop 执行工具调用。

    emit_log(node_id, message)  —— 日志回调，message 可为 str 或 dict
    emit_status(node_id, status) —— 节点状态回调
    on_finish(success, text)     —— 任务结束回调
    """
    _emit_log = emit_log or (lambda nid, msg: None)
    _emit_status = emit_status or (lambda nid, st: None)

    from .tools.impl.doc_impl import read_document_impl, extract_document_tables_impl
    from .tools.impl.file_impl import read_file_impl, write_file_impl, list_files_impl
    from .tools.impl.python_impl import run_python_impl
    from .tools.impl.duckdb_impl import duckdb_query_impl, duckdb_register_parquet_impl
    from .tools.impl.context_impl import read_context_impl
    from .tools.impl.setup_impl import list_tables_impl, read_plan_short_impl
    from .tools.impl.plan_check_impl import read_plan_impl, check_plan_impl, run_step_check
    from .tools.impl.validate_impl import validate_result_impl

    def _read_document(path: str) -> str:
        return read_document_impl(ws, path)

    def _extract_document_tables(path: str, sheet: str = "") -> str:
        return extract_document_tables_impl(ws, path, sheet)

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
        doc_name = topic if topic.endswith(".md") else f"{topic}.md"
        return read_context_impl(ws, doc_name)

    def _read_plan() -> str:
        plan_path = ws.resolve("plan.json")
        if not plan_path.exists():
            return "(plan 尚未初始化)"
        return json.dumps(json.loads(plan_path.read_text(encoding="utf-8")), ensure_ascii=False, indent=2)

    def _check_plan(step_index: int) -> str:
        plan_path = ws.resolve("plan.json")
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

    def _validate_result(result_json: str) -> str:
        try:
            parsed = json.loads(result_json) if isinstance(result_json, str) else result_json
        except json.JSONDecodeError as e:
            return json.dumps({"valid": False, "errors": f"JSON 解析失败: {e}"})
        return validate_result_impl(parsed)

    def _finish_task(success: bool, text: str) -> str:
        if success:
            try:
                plan_path = ws.resolve("plan.json")
                if not plan_path.exists():
                    return "调用出错：未找到 plan.json 计划文件。"
                plan = json.loads(plan_path.read_text(encoding="utf-8"))
                all_done = all(s.get("status") == "success" for s in plan)
            except Exception as e:
                return f"调用出错：读取或解析 plan.json 失败: {str(e)}"

            if not all_done:
                return "调用出错：当前还有未完成的步骤，请先完成所有步骤并调用 check_plan 验证后，再调用 finish_task。"

            if on_finish:
                on_finish(success=True, text=text)
            return "任务已成功结束。"
        else:
            if on_finish:
                on_finish(success=False, text=text)
            return "任务已报错终止。"

    return {
        "read_document": _read_document,
        "extract_document_tables": _extract_document_tables,
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
        "validate_result": _validate_result,
        "finish_task": _finish_task,
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
