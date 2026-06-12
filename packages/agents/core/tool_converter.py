"""tool_converter.py — 将 _impl 工具函数转成 OpenAI function-calling tools schema。

新增/删除工具只改这个文件，prompt 和 agent loop 都不需要动。
"""

import json
from ..workspace import Workspace


TOOL_BLACKLIST_BY_TASK_TYPE: dict[str, set[str]] = {
    "diagnosis": {
        "read_plan",
    },
    "price_recommendation": {
        "read_context",
        "read_plan",
    },
    # Chatbot 禁用任务上下文/计划流工具，文件读写类保持可用。
    "chatbot": {
        "read_context",
        "read_plan",
        "check_plan",
        "finish_task",
        "duckdb_query",
        "duckdb_register_parquet",
        "list_tables",
    },
}


def _filter_tools_by_task_type(tools: list[dict], task_type: str) -> list[dict]:
    blocked = TOOL_BLACKLIST_BY_TASK_TYPE.get(task_type, set())
    if not blocked:
        return tools
    return [tool for tool in tools if tool.get("function", {}).get("name") not in blocked]


def _filter_tool_map_by_task_type(tool_map: dict, task_type: str) -> dict:
    blocked = TOOL_BLACKLIST_BY_TASK_TYPE.get(task_type, set())
    if not blocked:
        return tool_map
    return {name: fn for name, fn in tool_map.items() if name not in blocked}


def available_tool_call_for_agent(ws: Workspace, task_type: str = "diagnosis") -> list[dict]:
    """返回 OpenAI tools 参数列表，由已有的 _impl 函数自动拼合。"""
    from .tools.impl.search_impl import search_files_impl

    tools = [
        # ── 文档解析 ──
        _make_tool(
            name="read_document_structure",
            description="读取任意文档的结构信息（xlsx/csv/pdf/docx/txt/md/json/sqlite/zip），返回表结构摘要、列名、行数和样本行。多域工作区中路径必须带域名前缀，例如 'chatbot/...' 或 'service_docs/...'. 遇到想读的文件优先使用此工具来快速了解文件内容。",
            parameters={
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "workspace 内的相对路径；多域时需带域名前缀，如 'chatbot/input/report.xlsx' 或 'service_docs/policy/faq.md'",
                    }
                },
                "required": ["path"],
            },
        ),
        # ── 文件读写 ──
        _make_tool(
            name="read_file",
            description="读取 workspace 内的文本文件内容。多域工作区中路径必须带域名前缀，例如 'chatbot/...' 或 'service_docs/...'. 禁止直接使用此工具，强烈建议优先使用read_document_structure + python脚本结构化筛选处理 来解析文件。实在无法处理便可以使用此工具。",
            parameters={
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "workspace 内的相对路径；多域时需带域名前缀"},
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
            name="replace_text",
            description="在指定文本文件中做唯一匹配替换。只有 old_text 在文件里恰好匹配 1 次时才允许替换；匹配数为 0 或大于等于 2 都会报错。",
            parameters={
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "workspace 内的相对路径"},
                    "old_text": {"type": "string", "description": "要被替换的原始文本，要求在文件中唯一匹配"},
                    "new_text": {"type": "string", "description": "替换后的新文本"},
                },
                "required": ["path", "old_text", "new_text"],
            },
        ),
        _make_tool(
            name="copy_file",
            description="复制 workspace 内的单个文件到新路径。目标父目录会自动创建，若目标已存在则覆盖。",
            parameters={
                "type": "object",
                "properties": {
                    "source_path": {"type": "string", "description": "源文件在 workspace 内的相对路径"},
                    "destination_path": {"type": "string", "description": "目标文件在 workspace 内的相对路径"},
                },
                "required": ["source_path", "destination_path"],
            },
        ),
        _make_tool(
            name="list_files",
            description="按文件域列出 workspace 子目录下的文件和目录，返回每个域的 root 绝对路径、条目绝对路径和统计信息；支持空目录。多域工作区中 subdir 需带域名前缀，例如 'chatbot/' 或 'service_docs/'.",
            parameters={
                "type": "object",
                "properties": {
                    "subdir": {
                        "type": "string",
                        "description": "子目录名；多域时需写成 'chatbot/' 或 'service_docs/' 这样的域前缀",
                    }
                },
            },
        ),
        _make_tool(
            name="search",
            description="在 workspace 内按文件域检索名称、文本内容和常见文档/数据文件的轻量结构预览。domain 默认 all，可取 'all'、'chat_history'，或指定某个域路径（例如 'chatbot/'、'service_docs/'）。若 domain='chat_history'，会在聊天历史中搜索，并保留 reasoning_content 以便匹配。",
            parameters={
                "type": "object",
                "properties": {
                    "domain": {
                        "type": "string",
                        "description": "检索范围；默认 'all'。可选 'all'、'chat_history'，或指定域路径如 'chatbot/'、'service_docs/'",
                    },
                    "pattern": {"type": "string", "description": "要搜索的关键词或正则"},
                    "path": {"type": "string", "description": "可选：限定在某个带域名的路径下搜索"},
                    "regex": {"type": "boolean", "description": "是否按正则搜索"},
                    "max_matches": {"type": "integer", "description": "最多返回多少条匹配"},
                },
                "required": ["pattern"],
            },
        ),
        # ── Python 脚本执行 ──
        _make_tool(
            name="run_python",
            description=(
                "在 workspace 沙箱内执行一个 Python 脚本，返回 stdout。\n"
                "两种用法：\n"
                "1. 只传 script_path —— 运行已有脚本（如 'scripts/foo.py'）。\n"
                "2. 同时传 script_path + content —— 自动将 content 写入 script_path 再运行，"
                "相当于 write_file + run_python 一步到位。\n"
                "提示：可以直接对 'scripts/old_session_scripts/[run_id]/[脚本名]' 执行 run_python，"
                "系统会自动将其复制一份到 'scripts/[脚本名]' 里面并运行。\n"
                "如果脚本会产生较长输出、触发外部请求，或你希望工具完成后稍等再继续总结，"
                "可传 wait_seconds；系统会在工具结果写入历史后等待对应秒数，再恢复 Agent 循环。"
            ),
            parameters={
                "type": "object",
                "properties": {
                    "script_path": {
                        "type": "string",
                        "description": "脚本路径，如 'scripts/flatten.py'",
                    },
                    "content": {
                        "type": "string",
                        "description": "（可选）脚本代码内容。传入后自动写入 script_path 再执行，无需先调用 write_file。",
                    },
                    "wait_seconds": {
                        "type": "integer",
                        "description": "（可选）工具结果写入后等待多少秒再恢复 Agent 循环，建议 1-30 秒。",
                    },
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
    if task_type == "chatbot":
        tools.extend([
            _make_tool(
                name="get_resource_link",
                description="获取图片或文件的资源引用/下载链接。当你要向用户展示图片、提供文件下载链接时，必须调用此工具。传入带域前缀的路径（如 'service_docs/faq/logo.png' 或 'chatbot/workspace/report.xlsx'）。",
                parameters={
                    "type": "object",
                    "properties": {
                        "path": {
                            "type": "string",
                            "description": "带域前缀的相对路径，例如 'service_docs/faq/logo.png' 或 'chatbot/workspace/report.xlsx'"
                        }
                    },
                    "required": ["path"],
                },
            ),
            _make_tool(
                name="wait",
                description=(
                    "登记一个未来时间点来自动唤醒当前客服 Agent，不会阻塞当前对话。"
                    "用户要求倒计时、稍后提醒、到某个时间提醒时使用。"
                    "如果你提交了异步任务、调用了外部 API，且需要稍后继续查询或总结，也可以使用。"
                    "小任务可以不传任何参数，系统默认 1 秒后唤醒。"
                    "mode='delay' 时传 delay_seconds；mode='alarm' 时传 ISO 格式 run_at。"
                    "特殊用法(使回答更拟人化): 推荐当 长回答/回答包括图片 时，通过使用此工具(如先回答第一部分或先塞入图片，同时调用无参数wait，然后便可以等待输出第二段内容)来实现多条回复。"
                ),
                parameters={
                    "type": "object",
                    "properties": {
                        "mode": {
                            "type": "string",
                            "enum": ["delay", "alarm"],
                            "description": "可选。delay 表示多少秒后继续；alarm 表示到指定时间提醒；不传默认 delay。",
                        },
                        "delay_seconds": {
                            "type": "integer",
                            "description": "可选。delay 模式使用，至少 1 秒；不传或解析失败默认 3 秒。用户说分钟或小时提醒时，请换算成秒。",
                        },
                        "run_at": {
                            "type": "string",
                            "description": "可选。alarm 模式使用，ISO 时间字符串，例如 2026-06-12T18:30:00+08:00。精度按分钟处理；解析失败默认 3 秒 delay。",
                        },
                        "resume_prompt": {
                            "type": "string",
                            "description": "可选。到点后给 Agent 的恢复指令；应说明要提醒什么、查询什么或继续处理什么。不传时按前文继续处理。",
                        },
                        "reason": {
                            "type": "string",
                            "description": "可选。简短说明为什么设置这个等待，方便管理员查看 scheduler jsonl。",
                        },
                    },
                },
            ),
            _make_tool(
                name="list_system_functions",
                description="列出系统所有的可用服务功能。功能以树形目录层级结构展示（不包含 .py 后缀）。",
                parameters={"type": "object", "properties": {}},
            ),
            _make_tool(
                name="view_system_function_doc",
                description="查看指定系统服务功能说明文档。在执行功能前，必须先调用此工具查看说明，确认如何正确填写参数。",
                parameters={
                    "type": "object",
                    "properties": {
                        "path": {
                            "type": "string",
                            "description": "功能路径（例如 'ai_analyse/diagnosis/launch_diagnosis'）"
                        }
                    },
                    "required": ["path"],
                },
            ),
            _make_tool(
                name="get_user_service_token",
                description="请求用户授权代理操作。只有当你需要代表用户执行系统操作时调用；调用后系统会向用户展示确认卡片，用户同意后你会收到客服 token。",
                parameters={
                    "type": "object",
                    "properties": {
                        "reason": {
                            "type": "string",
                            "description": "一句话说明为什么需要用户授权代理操作。"
                        }
                    },
                    "required": ["reason"],
                },
            ),
            _make_tool(
                name="execute_system_function",
                description="执行指定系统服务功能。执行前必须先调用 get_user_service_token 获得客服 token，并把该 token 放入 params.service_token。",
                parameters={
                    "type": "object",
                    "properties": {
                        "path": {
                            "type": "string",
                            "description": "功能路径（例如 'ai_analyse/diagnosis/launch_diagnosis'）"
                        },
                        "params": {
                            "type": "object",
                            "description": "功能参数对象；必须包含 service_token 字段，其他字段按 view_system_function_doc 的说明填写。"
                        },
                    },
                    "required": ["path", "params"],
                },
            ),
        ])
    return _filter_tools_by_task_type(tools, task_type)


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


def build_tool_map(ws: Workspace, task_type: str = "diagnosis", emit_log=None, emit_status=None, on_finish=None, llm_preset: dict = None) -> dict:
    """构建 {tool_name: callable} 映射，供 agent loop 执行工具调用。

    emit_log(node_id, message)  —— 日志回调，message 可为 str 或 dict
    emit_status(node_id, status) —— 节点状态回调
    on_finish(success, text)     —— 任务结束回调
    """
    _emit_log = emit_log or (lambda nid, msg: None)
    _emit_status = emit_status or (lambda nid, st: None)

    def _emit_tool_success(tool_name: str, detail: str = "") -> None:
        message = f"✅ {tool_name} 调用成功"
        if detail:
            message += f": {detail}"
        _emit_log("custom_agent", {"level": "info", "message": message})

    from .tools.impl.doc_impl import read_document_structure_impl
    from .tools.impl.file_impl import (
        copy_file_impl,
        list_files_impl,
        read_file_impl,
        replace_text_impl,
        write_file_impl,
    )
    from .tools.impl.python_impl import run_python_impl
    from .tools.impl.duckdb_impl import duckdb_query_impl, duckdb_register_parquet_impl
    from .tools.impl.context_impl import read_context_impl
    from .tools.impl.setup_impl import list_tables_impl, read_plan_short_impl
    from .tools.impl.plan_check_impl import read_plan_impl, check_plan_impl, run_step_check
    from .tools.impl.search_impl import search_files_impl

    def _read_document_structure(path: str) -> str:
        return read_document_structure_impl(ws, path, emit_log=_emit_log)

    def _read_file(path: str, offset: int = 0, limit: int = 800, head: int = None, tail: int = None) -> str:
        return read_file_impl(ws, path, offset=offset, limit=limit, head=head, tail=tail, emit_log=_emit_log)

    def _write_file(path: str, content: str, mode: str = "overwrite") -> str:
        return write_file_impl(ws, path, content, mode=mode, emit_log=_emit_log)

    def _replace_text(path: str, old_text: str, new_text: str) -> str:
        return replace_text_impl(ws, path, old_text, new_text, emit_log=_emit_log)

    def _copy_file(source_path: str, destination_path: str) -> str:
        return copy_file_impl(ws, source_path, destination_path, emit_log=_emit_log)

    def _list_files(subdir: str = "") -> str:
        files = list_files_impl(ws, subdir, emit_log=_emit_log)
        return json.dumps(files, ensure_ascii=False)

    def _search(pattern: str, domain: str = "all", path: str = None, regex: bool = False, max_matches: int = 50) -> str:
        return search_files_impl(ws, pattern, domain=domain, path=path, regex=regex, max_matches=max_matches)

    def _run_python(script_path: str, content: str = None, wait_seconds: int = 0) -> str:
        return run_python_impl(ws, script_path, content=content, emit_log=_emit_log)

    def _duckdb_query(sql: str) -> str:
        return duckdb_query_impl(ws, sql, emit_log=_emit_log)

    def _duckdb_register_parquet(table_name: str, parquet_path: str) -> str:
        return duckdb_register_parquet_impl(ws, table_name, parquet_path, emit_log=_emit_log)

    def _list_tables() -> str:
        return list_tables_impl(ws, emit_log=_emit_log)

    def _read_context(topic: str) -> str:
        doc_name = topic if topic.endswith(".md") else f"{topic}.md"
        return read_context_impl(ws, doc_name, emit_log=_emit_log)

    def _read_plan() -> str:
        plan_path = ws.resolve("plan.json")
        if not plan_path.exists():
            return "(plan 尚未初始化)"
        result = json.dumps(json.loads(plan_path.read_text(encoding="utf-8")), ensure_ascii=False, indent=2)
        _emit_tool_success("read_plan", "plan.json")
        return result

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
            result["all_done"] = next_idx is None
            _emit_tool_success("check_plan", f"step_index={step_index}")
        return json.dumps(result, ensure_ascii=False)



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
            _emit_tool_success("finish_task", "success=true")
            return "任务已成功结束。"
        else:
            if on_finish:
                on_finish(success=False, text=text)
            _emit_tool_success("finish_task", "success=false")
            return "任务已报错终止。"

    tool_map = {
        "read_document_structure": _read_document_structure,
        "read_file": _read_file,
        "write_file": _write_file,
        "replace_text": _replace_text,
        "copy_file": _copy_file,
        "list_files": _list_files,
        "search": _search,
        "run_python": _run_python,
        "duckdb_query": _duckdb_query,
        "duckdb_register_parquet": _duckdb_register_parquet,
        "list_tables": _list_tables,
        "read_context": _read_context,
        "read_plan": _read_plan,
        "check_plan": _check_plan,
        "finish_task": _finish_task,
    }

    if task_type == "chatbot":
        from .tools.impl.system_function_impl import (
            execute_system_function_impl,
            list_system_functions_impl,
            view_system_function_doc_impl,
        )
        from .tools.impl.wait_impl import schedule_wait_impl
        from .tools.impl.resource_link_impl import get_resource_link_impl

        def _get_resource_link(path: str) -> str:
            return get_resource_link_impl(ws, path, emit_log=_emit_log)

        def _list_system_functions() -> str:
            return list_system_functions_impl()

        def _view_system_function_doc(path: str) -> str:
            return view_system_function_doc_impl(path)

        def _get_user_service_token(reason: str) -> str:
            return json.dumps({
                "status": "pending_user_authorization",
                "reason": str(reason or "").strip(),
            }, ensure_ascii=False)

        def _execute_system_function(path: str, params: dict | str) -> str:
            return execute_system_function_impl(ws, path, params, llm_preset or {})

        def _wait(mode: str = "", delay_seconds: int = None, run_at: str = None, resume_prompt: str = "", reason: str = "") -> str:
            return schedule_wait_impl(
                ws,
                mode=mode,
                delay_seconds=delay_seconds,
                run_at=run_at,
                resume_prompt=resume_prompt,
                reason=reason,
            )

        tool_map.update({
            "get_resource_link": _get_resource_link,
            "wait": _wait,
            "list_system_functions": _list_system_functions,
            "view_system_function_doc": _view_system_function_doc,
            "get_user_service_token": _get_user_service_token,
            "execute_system_function": _execute_system_function,
        })

    return _filter_tool_map_by_task_type(tool_map, task_type)


def _make_tool(name: str, description: str, parameters: dict) -> dict:
    return {
        "type": "function",
        "function": {
            "name": name,
            "description": description,
            "parameters": parameters,
        },
    }
