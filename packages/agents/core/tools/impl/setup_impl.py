"""底层纯函数：workspace 生命周期管理"""
import json

from ...workspace import Workspace


def setup_workspace_impl(ws: Workspace) -> str:
    """扫描 workspace 现有 parquet → 注册 DuckDB → 返回状态摘要"""
    inputs = ws.list_inputs()
    tables = ws.scan_parquet_tables()
    db_summary = ws.init_duckdb()
    context_docs = [p.name for p in ws.context_dir.iterdir() if p.is_file()]

    status = {
        "report_id": ws.report_id,
        "workspace_dir": str(ws.dir),
        "inputs": inputs,
        "tables": [{"name": t["name"], "columns": t["columns"], "row_count": t["row_count"]} for t in tables],
        "context_docs": context_docs,
        "duckdb": ws.duckdb_path,
    }
    ws.save_trace({"step": "setup", "status": status})
    return json.dumps(status, ensure_ascii=False, indent=2)


def cleanup_workspace_impl(ws: Workspace, mode: str = "large") -> str:
    """清理 workspace

    mode:
      large — 仅删大文件 (parquet + duckdb)，保留 trace/scripts
      all   — 删除整个 workspace
    """
    if mode == "all":
        ws.cleanup()
        return f"workspace {ws.report_id} 已完全删除"
    else:
        ws.cleanup_large_files()
        return f"workspace {ws.report_id} 大文件已清理，保留 audit trail"


def list_tables_impl(ws: Workspace) -> str:
    """列出 DuckDB 中可用表"""
    import duckdb
    con = duckdb.connect(ws.duckdb_path)
    try:
        rows = con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
        ).fetchall()
        names = [r[0] for r in rows]
        return f"DuckDB 可用表 ({len(names)}): {', '.join(names)}" if names else "DuckDB 中暂无表"
    finally:
        con.close()


def design_plan_impl(ws: Workspace, plan_json: str) -> str:
    """初始化任务清单：写入 output/plan.json。仅启动时由编排器调用，不暴露给 Agent。"""
    plan = json.loads(plan_json) if isinstance(plan_json, str) else plan_json
    plan_path = ws.resolve("output/plan.json")
    plan_path.parent.mkdir(parents=True, exist_ok=True)
    plan_path.write_text(json.dumps(plan, ensure_ascii=False, indent=2), encoding="utf-8")
    return f"Plan registered: {len(plan)} steps"


def read_plan_short_impl(ws: Workspace) -> str:
    """读取 plan.json，返回简短版本。由 model wrapper 自动注入。

    展示规则：
      只展示当前 in_progress（或 failed）步骤的完整信息，
      已完成和 pending 步骤不展示任何细节，只计个数。
      目的是强制 LLM 必须调 check_plan 才能解锁下一步的指令。
    """

    plan_path = ws.resolve("output/plan.json")
    if not plan_path.exists():
        return "(plan 尚未初始化)"
    plan = json.loads(plan_path.read_text(encoding="utf-8"))

    done_lines = []
    current_lines = []
    pending_lines = []

    for idx, step in enumerate(plan):
        status = step["status"]
        if status == "success":
            done_lines.append(f"- [已完成步骤] {step['title']}")
        elif status in ("in_progress", "partial", "failed"):
            current_lines.append(f"--- 当前步骤：{step['title']} ---")
            current_lines.append(f"detail: {step['detail']}")
            check = step.get("check", "")
            if check:
                current_lines.append(f"check: {check.strip().split(chr(10))[0]}")
            errors = step.get("errors", [])
            if errors:
                current_lines.append(f"errors: {'; '.join(errors)}")
            current_lines.append("")
            current_lines.append(f"完成后调 check_plan({idx}) 验证并查看下一步的detail信息。")
        elif status == "pending":
            pending_lines.append(f"- [待做步骤] {step['title']}")

    final_lines = []
    if done_lines:
        final_lines.append("已完成步骤：")
        final_lines.extend(done_lines)
        final_lines.append("")

    if current_lines:
        final_lines.extend(current_lines)

    if pending_lines:
        if current_lines:
            final_lines.append("")
        final_lines.append("后续待做步骤：")
        final_lines.extend(pending_lines)

    if not current_lines:
        final_lines.append("所有步骤已完成。请调用 finish_task 结束任务。")

    return "\n".join(final_lines)
