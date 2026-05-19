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
      - success: 仅 title/status
      - 第一个非 success: title/status/detail/check_summary/errors
      - 后续 pending: 仅 title/status
      - failed: title/status/detail/check_summary/errors
    """
    from .plan_check_impl import extract_check_summary

    plan_path = ws.resolve("output/plan.json")
    if not plan_path.exists():
        return "(plan 尚未初始化)"
    plan = json.loads(plan_path.read_text(encoding="utf-8"))

    first_non_success = None
    for idx, s in enumerate(plan):
        if s["status"] != "success":
            first_non_success = idx
            break

    lines = ["以下为plan进度："]
    for idx, step in enumerate(plan):
        status = step["status"]
        check_summary = extract_check_summary(step.get("check", ""))
        errors = step.get("errors", [])

        if status == "success":
            lines.append(json.dumps({"title": step["title"], "status": status}, ensure_ascii=False))

        elif status in ("in_progress", "partial"):
            entry = {"title": step["title"], "status": status, "detail": step["detail"],
                     "errors": errors}
            if check_summary:
                entry["check_summary"] = check_summary
            lines.append(json.dumps(entry, ensure_ascii=False, indent=2))

        elif status == "failed":
            entry = {"title": step["title"], "status": status, "detail": step["detail"],
                     "errors": errors}
            if check_summary:
                entry["check_summary"] = check_summary
            lines.append(json.dumps(entry, ensure_ascii=False, indent=2))

        elif status == "pending":
            if idx == first_non_success:
                entry = {"title": step["title"], "status": status, "detail": step["detail"]}
                if check_summary:
                    entry["check_summary"] = check_summary
                lines.append(json.dumps(entry, ensure_ascii=False, indent=2))
            else:
                lines.append(json.dumps({"title": step["title"], "status": status}, ensure_ascii=False))

    lines.append("使用 read_plan(show_checks=True) 查看完整检查脚本")
    lines.append("每完成一步后调用 check_plan(step_index=N) 自动验证产物")
    return "\n".join(lines)
