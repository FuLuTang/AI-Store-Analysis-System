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
    """读取 plan.json，pending/in_progress 时附带 detail 字段。由 model wrapper 自动注入。"""
    plan_path = ws.resolve("output/plan.json")
    if not plan_path.exists():
        return "(plan 尚未初始化)"
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    lines = ["以下为plan进度："]
    for step in plan:
        status = step["status"]
        if status in ("in_progress", "failed"):
            lines.append(json.dumps({"title": step["title"], "status": status, "detail": step["detail"]}, ensure_ascii=False))
        else:
            lines.append(json.dumps({"title": step["title"], "status": status}, ensure_ascii=False))
    lines.append("使用read_plan工具阅读完整plan")
    return "\n".join(lines)
