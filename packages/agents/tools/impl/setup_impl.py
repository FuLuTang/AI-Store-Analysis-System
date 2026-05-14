"""底层纯函数：workspace 生命周期管理"""
import json

from ...workspace import Workspace


def setup_workspace_impl(ws: Workspace) -> str:
    """扫描输入 → 写 parquet → 注册 DuckDB → 返回状态摘要"""
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
