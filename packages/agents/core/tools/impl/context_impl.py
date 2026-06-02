"""底层纯函数：读取上下文文档"""
from typing import Callable, Optional
from pathlib import Path
from ...workspace import Workspace


def read_context_impl(
    ws: Workspace,
    doc_name: str,
    emit_log: Optional[Callable[[str, str | dict], None]] = None,
) -> str:
    if Path(doc_name).name == "plan.json":
        return "不允许读取受限文件: plan.json"
    doc = ws.context_dir / doc_name
    if not doc.exists():
        available = [p.name for p in ws.context_dir.iterdir() if p.is_file()]
        return f"文档不存在: {doc_name}。可用文档: {available}"
    result = doc.read_text(encoding="utf-8")
    if emit_log:
        emit_log("custom_agent", {"level": "info", "message": f"✅ read_context 调用成功: {doc_name}"})
    return result
