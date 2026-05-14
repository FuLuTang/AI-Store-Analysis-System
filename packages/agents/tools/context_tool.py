"""
context_tool.py — 读上下文文档工具（两边共用）
"""
from .file_tool import get_workspace


def read_context(doc_name: str) -> str:
    """读取已注入 context/ 目录的文档"""
    ws = get_workspace()
    doc = ws.context_dir / doc_name
    if not doc.exists():
        available = [p.name for p in ws.context_dir.iterdir() if p.is_file()]
        return f"文档不存在: {doc_name}。可用文档: {available}"
    return doc.read_text(encoding="utf-8")
