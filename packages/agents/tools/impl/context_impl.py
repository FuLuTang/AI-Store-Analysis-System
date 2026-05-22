"""底层纯函数：读取上下文文档"""
from ...workspace import Workspace


def read_context_impl(ws: Workspace, doc_name: str) -> str:
    doc = ws.context_dir / doc_name
    if not doc.exists():
        available = [p.name for p in ws.context_dir.iterdir() if p.is_file()]
        return f"文档不存在: {doc_name}。可用文档: {available}"
    return doc.read_text(encoding="utf-8")
