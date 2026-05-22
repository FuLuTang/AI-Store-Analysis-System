"""上下文文档读取（委托到 impl）"""
from .impl.context_impl import read_context_impl
from ..workspace import Workspace


def read_context(ws: Workspace, doc_name: str) -> str:
    return read_context_impl(ws, doc_name)
