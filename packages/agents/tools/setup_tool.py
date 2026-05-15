"""workspace 初始化/清理（委托到 impl）"""
from .impl.setup_impl import setup_workspace_impl, cleanup_workspace_impl, list_tables_impl
from ..workspace import Workspace


def setup_workspace(ws: Workspace) -> str:
    return setup_workspace_impl(ws)


def cleanup_workspace(ws: Workspace, mode: str = "large") -> str:
    return cleanup_workspace_impl(ws, mode)


def list_tables(ws: Workspace) -> str:
    return list_tables_impl(ws)
