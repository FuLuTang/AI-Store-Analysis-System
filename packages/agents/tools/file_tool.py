"""workspace 文件读写（委托到 impl）"""
from .impl.file_impl import read_file_impl, write_file_impl, list_files_impl
from ..workspace import Workspace


def read_workspace_file(ws: Workspace, path: str) -> str:
    return read_file_impl(ws, path)


def write_workspace_file(ws: Workspace, path: str, content: str) -> str:
    return write_file_impl(ws, path, content)


def list_workspace_files(ws: Workspace, subdir: str = "") -> list[str]:
    return list_files_impl(ws, subdir)
