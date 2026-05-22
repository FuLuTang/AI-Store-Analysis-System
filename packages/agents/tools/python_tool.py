"""Python 沙箱执行（委托到 impl）"""
from .impl.python_impl import run_python_impl
from ..workspace import Workspace


def run_python_script(ws: Workspace, script_path: str, timeout: int = 300) -> str:
    return run_python_impl(ws, script_path, timeout)
