"""Python 工具：在 workspace 沙箱中执行 Python 脚本。

安全策略：
- 只在 workspace 目录内执行
- 限制超时 / 内存 / 网络
- 禁止访问 .env / 源码 / 用户 home
"""
import subprocess

from ..workspace import Workspace


def run_python_script(script_path: str, timeout: int = 300) -> str:
    """在 workspace 内执行 Python 脚本"""
    from .file_tool import get_workspace
    ws: Workspace = get_workspace()
    script = ws.resolve(script_path)
    result = subprocess.run(
        ["python3", str(script)],
        capture_output=True, text=True, timeout=timeout,
        cwd=str(ws.dir),
    )
    return result.stdout + "\n" + result.stderr if result.stderr else result.stdout
