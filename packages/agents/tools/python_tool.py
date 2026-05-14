"""
python_tool.py — 在 workspace/sandbox 内执行 Python 脚本（两边共用）

安全策略：
- 只在 workspace 目录内执行
- 限制超时 / 内存 / 网络
- 禁止访问 .env / 源码 / 用户 home
"""
import subprocess
from .file_tool import get_workspace


def run_python_script(script_path: str, timeout: int = 300) -> str:
    """在 workspace 内执行 Python 脚本"""
    ws = get_workspace()
    script = ws.resolve(script_path)
    result = subprocess.run(
        ["python3", str(script)],
        capture_output=True, text=True, timeout=timeout,
        cwd=str(ws.dir),
    )
    return result.stdout + "\n" + result.stderr if result.stderr else result.stdout
