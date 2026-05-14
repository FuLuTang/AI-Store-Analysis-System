"""底层纯函数：在 workspace 沙箱中执行 Python 脚本"""
import subprocess
from ...workspace import Workspace


def run_python_impl(ws: Workspace, script_path: str, timeout: int = 300) -> str:
    script = ws.resolve(script_path)
    result = subprocess.run(
        ["python3", str(script)],
        capture_output=True, text=True, timeout=timeout,
        cwd=str(ws.dir),
    )
    return result.stdout + "\n" + result.stderr if result.stderr else result.stdout
