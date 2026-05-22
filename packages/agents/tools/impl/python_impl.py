"""底层纯函数：在 workspace 沙箱中执行 Python 脚本"""
import subprocess
from ...workspace import Workspace


def run_python_impl(ws: Workspace, script_path: str, timeout: int = 300) -> str:
    script = ws.resolve(script_path)
    if not script.exists():
        raise FileNotFoundError(f"脚本不存在: {script}")
    if ws.scripts_dir not in script.parents and script.parent != ws.scripts_dir:
        raise ValueError(f"只能执行 scripts/ 下的 Python 脚本，收到: {script_path}")
    if script.suffix != ".py":
        raise ValueError(f"只能执行 .py 文件，收到: {script_path}")
    result = subprocess.run(
        ["python3", str(script)],
        capture_output=True, text=True, timeout=timeout,
        cwd=str(ws.dir),
    )
    return result.stdout + "\n" + result.stderr if result.stderr else result.stdout
