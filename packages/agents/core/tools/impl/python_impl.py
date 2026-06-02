"""底层纯函数：在 workspace 沙箱中执行 Python 脚本"""
import subprocess
import sys
from typing import Callable, Optional
from ...workspace import Workspace


def run_python_impl(
    ws: Workspace,
    script_path: str,
    timeout: int = 300,
    emit_log: Optional[Callable[[str, str | dict], None]] = None,
) -> str:
    script = ws.resolve(script_path)
    if not script.exists():
        raise FileNotFoundError(f"脚本不存在: {script}")
    
    # 自动复制旧脚本逻辑
    old_scripts_dir = ws.scripts_dir / "old_session_scripts"
    try:
        if old_scripts_dir.resolve() in script.resolve().parents:
            import shutil
            copied_script_path = ws.scripts_dir / script.name
            shutil.copyfile(script, copied_script_path)
            script = copied_script_path
    except Exception:
        pass

    if ws.scripts_dir not in script.parents and script.parent != ws.scripts_dir:
        raise ValueError(f"只能执行 scripts/ 下的 Python 脚本，收到: {script_path}")
    if script.suffix != ".py":
        raise ValueError(f"只能执行 .py 文件，收到: {script_path}")
    result = subprocess.run(
        [sys.executable, str(script)],
        capture_output=True, text=True, timeout=timeout,
        cwd=str(ws.dir),
    )
    output = result.stdout + "\n" + result.stderr if result.stderr else result.stdout
    if result.returncode == 0 and emit_log:
        emit_log("custom_agent", {"level": "info", "message": f"✅ run_python 调用成功: {script_path}"})
    return output
