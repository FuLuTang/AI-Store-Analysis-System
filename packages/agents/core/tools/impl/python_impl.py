"""底层纯函数：在 workspace 沙箱中执行 Python 脚本（内核执行）"""
import io
import os
import sys
import runpy
from contextlib import redirect_stdout, redirect_stderr
from typing import Callable, Optional
from ...workspace import Workspace


def run_python_impl(
    ws: Workspace,
    script_path: str,
    content: Optional[str] = None,
    timeout: int = 300,
    emit_log: Optional[Callable[[str, str | dict], None]] = None,
) -> str:
    script = ws.resolve_script(script_path)
    if content is not None:
        script.parent.mkdir(parents=True, exist_ok=True)
        script.write_text(content, encoding="utf-8")
        if emit_log:
            emit_log("custom_agent", {"level": "info", "message": f"✏️ 写入并执行 {script_path}"})
    elif not script.exists():
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

    script_root = ws.script_root.resolve()
    try:
        script.resolve().relative_to(script_root)
    except ValueError:
        raise ValueError(f"只能执行 {ws.script_root} 下的 Python 脚本，收到: {script_path}")
    if script.suffix != ".py":
        raise ValueError(f"只能执行 .py 文件，收到: {script_path}")

    old_cwd = os.getcwd()
    stdout_buf = io.StringIO()
    stderr_buf = io.StringIO()
    try:
        os.chdir(str(ws.dir))
        with redirect_stdout(stdout_buf), redirect_stderr(stderr_buf):
            try:
                runpy.run_path(str(script), run_name="__main__")
            except SystemExit:
                pass
    except Exception as e:
        output = stdout_buf.getvalue()
        err = stderr_buf.getvalue()
        if err:
            output += "\n" + err
        output += f"\n脚本异常: {e}"
        return output
    finally:
        os.chdir(old_cwd)

    output = stdout_buf.getvalue()
    err = stderr_buf.getvalue()
    if err:
        output += "\n" + err
    if emit_log:
        emit_log("custom_agent", {"level": "info", "message": f"✅ run_python 调用成功: {script_path}"})
    return output
