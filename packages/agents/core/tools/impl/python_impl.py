"""底层纯函数：在 workspace 沙箱中执行 Python 脚本。"""
import os
import subprocess
import sys
from typing import Callable, Optional
from ...workspace import Workspace


def _normalize_script_path(script_path: str) -> str:
    rel = str(script_path or "").strip().replace("\\", "/")
    while rel.startswith("./"):
        rel = rel[2:]
    changed = True
    while changed:
        changed = False
        for prefix in ("chatbot/workspace/",):
            if rel.startswith(prefix):
                rel = rel[len(prefix):]
                changed = True
    return rel


def run_python_impl(
    ws: Workspace,
    script_path: str,
    content: Optional[str] = None,
    timeout: int = 5,
    emit_log: Optional[Callable[[str, str | dict], None]] = None,
) -> str:
    script_path = _normalize_script_path(script_path)
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
    try:
        os.chdir(str(ws.dir))
        proc = subprocess.run(
            [sys.executable, str(script)],
            cwd=str(ws.dir),
            capture_output=True,
            text=True,
            timeout=max(1, int(timeout or 5)),
        )
        output = proc.stdout or ""
        err = proc.stderr or ""
        if err:
            output += ("\n" if output else "") + err
        if proc.returncode != 0:
            output += ("" if output.endswith("\n") or not output else "\n") + f"脚本退出码: {proc.returncode}"
            return output
    except subprocess.TimeoutExpired as e:
        output = (e.stdout or "") if isinstance(e.stdout, str) else ""
        err = (e.stderr or "") if isinstance(e.stderr, str) else ""
        if err:
            output += ("\n" if output else "") + err
        output += ("" if output.endswith("\n") or not output else "\n") + f"脚本超时: {max(1, int(timeout or 5))} 秒"
        return output
    except Exception as e:
        return f"脚本异常: {e}"
    finally:
        os.chdir(old_cwd)

    if emit_log:
        emit_log("custom_agent", {"level": "info", "message": f"✅ run_python 调用成功: {script_path}"})
    return output
