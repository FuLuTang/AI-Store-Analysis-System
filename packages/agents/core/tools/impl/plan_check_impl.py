"""check_plan 的执行器：subprocess 跑 Python assert 脚本"""
import sys
import subprocess
from pathlib import Path

from ...workspace import Workspace

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent.parent


def extract_check_summary(check: str) -> str:
    """从 check 脚本第一行注释提取摘要，供 LLM 了解验收标准。"""
    if not check or not check.strip():
        return ""
    lines = check.strip().split("\n")
    for line in lines:
        line = line.strip()
        if line.startswith("#"):
            return line.lstrip("#").strip()
    return "有自动验收检查"


def run_step_check(ws: Workspace, step: dict) -> tuple[bool, list[str]]:
    """执行 step["check"] Python 脚本。

    返回 (ok, errors)：
      - check 为空 → (True, [])
      - check 执行成功 → (True, [])
      - check 执行失败 → (False, [错误信息])
    """
    check = step.get("check", "").strip()
    if not check:
        return True, []

    prelude = f"import sys; sys.path.insert(0, {str(_PROJECT_ROOT)!r})\n"
    full_script = prelude + check

    try:
        result = subprocess.run(
            [sys.executable, "-c", full_script],
            cwd=str(ws.dir),
            capture_output=True,
            text=True,
            timeout=10,
        )
        if result.returncode == 0:
            return True, []
        else:
            stderr = result.stderr.strip()
            err_lines = [l for l in stderr.split("\n") if l.strip()]
            if err_lines:
                msg = err_lines[-1]
            else:
                msg = "检查执行失败（无错误输出）"
            return False, [msg]
    except subprocess.TimeoutExpired:
        return False, ["检查超时（10秒）"]
    except Exception as e:
        return False, [str(e)]
