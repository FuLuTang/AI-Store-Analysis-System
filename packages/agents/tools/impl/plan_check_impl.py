"""check_plan 的执行器：subprocess 跑 Python assert 脚本"""
import json
import sys
import subprocess
from pathlib import Path
from typing import Callable, Optional

from ...workspace import Workspace

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent.parent


def read_plan_impl(ws: Workspace) -> dict | None:
    """读取完整 plan，去掉 check 字段。返回 plan 列表，或 None（plan.json 不存在时）。"""
    plan_path = ws.resolve("output/plan.json")
    if not plan_path.exists():
        return None
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    for step in plan:
        step.pop("check", None)
    return plan


def check_plan_impl(ws: Workspace, step_index: int, emit_log: Optional[Callable] = None) -> dict:
    """执行 check_plan 核心逻辑：读 plan → 跑 step check → 更新状态 → 推进下一步 → 写回。

    Args:
        ws: Workspace 引用。
        step_index: plan 中步骤的 0-based 索引。
        emit_log: 可选的回调，用于推送日志（如 smol_tools 的场景）。

    Returns:
        包含 step_index / ok / errors / next_action（可选）等字段的 dict。
    """
    plan_path = ws.resolve("output/plan.json")
    if not plan_path.exists():
        return {"error": "plan.json not found"}
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    if step_index < 0 or step_index >= len(plan):
        return {"error": f"step_index {step_index} out of range (0-{len(plan)-1})"}

    step = plan[step_index]
    ok, errors = run_step_check(ws, step)
    step["errors"] = errors
    step["status"] = "success" if ok else "failed"

    if ok:
        for i in range(step_index + 1, len(plan)):
            if plan[i]["status"] == "pending":
                plan[i]["status"] = "in_progress"
                if emit_log:
                    emit_log("plan", f"Step {i} 开始: {plan[i]['title']}")
                break

    plan_path.write_text(json.dumps(plan, ensure_ascii=False, indent=2), encoding="utf-8")

    result = {"step_index": step_index, "ok": ok, "errors": errors}
    if errors:
        result["next_action"] = (
            f"Step {step_index}（{step['title']}）未通过检查，"
            f"请根据报错排查后重新 check_plan({step_index})\n"
            + "\n".join(errors)
        )
    return result


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
