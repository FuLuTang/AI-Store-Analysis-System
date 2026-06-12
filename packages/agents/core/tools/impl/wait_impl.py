"""Chatbot delayed wake-up scheduling backed by an editable JSONL file."""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from threading import Lock
from typing import Optional

from ...workspace import Workspace


SCHEDULER_FILENAME = "chatbot_scheduler.jsonl"
SCHEDULER_LOCK = Lock()
WAIT_MODES = {"delay", "alarm"}
DEFAULT_DELAY_SECONDS = 3
DEFAULT_RESUME_PROMPT = "请根据前文和最新上下文继续处理。"


def _now() -> datetime:
    return datetime.now().astimezone()


def _iso(dt: datetime) -> str:
    return dt.astimezone().isoformat(timespec="seconds")


def _parse_run_at(value: str) -> datetime:
    text = str(value or "").strip()
    if not text:
        raise ValueError("alarm 模式必须提供 run_at")
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"run_at 必须是 ISO 时间字符串，收到: {value}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=_now().tzinfo)
    return parsed.astimezone().replace(second=0, microsecond=0)


def _account_dir_from_workspace(ws: Workspace) -> Path:
    root = ws.dir.resolve()
    if root.name == "chatbot":
        return root.parent
    if root.parent.name == "chatbot":
        return root.parent.parent
    raise ValueError(f"wait 只能在 chatbot workspace 中使用，当前 workspace={root}")


def scheduler_path_for_accounts_dir(accounts_dir: Path) -> Path:
    return Path(accounts_dir) / SCHEDULER_FILENAME


def _scheduler_path_for_account(account_dir: Path) -> Path:
    return scheduler_path_for_accounts_dir(Path(account_dir).parent)


def load_scheduled_waits(accounts_dir: Path) -> list[dict]:
    tasks, _mtime_ns = load_scheduled_waits_with_mtime(accounts_dir)
    return tasks


def load_scheduled_waits_with_mtime(accounts_dir: Path) -> tuple[list[dict], Optional[int]]:
    path = scheduler_path_for_accounts_dir(accounts_dir)
    if not path.exists():
        return [], None
    tasks: list[dict] = []
    with SCHEDULER_LOCK:
        mtime_ns = path.stat().st_mtime_ns if path.exists() else None
        with path.open("r", encoding="utf-8") as fh:
            for raw_line in fh:
                line = raw_line.strip()
                if not line:
                    continue
                try:
                    item = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(item, dict):
                    tasks.append(item)
    return tasks, mtime_ns


def rewrite_scheduled_waits(accounts_dir: Path, tasks: list[dict], expected_mtime_ns: Optional[int] = None) -> bool:
    path = scheduler_path_for_accounts_dir(accounts_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    with SCHEDULER_LOCK:
        current_mtime_ns = path.stat().st_mtime_ns if path.exists() else None
        if expected_mtime_ns is not None and current_mtime_ns != expected_mtime_ns:
            return False
        tmp.write_text(
            "".join(json.dumps(task, ensure_ascii=False) + "\n" for task in tasks),
            encoding="utf-8",
        )
        tmp.replace(path)
    return True


def append_scheduled_wait(account_dir: Path, task: dict) -> None:
    path = _scheduler_path_for_account(account_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    with SCHEDULER_LOCK:
        with path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(task, ensure_ascii=False) + "\n")


def schedule_wait_impl(
    ws: Workspace,
    mode: str = "",
    delay_seconds: Optional[int] = None,
    run_at: Optional[str] = None,
    resume_prompt: str = "",
    reason: str = "",
) -> str:
    mode = str(mode or "").strip().lower()
    if mode not in WAIT_MODES:
        mode = "delay"

    prompt = str(resume_prompt or "").strip() or DEFAULT_RESUME_PROMPT

    now = _now()
    if mode == "delay":
        try:
            seconds = int(delay_seconds or 0)
        except (TypeError, ValueError):
            seconds = DEFAULT_DELAY_SECONDS
        if seconds < 1:
            seconds = DEFAULT_DELAY_SECONDS
        due_at = now + timedelta(seconds=seconds)
    else:
        try:
            due_at = _parse_run_at(str(run_at or ""))
        except ValueError as exc:
            mode = "delay"
            due_at = now + timedelta(seconds=DEFAULT_DELAY_SECONDS)
            reason = str(reason or "").strip() or f"alarm 时间解析失败，改为默认等待 {DEFAULT_DELAY_SECONDS} 秒: {exc}"

    account_dir = _account_dir_from_workspace(ws)
    task = {
        "id": uuid.uuid4().hex,
        "mode": mode,
        "run_at": _iso(due_at),
        "account_id": account_dir.name,
        "account_dir": str(account_dir),
        "resume_prompt": prompt,
        "reason": str(reason or "").strip(),
        "created_at": _iso(now),
    }
    append_scheduled_wait(account_dir, task)
    return json.dumps({
        "ok": True,
        "scheduled": True,
        "id": task["id"],
        "mode": mode,
        "run_at": task["run_at"],
    }, ensure_ascii=False)
