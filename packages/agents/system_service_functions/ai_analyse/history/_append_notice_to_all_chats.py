"""批量向所有 chat.jsonl 末尾追加 notice 消息的内部实现。"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any


NOTICE_NAME = "notice"


def _now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def _is_notice_message(message: dict[str, Any]) -> bool:
    return (
        str(message.get("role", "")).strip().lower() == "system"
        and str(message.get("name", "")).strip().lower() == NOTICE_NAME
    )


def _parse_jsonl(path: Path) -> list[dict[str, Any]]:
    messages: list[dict[str, Any]] = []
    if not path.exists():
        return messages
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
                messages.append(item)
    return messages


def _write_jsonl(path: Path, messages: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(item, ensure_ascii=False) + "\n" for item in messages),
        encoding="utf-8",
    )


def broadcast_notice_to_all_chats(accounts_dir: Path, content: str) -> dict[str, Any]:
    text = str(content or "").strip()
    if not text:
        raise ValueError("notice 内容不能为空")

    now = _now_iso()
    updated = 0
    skipped = 0
    affected: list[str] = []

    if not accounts_dir.exists():
        return {"updated": 0, "skipped": 0, "accounts": []}

    for account_dir in sorted(accounts_dir.iterdir()):
        if not account_dir.is_dir():
            continue
        chat_path = account_dir / "chatbot" / "chat.jsonl"
        if not chat_path.exists():
            continue

        messages = _parse_jsonl(chat_path)
        if not messages:
            messages = []

        if messages:
            last = messages[-1]
            if (
                _is_notice_message(last)
                and str(last.get("content") or "") == text
            ):
                messages.pop()
                skipped += 1

        messages.append({
            "role": "system",
            "name": NOTICE_NAME,
            "content": text,
            "datetime": now,
        })
        _write_jsonl(chat_path, messages)
        updated += 1
        affected.append(account_dir.name)

    return {
        "updated": updated,
        "skipped": skipped,
        "accounts": affected,
        "content": text,
        "datetime": now,
    }
