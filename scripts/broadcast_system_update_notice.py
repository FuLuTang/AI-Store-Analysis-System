#!/usr/bin/env python3
"""向所有 chat.jsonl 追加系统功能更新 notice。"""

from __future__ import annotations

import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from packages.agents.system_service_functions.ai_analyse.history._append_notice_to_all_chats import (  # noqa: E402
    broadcast_notice_to_all_chats,
)


DEFAULT_NOTICE = "系统功能性更新\n有部分现有功能变动或开放新功能"


def main() -> int:
    accounts_dir = Path("/storage/accounts")
    content = DEFAULT_NOTICE
    if len(sys.argv) > 1:
        content = " ".join(sys.argv[1:]).strip() or DEFAULT_NOTICE

    result = broadcast_notice_to_all_chats(accounts_dir, content)
    print(
        f"updated={result.get('updated', 0)} "
        f"skipped={result.get('skipped', 0)} "
        f"accounts={len(result.get('accounts', []))}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
