"""ai_analyse 体系下的内部 HTTP 辅助函数。

这个文件以 `_` 开头，不会出现在 `list_system_functions` 的公开列表里。
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import httpx


DEFAULT_API_BASE_URL = "http://localhost:3000"


def api_base_url(params: dict[str, Any]) -> str:
    return str(params.get("api_base_url") or DEFAULT_API_BASE_URL).rstrip("/")


def service_token(params: dict[str, Any]) -> str:
    return str(params.get("service_token") or params.get("token") or "").strip()


def auth_headers(params: dict[str, Any]) -> dict[str, str]:
    token = service_token(params)
    if not token:
        raise ValueError("缺少 service_token")
    return {"X-Auth-Token": token}


def pretty_json(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, indent=2)


def response_payload(response: httpx.Response) -> Any:
    try:
        return response.json()
    except Exception:
        return response.text


def api_error_message(response: httpx.Response) -> str:
    payload = response_payload(response)
    if isinstance(payload, str):
        return payload
    return pretty_json(payload)


def save_download_to_workspace(ws, response: httpx.Response, fallback_name: str) -> Path:
    downloads_dir = ws.write_root / "system_function_downloads"
    downloads_dir.mkdir(parents=True, exist_ok=True)

    filename = fallback_name
    disposition = response.headers.get("content-disposition", "")
    if "filename=" in disposition:
        raw_name = disposition.split("filename=", 1)[1].strip().strip('"')
        if raw_name:
            filename = Path(raw_name).name

    target = downloads_dir / filename
    target.write_bytes(response.content)
    return target
