import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import Callable, Optional

import httpx
from fastapi import APIRouter, Header, HTTPException, Request
from fastapi.responses import StreamingResponse

logger = logging.getLogger("app.chatbot_service")
if not logger.handlers:
    _handler = logging.StreamHandler(sys.stdout)
    _handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
    logger.addHandler(_handler)
logger.setLevel(logging.INFO)
logger.propagate = False


CHATBOT_DIRNAME = "chatbot"
CHATBOT_HISTORY_FILENAME = "chat.jsonl"
ALLOWED_MESSAGE_ROLES = {"system", "user", "assistant", "tool"}
MODEL_MESSAGE_KEYS = {
    "role",
    "content",
    "name",
    "tool_call_id",
    "tool_calls",
    "reasoning_content",
    "refusal",
    "audio",
}


class ChatbotStreamError(Exception):
    def __init__(self, message: str):
        super().__init__(message)
        self.message = message


def _now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def _preview_text(text: str, limit: int = 180) -> str:
    compact = " ".join(str(text or "").split())
    if len(compact) <= limit:
        return compact
    return compact[:limit] + "..."


def _clone_message_fields(message: dict, *, include_datetime: bool) -> dict:
    role = str(message.get("role", "")).strip().lower()
    if not role:
        raise HTTPException(status_code=400, detail="消息缺少 role")

    cloned: dict = {"role": role}
    for key in MODEL_MESSAGE_KEYS - {"role"}:
        if key not in message:
            continue
        value = message.get(key)
        if value is None:
            continue
        cloned[key] = value

    if include_datetime and role in {"user", "assistant"}:
        cloned["datetime"] = message.get("datetime") or _now_iso()
    elif include_datetime and message.get("datetime"):
        cloned["datetime"] = message.get("datetime")

    if role == "assistant" and cloned.get("content") is None:
        cloned["content"] = None if "tool_calls" in cloned else ""

    return cloned


def _normalize_message_for_model(message: dict) -> dict:
    cloned = _clone_message_fields(message, include_datetime=False)
    cloned.pop("datetime", None)
    return cloned


def _validate_message_record(record: object) -> dict | None:
    if not isinstance(record, dict):
        return None
    role = str(record.get("role", "")).strip().lower()
    if role not in ALLOWED_MESSAGE_ROLES:
        return None
    try:
        return _clone_message_fields(record, include_datetime=True)
    except HTTPException:
        return None


class ChatbotHistoryStore:
    def __init__(self):
        self._locks: dict[str, Lock] = {}
        self._locks_guard = Lock()

    def history_path(self, account_dir: Path) -> Path:
        return account_dir / CHATBOT_DIRNAME / CHATBOT_HISTORY_FILENAME

    def _get_lock(self, account_dir: Path) -> Lock:
        key = str(account_dir)
        with self._locks_guard:
            lock = self._locks.get(key)
            if not lock:
                lock = Lock()
                self._locks[key] = lock
            return lock

    def load_messages(self, account_dir: Path) -> list[dict]:
        path = self.history_path(account_dir)
        if not path.exists():
            return []

        messages: list[dict] = []
        try:
            with path.open("r", encoding="utf-8") as fh:
                for line in fh:
                    raw = line.strip()
                    if not raw:
                        continue
                    try:
                        record = json.loads(raw)
                    except json.JSONDecodeError:
                        continue
                    normalized = _validate_message_record(record)
                    if normalized:
                        messages.append(normalized)
        except FileNotFoundError:
            return []
        return messages

    def append_messages(self, account_dir: Path, messages: list[dict]) -> None:
        if not messages:
            return

        path = self.history_path(account_dir)
        path.parent.mkdir(parents=True, exist_ok=True)
        lock = self._get_lock(account_dir)

        with lock:
            with path.open("a", encoding="utf-8") as fh:
                for message in messages:
                    fh.write(json.dumps(message, ensure_ascii=False) + "\n")


history_store = ChatbotHistoryStore()


def _build_model_messages(history: list[dict], content: str) -> list[dict]:
    messages = [_normalize_message_for_model(item) for item in history]
    messages.append({"role": "user", "content": content})
    return messages


async def _stream_chat_completion(base_url: str, api_key: str, payload: dict):
    url = base_url.rstrip("/") + "/chat/completions"
    buffer = ""

    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(180.0)) as client:
            async with client.stream(
                "POST",
                url,
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {api_key}",
                },
                json=payload,
            ) as response:
                if response.status_code != 200:
                    error_bytes = await response.aread()
                    message = error_bytes.decode("utf-8", errors="ignore")[:500]
                    raise ChatbotStreamError(f"AI 对话调用失败: {message}")

                async for chunk in response.aiter_text():
                    buffer += chunk
                    lines = buffer.split("\n")
                    buffer = lines.pop()
                    for line in lines:
                        raw = line.strip()
                        if not raw.startswith("data:"):
                            continue
                        data_str = raw[5:].strip()
                        if not data_str or data_str == "[DONE]":
                            continue
                        try:
                            data_obj = json.loads(data_str)
                        except json.JSONDecodeError:
                            continue
                        yield data_obj
    except ChatbotStreamError:
        raise
    except Exception as exc:
        raise ChatbotStreamError(f"对话流转发失败: {str(exc)}")


def register_chatbot_routes(
    app,
    *,
    resolve_session: Callable,
    get_chatbot_preset: Callable,
):
    router = APIRouter()

    @router.get("/api/chatbot/history")
    def get_chatbot_history(x_fzt_key: Optional[str] = Header(default=None)):
        session = resolve_session(x_fzt_key)
        return {"messages": history_store.load_messages(session.account_dir)}

    @router.post("/api/chatbot")
    async def chat_stream(request: Request, x_fzt_key: Optional[str] = Header(default=None)):
        t0 = time.perf_counter()
        session = resolve_session(x_fzt_key)
        payload = await request.json()
        if not isinstance(payload, dict):
            raise HTTPException(status_code=400, detail="请求体必须是 JSON 对象")

        content = str(
            payload.get("content")
            or payload.get("message")
            or payload.get("text")
            or ""
        ).strip()
        if not content:
            raise HTTPException(status_code=400, detail="缺少聊天内容")

        request_id = f"chatbot:{int(time.time() * 1000)}"

        preset = get_chatbot_preset()
        base_url = preset.get("baseUrl", "")
        api_key = preset.get("apiKey", "")
        model = preset.get("model", "")
        if not base_url or not api_key or not model:
            raise HTTPException(status_code=500, detail="LLM 配置不完整")

        history = history_store.load_messages(session.account_dir)
        user_message = {"role": "user", "content": content, "datetime": _now_iso()}
        history_store.append_messages(session.account_dir, [user_message])

        upstream_messages = _build_model_messages(history, content)
        upstream_payload = {
            "model": model,
            "messages": upstream_messages,
            "stream": True,
        }

        logger.info(
            "[chatbot] stage=web_to_server_received request_id=%s remote=%s model=%s messages=%d",
            request_id,
            getattr(request.client, "host", "-"),
            model,
            len(upstream_messages),
        )
        logger.info(
            "[chatbot] request_id=%s history_chars=%d content_preview=%s",
            request_id,
            sum(len(str(item.get("content", "") or "")) for item in history),
            _preview_text(content),
        )

        async def _logged_stream():
            content_parts: list[str] = []
            reasoning_parts: list[str] = []
            tool_call_buffers: dict[int, dict] = {}
            finish_reason = "stop"
            usage = None

            try:
                logger.info(
                    "[chatbot] stage=server_to_llm_prepare request_id=%s model=%s upstream_messages=%d base_url=%s",
                    request_id,
                    model,
                    len(upstream_messages),
                    base_url,
                )
                async for data_obj in _stream_chat_completion(base_url, api_key, upstream_payload):
                    choices = data_obj.get("choices", []) if isinstance(data_obj, dict) else []
                    if not choices:
                        continue
                    choice = choices[0] or {}
                    delta = choice.get("delta", {}) or {}
                    if choice.get("finish_reason"):
                        finish_reason = choice.get("finish_reason")

                    reasoning_content = delta.get("reasoning_content")
                    if reasoning_content:
                        reasoning_parts.append(reasoning_content)

                    content_delta = delta.get("content")
                    if content_delta:
                        content_parts.append(content_delta)
                        yield content_delta.encode("utf-8")

                    if delta.get("tool_calls"):
                        for tc_delta in delta.get("tool_calls"):
                            idx = tc_delta.get("index", 0)
                            if idx not in tool_call_buffers:
                                tool_call_buffers[idx] = {
                                    "id": "",
                                    "type": "function",
                                    "function": {"name": "", "arguments": ""},
                                }
                            if tc_delta.get("id"):
                                tool_call_buffers[idx]["id"] = tc_delta["id"]
                            fn = tc_delta.get("function") or {}
                            if fn.get("name"):
                                tool_call_buffers[idx]["function"]["name"] = fn["name"]
                            if fn.get("arguments"):
                                tool_call_buffers[idx]["function"]["arguments"] += fn["arguments"]

                    if data_obj.get("usage"):
                        usage = data_obj["usage"]

                assistant_content = "".join(content_parts)
                assistant_reasoning = "".join(reasoning_parts)
                tool_calls = [tool_call_buffers[i] for i in sorted(tool_call_buffers.keys())] if tool_call_buffers else None
                assistant_record: dict = {
                    "role": "assistant",
                    "content": assistant_content if assistant_content or not tool_calls else None,
                    "datetime": _now_iso(),
                }
                if assistant_reasoning or tool_calls:
                    assistant_record["reasoning_content"] = assistant_reasoning
                if tool_calls:
                    assistant_record["tool_calls"] = tool_calls
                if assistant_record.get("content") is None and not assistant_record.get("tool_calls"):
                    assistant_record["content"] = ""

                history_store.append_messages(session.account_dir, [assistant_record])
                elapsed_ms = round((time.perf_counter() - t0) * 1000, 1)
                logger.info(
                    "[chatbot] stage=server_to_web_done request_id=%s elapsed_ms=%s output_chars=%d finish_reason=%s",
                    request_id,
                    elapsed_ms,
                    len(assistant_content),
                    finish_reason,
                )
                logger.info(
                    json.dumps(
                        {
                            "kind": "chatbot_service",
                            "request_id": request_id,
                            "model": model,
                            "input_messages": [
                                {"role": item.get("role"), "chars": len(str(item.get("content", "") or ""))}
                                for item in upstream_messages
                            ],
                            "output_chars": len(assistant_content),
                            "reasoning_chars": len(assistant_reasoning),
                            "tool_calls": len(tool_calls or []),
                            "usage": usage,
                            "elapsed_ms": elapsed_ms,
                        },
                        ensure_ascii=False,
                    )
                )
            except ChatbotStreamError as exc:
                elapsed_ms = round((time.perf_counter() - t0) * 1000, 1)
                logger.exception(
                    "[chatbot] stage=llm_or_bridge_failed request_id=%s elapsed_ms=%s error=%s",
                    request_id,
                    elapsed_ms,
                    exc.message,
                )
                error_text = f"\n[对话失败] {exc.message}"
                logger.info(
                    "[chatbot] stage=server_to_web_error_text request_id=%s error_preview=%s",
                    request_id,
                    _preview_text(error_text),
                )
                yield error_text.encode("utf-8")
            except Exception as exc:
                elapsed_ms = round((time.perf_counter() - t0) * 1000, 1)
                logger.exception(
                    "[chatbot] stage=unexpected_failed request_id=%s elapsed_ms=%s error=%s",
                    request_id,
                    elapsed_ms,
                    str(exc),
                )
                error_text = f"\n[对话失败] {str(exc)}"
                logger.info(
                    "[chatbot] stage=server_to_web_error_text request_id=%s error_preview=%s",
                    request_id,
                    _preview_text(error_text),
                )
                yield error_text.encode("utf-8")

        return StreamingResponse(_logged_stream(), media_type="text/plain; charset=utf-8")

    app.include_router(router)
