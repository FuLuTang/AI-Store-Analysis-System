import json
import logging
import sys
import time
import asyncio
import hashlib
import mimetypes
import re
import uuid
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import Callable, Optional

from openai import OpenAI
from fastapi import APIRouter, File, Header, HTTPException, Request, UploadFile
from fastapi.responses import JSONResponse

from packages.agents.core.chat_agent_loop import ChatAgentLoop
from packages.agents.core.workspace import Workspace
from packages.agents.chatbot.prompt_builder import build_system_content as build_chatbot_system_content
from packages.auth import (
    append_token_event,
    check_token_logable,
    generate_service_token,
    token_hash,
)

logger = logging.getLogger("app.chatbot_service")
if not logger.handlers:
    _handler = logging.StreamHandler(sys.stdout)
    _handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
    logger.addHandler(_handler)
logger.setLevel(logging.INFO)
logger.propagate = False


CHATBOT_DIRNAME = "chatbot"
SERVICE_DOCS_DIRNAME = "service_docs"
CHATBOT_HISTORY_FILENAME = "chat.jsonl"
CHATBOT_WORKSPACE_DIRNAME = "workspace"
CHATBOT_FILES_DIRNAME = "files"
CHATBOT_ATTACHMENTS_FILENAME = "attachments.jsonl"
CHATBOT_MAX_ATTACHMENT_SIZE = 100 * 1024 * 1024
CHATBOT_MAX_ATTACHMENTS_PER_MESSAGE = 20
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
    "title",
    "detail",
    "options",
    "choice",
}
HISTORY_EXTRA_KEYS = {"attachments"}
NOTICE_NAME = "notice"
ASK_TOKEN_AUTH_NAME = "ask_token_auth"
SERVICE_TOKEN_TOOL_NAME = "get_user_service_token"
CHATBOT_TYPING_STATE = "AI客服输入中......"
CHATBOT_STATUS_MIN_INTERVAL_SECONDS = 2

CHATBOT_RUNNING: dict[str, bool] = {}
CHATBOT_RUNNING_LOCK = Lock()
CHATBOT_STATUS_POLLS: dict[str, float] = {}
CHATBOT_STATUS_LOCK = Lock()


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


def _safe_original_name(filename: str | None) -> str:
    name = Path(str(filename or "unnamed")).name.strip()
    return name or "unnamed"


def _safe_extension(filename: str) -> str:
    suffix = Path(filename).suffix.lower()
    if not suffix:
        return ""
    if not re.fullmatch(r"\.[a-z0-9]{1,16}", suffix):
        return ""
    return suffix


def _public_attachment_record(record: dict) -> dict:
    relative_path = str(record.get("relativePath", "") or "")
    if relative_path and not relative_path.startswith(f"{CHATBOT_DIRNAME}/"):
        relative_path = f"{CHATBOT_DIRNAME}/{relative_path.lstrip('/')}"
    return {
        "attachmentId": record.get("attachmentId", ""),
        "originalName": record.get("originalName", ""),
        "storedName": record.get("storedName", ""),
        "mimeType": record.get("mimeType", "application/octet-stream"),
        "size": record.get("size", 0),
        "sha256": record.get("sha256", ""),
        "createdAt": record.get("createdAt", ""),
        "relativePath": relative_path,
    }


def _normalize_attachment_relative_path(value: str) -> str:
    text = str(value or "").strip().replace("\\", "/")
    if not text:
        return ""
    if text.startswith(f"{CHATBOT_DIRNAME}/") or text.startswith(f"{SERVICE_DOCS_DIRNAME}/"):
        return text
    return f"{CHATBOT_DIRNAME}/{text.lstrip('/')}"


def _format_attachment_context(attachments: list[dict]) -> str:
    if not attachments:
        return ""
    lines = ["", "", "该条客服会话消息引用了以下附件，必要时请用工具读取相对路径："]
    for idx, item in enumerate(attachments, 1):
        lines.append(
            f"{idx}. {item.get('originalName', 'unnamed')} "
            f"(attachmentId={item.get('attachmentId', '')}, "
            f"path={_normalize_attachment_relative_path(item.get('relativePath', ''))}, "
            f"mime={item.get('mimeType', 'application/octet-stream')}, "
            f"size={item.get('size', 0)} bytes)"
        )
    return "\n".join(lines)


def _is_notice_message(message: dict) -> bool:
    return (
        str(message.get("role", "")).strip().lower() == "system"
        and str(message.get("name", "")).strip().lower() == NOTICE_NAME
    )


def _is_card_message(message: dict) -> bool:
    return (
        str(message.get("role", "")).strip().lower() == "system"
        and bool(str(message.get("title") or "").strip())
    )


def _history_time(message: dict) -> str:
    return str(message.get("datetime") or message.get("time") or "").strip()


def _notice_messages_for_model(messages: list[dict]) -> list[dict]:
    notices: list[dict] = []
    for message in messages:
        if not _is_notice_message(message):
            continue
        content = str(message.get("content") or "").strip()
        if not content:
            continue
        notice: dict = {
            "role": "system",
            "name": NOTICE_NAME,
            "content": content,
        }
        notice_time = _history_time(message)
        if notice_time:
            notice["content"] = f"【系统提醒 {notice_time}】\n{content}"
        notices.append(notice)
    return notices


def _system_card_message_for_model(message: dict) -> dict:
    lines = [
        "【系统卡片消息】",
        f"name: {str(message.get('name') or '').strip() or '(未命名)'}",
        f"title: {str(message.get('title') or '').strip() or '(无标题)'}",
    ]
    detail = str(message.get("detail") or "").strip()
    if detail:
        lines.append(f"detail: {detail}")
    options = message.get("options")
    if isinstance(options, list) and options:
        lines.append("options: " + " / ".join(str(item) for item in options))
    if "choice" in message:
        lines.append(f"choice: {str(message.get('choice') or '').strip()}")
    card_time = _history_time(message)
    if card_time:
        lines.append(f"time: {card_time}")
    return {"role": "system", "content": "\n".join(lines)}


def _has_pending_token_auth(messages: list[dict]) -> bool:
    for message in reversed(messages):
        if (
            str(message.get("role", "")).strip().lower() == "system"
            and str(message.get("name", "")).strip() == ASK_TOKEN_AUTH_NAME
        ):
            return "choice" not in message
    return False


def _public_history_messages(messages: list[dict]) -> list[dict]:
    visible: list[dict] = []
    for message in messages:
        role = str(message.get("role", "")).strip().lower()
        if _is_notice_message(message):
            notice = {
                "role": NOTICE_NAME,
                "content": str(message.get("content") or ""),
            }
            notice_time = _history_time(message)
            if notice_time:
                notice["datetime"] = notice_time
            visible.append(notice)
            continue
        if _is_card_message(message):
            card = {
                "role": "card",
                "name": str(message.get("name") or ""),
                "title": str(message.get("title") or ""),
                "detail": str(message.get("detail") or ""),
            }
            options = message.get("options")
            if isinstance(options, list):
                card["options"] = [str(item) for item in options]
            if "choice" in message:
                card["choice"] = str(message.get("choice") or "")
            card_time = _history_time(message)
            if card_time:
                card["datetime"] = card_time
            visible.append(card)
            continue
        if role == "system":
            continue
        public_message = dict(message)
        attachments = public_message.get("attachments")
        if isinstance(attachments, list):
            public_message["attachments"] = [
                _public_attachment_record(item) if isinstance(item, dict) else item
                for item in attachments
            ]
        visible.append(public_message)
    return visible


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
    elif include_datetime and _is_notice_message(message) and message.get("time"):
        cloned["time"] = message.get("time")

    if include_datetime:
        for key in HISTORY_EXTRA_KEYS:
            value = message.get(key)
            if isinstance(value, list):
                cloned[key] = value

    if role == "assistant" and cloned.get("content") is None:
        cloned["content"] = None if "tool_calls" in cloned else ""

    return cloned


def _normalize_message_for_model(message: dict) -> dict:
    cloned = _clone_message_fields(message, include_datetime=False)
    cloned.pop("datetime", None)
    if cloned.get("role") == "user":
        attachment_context = _format_attachment_context(message.get("attachments") or [])
        if attachment_context:
            cloned["content"] = f"{str(cloned.get('content') or '').strip()}{attachment_context}".strip()
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

    def last_update(self, account_dir: Path) -> str:
        messages = self.load_messages(account_dir)
        for message in reversed(messages):
            ts = _history_time(message)
            if ts:
                return ts
        return ""

    def complete_token_auth(self, account_dir: Path, choice: str, tool_content: str) -> None:
        path = self.history_path(account_dir)
        path.parent.mkdir(parents=True, exist_ok=True)
        lock = self._get_lock(account_dir)
        now = _now_iso()

        with lock:
            raw_messages: list[dict] = []
            if path.exists():
                with path.open("r", encoding="utf-8") as fh:
                    for line in fh:
                        raw = line.strip()
                        if not raw:
                            continue
                        try:
                            data = json.loads(raw)
                            if isinstance(data, dict):
                                raw_messages.append(data)
                        except json.JSONDecodeError:
                            continue

            card_index = None
            for idx in range(len(raw_messages) - 1, -1, -1):
                msg = raw_messages[idx]
                if (
                    str(msg.get("role", "")).strip().lower() == "system"
                    and str(msg.get("name", "")).strip() == ASK_TOKEN_AUTH_NAME
                    and "choice" not in msg
                ):
                    card_index = idx
                    break
            if card_index is None:
                raise HTTPException(status_code=400, detail="未找到待确认的授权请求")
            raw_messages[card_index]["choice"] = choice
            raw_messages[card_index]["datetime"] = raw_messages[card_index].get("datetime") or now

            tool_call_id = ""
            for msg in reversed(raw_messages):
                if str(msg.get("role", "")).strip().lower() != "assistant":
                    continue
                calls = msg.get("tool_calls")
                if not isinstance(calls, list):
                    continue
                for call in reversed(calls):
                    if not isinstance(call, dict):
                        continue
                    function = call.get("function") if isinstance(call.get("function"), dict) else {}
                    name = function.get("name") or call.get("name")
                    if name == SERVICE_TOKEN_TOOL_NAME:
                        tool_call_id = str(call.get("id") or "")
                        break
                if tool_call_id:
                    break
            if not tool_call_id:
                raise HTTPException(status_code=400, detail="未找到对应的工具调用记录")

            raw_messages.append({
                "role": "tool",
                "tool_call_id": tool_call_id,
                "name": SERVICE_TOKEN_TOOL_NAME,
                "content": tool_content,
                "datetime": now,
            })
            path.write_text(
                "".join(json.dumps(item, ensure_ascii=False) + "\n" for item in raw_messages),
                encoding="utf-8",
            )


history_store = ChatbotHistoryStore()


class ChatbotAttachmentStore:
    def __init__(self):
        self._locks: dict[str, Lock] = {}
        self._locks_guard = Lock()

    def files_dir(self, account_dir: Path) -> Path:
        return account_dir / CHATBOT_DIRNAME / CHATBOT_FILES_DIRNAME

    def metadata_path(self, account_dir: Path) -> Path:
        return account_dir / CHATBOT_DIRNAME / CHATBOT_ATTACHMENTS_FILENAME

    def _get_lock(self, account_dir: Path) -> Lock:
        key = str(account_dir)
        with self._locks_guard:
            lock = self._locks.get(key)
            if not lock:
                lock = Lock()
                self._locks[key] = lock
            return lock

    def load_index(self, account_dir: Path) -> dict[str, dict]:
        path = self.metadata_path(account_dir)
        if not path.exists():
            return {}

        index: dict[str, dict] = {}
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
                    attachment_id = str(record.get("attachmentId") or "").strip()
                    if attachment_id:
                        index[attachment_id] = _public_attachment_record(record)
        except FileNotFoundError:
            return {}
        return index

    def resolve_many(self, account_dir: Path, attachment_ids: list[str]) -> list[dict]:
        if len(attachment_ids) > CHATBOT_MAX_ATTACHMENTS_PER_MESSAGE:
            raise HTTPException(status_code=400, detail=f"单次会话最多引用 {CHATBOT_MAX_ATTACHMENTS_PER_MESSAGE} 个附件")
        index = self.load_index(account_dir)
        records: list[dict] = []
        missing: list[str] = []
        seen: set[str] = set()
        for attachment_id in attachment_ids:
            clean_id = str(attachment_id or "").strip()
            if not clean_id or clean_id in seen:
                continue
            seen.add(clean_id)
            record = index.get(clean_id)
            if not record:
                missing.append(clean_id)
                continue
            records.append(record)
        if missing:
            raise HTTPException(status_code=400, detail=f"附件不存在或无权访问: {', '.join(missing)}")
        return records

    async def save_uploads(self, account_dir: Path, uploads: list[UploadFile]) -> list[dict]:
        if not uploads:
            raise HTTPException(status_code=400, detail="缺少附件")
        if len(uploads) > CHATBOT_MAX_ATTACHMENTS_PER_MESSAGE:
            raise HTTPException(status_code=400, detail=f"单次最多上传 {CHATBOT_MAX_ATTACHMENTS_PER_MESSAGE} 个附件")

        files_dir = self.files_dir(account_dir)
        metadata_path = self.metadata_path(account_dir)
        files_dir.mkdir(parents=True, exist_ok=True)
        metadata_path.parent.mkdir(parents=True, exist_ok=True)

        records: list[dict] = []
        for upload in uploads:
            original_name = _safe_original_name(upload.filename)
            raw = await upload.read()
            if len(raw) > CHATBOT_MAX_ATTACHMENT_SIZE:
                raise HTTPException(status_code=400, detail=f"附件过大(>100MB): {original_name}")

            attachment_id = uuid.uuid4().hex
            ext = _safe_extension(original_name)
            stored_name = f"{attachment_id}{ext}"
            target = files_dir / stored_name
            target.write_bytes(raw)
            mime_type = upload.content_type or mimetypes.guess_type(original_name)[0] or "application/octet-stream"
            record = {
                "attachmentId": attachment_id,
                "originalName": original_name,
                "storedName": stored_name,
                "mimeType": mime_type,
                "size": len(raw),
                "sha256": hashlib.sha256(raw).hexdigest(),
                "createdAt": _now_iso(),
                "relativePath": f"{CHATBOT_DIRNAME}/{CHATBOT_FILES_DIRNAME}/{stored_name}",
            }
            records.append(record)

        lock = self._get_lock(account_dir)
        with lock:
            with metadata_path.open("a", encoding="utf-8") as fh:
                for record in records:
                    fh.write(json.dumps(record, ensure_ascii=False) + "\n")
        return [_public_attachment_record(record) for record in records]


attachment_store = ChatbotAttachmentStore()


def _chatbot_root(account_dir: Path) -> Path:
    return account_dir / CHATBOT_DIRNAME


def _build_chatbot_workspace(account_dir: Path) -> Workspace:
    root = _chatbot_root(account_dir)
    workspace_dir = root / CHATBOT_WORKSPACE_DIRNAME
    service_docs_root = Path(__file__).resolve().parents[3] / "storage" / SERVICE_DOCS_DIRNAME
    return Workspace(
        base_dir=root,
        read_roots={
            CHATBOT_DIRNAME: root,
            SERVICE_DOCS_DIRNAME: service_docs_root,
        },
        default_read_domain=CHATBOT_DIRNAME,
        write_root=workspace_dir,
        script_root=workspace_dir,
    )


def _build_initial_messages(history: list[dict], content: str, attachments: list[dict]) -> tuple[list[dict], list[dict]]:
    initial_messages = _build_messages_from_history(history)
    prefix_to_persist: list[dict] = []

    user_message = {"role": "user", "content": content, "datetime": _now_iso()}
    if attachments:
        user_message["attachments"] = attachments
    initial_messages.append(_normalize_message_for_model(user_message))
    prefix_to_persist.append(user_message)
    return initial_messages, prefix_to_persist


def _build_messages_from_history(history: list[dict]) -> list[dict]:
    messages: list[dict] = [{"role": "system", "content": build_chatbot_system_content()}]
    pending_card_messages: list[dict] = []
    for item in history:
        role = str(item.get("role", "")).strip().lower()
        if _is_notice_message(item):
            messages.extend(_notice_messages_for_model([item]))
            continue
        if _is_card_message(item):
            pending_card_messages.append(_system_card_message_for_model(item))
            continue
        if role == "system":
            continue
        messages.append(_normalize_message_for_model(item))
        if role == "tool" and pending_card_messages:
            messages.extend(pending_card_messages)
            pending_card_messages = []
    if pending_card_messages:
        last = messages[-1] if messages else {}
        # 未闭合的 assistant tool_call 不能后接 system 消息；这种情况通常会被发送入口拦截。
        if not (
            str(last.get("role", "")).strip().lower() == "assistant"
            and isinstance(last.get("tool_calls"), list)
            and last.get("tool_calls")
        ):
            messages.extend(pending_card_messages)
    return messages


def _extract_final_text(result: dict) -> str:
    full_report = str(result.get("full_report") or "").strip()
    if full_report:
        return full_report
    compact = {k: v for k, v in result.items() if k != "_token_usage"}
    return json.dumps(compact, ensure_ascii=False)


def _running_key(account_dir: Path) -> str:
    return str(account_dir)


def _set_chatbot_running(account_dir: Path, running: bool):
    with CHATBOT_RUNNING_LOCK:
        CHATBOT_RUNNING[_running_key(account_dir)] = running


def _is_chatbot_running(account_dir: Path) -> bool:
    with CHATBOT_RUNNING_LOCK:
        return bool(CHATBOT_RUNNING.get(_running_key(account_dir), False))


def _check_status_poll_limit(token: str):
    hashed = token_hash(token)
    now = time.time()
    with CHATBOT_STATUS_LOCK:
        last = CHATBOT_STATUS_POLLS.get(hashed, 0)
        if now - last < CHATBOT_STATUS_MIN_INTERVAL_SECONDS:
            raise HTTPException(status_code=429, detail="chatbot status 查询过于频繁")
        CHATBOT_STATUS_POLLS[hashed] = now


def _build_service_token_from_user_token(accounts_dir: Path, user_token: str) -> str:
    auth_result = check_token_logable(accounts_dir, user_token)
    if not auth_result or auth_result.is_service:
        raise HTTPException(status_code=401, detail="用户 token 无效或已过期")
    service_token = generate_service_token(auth_result.account)
    append_token_event(auth_result.account.account_dir, {
        "action": "serv_creation",
        "token_hash": token_hash(service_token),
        "parent_token_hash": auth_result.token_hash,
    })
    return service_token


def _is_token_auth_choice(payload: dict) -> bool:
    return (
        str(payload.get("name") or "").strip() == ASK_TOKEN_AUTH_NAME
        and ("choice" in payload or "choise" in payload)
        and "detail" in payload
    )


def register_chatbot_routes(
    app,
    *,
    resolve_session: Callable,
    get_chatbot_preset: Callable,
):
    router = APIRouter()

    @router.get("/api/chatbot/history")
    def get_chatbot_history(x_auth_token: Optional[str] = Header(default=None)):
        session = resolve_session(x_auth_token)
        messages = history_store.load_messages(session.account_dir)
        return {
            "messages": _public_history_messages(messages),
            "last_update": history_store.last_update(session.account_dir),
        }

    @router.post("/api/chatbot/attachments")
    async def upload_chatbot_attachments(
        attachments: list[UploadFile] = File(...),
        x_auth_token: Optional[str] = Header(default=None),
    ):
        session = resolve_session(x_auth_token)
        saved = await attachment_store.save_uploads(session.account_dir, attachments)
        logger.info(
            "[chatbot] stage=attachments_uploaded account=%s count=%d bytes=%d",
            getattr(session, "account_id", "-"),
            len(saved),
            sum(int(item.get("size") or 0) for item in saved),
        )
        return {"attachments": saved}

    async def _run_chatbot_agent_background(session, request_id: str, preset: dict, base_url: str, api_key: str, model: str, t0: float):
        _set_chatbot_running(session.account_dir, True)
        try:
            history = history_store.load_messages(session.account_dir)
            initial_messages = _build_messages_from_history(history)
            ws = _build_chatbot_workspace(session.account_dir)
            logger.info(
                "[chatbot] stage=server_to_llm_prepare request_id=%s model=%s upstream_messages=%d base_url=%s workspace=%s",
                request_id,
                model,
                len(initial_messages),
                base_url,
                ws.dir,
            )
            llm_preset = {
                "model": model,
                "baseUrl": base_url,
                "apiKey": api_key,
            }
            client = OpenAI(api_key=api_key, base_url=base_url, max_retries=0)
            loop = ChatAgentLoop(
                client=client,
                ws=ws,
                llm_preset=llm_preset,
                initial_messages=initial_messages,
                load_messages=lambda: _build_messages_from_history(history_store.load_messages(session.account_dir)),
                persist_messages=lambda msgs: history_store.append_messages(session.account_dir, msgs),
                emit_log=lambda nid, msg: logger.info(
                    "[chatbot-agent] request_id=%s node=%s message=%s",
                    request_id,
                    nid,
                    json.dumps(msg, ensure_ascii=False) if isinstance(msg, dict) else str(msg),
                ),
                emit_status=lambda nid, st: logger.info(
                    "[chatbot-agent] request_id=%s node=%s status=%s",
                    request_id,
                    nid,
                    st,
                ),
                check_aborted=None,
            )
            result = await asyncio.to_thread(loop.run)
            final_text = _extract_final_text(result)
            elapsed_ms = round((time.perf_counter() - t0) * 1000, 1)
            logger.info(
                "[chatbot] stage=agent_done request_id=%s elapsed_ms=%s output_chars=%d new_messages=%d waiting_auth=%s",
                request_id,
                elapsed_ms,
                len(final_text),
                max(len(loop.messages) - len(initial_messages), 0),
                bool(result.get("_waiting_auth")),
            )
            logger.info(
                json.dumps(
                    {
                        "kind": "chatbot_service",
                        "request_id": request_id,
                        "model": model,
                        "output_chars": len(final_text),
                        "new_messages": max(len(loop.messages) - len(initial_messages), 0),
                        "usage": result.get("_token_usage", {}),
                        "elapsed_ms": elapsed_ms,
                    },
                    ensure_ascii=False,
                )
            )
        except Exception as exc:
            elapsed_ms = round((time.perf_counter() - t0) * 1000, 1)
            logger.exception(
                "[chatbot] stage=background_failed request_id=%s elapsed_ms=%s error=%s",
                request_id,
                elapsed_ms,
                str(exc),
            )
            history_store.append_messages(session.account_dir, [{
                "role": "assistant",
                "content": f"\n[对话失败] {str(exc)}",
                "datetime": _now_iso(),
            }])
        finally:
            _set_chatbot_running(session.account_dir, False)

    @router.post("/api/chatbot")
    async def chat_stream(request: Request, x_auth_token: Optional[str] = Header(default=None)):
        t0 = time.perf_counter()
        session = resolve_session(x_auth_token)
        payload = await request.json()
        if not isinstance(payload, dict):
            raise HTTPException(status_code=400, detail="请求体必须是 JSON 对象")

        if _is_chatbot_running(session.account_dir):
            raise HTTPException(status_code=409, detail="AI 正在输入中，请稍后再发送")

        request_id = f"chatbot:{int(time.time() * 1000)}"
        preset = get_chatbot_preset()
        base_url = preset.get("baseUrl", "")
        api_key = preset.get("apiKey", "")
        model = preset.get("model", "")
        if not base_url or not api_key or not model:
            raise HTTPException(status_code=500, detail="LLM 配置不完整")

        if _is_token_auth_choice(payload):
            choice = str(payload.get("choice") or payload.get("choise") or "").strip()
            detail = str(payload.get("detail") or "").strip()
            if choice == "是":
                service_token = _build_service_token_from_user_token(session.account_dir.parent, detail)
                tool_content = service_token
            else:
                tool_content = json.dumps({"status": "denied", "message": "用户拒绝授权"}, ensure_ascii=False)
            history_store.complete_token_auth(session.account_dir, choice, tool_content)
            _set_chatbot_running(session.account_dir, True)
            asyncio.create_task(_run_chatbot_agent_background(session, request_id, preset, base_url, api_key, model, t0))
            return JSONResponse({"status": "ok"})

        history = history_store.load_messages(session.account_dir)
        if _has_pending_token_auth(history):
            raise HTTPException(status_code=409, detail="当前有待确认的代理操作许可，请先选择“是”或“否”")

        content = str(
            payload.get("content")
            or payload.get("message")
            or payload.get("text")
            or ""
        ).strip()

        raw_attachment_ids = payload.get("attachmentIds") or []
        if raw_attachment_ids is None:
            raw_attachment_ids = []
        if not isinstance(raw_attachment_ids, list):
            raise HTTPException(status_code=400, detail="attachmentIds 必须是数组")
        attachment_ids = [str(item).strip() for item in raw_attachment_ids if str(item or "").strip()]
        attachments = attachment_store.resolve_many(session.account_dir, attachment_ids)

        if not content and not attachments:
            raise HTTPException(status_code=400, detail="缺少聊天内容或附件")
        if not content and attachments:
            content = "请查看本次上传的附件。"

        initial_messages, prefix_to_persist = _build_initial_messages(history, content, attachments)
        history_store.append_messages(session.account_dir, prefix_to_persist)

        logger.info(
            "[chatbot] stage=web_to_server_received request_id=%s remote=%s model=%s messages=%d",
            request_id,
            getattr(request.client, "host", "-"),
            model,
            len(initial_messages),
        )
        logger.info(
            "[chatbot] request_id=%s history_chars=%d attachments=%d content_preview=%s",
            request_id,
            sum(len(str(item.get("content", "") or "")) for item in history),
            len(attachments),
            _preview_text(content),
        )

        _set_chatbot_running(session.account_dir, True)
        asyncio.create_task(_run_chatbot_agent_background(session, request_id, preset, base_url, api_key, model, t0))
        return JSONResponse({"status": "ok"})

    @router.get("/api/chatbot/status")
    def get_chatbot_status(x_auth_token: Optional[str] = Header(default=None)):
        token = (x_auth_token or "").strip()
        session = resolve_session(token)
        _check_status_poll_limit(token)
        payload = {"last_update": history_store.last_update(session.account_dir)}
        if _is_chatbot_running(session.account_dir):
            payload["state"] = CHATBOT_TYPING_STATE
        return payload

    app.include_router(router)
