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
from types import SimpleNamespace
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
from packages.agents.core.tools.impl.wait_impl import load_scheduled_waits_with_mtime, rewrite_scheduled_waits

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
HISTORY_SCALAR_KEYS = {"token_count"}
NOTICE_NAME = "notice"
ASK_TOKEN_AUTH_NAME = "ask_token_auth"
SERVICE_TOKEN_TOOL_NAME = "get_user_service_token"
CHATBOT_TYPING_STATE = "AI客服输入中......"
CHATBOT_STATUS_MIN_INTERVAL_SECONDS = 2
CHATBOT_HISTORY_COMPRESS_EVERY = 150
CHATBOT_HISTORY_KEEP_AT_LEAST = 20

CHATBOT_STATUS_STATE: dict[str, Optional[str]] = {}
CHATBOT_RUNNING_LOCK = Lock()
CHATBOT_STATUS_POLLS: dict[str, float] = {}
CHATBOT_STATUS_LOCK = Lock()
CHATBOT_SCHEDULER_TASK: Optional[asyncio.Task] = None


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
        if role in {"user", "assistant"}:
            content_val = str(message.get("content") or "")
            if role == "assistant" and not content_val.strip():
                continue
            public_msg = {
                "role": role,
                "content": content_val,
            }
            msg_time = _history_time(message)
            if msg_time:
                public_msg["datetime"] = msg_time
            if role == "assistant":
                token_count = message.get("token_count")
                if token_count is not None:
                    public_msg["token_count"] = token_count
            attachments = message.get("attachments")
            if isinstance(attachments, list):
                public_msg["attachments"] = [
                    _public_attachment_record(item) if isinstance(item, dict) else item
                    for item in attachments
                ]
            visible.append(public_msg)
            continue
        # Strictly ignore all other roles (system, tool, etc.) as per API docs
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
        for key in HISTORY_SCALAR_KEYS:
            value = message.get(key)
            if value is not None:
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


def is_history_message_compressed(
    total: int,
    index: int,
    compress_every: int = CHATBOT_HISTORY_COMPRESS_EVERY,
    keep_at_least: int = CHATBOT_HISTORY_KEEP_AT_LEAST,
) -> bool:
    compressed_prefix_length = compress_every * max(0, (total - keep_at_least) // compress_every)
    return index < compressed_prefix_length


def _tool_call_ids_from_message(message: dict) -> set[str]:
    tool_call_ids: set[str] = set()
    calls = message.get("tool_calls")
    if not isinstance(calls, list):
        return tool_call_ids
    for call in calls:
        if not isinstance(call, dict):
            continue
        tool_call_id = str(call.get("id") or "").strip()
        if tool_call_id:
            tool_call_ids.add(tool_call_id)
    return tool_call_ids


def _compress_history_for_model(history: list[dict]) -> list[dict]:
    total = len(history)
    compressed_tool_call_ids: set[str] = set()
    for index, message in enumerate(history):
        if is_history_message_compressed(total, index) and str(message.get("role", "")).strip().lower() == "assistant":
            compressed_tool_call_ids.update(_tool_call_ids_from_message(message))

    compressed_history: list[dict] = []
    for index, message in enumerate(history):
        role = str(message.get("role", "")).strip().lower()
        if not is_history_message_compressed(total, index):
            if role == "tool" and str(message.get("tool_call_id") or "").strip() in compressed_tool_call_ids:
                continue
            compressed_history.append(message)
            continue

        if role in {"system", "user"}:
            compressed_history.append(message)
            continue

        if role == "tool":
            tool_call_id = str(message.get("tool_call_id") or "").strip()
            if tool_call_id in compressed_tool_call_ids:
                continue
            continue

        if role == "assistant":
            compressed = dict(message)
            compressed.pop("reasoning_content", None)
            compressed.pop("tool_calls", None)
            compressed_history.append(compressed)
            continue

        compressed_history.append(message)
    return compressed_history


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

        import zipfile
        import io

        records: list[dict] = []
        total_files_count = 0

        for upload in uploads:
            original_name = _safe_original_name(upload.filename)
            raw = await upload.read()
            if len(raw) > CHATBOT_MAX_ATTACHMENT_SIZE:
                raise HTTPException(status_code=400, detail=f"附件过大(>100MB): {original_name}")

            # 检查是否为 zip 压缩文件
            if original_name.lower().endswith(".zip") or zipfile.is_zipfile(io.BytesIO(raw)):
                with zipfile.ZipFile(io.BytesIO(raw)) as zf:
                    zip_files = [m for m in zf.infolist() if not m.is_dir()]
                    total_files_count += len(zip_files)
                    if total_files_count > CHATBOT_MAX_ATTACHMENTS_PER_MESSAGE:
                        raise HTTPException(
                            status_code=400, 
                            detail=f"解压后文件总数超过单次会话上限 ({CHATBOT_MAX_ATTACHMENTS_PER_MESSAGE} 个)"
                        )

                    for member in zip_files:
                        clean_filename = member.filename.replace("\\", "/").strip("/")
                        if ".." in clean_filename or clean_filename.startswith("/"):
                            raise HTTPException(status_code=400, detail=f"压缩包内包含非法路径: {member.filename}")
                        
                        member_data = zf.read(member.filename)
                        attachment_id = uuid.uuid4().hex
                        ext = _safe_extension(clean_filename)
                        stored_name = f"{attachment_id}{ext}"
                        target = files_dir / stored_name
                        target.write_bytes(member_data)
                        
                        mime_type = mimetypes.guess_type(clean_filename)[0] or "application/octet-stream"
                        record = {
                            "attachmentId": attachment_id,
                            "originalName": clean_filename,
                            "storedName": stored_name,
                            "mimeType": mime_type,
                            "size": len(member_data),
                            "sha256": hashlib.sha256(member_data).hexdigest(),
                            "createdAt": _now_iso(),
                            "relativePath": f"{CHATBOT_DIRNAME}/{CHATBOT_FILES_DIRNAME}/{stored_name}",
                        }
                        records.append(record)
            else:
                total_files_count += 1
                if total_files_count > CHATBOT_MAX_ATTACHMENTS_PER_MESSAGE:
                    raise HTTPException(
                        status_code=400, 
                        detail=f"解压后文件总数超过单次会话上限 ({CHATBOT_MAX_ATTACHMENTS_PER_MESSAGE} 个)"
                    )

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
    history = _compress_history_for_model(history)
    messages: list[dict] = [{"role": "system", "content": build_chatbot_system_content()}]
    pending_card_messages: list[dict] = []
    for index, item in enumerate(history):
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
            next_role = ""
            if index + 1 < len(history):
                next_item = history[index + 1]
                next_role = str(next_item.get("role", "")).strip().lower()
            if next_role != "tool":
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


def _parse_scheduler_time(value: str) -> float:
    try:
        return datetime.fromisoformat(str(value or "").replace("Z", "+00:00")).timestamp()
    except Exception:
        return 0.0


def _extract_final_text(result: dict) -> str:
    full_report = str(result.get("full_report") or "").strip()
    if full_report:
        return full_report
    compact = {k: v for k, v in result.items() if k != "_token_usage"}
    return json.dumps(compact, ensure_ascii=False)


def _running_key(account_dir: Path) -> str:
    return str(account_dir)


def _set_chatbot_state(account_dir: Path, state: Optional[str]):
    with CHATBOT_RUNNING_LOCK:
        CHATBOT_STATUS_STATE[_running_key(account_dir)] = None if state is None else str(state)


def _get_chatbot_state(account_dir: Path) -> Optional[str]:
    with CHATBOT_RUNNING_LOCK:
        return CHATBOT_STATUS_STATE.get(_running_key(account_dir))


def _is_chatbot_running(account_dir: Path) -> bool:
    return _get_chatbot_state(account_dir) is not None


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
    accounts_dir: Path,
):
    global CHATBOT_SCHEDULER_TASK
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

    async def _resume_chatbot_agent_after(
        session,
        request_id: str,
        preset: dict,
        base_url: str,
        api_key: str,
        model: str,
        delay_seconds: int,
        t0: float,
    ):
        await asyncio.sleep(max(0, delay_seconds))
        await _run_chatbot_agent_background(session, f"{request_id}:resume", preset, base_url, api_key, model, t0)

    async def _run_chatbot_agent_background(
        session,
        request_id: str,
        preset: dict,
        base_url: str,
        api_key: str,
        model: str,
        t0: float,
        transient_system_prompt: str = "",
    ):
        _set_chatbot_state(session.account_dir, CHATBOT_TYPING_STATE)
        keep_state_for_resume = False
        try:
            history = history_store.load_messages(session.account_dir)
            initial_messages = _build_messages_from_history(history)
            transient_system_prompt = str(transient_system_prompt or "").strip()
            if transient_system_prompt:
                initial_messages.append({
                    "role": "system",
                    "content": f"【定时唤醒】{transient_system_prompt}",
                })
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

            def load_messages_for_loop():
                return _build_messages_from_history(history_store.load_messages(session.account_dir))

            loop = ChatAgentLoop(
                client=client,
                ws=ws,
                llm_preset=llm_preset,
                initial_messages=initial_messages,
                service_docs_account_dir=session.account_dir,
                load_messages=load_messages_for_loop,
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
                set_status=lambda state: _set_chatbot_state(session.account_dir, state),
                check_aborted=None,
            )
            result = await asyncio.to_thread(loop.run)
            resume_after_seconds = int(result.get("_resume_after_seconds") or 0)
            if resume_after_seconds > 0:
                keep_state_for_resume = True
                _set_chatbot_state(session.account_dir, f"等待 {resume_after_seconds} 秒后继续处理......")
                logger.info(
                    "[chatbot] stage=agent_scheduled_resume request_id=%s delay_seconds=%d",
                    request_id,
                    resume_after_seconds,
                )
                asyncio.create_task(
                    _resume_chatbot_agent_after(
                        session,
                        request_id,
                        preset,
                        base_url,
                        api_key,
                        model,
                        resume_after_seconds,
                        t0,
                    )
                )
                return
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
            if not keep_state_for_resume:
                _set_chatbot_state(session.account_dir, None)

    def _scheduled_task_account_dir(task: dict) -> Optional[Path]:
        raw_dir = str(task.get("account_dir") or "").strip()
        if raw_dir:
            account_dir = Path(raw_dir)
        else:
            account_id = str(task.get("account_id") or "").strip()
            if not account_id:
                return None
            account_dir = Path(accounts_dir) / account_id
        try:
            account_dir.resolve().relative_to(Path(accounts_dir).resolve())
        except Exception:
            return None
        return account_dir

    async def _trigger_scheduled_wait(task: dict) -> None:
        account_dir = _scheduled_task_account_dir(task)
        if account_dir is None:
            return

        mode = str(task.get("mode") or "delay").strip().lower()
        prompt = str(task.get("resume_prompt") or "请根据前文和最新上下文继续处理。").strip()
        request_id = f"chatbot-scheduled:{task.get('id') or int(time.time() * 1000)}"
        preset = get_chatbot_preset()
        base_url = preset.get("baseUrl", "")
        api_key = preset.get("apiKey", "")
        model = preset.get("model", "")
        if not base_url or not api_key or not model:
            logger.error("[chatbot-scheduler] skip task=%s reason=llm_config_incomplete", task.get("id"))
            return

        if mode == "alarm":
            history_store.append_messages(account_dir, [{
                "role": "system",
                "name": NOTICE_NAME,
                "content": prompt,
                "datetime": _now_iso(),
            }])
            transient_prompt = ""
        else:
            transient_prompt = prompt

        session = SimpleNamespace(
            account_id=account_dir.name,
            account_dir=account_dir,
        )
        _set_chatbot_state(account_dir, CHATBOT_TYPING_STATE)
        asyncio.create_task(
            _run_chatbot_agent_background(
                session,
                request_id,
                preset,
                base_url,
                api_key,
                model,
                time.perf_counter(),
                transient_system_prompt=transient_prompt,
            )
        )
        logger.info(
            "[chatbot-scheduler] triggered task=%s mode=%s account=%s",
            task.get("id"),
            mode,
            account_dir.name,
        )

    async def _process_scheduled_waits_once() -> None:
        tasks, scheduler_mtime_ns = load_scheduled_waits_with_mtime(accounts_dir)
        if not tasks:
            return

        now_ts = time.time()
        remaining: list[dict] = []
        due: list[dict] = []
        due_accounts: set[str] = set()
        for task in tasks:
            account_dir = _scheduled_task_account_dir(task)
            run_at_ts = _parse_scheduler_time(str(task.get("run_at") or ""))
            if account_dir is None or run_at_ts <= 0:
                remaining.append(task)
                continue
            if run_at_ts > now_ts:
                remaining.append(task)
                continue
            if _is_chatbot_running(account_dir):
                remaining.append(task)
                continue
            account_key = str(account_dir)
            if account_key in due_accounts:
                remaining.append(task)
                continue
            due_accounts.add(account_key)
            due.append(task)

        if len(remaining) != len(tasks):
            if not rewrite_scheduled_waits(accounts_dir, remaining, expected_mtime_ns=scheduler_mtime_ns):
                logger.info("[chatbot-scheduler] scheduler file changed during tick; retry next tick")
                return

        for task in due:
            await _trigger_scheduled_wait(task)

    async def _chatbot_scheduler_loop() -> None:
        logger.info("[chatbot-scheduler] started path=%s", Path(accounts_dir) / "chatbot_scheduler.jsonl")
        while True:
            try:
                await _process_scheduled_waits_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.exception("[chatbot-scheduler] tick failed: %s", exc)
            await asyncio.sleep(1)

    async def _start_chatbot_scheduler() -> None:
        global CHATBOT_SCHEDULER_TASK
        if CHATBOT_SCHEDULER_TASK is None or CHATBOT_SCHEDULER_TASK.done():
            CHATBOT_SCHEDULER_TASK = asyncio.create_task(_chatbot_scheduler_loop())

    async def _stop_chatbot_scheduler() -> None:
        global CHATBOT_SCHEDULER_TASK
        if CHATBOT_SCHEDULER_TASK and not CHATBOT_SCHEDULER_TASK.done():
            CHATBOT_SCHEDULER_TASK.cancel()
            try:
                await CHATBOT_SCHEDULER_TASK
            except asyncio.CancelledError:
                pass
        CHATBOT_SCHEDULER_TASK = None

    app.add_event_handler("startup", _start_chatbot_scheduler)
    app.add_event_handler("shutdown", _stop_chatbot_scheduler)

    @router.post("/api/chatbot")
    async def chat_stream(request: Request, x_auth_token: Optional[str] = Header(default=None)):
        t0 = time.perf_counter()
        session = resolve_session(x_auth_token)
        payload = await request.json()
        if not isinstance(payload, dict):
            raise HTTPException(status_code=400, detail="请求体必须是 JSON 对象")

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
            _set_chatbot_state(session.account_dir, CHATBOT_TYPING_STATE)
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

        if _is_chatbot_running(session.account_dir):
            logger.info(
                "[chatbot] stage=user_message_appended_during_run request_id=%s content_preview=%s",
                request_id,
                _preview_text(content),
            )
            return JSONResponse({"status": "ok", "queued": True})

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

        _set_chatbot_state(session.account_dir, CHATBOT_TYPING_STATE)
        asyncio.create_task(_run_chatbot_agent_background(session, request_id, preset, base_url, api_key, model, t0))
        return JSONResponse({"status": "ok"})

    def _build_chatbot_state_payload(x_auth_token: Optional[str] = Header(default=None)):
        token = (x_auth_token or "").strip()
        session = resolve_session(token)
        _check_status_poll_limit(token)
        payload = {"last_update": history_store.last_update(session.account_dir)}
        state = _get_chatbot_state(session.account_dir)
        if state is not None:
            payload["state"] = state
        return payload

    @router.get("/api/chatbot/status")
    def get_chatbot_status(x_auth_token: Optional[str] = Header(default=None)):
        return _build_chatbot_state_payload(x_auth_token)

    @router.get("/api/chatbot/state")
    def get_chatbot_state(x_auth_token: Optional[str] = Header(default=None)):
        return _build_chatbot_state_payload(x_auth_token)

    app.include_router(router)
