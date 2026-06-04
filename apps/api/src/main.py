import os
import json
import asyncio
import time
import shutil
import base64
import re
import uuid
import datetime
import hashlib
import logging
from typing import List, Optional, Dict, Tuple
from threading import Lock
from fastapi import FastAPI, Request, HTTPException, UploadFile, File, Form, Header, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from dotenv import load_dotenv
from pathlib import Path
import sys

# 路径处理
ROOT_DIR = Path(__file__).parent.parent.parent.parent
sys.path.append(str(ROOT_DIR))

from packages.core.input_adapter import parse_uploaded_files, adapt_to_dataset_bundle
from packages.agents.core.models import RawFile
from packages.agents.core.errors import PipelineAbortedError
from packages.agents.registry import create_pipeline
from packages.agents.analysis_params import wash_analysis_params, validate_analysis_params
from packages.auth import generate_user_key, hash_user_key, mask_user_key, RegisterRateLimiter
from packages.price_recommendation.models import (
    DEFAULT_CANDIDATE_COUNT,
    MAX_CANDIDATE_COUNT,
    PRICE_RECOMMENDATION_TASK_TYPE,
    PRICE_SUPPORTED_EXTENSIONS,
)
from packages.price_recommendation.service import (
    read_price_service_result,
    run_price_precheck,
    run_price_workflow,
)
from apps.api.src.chatbot_service import register_chatbot_routes

load_dotenv()

app = FastAPI(title="福州门店 AI 分析系统")
logger = logging.getLogger(__name__)

# 文件日志（持久化，不依赖 journalctl）—— 延时到 STORAGE_DIR 可用后
_LOG_FILE_HANDLER = None
_LOG_DIR_INIT = False

def _ensure_file_log():
    global _LOG_FILE_HANDLER, _LOG_DIR_INIT
    if _LOG_DIR_INIT:
        return
    _LOG_DIR_INIT = True
    try:
        log_dir = STORAGE_DIR / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        _fh = logging.FileHandler(str(log_dir / "app.log"), encoding="utf-8")
        _fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
        logger.addHandler(_fh)
    except Exception:
        pass

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

STORAGE_DIR = ROOT_DIR / "storage"
_ensure_file_log()
ACCOUNTS_DIR = STORAGE_DIR / "accounts"
LEGACY_ACCOUNT_KEY = os.getenv("LEGACY_USER_KEY", "fzt_legacy_local")
MAX_UPLOAD_FILE_SIZE = 100 * 1024 * 1024
MAX_UPLOAD_FILE_SIZE_LABEL = f"{MAX_UPLOAD_FILE_SIZE // (1024 * 1024)}MB"
DEFAULT_REASONING_EFFORT = "medium"
REASONING_EFFORT_OPTIONS = {"low", "medium", "high"}
LLM_PRESETS_FILE = STORAGE_DIR / "llm_presets.json"
LLM_PRESETS_LOCK = Lock()
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "").strip()
PRESET_SECRET = os.getenv("LLM_PRESET_SECRET", "").strip()

GLOBAL_AGENT_PIPELINE = "custom"


def normalize_reasoning_effort(value: Optional[str]) -> str:
    if not isinstance(value, str):
        return DEFAULT_REASONING_EFFORT
    effort = value.strip().lower()
    return effort if effort in REASONING_EFFORT_OPTIONS else DEFAULT_REASONING_EFFORT


def normalize_cost_tier(value: Optional[str]) -> str:
    return normalize_reasoning_effort(value)


def _default_llm_presets() -> dict:
    base_url = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1").strip()
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    model = os.getenv("OPENAI_MODEL", "gpt-5.4-mini").strip()
    
    def _sub_config(effort):
        return {
            "baseUrl": base_url,
            "apiKey": api_key,
            "model": model,
            "reasoningEffort": effort
        }
        
    return {
        "low": {"call": _sub_config("low"), "fastcall": _sub_config("low")},
        "medium": {"call": _sub_config("medium"), "fastcall": _sub_config("medium")},
        "high": {"call": _sub_config("high"), "fastcall": _sub_config("high")},
        "chatbot": {
            "baseUrl": base_url,
            "apiKey": api_key,
            "model": model,
        },
    }


def _mask_sensitive_text(text: str) -> str:
    if not isinstance(text, str):
        return str(text)
    masked = re.sub(r"sk-[A-Za-z0-9\-_]{8,}", "sk-***", text)
    masked = re.sub(
        r"(?i)(api[_-]?key\s*[:=]\s*)([^\s,;]+)",
        lambda m: f"{m.group(1)}***",
        masked
    )
    return masked


def _encrypt_text(raw: str) -> str:
    text = (raw or "").encode("utf-8")
    if not text:
        return ""
    key_source = PRESET_SECRET or "fzt-default-preset-secret"
    key = hashlib.sha256(key_source.encode("utf-8")).digest()
    encrypted = bytes(b ^ key[i % len(key)] for i, b in enumerate(text))
    return base64.b64encode(encrypted).decode("utf-8")


def _decrypt_text(raw: str) -> str:
    if not isinstance(raw, str) or not raw:
        return ""
    try:
        encrypted = base64.b64decode(raw.encode("utf-8"))
    except Exception:
        return ""
    key_source = PRESET_SECRET or "fzt-default-preset-secret"
    key = hashlib.sha256(key_source.encode("utf-8")).digest()
    decrypted = bytes(b ^ key[i % len(key)] for i, b in enumerate(encrypted))
    try:
        return decrypted.decode("utf-8")
    except Exception:
        return ""


def _sanitize_sub_item(raw_sub: Optional[dict], fallback_sub: dict) -> dict:
    data = raw_sub if isinstance(raw_sub, dict) else {}
    base_url = data.get("baseUrl", fallback_sub.get("baseUrl", ""))
    encoded_key = data.get("apiKeyEnc")
    if isinstance(encoded_key, str):
        api_key = _decrypt_text(encoded_key)
    else:
        api_key = data.get("apiKey", fallback_sub.get("apiKey", ""))
    model = data.get("model", fallback_sub.get("model", ""))
    reasoning_effort = data.get("reasoningEffort", fallback_sub.get("reasoningEffort", ""))
    return {
        "baseUrl": base_url.strip() if isinstance(base_url, str) else str(fallback_sub.get("baseUrl", "")),
        "apiKey": api_key.strip() if isinstance(api_key, str) else str(fallback_sub.get("apiKey", "")),
        "model": model.strip() if isinstance(model, str) else str(fallback_sub.get("model", "")),
        "reasoningEffort": reasoning_effort.strip() if isinstance(reasoning_effort, str) else str(fallback_sub.get("reasoningEffort", ""))
    }


def _sanitize_preset_item(raw: Optional[dict], fallback: dict) -> dict:
    data = raw if isinstance(raw, dict) else {}
    if "call" in data or "fastcall" in data:
        return {
            "call": _sanitize_sub_item(data.get("call"), fallback.get("call", fallback)),
            "fastcall": _sanitize_sub_item(data.get("fastcall"), fallback.get("fastcall", fallback))
        }
    else:
        # Legacy single-config format: copy to both call and fastcall
        sanitized = _sanitize_sub_item(data, fallback.get("call", fallback))
        return {
            "call": sanitized,
            "fastcall": sanitized.copy()
        }


def _sanitize_chatbot_preset(raw: Optional[dict], fallback: dict) -> dict:
    data = raw if isinstance(raw, dict) else {}
    base_url = data.get("baseUrl", fallback.get("baseUrl", ""))
    api_key = data.get("apiKeyEnc")
    if isinstance(api_key, str):
        api_key = _decrypt_text(api_key)
    else:
        api_key = data.get("apiKey", fallback.get("apiKey", ""))
    model = data.get("model", fallback.get("model", ""))
    return {
        "baseUrl": base_url.strip() if isinstance(base_url, str) else str(fallback.get("baseUrl", "")),
        "apiKey": api_key.strip() if isinstance(api_key, str) else str(fallback.get("apiKey", "")),
        "model": model.strip() if isinstance(model, str) else str(fallback.get("model", "")),
    }


def _load_llm_presets() -> dict:
    defaults = _default_llm_presets()
    if LLM_PRESETS_FILE.exists():
        try:
            payload = json.loads(LLM_PRESETS_FILE.read_text(encoding="utf-8"))
            source = payload.get("presets") if isinstance(payload, dict) and "presets" in payload else payload
            if isinstance(source, dict):
                for effort in REASONING_EFFORT_OPTIONS:
                    defaults[effort] = _sanitize_preset_item(source.get(effort), defaults[effort])
                defaults["chatbot"] = _sanitize_chatbot_preset(source.get("chatbot"), defaults["chatbot"])
        except Exception:
            pass
    return defaults


GLOBAL_LLM_PRESETS = _load_llm_presets()


class TaskAbortedError(Exception):
    pass


def get_global_api_key() -> str:
    with LLM_PRESETS_LOCK:
        medium_preset = GLOBAL_LLM_PRESETS["medium"]
        if "call" in medium_preset:
            return medium_preset["call"]["apiKey"]
        return medium_preset.get("apiKey", "")


def set_global_api_key(raw_key: str):
    key_value = (raw_key or "").strip()
    with LLM_PRESETS_LOCK:
        for effort in REASONING_EFFORT_OPTIONS:
            preset = GLOBAL_LLM_PRESETS[effort]
            if "call" in preset:
                preset["call"]["apiKey"] = key_value
            if "fastcall" in preset:
                preset["fastcall"]["apiKey"] = key_value
        chatbot_preset = GLOBAL_LLM_PRESETS.get("chatbot")
        if isinstance(chatbot_preset, dict):
            chatbot_preset["apiKey"] = key_value
        save_llm_presets_locked()


def get_llm_preset(reasoning_effort: Optional[str]) -> dict:
    effort = normalize_reasoning_effort(reasoning_effort)
    with LLM_PRESETS_LOCK:
        preset = GLOBAL_LLM_PRESETS.get(effort, GLOBAL_LLM_PRESETS[DEFAULT_REASONING_EFFORT]).copy()
    
    # Expose root-level fields mapping to 'call' for backward compatibility
    call_config = preset.get("call", {})
    preset["baseUrl"] = call_config.get("baseUrl", "")
    preset["apiKey"] = call_config.get("apiKey", "")
    preset["model"] = call_config.get("model", "")
    preset["reasoningEffort"] = call_config.get("reasoningEffort", effort)
    return preset


def get_chatbot_preset() -> dict:
    with LLM_PRESETS_LOCK:
        preset = GLOBAL_LLM_PRESETS.get("chatbot", {}).copy()
    return {
        "baseUrl": preset.get("baseUrl", ""),
        "apiKey": preset.get("apiKey", ""),
        "model": preset.get("model", ""),
    }


def get_all_llm_presets() -> dict:
    with LLM_PRESETS_LOCK:
        return {k: v.copy() for k, v in GLOBAL_LLM_PRESETS.items()}


def save_llm_presets_locked():
    LLM_PRESETS_FILE.parent.mkdir(parents=True, exist_ok=True)
    safe_presets = {}
    for effort, preset in GLOBAL_LLM_PRESETS.items():
        if effort in REASONING_EFFORT_OPTIONS:
            call_config = preset.get("call", {})
            fast_config = preset.get("fastcall", {})
            safe_presets[effort] = {
                "call": {
                    "baseUrl": call_config.get("baseUrl", ""),
                    "apiKeyEnc": _encrypt_text(call_config.get("apiKey", "")),
                    "model": call_config.get("model", ""),
                    "reasoningEffort": call_config.get("reasoningEffort", effort)
                },
                "fastcall": {
                    "baseUrl": fast_config.get("baseUrl", ""),
                    "apiKeyEnc": _encrypt_text(fast_config.get("apiKey", "")),
                    "model": fast_config.get("model", ""),
                    "reasoningEffort": fast_config.get("reasoningEffort", effort)
                }
            }
        elif effort == "chatbot":
            safe_presets["chatbot"] = {
                "baseUrl": preset.get("baseUrl", ""),
                "apiKeyEnc": _encrypt_text(preset.get("apiKey", "")),
                "model": preset.get("model", ""),
            }
    LLM_PRESETS_FILE.write_text(
        json.dumps({"presets": safe_presets}, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )


def update_llm_presets(raw_presets: dict):
    if not isinstance(raw_presets, dict):
        raise HTTPException(status_code=400, detail="presets 必须是对象")
    with LLM_PRESETS_LOCK:
        next_presets = {k: v.copy() for k, v in GLOBAL_LLM_PRESETS.items()}
        for effort in REASONING_EFFORT_OPTIONS:
            if effort in raw_presets:
                next_presets[effort] = _sanitize_preset_item(raw_presets.get(effort), next_presets[effort])
        if "chatbot" in raw_presets:
            next_presets["chatbot"] = _sanitize_chatbot_preset(raw_presets.get("chatbot"), next_presets.get("chatbot", {}))
        GLOBAL_LLM_PRESETS.clear()
        GLOBAL_LLM_PRESETS.update(next_presets)
        save_llm_presets_locked()


def require_admin_authorization(x_admin_token: Optional[str]):
    return  # 内部工具，不做管理员鉴权


# ── 账号 / Run 目录辅助 ──

def _make_account_slug(user_key: str, key_hash: str) -> str:
    prefix = user_key[:10] if len(user_key) >= 10 else user_key
    return f"{prefix}_{key_hash[:6]}"


def _make_run_id() -> str:
    ts = datetime.datetime.now().strftime("%Y%m%dT%H%M%S")
    short = uuid.uuid4().hex[:6]
    return f"{ts}_{short}"


def _ensure_run_dir(session: "SessionState") -> Path:
    if not session.run_id:
        session.run_id = _make_run_id()
        session.run_dir = session.account_dir / "runs" / session.task_type / session.run_id
    session.run_dir.mkdir(parents=True, exist_ok=True)
    (session.run_dir / "workspace").mkdir(parents=True, exist_ok=True)
    return session.run_dir


def _save_session_json(session: "SessionState"):
    run_dir = session.run_dir
    if not run_dir:
        return
    run_dir.mkdir(parents=True, exist_ok=True)
    data = {
        "taskType": session.task_type,
        "status": session.status,
        "errorMessage": session.error_message,
        "result": session.result,
        "fullResult": session.full_result,
    }
    try:
        (run_dir / "session.json").write_text(
            json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass


def _save_latest_run_json(session: "SessionState"):
    if not session.run_id:
        return
    try:
        (session.account_dir / "latest_run.json").write_text(json.dumps({
            "runId": session.run_id,
            "createdAt": datetime.datetime.now().isoformat(),
        }, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass


def _save_account_json(account_dir: Path, user_key: str, key_hash: str):
    path = account_dir / "account.json"
    now = datetime.datetime.now().isoformat()
    if path.exists():
        try:
            existing = json.loads(path.read_text(encoding="utf-8"))
            existing["lastSeenAt"] = now
            path.write_text(json.dumps(existing, ensure_ascii=False, indent=2), encoding="utf-8")
            return
        except Exception:
            pass
    path.write_text(json.dumps({
        "keyHash": key_hash,
        "keyMask": mask_user_key(user_key),
        "createdAt": now,
        "lastSeenAt": now,
    }, ensure_ascii=False, indent=2), encoding="utf-8")


SERVER_SECRET_KEY = os.getenv("SERVER_SECRET_KEY", "fzt_default_secret_key_123456").strip()

def generate_share_sign(run_id: str) -> str:
    h = hashlib.sha256(f"{run_id}:{SERVER_SECRET_KEY}".encode("utf-8"))
    return h.hexdigest()[:16]

def _load_runs(account_dir: Path) -> list:
    runs_file = account_dir / "runs.json"
    if runs_file.exists():
        try:
            data = json.loads(runs_file.read_text(encoding="utf-8"))
            if isinstance(data, list):
                return data
        except Exception:
            pass
    return []

def _save_runs(account_dir: Path, runs: list):
    runs_file = account_dir / "runs.json"
    try:
        runs_file.write_text(json.dumps(runs, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass

def _add_run_to_meta(session: "SessionState", file_names: list[str]):
    runs = _load_runs(session.account_dir)
    if any(r.get("runId") == session.run_id for r in runs):
        return
    new_run = {
        "runId": session.run_id,
        "taskType": session.task_type,
        "createdAt": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "status": session.status,
        "fileNames": file_names,
        "isPublic": False
    }
    runs.insert(0, new_run)
    _save_runs(session.account_dir, runs)

def _update_run_status_in_meta(session: "SessionState", status: str):
    if not session.run_id:
        return
    runs = _load_runs(session.account_dir)
    updated = False
    for r in runs:
        if r.get("runId") == session.run_id:
            r["status"] = status
            updated = True
            break
    if updated:
        _save_runs(session.account_dir, runs)


class SessionState:
    def __init__(self, user_key: str, key_hash: str, account_slug: str, account_dir: Path, config: dict):
        self.user_key = user_key
        self.key_hash = key_hash
        self.account_slug = account_slug
        self.account_dir = account_dir
        self.cache_dir = account_dir / "cache"
        self.status = "idle"  # idle, queued, running, completed, error, aborted, interrupted
        self.error_message = ""
        self.logs = []
        self.result = None      # 精简报告 (JSON string)
        self.full_result = None # 完整报告 (Markdown string)
        self.config = config
        self.force_stop = False
        self.run_id: Optional[str] = None
        self.run_dir: Optional[Path] = None
        self.runtime_lock = Lock()
        self.event_lock = Lock()
        self._pipeline_task: Optional[asyncio.Task] = None
        self.task_type = "diagnosis"

    @property
    def logs_path(self) -> Path:
        if self.run_dir:
            return self.run_dir / "logs.jsonl"
        return self.account_dir / "latest_logs.jsonl"

    @property
    def report_path(self) -> Path:
        if self.run_dir:
            return self.run_dir / "latest_report.md"
        return self.account_dir / "latest_report.md"


class SessionManager:
    def __init__(self, base_dir: Path):
        self.base_dir = base_dir
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self._sessions: Dict[Tuple[str, str], SessionState] = {}
        self._lock = Lock()

    def _default_config(self) -> dict:
        return {
            "reasoningEffort": DEFAULT_REASONING_EFFORT
        }

    def _load_profile(self, account_dir: Path) -> dict:
        return self._default_config()

    def _account_dir(self, user_key: str) -> tuple[str, str, Path]:
        key_hash = hash_user_key(user_key)
        slug = _make_account_slug(user_key, key_hash)
        return key_hash, slug, self.base_dir / slug

    def _try_load_session(self, account_dir: Path, task_type: str = "diagnosis") -> dict | None:
        target_dir = account_dir / "runs" / task_type
        state = None
        run_id = None
        run_dir = None

        # 1. 尝试通过扫描 runs/{task_type}/ 下的最新子目录
        if target_dir.exists() and target_dir.is_dir():
            try:
                subdirs = sorted(
                    [d for d in target_dir.iterdir() if d.is_dir() and not d.name.startswith(".")],
                    key=lambda x: x.name,
                    reverse=True
                )
                for subdir in subdirs:
                    session_path = subdir / "session.json"
                    if session_path.exists():
                        try:
                            state = json.loads(session_path.read_text(encoding="utf-8"))
                            run_id = subdir.name
                            run_dir = subdir
                            break
                        except Exception:
                            continue
            except Exception:
                pass

        # 2. 向后兼容：如果在子目录下未找到，且为诊断分析任务，则退化到读取 latest_run.json 并在根目录 runs 下查找
        if not state and task_type == "diagnosis":
            latest = account_dir / "latest_run.json"
            if latest.exists():
                try:
                    meta = json.loads(latest.read_text(encoding="utf-8"))
                    run_id = meta.get("runId")
                    if run_id:
                        run_dir = account_dir / "runs" / run_id
                        session_path = run_dir / "session.json"
                        if session_path.exists():
                            state = json.loads(session_path.read_text(encoding="utf-8"))
                except Exception:
                    pass

        if not state or not run_id or not run_dir:
            return None

        # 3. 校验并修复被中断的未决任务状态
        if state.get("status") in ("running", "queued"):
            state["status"] = "interrupted"
            state["errorMessage"] = "服务重启导致任务中断，请重新提交分析。"
            try:
                (run_dir / "session.json").write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
            except Exception:
                pass

        state["_runId"] = run_id
        state["_runDir"] = str(run_dir)
        return state

    def get_session(self, user_key: str, create_if_missing: bool = True, task_type: str = "diagnosis") -> SessionState:
        key_hash, slug, account_dir = self._account_dir(user_key)
        task_type = (task_type or "diagnosis").strip() or "diagnosis"
        cache_key = (key_hash, task_type)
        with self._lock:
            existing = self._sessions.get(cache_key)
            if existing:
                _save_account_json(account_dir, user_key, key_hash)
                return existing

            account_json_path = account_dir / "account.json"
            if not create_if_missing and not account_json_path.exists():
                raise KeyError("invalid_key")

            account_dir.mkdir(parents=True, exist_ok=True)
            _save_account_json(account_dir, user_key, key_hash)
            cfg = self._load_profile(account_dir)
            session = SessionState(user_key=user_key, key_hash=key_hash, account_slug=slug, account_dir=account_dir, config=cfg)
            session.task_type = task_type

            saved = self._try_load_session(account_dir, task_type=task_type)
            if saved:
                session.task_type = saved.get("taskType") or task_type
                session.status = saved.get("status", "idle")
                session.error_message = saved.get("errorMessage", "")
                session.result = saved.get("result")
                session.full_result = saved.get("fullResult")
                session.run_id = saved.get("_runId")
                rd = saved.get("_runDir")
                if rd:
                    session.run_dir = Path(rd)
                    session.logs = _load_logs_from_file(session)

            self._sessions[cache_key] = session
            return session

    def get_legacy_session(self) -> SessionState:
        return self.get_session(LEGACY_ACCOUNT_KEY, create_if_missing=True)

    def save_profile(self, session: SessionState):
        session.account_dir.mkdir(parents=True, exist_ok=True)
        session.profile_path.write_text(
            json.dumps({
                "reasoningEffort": normalize_reasoning_effort(session.config.get("reasoningEffort"))
            }, ensure_ascii=False, indent=2),
            encoding="utf-8"
        )

    def drop_session(self, key_hash: str, task_type: Optional[str] = None):
        with self._lock:
            if task_type:
                self._sessions.pop((key_hash, task_type), None)
                return
            for cache_key in [k for k in self._sessions if k[0] == key_hash]:
                self._sessions.pop(cache_key, None)


session_manager = SessionManager(ACCOUNTS_DIR)
register_limiter = RegisterRateLimiter(window_seconds=180, max_requests=5)


def _now_time():
    return time.strftime("%H:%M:%S")


def _ensure_session_dirs(session: SessionState):
    dirs = [session.account_dir, session.cache_dir]
    if session.run_dir:
        dirs.append(session.run_dir)
    for d in dirs:
        d.mkdir(parents=True, exist_ok=True)


def persist_latest_logs(session: SessionState):
    _ensure_session_dirs(session)
    try:
        event = session.logs[-1] if session.logs else None
        if not event:
            return
        with session.logs_path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(event, ensure_ascii=False) + "\n")
    except Exception:
        pass


def emit_event(session: SessionState, event_type: str, payload: dict):
    event = {"type": event_type, "time": _now_time(), **payload}
    with session.event_lock:
        session.logs.append(event)
        persist_latest_logs(session)
    return event


def reset_events(session: SessionState):
    # 不清空文件（保留历史日志供页面刷新后加载），仅清空内存列表
    session.logs = []
    # 文件里追加一个 reset 标记
    with session.event_lock:
        event = {"type": "reset", "time": _now_time()}
        session.logs.append(event)
        persist_latest_logs(session)
        # 不 overwrite 文件，只追加一行日志


def add_status(session: SessionState, node_id: str, status: str):
    emit_event(session, "status", {"nodeId": node_id, "status": status})


def add_log(session: SessionState, node_id: str, message):
    """支持 str 或 dict 格式的日志消息。

    dict 格式示例:
      {"level": "info", "message": "...", "step": {...}, "error_details": "..."}
    level 可选: debug / info / status / error（默认 info）
    """
    if isinstance(message, dict):
        level = message.get("level", "info")
        raw_msg = message.get("message", "")
        safe_msg = _mask_sensitive_text(raw_msg)
        payload = {"nodeId": node_id, "level": level, "message": safe_msg}
        if "step" in message:
            payload["step"] = message["step"]
        if "error_details" in message:
            payload["error_details"] = _mask_sensitive_text(str(message["error_details"]))
        if "progress" in message:
            payload["progress"] = message["progress"]
        if "terminal" in message:
            payload["terminal"] = bool(message["terminal"])
    else:
        level = "info"
        safe_msg = _mask_sensitive_text(str(message))
        payload = {"nodeId": node_id, "level": level, "message": safe_msg}

    log_entry = emit_event(session, "log", payload)
    hash_prefix = session.key_hash[:8]
    logger.info("[%s] %s %s [%s]", log_entry["time"], hash_prefix, node_id, level)


def add_progress(session: SessionState, node_id: str, current: int, total: int):
    emit_event(session, "progress", {"nodeId": node_id, "current": current, "total": total})


def add_tally(session: SessionState, node_id: str, tally: dict):
    emit_event(session, "tally", {"nodeId": node_id, "tally": tally})


def ensure_not_stopped(session: SessionState):
    with session.runtime_lock:
        is_stopped = session.force_stop or session.status == "aborted"
    if is_stopped:
        raise TaskAbortedError("任务被用户强制终止。")



def save_latest_report(session: SessionState):
    _ensure_session_dirs(session)
    if isinstance(session.full_result, str) and session.full_result:
        session.report_path.write_text(session.full_result, encoding="utf-8")


def sanitize_settings(raw: Optional[dict]) -> dict:
    if not isinstance(raw, dict):
        return {}
    return {
        "reasoningEffort": normalize_reasoning_effort(raw.get("reasoningEffort") or raw.get("reasoning_effort"))
    }


def resolve_session(x_fzt_key: Optional[str], task_type: str = "diagnosis") -> SessionState:
    key = (x_fzt_key or "").strip()
    if not key:
        raise HTTPException(status_code=401, detail="Authentication required")
    try:
        return session_manager.get_session(key, create_if_missing=False, task_type=task_type)
    except KeyError:
        raise HTTPException(status_code=401, detail="Invalid or expired key")


def _normalize_candidate_count(value: Optional[int]) -> int:
    if not isinstance(value, int):
        return DEFAULT_CANDIDATE_COUNT
    return min(max(value, 1), MAX_CANDIDATE_COUNT)


async def _read_uploaded_files(files: List[UploadFile], supported_extensions: set[str]) -> list[dict]:
    decoded_files: list[dict] = []
    for uploaded_file in files:
        filename = uploaded_file.filename or "unnamed"
        ext = os.path.splitext(filename)[1].lower()
        if ext not in supported_extensions:
            raise HTTPException(status_code=400, detail=f"不支持的文件格式: {ext or '未知'}，支持: {', '.join(sorted(supported_extensions))}")
        raw = await uploaded_file.read()
        if len(raw) > MAX_UPLOAD_FILE_SIZE:
            raise HTTPException(status_code=400, detail=f"文件过大(>{MAX_UPLOAD_FILE_SIZE_LABEL}): {filename}")
        decoded_files.append({"name": filename, "bytes": raw})
    return decoded_files



@app.post("/api/auth/register")
async def auth_register(request: Request):
    identity = request.client.host if request.client else "unknown"
    if not register_limiter.allow(identity):
        raise HTTPException(status_code=429, detail="注册过于频繁，请稍后再试")

    try:
        data = await request.json()
    except Exception:
        data = {}

    user_key = generate_user_key()
    session = session_manager.get_session(user_key, create_if_missing=True)
    _ensure_session_dirs(session)

    api_key = data.get("apiKey") or data.get("openaiKey")
    if isinstance(api_key, str) and api_key.strip():
        set_global_api_key(api_key)

    return {"userKey": user_key, "status": "ok"}


@app.post("/api/auth/verify")
def auth_verify(x_fzt_key: Optional[str] = Header(default=None)):
    session = resolve_session(x_fzt_key)
    return {
        "status": "ok",
        "userKey": mask_user_key(session.user_key)
    }


@app.get("/api/config")
def get_config(x_fzt_key: Optional[str] = Header(default=None)):
    session = resolve_session(x_fzt_key)
    effort = normalize_reasoning_effort(session.config.get("reasoningEffort"))
    preset = get_llm_preset(effort)
    return {
        "reasoningEffort": effort,
        "availableReasoningEfforts": sorted(REASONING_EFFORT_OPTIONS),
        "baseUrl": preset.get("baseUrl", ""),
        "model": preset.get("model", ""),
        "hasKey": bool(preset.get("apiKey")),
        "call": {
            "baseUrl": preset.get("call", {}).get("baseUrl", ""),
            "model": preset.get("call", {}).get("model", ""),
            "hasKey": bool(preset.get("call", {}).get("apiKey"))
        },
        "fastcall": {
            "baseUrl": preset.get("fastcall", {}).get("baseUrl", ""),
            "model": preset.get("fastcall", {}).get("model", ""),
            "hasKey": bool(preset.get("fastcall", {}).get("apiKey"))
        }
    }


@app.get("/api/admin/llm-presets")
def get_admin_llm_presets(x_admin_token: Optional[str] = Header(default=None)):
    require_admin_authorization(x_admin_token)
    return {
        "status": "ok",
        "presets": get_all_llm_presets()
    }


@app.post("/api/admin/llm-presets")
async def save_admin_llm_presets(request: Request, x_admin_token: Optional[str] = Header(default=None)):
    require_admin_authorization(x_admin_token)
    payload = await request.json()
    source = payload.get("presets") if isinstance(payload, dict) and "presets" in payload else payload
    update_llm_presets(source)
    return {
        "status": "ok",
        "presets": get_all_llm_presets()
    }

@app.get("/api/status")
def get_status(
    run_id: Optional[str] = Query(None),
    x_fzt_key: Optional[str] = Header(default=None)
):
    session = resolve_session(x_fzt_key)
    
    target_run_id = run_id or session.run_id
    if not target_run_id:
        return {
            "status": "idle",
            "errorMessage": "",
            "result": None,
            "fullResult": None,
            "runId": None
        }
        
    run_dir = session.account_dir / "runs" / session.task_type / target_run_id
        
    status = session.status if target_run_id == session.run_id else "completed"
    error_msg = session.error_message if target_run_id == session.run_id else ""
    result_str = session.result if target_run_id == session.run_id else None
    full_str = session.full_result if target_run_id == session.run_id else None
    
    if run_dir.exists():
        session_json_path = run_dir / "session.json"
        if session_json_path.exists():
            try:
                sdata = json.loads(session_json_path.read_text(encoding="utf-8"))
                status = sdata.get("status", "completed")
                error_msg = sdata.get("errorMessage", "")
                result_str = sdata.get("result")
                full_str = sdata.get("fullResult")
            except Exception:
                pass
                
        ws_dir = run_dir / "workspace"
        short_path = ws_dir / "output" / "summary_short.json"
        full_path = ws_dir / "output" / "summary.md"
        if short_path.exists():
            result_str = short_path.read_text(encoding="utf-8")
        if full_path.exists():
            full_str = full_path.read_text(encoding="utf-8")
            
    if status == "aborted":
        safe_error = "任务被用户强行终止。"
    elif status == "error":
        safe_error = f"任务执行失败: {error_msg}" if error_msg else "任务执行失败，请在后台监控流查看 system 节点日志"
    else:
        safe_error = ""
        
    if isinstance(result_str, (dict, list)):
        result_str = json.dumps(result_str, ensure_ascii=False)
        
    return {
        "status": status,
        "errorMessage": safe_error,
        "result": result_str,
        "fullResult": full_str,
        "runId": target_run_id
    }


@app.get("/api/logs")
def get_logs(
    run_id: Optional[str] = Query(None),
    x_fzt_key: Optional[str] = Header(default=None)
):
    session = resolve_session(x_fzt_key)  # 确保 session 缓存存在
    return {"logs": _load_logs_from_file(session, run_id)}


def _load_logs_from_file(session, run_id: Optional[str] = None) -> list:
    """强制从文件加载日志，不依赖 session.logs 的内存状态。"""
    try:
        run_dir = session.run_dir
        if run_id:
            run_dir = session.account_dir / "runs" / session.task_type / run_id
        if run_dir:
            jsonl_path = run_dir / "logs.jsonl"
            if jsonl_path.exists():
                lines = [line.strip() for line in jsonl_path.read_text(encoding="utf-8").splitlines() if line.strip()]
                return [json.loads(line) for line in lines]

            legacy_path = run_dir / "logs.json"
            if legacy_path.exists():
                loaded = json.loads(legacy_path.read_text(encoding="utf-8"))
                if isinstance(loaded, list):
                    return loaded
                if isinstance(loaded, dict):
                    return [loaded]
    except Exception:
        pass
    return []


async def sse_generator(session: SessionState, run_id: Optional[str] = None):
    """SSE 日志推送，适配前端 EventSource('/api/stream')"""
    session.logs = _load_logs_from_file(session, run_id)

    target_status = session.status
    if run_id:
        try:
            run_dir = session.account_dir / "runs" / session.task_type / run_id
            session_json_path = run_dir / "session.json"
            if session_json_path.exists():
                sdata = json.loads(session_json_path.read_text(encoding="utf-8"))
                target_status = sdata.get("status", "completed")
            else:
                target_status = "completed"
        except Exception:
            target_status = "completed"

    last_idx = 0
    yield f"data: {json.dumps({'type': 'reset', 'time': _now_time()})}\n\n"

    while True:
        if last_idx > len(session.logs):
            last_idx = 0
        if last_idx < len(session.logs):
            for i in range(last_idx, len(session.logs)):
                yield f"data: {json.dumps(session.logs[i], ensure_ascii=False)}\n\n"
            last_idx = len(session.logs)

        if target_status in ("completed", "error", "aborted") and last_idx >= len(session.logs):
            yield f"data: {json.dumps({'type': 'done', 'time': _now_time()})}\n\n"
            break

        try:
            await asyncio.sleep(0.5)
        except asyncio.CancelledError:
            break


@app.get("/api/stream")
async def stream(
    run_id: Optional[str] = Query(None),
    x_fzt_key: Optional[str] = Query(default=None, alias="x-fzt-key")
):
    session = resolve_session(x_fzt_key)
    return StreamingResponse(sse_generator(session, run_id), media_type="text/event-stream")



async def run_pipeline_task(session: SessionState, pipeline_name: str, active_preset: dict, bundle):
    """统一管线执行入口：当前固定为 custom 管线。"""
    with session.runtime_lock:
        if session.force_stop or session.status == "aborted":
            return
        session.status = "running"
    _update_run_status_in_meta(session, "running")
    reset_events(session)
    emit_event(session, "pipeline", {"pipeline": pipeline_name})

    def check_aborted():
        try:
            ensure_not_stopped(session)
        except TaskAbortedError as e:
            raise PipelineAbortedError(str(e))

    ws_dir = session.run_dir / "workspace" if session.run_dir else None
    analysis_params = ""
    params_path = session.account_dir / "analysis_params.json"
    if params_path.exists():
        try:
            data = json.loads(params_path.read_text(encoding="utf-8"))
            raw = data.get("analysis_params", "")
            analysis_params = wash_analysis_params(raw)
        except Exception:
            pass
    pipe = create_pipeline(pipeline_name, llm_preset=active_preset, check_aborted=check_aborted, workspace_dir=ws_dir, analysis_params=analysis_params)
    pipe.set_event_callbacks(
        on_status=lambda nid, st: add_status(session, nid, st),
        on_log=lambda nid, msg: add_log(session, nid, msg),
        on_progress=lambda nid, cur, tot: add_progress(session, nid, cur, tot),
        on_tally=lambda nid, t: add_tally(session, nid, t),
    )

    try:
        result = await pipe.run(bundle)
        full_text = result.full_report or f"# {pipeline_name} 管线报告\n\npipeline: {result.pipeline}\n耗时: {result.elapsed_ms:.0f}ms"
        session.full_result = full_text
        def _serialize_card(c) -> dict:
            if isinstance(c, dict):
                return {
                    "title": c.get("title", ""),
                    "explanation": c.get("explanation", ""),
                    "suggestion": c.get("suggestion", ""),
                    "evidence": c.get("evidence", ""),
                    "color": c.get("color", "green"),
                }
            return {"title": c.title, "explanation": c.explanation, "suggestion": c.suggestion, "evidence": c.evidence, "color": c.color}

        session.result = json.dumps({
            "health_status": "分析完成",
            "overview_text": f"共 {len(result.cards)} 项待关注",
            "cards": [_serialize_card(c) for c in result.cards]
        }, ensure_ascii=False)

        add_log(session, "system", "✅ 分析完成")
        with session.runtime_lock:
            if session.force_stop or session.status == "aborted":
                _save_session_json(session)
                return
            session.status = "completed"
        add_log(session, "system", {"level": "status", "message": "", "progress": 100, "terminal": True})
        _update_run_status_in_meta(session, "completed")
        save_latest_report(session)
        _save_session_json(session)

        # ── 发送 XiaoTangPush ERP 推送 ──
        try:
            from packages.core.connectors import format_xiaotang_push_payload, send_xiaotang_push_async
            add_log(session, "system", "⏳ 正在推送分析报告至商搏 ERP (XiaoTangPush)...")
            
            payload = format_xiaotang_push_payload(
                full_report=session.full_result,
                summary_short=session.result
            )
            
            res = await send_xiaotang_push_async(payload)
            retstatus = res.get("retstatus")
            retvalue = res.get("retvalue")
            
            # 兼容 retvalue 可能为字符串（如 "获取参数失败"）或者为 dict 的情况
            if isinstance(retvalue, dict):
                msg = retvalue.get("msg", "无")
                status = retvalue.get("status")
            else:
                msg = str(retvalue) if retvalue is not None else "无"
                status = None
            
            if retstatus == 1 or status == 1:
                add_log(session, "system", f"✅ ERP 报告推送成功！服务器响应: {msg}")
            else:
                add_log(session, "system", f"⚠️ ERP 报告推送未成功，业务响应: {msg}")
        except Exception as e:
            add_log(session, "system", f"❌ ERP 报告推送失败: {str(e)}")
            
    except (PipelineAbortedError, asyncio.CancelledError):
        add_log(session, "system", "⚠️ 任务被用户强制终止。")
        add_log(session, "system", {"level": "status", "message": "", "terminal": True})
        with session.runtime_lock:
            session.status = "aborted"
            session.error_message = "任务被用户强行终止。"
        _update_run_status_in_meta(session, "aborted")
        _save_session_json(session)
    except Exception as e:
        add_log(session, "system", f"管线执行失败: {str(e)}")
        add_log(session, "system", {"level": "status", "message": "", "terminal": True})
        with session.runtime_lock:
            session.status = "error"
            session.error_message = str(e)
        _update_run_status_in_meta(session, "error")
        _save_session_json(session)


async def run_price_recommendation_task(session: SessionState, decoded_files: list[dict], product_name: str, candidate_count: int):
    """Run the price recommendation workflow for a price_recommendation session."""
    with session.runtime_lock:
        if session.force_stop or session.status == "aborted":
            return
        session.status = "running"
    reset_events(session)
    emit_event(session, "workflow", {"workflow": PRICE_RECOMMENDATION_TASK_TYPE})

    def check_aborted():
        ensure_not_stopped(session)

    try:
        ws_dir = session.run_dir / "workspace" if session.run_dir else None
        if not ws_dir:
            raise RuntimeError("价格推荐 workspace 未初始化")

        active_preset = get_llm_preset(session.config.get("reasoningEffort", "high"))

        result, full_result = await asyncio.to_thread(
            run_price_workflow,
            decoded_files=decoded_files,
            product_name=product_name,
            candidate_count=candidate_count,
            workspace_dir=ws_dir,
            llm_preset=active_preset,
            emit_log=lambda nid, payload: add_log(session, nid, payload),
            check_aborted=check_aborted,
        )
        session.result = json.dumps(result, ensure_ascii=False) if isinstance(result, (dict, list)) else result
        session.full_result = full_result
        with session.runtime_lock:
            if session.force_stop or session.status == "aborted":
                _save_session_json(session)
                return
            session.status = "completed"
        _update_run_status_in_meta(session, "completed")
        _save_session_json(session)
    except (TaskAbortedError, asyncio.CancelledError):
        add_log(session, "price_done", {"level": "error", "message": "价格推荐任务被用户强制终止。", "progress": 100})
        with session.runtime_lock:
            session.status = "aborted"
            session.error_message = "任务被用户强行终止。"
        _update_run_status_in_meta(session, "aborted")
        _save_session_json(session)
    except Exception as e:
        add_log(session, "price_done", {"level": "error", "message": f"价格推荐任务失败: {str(e)}", "error_details": str(e)})
        with session.runtime_lock:
            session.status = "error"
            session.error_message = str(e)
        _update_run_status_in_meta(session, "error")
        _save_session_json(session)


@app.post("/api/price-recommendations/precheck")
async def price_recommendation_precheck(
    files: List[UploadFile] = File(...),
    productName: str = Form(...),
    x_fzt_key: Optional[str] = Header(default=None),
    reasoningEffort: Optional[str] = Form(None),
    costTier: Optional[str] = Form(None),
):
    resolve_session(x_fzt_key, task_type=PRICE_RECOMMENDATION_TASK_TYPE)
    product_name = (productName or "").strip()
    if not product_name:
        raise HTTPException(status_code=400, detail="缺少 productName")
    decoded_files = await _read_uploaded_files(files, PRICE_SUPPORTED_EXTENSIONS)
    return run_price_precheck(decoded_files, product_name)


@app.post("/api/price-recommendations")
async def start_price_recommendation(
    files: List[UploadFile] = File(...),
    productName: str = Form(...),
    x_fzt_key: Optional[str] = Header(default=None),
    reasoningEffort: Optional[str] = Form(None),
    costTier: Optional[str] = Form(None),
    candidateCount: Optional[int] = Form(None),
):
    session = resolve_session(x_fzt_key, task_type=PRICE_RECOMMENDATION_TASK_TYPE)
    product_name = (productName or "").strip()
    if not product_name:
        raise HTTPException(status_code=400, detail="缺少 productName")
    with session.runtime_lock:
        if session.status in ("queued", "running"):
            raise HTTPException(status_code=400, detail="任务正在运行中")

    decoded_files = await _read_uploaded_files(files, PRICE_SUPPORTED_EXTENSIONS)

    session.config["reasoningEffort"] = normalize_reasoning_effort(costTier or reasoningEffort or "high")
    candidate_count = _normalize_candidate_count(candidateCount)
    with session.runtime_lock:
        if session.status in ("queued", "running"):
            raise HTTPException(status_code=400, detail="任务正在运行中")
        session.status = "queued"
        session.force_stop = False
        session.error_message = ""
        session.result = None
        session.full_result = None
        session.run_id = None
        session.run_dir = None

    _ensure_run_dir(session)
    # 记录运行历史
    file_names = [df["name"] for df in decoded_files]
    try:
        _add_run_to_meta(session, file_names)
    except Exception as e:
        logger.error("Failed to record run metadata: %s", str(e))
    _save_session_json(session)
    session._pipeline_task = asyncio.create_task(
        run_price_recommendation_task(session, decoded_files, product_name, candidate_count)
    )
    return {
        "status": "started",
        "taskType": PRICE_RECOMMENDATION_TASK_TYPE,
        "workflow": PRICE_RECOMMENDATION_TASK_TYPE,
        "runId": session.run_id,
    }


@app.get("/api/price-recommendations/status")
def get_price_recommendation_status(
    run_id: Optional[str] = Query(None),
    x_fzt_key: Optional[str] = Header(default=None)
):
    session = resolve_session(x_fzt_key, task_type=PRICE_RECOMMENDATION_TASK_TYPE)
    
    target_run_id = run_id or session.run_id
    if not target_run_id:
        return {
            "status": "idle",
            "errorMessage": "",
            "result": None,
            "fullResult": None,
            "runId": None
        }

    run_dir = session.account_dir / "runs" / session.task_type / target_run_id

    status = session.status if target_run_id == session.run_id else "completed"
    error_msg = session.error_message if target_run_id == session.run_id else ""
    result = session.result if target_run_id == session.run_id else None
    full_result = session.full_result if target_run_id == session.run_id else None

    if run_dir.exists():
        session_json_path = run_dir / "session.json"
        if session_json_path.exists():
            try:
                sdata = json.loads(session_json_path.read_text(encoding="utf-8"))
                status = sdata.get("status", "completed")
                error_msg = sdata.get("errorMessage", "")
                result = sdata.get("result")
                full_result = sdata.get("fullResult")
            except Exception:
                pass
        try:
            disk_result, disk_full = read_price_service_result(run_dir)
            result = disk_result if disk_result is not None else result
            full_result = disk_full if disk_full is not None else full_result
        except Exception:
            pass

    safe_error = ""
    if status == "aborted":
        safe_error = "任务被用户强行终止。"
    elif status == "error":
        safe_error = f"任务执行失败: {error_msg}" if error_msg else "任务执行失败"

    if isinstance(result, (dict, list)):
        result = json.dumps(result, ensure_ascii=False)

    return {
        "status": status,
        "errorMessage": safe_error,
        "result": result,
        "fullResult": full_result,
        "runId": target_run_id
    }


@app.get("/api/price-recommendations/logs")
def get_price_recommendation_logs(
    run_id: Optional[str] = Query(None),
    x_fzt_key: Optional[str] = Header(default=None)
):
    session = resolve_session(x_fzt_key, task_type=PRICE_RECOMMENDATION_TASK_TYPE)
    return {"logs": _load_logs_from_file(session, run_id)}


@app.get("/api/price-recommendations/stream")
async def stream_price_recommendation(
    run_id: Optional[str] = Query(None),
    x_fzt_key: Optional[str] = Query(default=None, alias="x-fzt-key")
):
    session = resolve_session(x_fzt_key, task_type=PRICE_RECOMMENDATION_TASK_TYPE)
    return StreamingResponse(sse_generator(session, run_id), media_type="text/event-stream")


@app.post("/api/price-recommendations/stop")
def stop_price_recommendation(x_fzt_key: Optional[str] = Header(default=None)):
    session = resolve_session(x_fzt_key, task_type=PRICE_RECOMMENDATION_TASK_TYPE)
    with session.runtime_lock:
        if session.status in ("queued", "running"):
            session.force_stop = True
            session.status = "aborted"
            session.error_message = "任务被用户强行终止。"
            add_log(session, "price_done", {"level": "error", "message": "用户强制终止了价格推荐任务。", "progress": 100})
            _save_session_json(session)
            task = getattr(session, "_pipeline_task", None)
            if task and not task.done():
                task.cancel()
    return {"status": "ok"}


@app.post("/api/analyze")
async def analyze(
    files: List[UploadFile] = File(...),
    x_fzt_key: Optional[str] = Header(default=None),
    reasoningEffort: Optional[str] = Form(None),
    costTier: Optional[str] = Form(None),
):
    session = resolve_session(x_fzt_key)
    session.config["reasoningEffort"] = normalize_reasoning_effort(costTier or reasoningEffort)
    with session.runtime_lock:
        if session.status in ("queued", "running"):
            raise HTTPException(status_code=400, detail="任务正在运行中")
        session.status = "queued"
        session.force_stop = False
        session.error_message = ""
        session.result = None
        session.full_result = None
        session.run_id = None
        session.run_dir = None

    SUPPORTED_EXTENSIONS = {".json", ".xlsx", ".xls", ".csv", ".pdf", ".docx", ".doc", ".txt", ".md", ".zip", ".rar", ".7z"}
    decoded_files: list[dict] = []
    for uploaded_file in files:
        filename = uploaded_file.filename or "unnamed"
        ext = os.path.splitext(filename)[1].lower()
        if ext not in SUPPORTED_EXTENSIONS:
            raise HTTPException(status_code=400, detail=f"不支持的文件格式: {ext or '未知'}，支持: {', '.join(sorted(SUPPORTED_EXTENSIONS))}")
        raw = await uploaded_file.read()
        if len(raw) > MAX_UPLOAD_FILE_SIZE:
            raise HTTPException(status_code=400, detail=f"文件过大(>{MAX_UPLOAD_FILE_SIZE_LABEL}): {filename}")
        decoded_files.append({"name": filename, "bytes": raw})

    try:
        raw_bundle = parse_uploaded_files(decoded_files)
        bundle = adapt_to_dataset_bundle(raw_bundle)
        for df in decoded_files:
            bundle.raw_files.append(RawFile(name=df["name"], data=df["bytes"]))
    except Exception as e:
        with session.runtime_lock:
            session.status = "error"
            session.error_message = str(e)
        raise HTTPException(status_code=400, detail=f"文件解析失败: {e}")

    pipeline_name = GLOBAL_AGENT_PIPELINE
    active_preset = get_llm_preset(session.config.get("reasoningEffort", "medium"))

    # 创建 run 目录
    _ensure_run_dir(session)
    _save_session_json(session)
    _save_latest_run_json(session)
    
    # 记录运行历史
    file_names = [df["name"] for df in decoded_files]
    try:
        _add_run_to_meta(session, file_names)
    except Exception as e:
        logger.error("Failed to record run metadata: %s", str(e))

    session._pipeline_task = asyncio.create_task(
        run_pipeline_task(session, pipeline_name, active_preset, bundle)
    )
    return {"status": "started", "pipeline": pipeline_name}


@app.post("/api/stop")
def stop(x_fzt_key: Optional[str] = Header(default=None)):
    session = resolve_session(x_fzt_key)
    with session.runtime_lock:
        if session.status == "running":
            session.force_stop = True
            session.status = "aborted"
            session.error_message = "任务被用户强行终止。"
            add_log(session, "system", "⚠️ 用户强制终止了任务！")
            _save_session_json(session)
            _update_run_status_in_meta(session, "aborted")
            task = getattr(session, "_pipeline_task", None)
            if task and not task.done():
                task.cancel()
    return {"status": "ok"}


@app.get("/api/reports/download")
async def download_report_zip(
    run_id: Optional[str] = Query(None),
    x_fzt_key: Optional[str] = Query(None, alias="x-fzt-key"),
    x_fzt_key_header: Optional[str] = Header(None, alias="X-FZT-Key")
):
    key = x_fzt_key or x_fzt_key_header
    session = resolve_session(key)
    
    target_run_id = run_id or session.run_id
    if not target_run_id:
        raise HTTPException(status_code=404, detail="未找到任何运行记录")
        
    task_type = session.task_type
    runs = _load_runs(session.account_dir)
    for r in runs:
        if r.get("runId") == target_run_id:
            task_type = r.get("taskType", task_type)
            break
            
    run_dir = session.account_dir / "runs" / task_type / target_run_id
        
    if not run_dir.exists() or not run_dir.is_dir():
        raise HTTPException(status_code=404, detail=f"运行目录不存在: {target_run_id}")
        
    output_dir = run_dir / "workspace" / "output"
    if not output_dir.exists() or not output_dir.is_dir():
        raise HTTPException(status_code=404, detail="未找到分析产物目录 (workspace/output)")
        
    # 内存打包 ZIP
    import io
    import zipfile
    
    def generate_zip_bytes():
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            for file_path in output_dir.rglob("*"):
                if file_path.is_file():
                    # 写入 zip 包，保持相对路径
                    zf.write(file_path, file_path.relative_to(output_dir))
        zip_buffer.seek(0)
        return zip_buffer.getvalue()
        
    loop = asyncio.get_running_loop()
    try:
        zip_bytes = await loop.run_in_executor(None, generate_zip_bytes)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"生成压缩包失败: {str(e)}")
        
    return StreamingResponse(
        io.BytesIO(zip_bytes),
        media_type="application/zip",
        headers={"Content-Disposition": f"attachment; filename=analysis_{target_run_id}.zip"}
    )


@app.get("/api/reports")
def get_reports_list(
    x_fzt_key: Optional[str] = Header(default=None),
    request: Request = None
):
    session = resolve_session(x_fzt_key)
    runs = _load_runs(session.account_dir)
    
    # Calculate share URL and sign dynamically for each run
    origin = str(request.base_url) if request else "http://localhost:3000"
    if origin.endswith("/"):
        origin = origin[:-1]
        
    for r in runs:
        run_id = r["runId"]
        sign = generate_share_sign(run_id)
        r["shareUrl"] = f"{origin}/public.html?run_id={run_id}&sign={sign}"
        r["downloadUrl"] = f"{origin}/api/reports/public/download?run_id={run_id}&sign={sign}"
        
    return {"status": "ok", "runs": runs}


@app.post("/api/reports/{run_id}/public")
async def toggle_report_public(
    run_id: str,
    request: Request,
    x_fzt_key: Optional[str] = Header(default=None)
):
    session = resolve_session(x_fzt_key)
    body = await request.json()
    is_public = bool(body.get("public", False))
    
    runs = _load_runs(session.account_dir)
    updated = False
    for r in runs:
        if r.get("runId") == run_id:
            r["isPublic"] = is_public
            updated = True
            break
            
    if not updated:
        raise HTTPException(status_code=404, detail="未找到该分析批次记录")
        
    _save_runs(session.account_dir, runs)
    
    sign = generate_share_sign(run_id)
    origin = str(request.base_url)
    if origin.endswith("/"):
        origin = origin[:-1]
        
    share_url = f"{origin}/public.html?run_id={run_id}&sign={sign}"
    
    # Also update session.json if it exists inside the run_dir
    task_type = "diagnosis"
    for r in runs:
        if r.get("runId") == run_id:
            task_type = r.get("taskType", task_type)
            break
    run_dir = session.account_dir / "runs" / task_type / run_id
    if run_dir.exists():
        session_json_path = run_dir / "session.json"
        if session_json_path.exists():
            try:
                sdata = json.loads(session_json_path.read_text(encoding="utf-8"))
                sdata["isPublic"] = is_public
                session_json_path.write_text(json.dumps(sdata, ensure_ascii=False, indent=2), encoding="utf-8")
            except Exception:
                pass
                
    download_url = f"{origin}/api/reports/public/download?run_id={run_id}&sign={sign}"
    return {
        "status": "ok",
        "isPublic": is_public,
        "shareUrl": share_url,
        "downloadUrl": download_url
    }


@app.delete("/api/reports/{run_id}")
def delete_report(
    run_id: str,
    x_fzt_key: Optional[str] = Header(default=None)
):
    session = resolve_session(x_fzt_key)
    runs = _load_runs(session.account_dir)
    
    # Find the run
    target_run = None
    for r in runs:
        if r.get("runId") == run_id:
            target_run = r
            break
            
    if not target_run:
        raise HTTPException(status_code=404, detail="未找到该分析批次记录")
        
    # Remove from metadata
    runs.remove(target_run)
    _save_runs(session.account_dir, runs)
    
    # Delete physically
    task_type = target_run.get("taskType", "diagnosis")
    run_dir = session.account_dir / "runs" / task_type / run_id
        
    if run_dir.exists() and run_dir.is_dir():
        try:
            shutil.rmtree(run_dir, ignore_errors=True)
        except Exception as e:
            logger.error("Failed to delete directory %s: %s", run_dir, e)
            
    # Also if this was the active session run, reset session state
    if session.run_id == run_id:
        session.run_id = None
        session.run_dir = None
        session.status = "idle"
        session.result = None
        session.full_result = None
        
    return {"status": "ok"}


@app.get("/api/reports/public/status")
def get_public_report_status(
    run_id: str = Query(...),
    sign: str = Query(...)
):
    # Verify signature
    expected_sign = generate_share_sign(run_id)
    if sign != expected_sign:
        raise HTTPException(status_code=403, detail="签名不匹配，无权访问")
        
    # Find account and session
    target_run = None
    target_account_dir = None
    
    for account_dir in ACCOUNTS_DIR.iterdir():
        if account_dir.is_dir():
            runs = _load_runs(account_dir)
            for r in runs:
                if r.get("runId") == run_id:
                    target_run = r
                    target_account_dir = account_dir
                    break
            if target_run:
                break
                
    if not target_run or not target_account_dir:
        raise HTTPException(status_code=404, detail="未找到该运行批次")
        
    # Check if run is marked as public
    if not target_run.get("isPublic", False):
        raise HTTPException(status_code=403, detail="该分析报告未公开分享")
        
    task_type = target_run.get("taskType", "diagnosis")
    run_dir = target_account_dir / "runs" / task_type / run_id
        
    session_json_path = run_dir / "session.json"
    if not session_json_path.exists():
        raise HTTPException(status_code=404, detail="未能读取运行结果数据")
        
    try:
        sdata = json.loads(session_json_path.read_text(encoding="utf-8"))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"解析运行数据失败: {str(e)}")
        
    result_str = sdata.get("result")
    full_str = sdata.get("fullResult")
    
    ws_dir = run_dir / "workspace"
    short_path = ws_dir / "output" / "summary_short.json"
    full_path = ws_dir / "output" / "summary.md"
    if short_path.exists():
        result_str = short_path.read_text(encoding="utf-8")
    if full_path.exists():
        full_str = full_path.read_text(encoding="utf-8")
        
    user_key = target_account_dir.name
    account_json_path = target_account_dir / "account.json"
    if account_json_path.exists():
        try:
            adata = json.loads(account_json_path.read_text(encoding="utf-8"))
            user_key = adata.get("keyMask", user_key)
        except Exception:
            pass

    if isinstance(result_str, (dict, list)):
        result_str = json.dumps(result_str, ensure_ascii=False)

    return {
        "status": sdata.get("status", "completed"),
        "errorMessage": sdata.get("errorMessage", ""),
        "result": result_str,
        "fullResult": full_str,
        "runId": run_id,
        "userKey": user_key
    }


@app.get("/api/reports/public/download")
async def download_public_report_zip(
    run_id: str = Query(...),
    sign: str = Query(...)
):
    # Verify signature
    expected_sign = generate_share_sign(run_id)
    if sign != expected_sign:
        raise HTTPException(status_code=403, detail="签名不匹配，无权访问")
        
    target_run = None
    target_account_dir = None
    
    for account_dir in ACCOUNTS_DIR.iterdir():
        if account_dir.is_dir():
            runs = _load_runs(account_dir)
            for r in runs:
                if r.get("runId") == run_id:
                    target_run = r
                    target_account_dir = account_dir
                    break
            if target_run:
                break
                
    if not target_run or not target_account_dir:
        raise HTTPException(status_code=404, detail="未找到该运行记录")
        
    # Check public status
    if not target_run.get("isPublic", False):
        raise HTTPException(status_code=403, detail="该分析报告未公开分享")
        
    task_type = target_run.get("taskType", "diagnosis")
    run_dir = target_account_dir / "runs" / task_type / run_id
    if not run_dir.exists():
        run_dir = target_account_dir / "runs" / run_id
        
    if not run_dir.exists() or not run_dir.is_dir():
        raise HTTPException(status_code=404, detail=f"运行目录不存在")
        
    output_dir = run_dir / "workspace" / "output"
    if not output_dir.exists() or not output_dir.is_dir():
        raise HTTPException(status_code=404, detail="未找到分析产物目录 (workspace/output)")
        
    # 内存打包 ZIP
    import io
    import zipfile
    
    def generate_zip_bytes():
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            for file_path in output_dir.rglob("*"):
                if file_path.is_file():
                    zf.write(file_path, file_path.relative_to(output_dir))
        zip_buffer.seek(0)
        return zip_buffer.getvalue()
        
    loop = asyncio.get_running_loop()
    try:
        zip_bytes = await loop.run_in_executor(None, generate_zip_bytes)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"生成压缩包失败: {str(e)}")
        
    return StreamingResponse(
        io.BytesIO(zip_bytes),
        media_type="application/zip",
        headers={"Content-Disposition": f"attachment; filename=analysis_{run_id}.zip"}
    )


@app.get("/api/examples")
def get_examples():
    """读取案例文件（Base64 编码）"""
    example_dir = ROOT_DIR / "data" / "samples"
    files_content = []
    if example_dir.exists():
        for f_path in example_dir.glob("*.json"):
            try:
                raw_bytes = f_path.read_bytes()
                b64 = base64.b64encode(raw_bytes).decode("utf-8")
                files_content.append({"name": f_path.name, "base64": b64})
            except Exception as e:
                logger.warning("案例文件加载失败: %s (%s)", f_path, str(e))
    if not files_content:
        return {"error": "未找到案例文件，请检查目录结构"}
    return {"files": files_content}


@app.get("/api/analysis-params")
def get_analysis_params(x_fzt_key: Optional[str] = Header(default=None)):
    session = resolve_session(x_fzt_key)
    params_path = session.account_dir / "analysis_params.json"
    if params_path.exists():
        try:
            data = json.loads(params_path.read_text(encoding="utf-8"))
            return data
        except Exception:
            pass
    return {"analysis_params": ""}


@app.put("/api/analysis-params")
async def update_analysis_params(request: Request, x_fzt_key: Optional[str] = Header(default=None)):
    session = resolve_session(x_fzt_key)
    body = await request.json()
    raw = body.get("analysis_params", "")
    validated = validate_analysis_params(raw)
    params_path = session.account_dir / "analysis_params.json"
    params_path.parent.mkdir(parents=True, exist_ok=True)
    params_path.write_text(json.dumps({"analysis_params": validated}, ensure_ascii=False), encoding="utf-8")
    return {"status": "ok"}


PRESETS_DIR = ROOT_DIR / "data" / "params-presets"


@app.get("/api/analysis-params/presets")
def get_params_presets():
    presets = {}
    if PRESETS_DIR.exists():
        for f_path in sorted(PRESETS_DIR.glob("*.json")):
            try:
                name = f_path.stem
                content = json.loads(f_path.read_text(encoding="utf-8"))
                presets[name] = content
            except Exception as e:
                logger.warning("预设加载失败: %s (%s)", f_path, e)
    return {"presets": presets}


# ── 新增：服务运行期生成的静态资产（如分析图表）的路由 ──
@app.get("/api/reports/{run_id}/assets/{filename}")
def get_report_asset(
    run_id: str,
    filename: str,
    x_fzt_key: Optional[str] = Header(default=None),
    x_fzt_key_query: Optional[str] = Query(default=None, alias="x-fzt-key")
):
    key = x_fzt_key or x_fzt_key_query
    session = resolve_session(key)

    # 路径安全检查，防止目录遍历攻击
    if ".." in filename or "/" in filename or "\\" in filename:
        raise HTTPException(status_code=400, detail="Invalid filename")

    task_type = "diagnosis"
    runs = _load_runs(session.account_dir)
    for r in runs:
        if r.get("runId") == run_id:
            task_type = r.get("taskType", "diagnosis")
            break
    target_run_dir = session.account_dir / "runs" / task_type / run_id
    if not target_run_dir.exists():
        raise HTTPException(status_code=404, detail="Run not found")

    asset_path = target_run_dir / "workspace" / "output" / filename
    if not asset_path.exists() or not asset_path.is_file():
        raise HTTPException(status_code=404, detail="Asset not found")

    # 根据后缀决定 content-type
    media_type = "application/octet-stream"
    ext = filename.lower()
    if ext.endswith(".png"):
        media_type = "image/png"
    elif ext.endswith((".jpg", ".jpeg")):
        media_type = "image/jpeg"
    elif ext.endswith(".svg"):
        media_type = "image/svg+xml"
    elif ext.endswith(".gif"):
        media_type = "image/gif"

    return FileResponse(asset_path, media_type=media_type)


@app.get("/api/reports/public/{run_id}/assets/{filename}")
def get_public_report_asset(
    run_id: str,
    filename: str,
    sign: str = Query(...)
):
    # 校验签名和公开状态
    expected_sign = generate_share_sign(run_id)
    if sign != expected_sign:
        raise HTTPException(status_code=403, detail="签名不匹配，无权访问")

    target_run = None
    target_account_dir = None

    for account_dir in ACCOUNTS_DIR.iterdir():
        if account_dir.is_dir():
            runs = _load_runs(account_dir)
            for r in runs:
                if r.get("runId") == run_id:
                    target_run = r
                    target_account_dir = account_dir
                    break
            if target_run:
                break

    if not target_run or not target_account_dir:
        raise HTTPException(status_code=404, detail="未找到该运行批次")

    if not target_run.get("isPublic", False):
        raise HTTPException(status_code=403, detail="该分析报告未公开分享")

    # 路径安全检查
    if ".." in filename or "/" in filename or "\\" in filename:
        raise HTTPException(status_code=400, detail="Invalid filename")

    task_type = target_run.get("taskType", "diagnosis")
    run_dir = target_account_dir / "runs" / task_type / run_id
 
    asset_path = run_dir / "workspace" / "output" / filename
    if not asset_path.exists() or not asset_path.is_file():
        raise HTTPException(status_code=404, detail="Asset not found")

    # 根据后缀决定 content-type
    media_type = "application/octet-stream"
    ext = filename.lower()
    if ext.endswith(".png"):
        media_type = "image/png"
    elif ext.endswith((".jpg", ".jpeg")):
        media_type = "image/jpeg"
    elif ext.endswith(".svg"):
        media_type = "image/svg+xml"
    elif ext.endswith(".gif"):
        media_type = "image/gif"

    return FileResponse(asset_path, media_type=media_type)


register_chatbot_routes(
    app,
    resolve_session=resolve_session,
    get_chatbot_preset=get_chatbot_preset,
)


# 挂载静态文件 (使用绝对路径更稳健)
static_path = ROOT_DIR / "apps" / "web" / "public"
if static_path.exists():
    app.mount("/", StaticFiles(directory=str(static_path), html=True), name="static")
else:
    print(f"警告: 静态文件目录不存在: {static_path}")

if __name__ == "__main__":
    import uvicorn
    # 统一使用 3000 端口
    uvicorn.run(app, host="0.0.0.0", port=3000)
