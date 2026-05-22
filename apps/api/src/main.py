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
from typing import List, Optional, Dict
from threading import Lock
from fastapi import FastAPI, Request, HTTPException, UploadFile, File, Form, Header, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles
from dotenv import load_dotenv
from pathlib import Path
import sys

# 路径处理
ROOT_DIR = Path(__file__).parent.parent.parent.parent
sys.path.append(str(ROOT_DIR))

from packages.core.input_adapter import parse_uploaded_files, adapt_to_dataset_bundle
from packages.agents.models import RawFile
from packages.agents.registry import create_pipeline
from packages.agents.analysis_params import wash_analysis_params, validate_analysis_params
from packages.auth import generate_user_key, hash_user_key, mask_user_key, RegisterRateLimiter

load_dotenv()

app = FastAPI(title="福州门店 AI 分析系统")
logger = logging.getLogger(__name__)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

STORAGE_DIR = ROOT_DIR / "storage"
ACCOUNTS_DIR = STORAGE_DIR / "accounts"
LEGACY_ACCOUNT_KEY = os.getenv("LEGACY_USER_KEY", "fzt_legacy_local")
MAX_UPLOAD_FILE_SIZE = 5 * 1024 * 1024
MAX_UPLOAD_FILE_SIZE_LABEL = f"{MAX_UPLOAD_FILE_SIZE // (1024 * 1024)}MB"
DEFAULT_REASONING_EFFORT = "medium"
REASONING_EFFORT_OPTIONS = {"low", "medium", "high"}
LLM_PRESETS_FILE = STORAGE_DIR / "llm_presets.json"
LLM_PRESETS_LOCK = Lock()
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "").strip()
PRESET_SECRET = os.getenv("LLM_PRESET_SECRET", "").strip()

# 全局分析处理管线 — traditional / pydantic / smol
GLOBAL_AGENT_PIPELINE = "traditional"
AGENT_PIPELINE_FILE = STORAGE_DIR / "agent_pipeline.json"
AGENT_PIPELINE_LOCK = Lock()
AGENT_PIPELINE_OPTIONS = {"traditional", "pydantic", "smol", "custom"}

def _load_agent_pipeline() -> str:
    if AGENT_PIPELINE_FILE.exists():
        try:
            payload = json.loads(AGENT_PIPELINE_FILE.read_text(encoding="utf-8"))
            val = payload.get("pipeline", "traditional")
            if val in AGENT_PIPELINE_OPTIONS:
                return val
        except Exception:
            pass
    return "traditional"

def _save_agent_pipeline(val: str):
    AGENT_PIPELINE_FILE.parent.mkdir(parents=True, exist_ok=True)
    AGENT_PIPELINE_FILE.write_text(
        json.dumps({"pipeline": val}, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )

GLOBAL_AGENT_PIPELINE = _load_agent_pipeline()


def normalize_reasoning_effort(value: Optional[str]) -> str:
    if not isinstance(value, str):
        return DEFAULT_REASONING_EFFORT
    effort = value.strip().lower()
    return effort if effort in REASONING_EFFORT_OPTIONS else DEFAULT_REASONING_EFFORT


def _default_llm_presets() -> dict:
    base_url = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1").strip()
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    model = os.getenv("OPENAI_MODEL", "gpt-5.4-mini").strip()
    return {
        "low": {"baseUrl": base_url, "apiKey": api_key, "model": model},
        "medium": {"baseUrl": base_url, "apiKey": api_key, "model": model},
        "high": {"baseUrl": base_url, "apiKey": api_key, "model": model}
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


def _sanitize_preset_item(raw: Optional[dict], fallback: dict) -> dict:
    data = raw if isinstance(raw, dict) else {}
    base_url = data.get("baseUrl", fallback.get("baseUrl", ""))
    encoded_key = data.get("apiKeyEnc")
    if isinstance(encoded_key, str):
        api_key = _decrypt_text(encoded_key)
    else:
        api_key = data.get("apiKey", fallback.get("apiKey", ""))
    model = data.get("model", fallback.get("model", ""))
    return {
        "baseUrl": base_url.strip() if isinstance(base_url, str) else str(fallback.get("baseUrl", "")),
        "apiKey": api_key.strip() if isinstance(api_key, str) else str(fallback.get("apiKey", "")),
        "model": model.strip() if isinstance(model, str) else str(fallback.get("model", ""))
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
        except Exception:
            pass
    return defaults


GLOBAL_LLM_PRESETS = _load_llm_presets()


class TaskAbortedError(Exception):
    pass


def get_global_api_key() -> str:
    with LLM_PRESETS_LOCK:
        return GLOBAL_LLM_PRESETS["medium"]["apiKey"]


def set_global_api_key(raw_key: str):
    key_value = (raw_key or "").strip()
    with LLM_PRESETS_LOCK:
        for effort in REASONING_EFFORT_OPTIONS:
            GLOBAL_LLM_PRESETS[effort]["apiKey"] = key_value
        save_llm_presets_locked()


def get_llm_preset(reasoning_effort: Optional[str]) -> dict:
    effort = normalize_reasoning_effort(reasoning_effort)
    with LLM_PRESETS_LOCK:
        preset = GLOBAL_LLM_PRESETS.get(effort, GLOBAL_LLM_PRESETS[DEFAULT_REASONING_EFFORT]).copy()
    preset["reasoningEffort"] = effort
    return preset


def get_all_llm_presets() -> dict:
    with LLM_PRESETS_LOCK:
        return {k: v.copy() for k, v in GLOBAL_LLM_PRESETS.items()}


def save_llm_presets_locked():
    LLM_PRESETS_FILE.parent.mkdir(parents=True, exist_ok=True)
    safe_presets = {}
    for effort, preset in GLOBAL_LLM_PRESETS.items():
        safe_presets[effort] = {
            "baseUrl": preset.get("baseUrl", ""),
            "apiKeyEnc": _encrypt_text(preset.get("apiKey", "")),
            "model": preset.get("model", "")
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
        session.run_dir = session.account_dir / "runs" / session.run_id
    session.run_dir.mkdir(parents=True, exist_ok=True)
    (session.run_dir / "workspace").mkdir(parents=True, exist_ok=True)
    return session.run_dir


def _save_session_json(session: "SessionState"):
    run_dir = session.run_dir
    if not run_dir:
        return
    run_dir.mkdir(parents=True, exist_ok=True)
    data = {
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


class SessionState:
    def __init__(self, user_key: str, key_hash: str, account_slug: str, account_dir: Path, config: dict):
        self.user_key = user_key
        self.key_hash = key_hash
        self.account_slug = account_slug
        self.account_dir = account_dir
        self.cache_dir = account_dir / "cache"
        self.profile_path = account_dir / "profile.json"
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

    @property
    def logs_path(self) -> Path:
        if self.run_dir:
            return self.run_dir / "logs.json"
        return self.account_dir / "latest_logs.json"

    @property
    def report_path(self) -> Path:
        if self.run_dir:
            return self.run_dir / "latest_report.md"
        return self.account_dir / "latest_report.md"


class SessionManager:
    def __init__(self, base_dir: Path):
        self.base_dir = base_dir
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self._sessions: Dict[str, SessionState] = {}
        self._lock = Lock()

    def _default_config(self) -> dict:
        return {
            "reasoningEffort": DEFAULT_REASONING_EFFORT
        }

    def _load_profile(self, account_dir: Path) -> dict:
        profile_path = account_dir / "profile.json"
        cfg = self._default_config()
        if profile_path.exists():
            try:
                profile = json.loads(profile_path.read_text(encoding="utf-8"))
                if isinstance(profile, dict):
                    cfg["reasoningEffort"] = normalize_reasoning_effort(
                        profile.get("reasoningEffort") or profile.get("reasoning_effort")
                    )
            except Exception:
                pass
        return cfg

    def _account_dir(self, user_key: str) -> tuple[str, str, Path]:
        key_hash = hash_user_key(user_key)
        slug = _make_account_slug(user_key, key_hash)
        return key_hash, slug, self.base_dir / slug

    def _try_load_session(self, account_dir: Path) -> dict | None:
        latest = account_dir / "latest_run.json"
        if not latest.exists():
            return None
        try:
            meta = json.loads(latest.read_text(encoding="utf-8"))
            run_id = meta.get("runId")
            if not run_id:
                return None
            run_dir = account_dir / "runs" / run_id
            session_path = run_dir / "session.json"
            if not session_path.exists():
                return None
            state = json.loads(session_path.read_text(encoding="utf-8"))
            if state.get("status") in ("running", "queued"):
                state["status"] = "interrupted"
                state["errorMessage"] = "服务重启导致任务中断，请重新提交分析。"
                try:
                    session_path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
                except Exception:
                    pass
            state["_runId"] = run_id
            state["_runDir"] = str(run_dir)
            return state
        except Exception:
            return None

    def get_session(self, user_key: str, create_if_missing: bool = True) -> SessionState:
        key_hash, slug, account_dir = self._account_dir(user_key)
        with self._lock:
            existing = self._sessions.get(key_hash)
            if existing:
                _save_account_json(account_dir, user_key, key_hash)
                return existing

            profile_path = account_dir / "profile.json"
            if not create_if_missing and not profile_path.exists():
                raise KeyError("invalid_key")

            account_dir.mkdir(parents=True, exist_ok=True)
            _save_account_json(account_dir, user_key, key_hash)
            cfg = self._load_profile(account_dir)
            session = SessionState(user_key=user_key, key_hash=key_hash, account_slug=slug, account_dir=account_dir, config=cfg)

            saved = self._try_load_session(account_dir)
            if saved:
                session.status = saved.get("status", "idle")
                session.error_message = saved.get("errorMessage", "")
                session.result = saved.get("result")
                session.full_result = saved.get("fullResult")
                session.run_id = saved.get("_runId")
                rd = saved.get("_runDir")
                if rd:
                    session.run_dir = Path(rd)
                    logs_file = session.run_dir / "logs.json"
                    if logs_file.exists():
                        try:
                            session.logs = json.loads(logs_file.read_text(encoding="utf-8"))
                        except Exception:
                            pass

            self._sessions[key_hash] = session
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

    def drop_session(self, key_hash: str):
        with self._lock:
            self._sessions.pop(key_hash, None)


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
        session.logs_path.write_text(json.dumps(session.logs, ensure_ascii=False, indent=2), encoding="utf-8")
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
        # 不 overwrite 文件，只保留原有日志


def add_status(session: SessionState, node_id: str, status: str):
    emit_event(session, "status", {"nodeId": node_id, "status": status})


def add_log(session: SessionState, node_id: str, message: str):
    safe_message = _mask_sensitive_text(message)
    log_entry = emit_event(session, "log", {"nodeId": node_id, "message": safe_message})
    hash_prefix = session.key_hash[:8]
    logger.info("[%s] %s %s", log_entry["time"], hash_prefix, node_id)


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


def resolve_session(x_fzt_key: Optional[str]) -> SessionState:
    key = (x_fzt_key or "").strip()
    if not key:
        raise HTTPException(status_code=401, detail="Authentication required")
    try:
        return session_manager.get_session(key, create_if_missing=False)
    except KeyError:
        raise HTTPException(status_code=401, detail="Invalid or expired key")



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

    try:
        session_manager.save_profile(session)
    except Exception as e:
        session_manager.drop_session(session.key_hash)
        raise HTTPException(status_code=500, detail=f"账号初始化失败: {str(e)}")
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
        "hasKey": bool(preset.get("apiKey"))
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

@app.get("/api/admin/pipeline")
def get_admin_pipeline(x_admin_token: Optional[str] = Header(default=None)):
    require_admin_authorization(x_admin_token)
    with AGENT_PIPELINE_LOCK:
        return {"status": "ok", "pipeline": GLOBAL_AGENT_PIPELINE, "options": sorted(AGENT_PIPELINE_OPTIONS)}


@app.post("/api/admin/pipeline")
async def save_admin_pipeline(request: Request, x_admin_token: Optional[str] = Header(default=None)):
    require_admin_authorization(x_admin_token)
    payload = await request.json()
    val = (payload.get("pipeline") or "").strip().lower()
    if val not in AGENT_PIPELINE_OPTIONS:
        raise HTTPException(status_code=400, detail=f"无效管线: {val}，可选 {sorted(AGENT_PIPELINE_OPTIONS)}")
    with AGENT_PIPELINE_LOCK:
        global GLOBAL_AGENT_PIPELINE
        GLOBAL_AGENT_PIPELINE = val
        _save_agent_pipeline(val)
    return {"status": "ok", "pipeline": GLOBAL_AGENT_PIPELINE}


@app.get("/api/status")
def get_status(x_fzt_key: Optional[str] = Header(default=None)):
    session = resolve_session(x_fzt_key)
    if session.status == "aborted":
        safe_error = "任务被用户强行终止。"
    elif session.status == "error":
        safe_error = "任务执行失败，请在后台监控流查看 system 节点日志"
    else:
        safe_error = ""

    result_str = session.result
    full_str = session.full_result
    if session.run_dir:
        ws_dir = session.run_dir / "workspace"
        short_path = ws_dir / "summary_short.json"
        full_path = ws_dir / "summary.md"
        if short_path.exists():
            result_str = short_path.read_text(encoding="utf-8")
        if full_path.exists():
            full_str = full_path.read_text(encoding="utf-8")

    return {
        "status": session.status,
        "errorMessage": safe_error,
        "result": result_str,
        "fullResult": full_str
    }


@app.get("/api/logs")
def get_logs(x_fzt_key: Optional[str] = Header(default=None)):
    resolve_session(x_fzt_key)  # 确保 session 缓存存在
    return {"logs": _load_logs_from_file(resolve_session(x_fzt_key))}


def _load_logs_from_file(session) -> list:
    """强制从文件加载日志，不依赖 session.logs 的内存状态。"""
    try:
        if session.run_dir:
            p = session.run_dir / "logs.json"
            if p.exists():
                return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        pass
    return []


async def sse_generator(session: SessionState):
    """SSE 日志推送，适配前端 EventSource('/api/stream')"""
    session.logs = _load_logs_from_file(session)

    last_idx = 0
    yield f"data: {json.dumps({'type': 'reset', 'time': _now_time()})}\n\n"

    while True:
        if last_idx < len(session.logs):
            for i in range(last_idx, len(session.logs)):
                yield f"data: {json.dumps(session.logs[i], ensure_ascii=False)}\n\n"
            last_idx = len(session.logs)

        await asyncio.sleep(0.5)


@app.get("/api/stream")
async def stream(
    x_fzt_key: Optional[str] = Query(default=None, alias="x-fzt-key")
):
    session = resolve_session(x_fzt_key)
    return StreamingResponse(sse_generator(session), media_type="text/event-stream")



async def run_pipeline_task(session: SessionState, pipeline_name: str, active_preset: dict, bundle):
    """统一管线执行入口：triaditional / pydantic / smol 均走此路径"""
    with session.runtime_lock:
        if session.force_stop or session.status == "aborted":
            return
        session.status = "running"
    reset_events(session)
    emit_event(session, "pipeline", {"pipeline": pipeline_name})

    def check_aborted():
        try:
            ensure_not_stopped(session)
        except TaskAbortedError as e:
            from packages.agents.traditional_pipeline import PipelineAbortedError
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
        from packages.agents.traditional_pipeline import PipelineAbortedError

        result = await pipe.run(bundle)
        full_text = result.full_report or f"# {pipeline_name} 管线报告\n\npipeline: {result.pipeline}\n耗时: {result.elapsed_ms:.0f}ms"
        session.full_result = full_text
        session.result = json.dumps({
            "health_status": "分析完成",
            "overview_text": f"共 {len(result.cards)} 项待关注",
            "cards": [{"title": c.title, "explanation": c.explanation, "suggestion": c.suggestion, "evidence": c.evidence, "color": c.color} for c in result.cards]
        }, ensure_ascii=False)
        with session.runtime_lock:
            if session.force_stop or session.status == "aborted":
                _save_session_json(session)
                return
            session.status = "completed"
        save_latest_report(session)
        _save_session_json(session)
    except (PipelineAbortedError, asyncio.CancelledError):
        add_log(session, "system", "⚠️ 任务被用户强制终止。")
        with session.runtime_lock:
            session.status = "aborted"
            session.error_message = "任务被用户强行终止。"
        _save_session_json(session)
    except Exception as e:
        add_log(session, "system", f"管线执行失败: {str(e)}")
        with session.runtime_lock:
            session.status = "error"
            session.error_message = str(e)
        _save_session_json(session)


@app.post("/api/analyze")
async def analyze(
    files: List[UploadFile] = File(...),
    x_fzt_key: Optional[str] = Header(default=None),
    reasoningEffort: Optional[str] = Form(None)
):
    session = resolve_session(x_fzt_key)
    session.config["reasoningEffort"] = normalize_reasoning_effort(reasoningEffort)
    with session.runtime_lock:
        if session.status in ("queued", "running"):
            raise HTTPException(status_code=400, detail="任务正在运行中")
        session.status = "queued"
        session.force_stop = False
        session.error_message = ""
        session.result = None
        session.full_result = None

    SUPPORTED_EXTENSIONS = {".json", ".xlsx", ".xls", ".csv", ".pdf", ".docx", ".doc", ".txt", ".md"}
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

    with AGENT_PIPELINE_LOCK:
        pipeline_name = GLOBAL_AGENT_PIPELINE
    active_preset = get_llm_preset(session.config.get("reasoningEffort", "medium"))

    # 创建 run 目录
    _ensure_run_dir(session)
    _save_session_json(session)
    _save_latest_run_json(session)

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
            task = getattr(session, "_pipeline_task", None)
            if task and not task.done():
                task.cancel()
    return {"status": "ok"}


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
