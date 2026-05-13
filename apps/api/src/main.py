import os
import json
import asyncio
import time
import shutil
import base64
import re
import base64
import hashlib
import logging
from typing import List, Optional, Dict
from threading import Lock
from fastapi import FastAPI, Request, BackgroundTasks, HTTPException, UploadFile, File, Header, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles
from dotenv import load_dotenv
from pathlib import Path
import sys

# 路径处理
ROOT_DIR = Path(__file__).parent.parent.parent.parent
sys.path.append(str(ROOT_DIR))

from packages.core.cleaner import clean_data, merge_hot_products, merge_hot_top500
from packages.core.metrics import (
    normalize_overview_rows,
    calc_channel_mix, calc_revenue_change, calc_o2o_vs_total,
    prepare_growth_decomposition, prepare_anomaly_summary
)
from packages.ai.ai_caller import call_ai, call_detailed_ai, call_simplified_ai, call_ai_new, call_detailed_ai_new
from packages.ai.error_reviewer import review_error, review_error_new
from packages.auth import generate_user_key, hash_user_key, mask_user_key, RegisterRateLimiter

# 新多文件管线
from packages.core.input_adapter import parse_uploaded_files, infer_source_type
from packages.core.profiler import profile_dataset
from packages.core.semantic_mapper import map_profiles
from packages.core.scene_classifier import classify_scene, classify_data_scope
from packages.core.canonical import build_canonical_dataset
from packages.core.metric_registry import match_metrics
from packages.core.metric_engine import run_metrics
from packages.core.threshold_resolver import resolve_all_statuses
from packages.core.evidence_builder import build_evidence_bundle

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
DEFAULT_TALLY = {"pass": 0, "attention": 0, "warning": 0, "uncountable": 0}
STATUS_ICON_MAP = {"warning": "🔴", "attention": "🟡", "uncountable": "⚪", "pass": "🟢"}
SAFE_UPLOAD_FILENAME = re.compile(r"^[A-Za-z0-9._\-\u4e00-\u9fff]+$")
MAX_UPLOAD_FILE_SIZE = 5 * 1024 * 1024
MAX_UPLOAD_FILE_SIZE_LABEL = f"{MAX_UPLOAD_FILE_SIZE // (1024 * 1024)}MB"
DEFAULT_REASONING_EFFORT = "medium"
REASONING_EFFORT_OPTIONS = {"low", "medium", "high"}
LLM_PRESETS_FILE = STORAGE_DIR / "llm_presets.json"
LLM_PRESETS_LOCK = Lock()
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "").strip()
PRESET_SECRET = os.getenv("LLM_PRESET_SECRET", "").strip()


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


class SessionState:
    def __init__(self, user_key: str, key_hash: str, account_dir: Path, config: dict):
        self.user_key = user_key
        self.key_hash = key_hash
        self.account_dir = account_dir
        self.upload_dir = account_dir / "uploads"
        self.cache_dir = account_dir / "cache"
        self.profile_path = account_dir / "profile.json"
        self.logs_path = account_dir / "latest_logs.json"
        self.report_path = account_dir / "latest_report.md"
        self.status = "idle"  # idle, running, completed, error, aborted
        self.error_message = ""
        self.logs = []
        self.result = None      # 精简报告 (JSON string)
        self.full_result = None # 完整报告 (Markdown string)
        self.config = config
        self.force_stop = False
        self.upload_lock = Lock()
        self.runtime_lock = Lock()


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

    def _account_dir(self, user_key: str) -> tuple[str, Path]:
        key_hash = hash_user_key(user_key)
        return key_hash, self.base_dir / key_hash

    def get_session(self, user_key: str, create_if_missing: bool = True) -> SessionState:
        key_hash, account_dir = self._account_dir(user_key)
        with self._lock:
            existing = self._sessions.get(key_hash)
            if existing:
                return existing

            profile_path = account_dir / "profile.json"
            if not create_if_missing and not profile_path.exists():
                raise KeyError("invalid_key")

            account_dir.mkdir(parents=True, exist_ok=True)
            cfg = self._load_profile(account_dir)
            session = SessionState(user_key=user_key, key_hash=key_hash, account_dir=account_dir, config=cfg)
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
    for d in [session.account_dir, session.upload_dir, session.cache_dir]:
        d.mkdir(parents=True, exist_ok=True)


def persist_latest_logs(session: SessionState):
    _ensure_session_dirs(session)
    try:
        session.logs_path.write_text(json.dumps(session.logs, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass


def emit_event(session: SessionState, event_type: str, payload: dict):
    event = {"type": event_type, "time": _now_time(), **payload}
    session.logs.append(event)
    persist_latest_logs(session)
    return event


def reset_events(session: SessionState):
    session.logs = []
    emit_event(session, "reset", {})


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


def sanitize_upload_filename(raw_name: Optional[str], index: int):
    if not raw_name:
        candidate = f"file_{index + 1}.json"
    else:
        if "/" in raw_name or "\\" in raw_name:
            raise HTTPException(status_code=400, detail=f"非法文件名: {raw_name}")
        candidate = Path(raw_name).name.strip() or f"file_{index + 1}.json"

    if len(candidate) > 128:
        raise HTTPException(status_code=400, detail=f"文件名过长: {candidate[:32]}...")
    if not SAFE_UPLOAD_FILENAME.fullmatch(candidate):
        raise HTTPException(status_code=400, detail=f"文件名包含非法字符: {candidate}")
    if not candidate.lower().endswith(".json"):
        candidate = f"{candidate}.json"
    return candidate
    if not raw_name:
        return f"file_{index + 1}"
    if "/" in raw_name or "\\" in raw_name:
        raise HTTPException(status_code=400, detail=f"非法文件名: {raw_name}")
    name = Path(raw_name).name.strip()
    if not name:
        name = f"file_{index + 1}"
    if len(name) > 128:
        raise HTTPException(status_code=400, detail=f"文件名过长: {name[:32]}...")
    if not SAFE_UPLOAD_FILENAME.fullmatch(name):
        raise HTTPException(status_code=400, detail=f"文件名包含非法字符: {name}")
    return name


def save_current_uploads(session: SessionState, decoded_files: List[dict]):
    _ensure_session_dirs(session)
    current_dir = session.upload_dir / "current"
    tmp_dir = session.upload_dir / ".current_tmp"

    with session.upload_lock:
        if tmp_dir.exists():
            shutil.rmtree(tmp_dir)
        tmp_dir.mkdir(parents=True, exist_ok=True)

        for i, item in enumerate(decoded_files):
            raw_name = item.get("name")
            data_bytes = item.get("bytes", b"")
            safe_name = sanitize_upload_filename(raw_name, i)
            output_path = tmp_dir / safe_name
            try:
                output_path.write_bytes(data_bytes)
            except OSError as e:
                raise HTTPException(status_code=500, detail=f"写入上传缓存失败: {str(e)}")

        if current_dir.exists():
            shutil.rmtree(current_dir)
        tmp_dir.rename(current_dir)


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


@app.get("/api/auth/me")
def auth_me(x_fzt_key: Optional[str] = Header(default=None)):
    session = resolve_session(x_fzt_key)
    effort = normalize_reasoning_effort(session.config.get("reasoningEffort"))
    preset = get_llm_preset(effort)
    return {
        "userKey": mask_user_key(session.user_key),
        "config": {
            "reasoningEffort": effort,
            "baseUrl": preset.get("baseUrl", ""),
            "model": preset.get("model", ""),
            "hasKey": bool(preset.get("apiKey"))
        }
    }


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


@app.post("/api/config")
async def save_config(request: Request, x_fzt_key: Optional[str] = Header(default=None)):
    session = resolve_session(x_fzt_key)
    data = sanitize_settings(await request.json())
    session.config.update(data)
    session_manager.save_profile(session)
    effort = normalize_reasoning_effort(session.config.get("reasoningEffort"))
    preset = get_llm_preset(effort)
    return {
        "status": "ok",
        "reasoningEffort": effort,
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

@app.get("/api/status")
def get_status(x_fzt_key: Optional[str] = Header(default=None)):
    session = resolve_session(x_fzt_key)
    if session.status == "aborted":
        safe_error = "任务被用户强行终止。"
    elif session.status == "error":
        safe_error = "任务执行失败，请在后台监控流查看 system 节点日志"
    else:
        safe_error = ""
    return {
        "status": session.status,
        "errorMessage": safe_error,
        "result": session.result,
        "fullResult": session.full_result
    }


@app.get("/api/logs")
def get_logs(x_fzt_key: Optional[str] = Header(default=None)):
    session = resolve_session(x_fzt_key)
    return session.logs


async def sse_generator(session: SessionState):
    """SSE 日志推送，适配前端 EventSource('/api/stream')"""
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


async def run_analysis_task(session: SessionState, files_data: List[dict], user_settings: Optional[dict]):
    with session.runtime_lock:
        session.status = "running"
        session.force_stop = False
        session.error_message = ""
        session.result = None
        session.full_result = None
    reset_events(session)

    updates = sanitize_settings(user_settings)
    session.config.update(updates)
    session.config["reasoningEffort"] = normalize_reasoning_effort(session.config.get("reasoningEffort"))
    active_settings = get_llm_preset(session.config["reasoningEffort"])
    session_manager.save_profile(session)

    try:
        # 1) Input
        add_status(session, "input", "active")
        add_log(session, "input", f"收到 {len(files_data)} 个 JSON 文件")
        for i, f in enumerate(files_data):
            page = f.get("page", {})
            add_log(session, "input", f"  [{i}] {page.get('module', '?')} - {page.get('title', '')}")
        add_status(session, "input", "success")
        ensure_not_stopped(session)

        # 2) Clean
        add_status(session, "clean", "active")
        add_log(session, "clean", "开始清洗数据...")
        cleaned_list = []
        for f in files_data:
            ensure_not_stopped(session)
            c = clean_data(f)
            if c:
                cleaned_list.append(c)

        # 合并处理
        hot_merged = merge_hot_products(files_data)
        top500_merged = merge_hot_top500(files_data)
        if hot_merged:
            cleaned_list.append(hot_merged)
        if top500_merged:
            cleaned_list.append(top500_merged)

        cleaned_texts = [json.dumps(c, ensure_ascii=False, indent=2) for c in cleaned_list]
        add_log(session, "clean", f"完成清洗，共 {len(cleaned_list)} 个有效模块")
        add_status(session, "clean", "success")
        ensure_not_stopped(session)

        # 3) Alg1
        add_status(session, "alg1", "active")
        add_log(session, "alg1", "启动算法引擎，加载并整理数据源...")
        overview_day = next((f for f in files_data if f.get("page", {}).get("module") == "business_overview" and f.get("page", {}).get("viewType") == "day"), None)
        o2o_day = next((f for f in files_data if f.get("page", {}).get("module") == "o2o_business_summary"), None)

        rows = normalize_overview_rows(overview_day) if overview_day else []
        source_distribution = overview_day.get("sourceDistribution", {}) if overview_day else {}
        o2o_rows = (o2o_day or {}).get("businessTable", {}).get("rows", [{}]) if o2o_day else [{}]
        o2o_rev = o2o_rows[0].get("total_revenue", 0)
        overview_revenue = rows[0].get("revenue") if rows else None

        add_log(session, "alg1", f"数据源就绪: 概览日={'是' if overview_day else '否'} / O2O日={'是' if o2o_day else '否'}")
        add_status(session, "alg1", "success")
        ensure_not_stopped(session)

        # 4) Alg2
        add_status(session, "alg2", "active")
        add_log(session, "alg2", "开始逐项计算指标...")
        metric_tasks = [
            ("calcRevenueChange", lambda: calc_revenue_change(rows)),
            ("prepareGrowthDecomposition", lambda: prepare_growth_decomposition(rows)),
            ("calcChannelMix", lambda: calc_channel_mix(source_distribution)),
            ("calcO2OvsTotal", lambda: calc_o2o_vs_total(o2o_rev, overview_revenue)),
        ]
        m_results = {}
        total = len(metric_tasks)
        for i, (name, fn) in enumerate(metric_tasks, start=1):
            ensure_not_stopped(session)
            result = fn()
            m_results[name] = result
            icon = STATUS_ICON_MAP.get(result.get("status"), "🟢")
            add_log(session, "alg2", f"  [{i}/{total}] {icon} {name} -> {result.get('status', 'unknown')}")
            add_progress(session, "alg2", i, total)
            await asyncio.sleep(0.05)

        add_log(session, "alg2", f"完成: {total} 个指标已计算")
        add_status(session, "alg2", "success")
        ensure_not_stopped(session)

        # 5) Alg3
        add_status(session, "alg3", "active")
        add_log(session, "alg3", "汇总异常检测结果...")
        anomaly_summary = prepare_anomaly_summary(m_results)
        tally = anomaly_summary.get("aiPromptData", {}).get("tally") or DEFAULT_TALLY
        add_tally(session, "alg3", tally)
        add_log(session, "alg3", f"  🟢 pass: {tally.get('pass', 0)}")
        add_log(session, "alg3", f"  🟡 attention: {tally.get('attention', 0)}")
        add_log(session, "alg3", f"  🔴 warning: {tally.get('warning', 0)}")
        if tally.get("uncountable", 0) > 0:
            add_log(session, "alg3", f"  ⚪ uncountable: {tally.get('uncountable', 0)}")
        add_status(session, "alg3", "success")
        ensure_not_stopped(session)

        # 6) AI Initial Call
        if not active_settings.get("apiKey"):
            add_status(session, "api", "active")
            add_log(session, "api", "未检测到 API Key，进入模拟模式...")
            await asyncio.sleep(0.3)
            add_status(session, "api", "simulated")

            add_status(session, "output", "active")
            add_log(session, "output", "使用模拟报告")
            add_status(session, "output", "simulated")

            add_status(session, "review", "active")
            add_log(session, "review", "模拟模式：跳过错误评审")
            add_status(session, "review", "simulated")

            add_status(session, "fusion", "active")
            add_log(session, "fusion", "模拟模式：融合默认报告与异常摘要")
            add_status(session, "fusion", "success")

            add_status(session, "rep1", "active")
            session.full_result = "# 模拟诊断报告\n\n这是一个模拟生成的报告，因为没有配置 API Key。"
            add_status(session, "rep1", "simulated")

            add_status(session, "rep2", "active")
            session.result = json.dumps({
                "health_status": "模拟运行",
                "overview_text": "系统处于模拟测试模式。",
                "cards": [{"title": "演示问题", "explanation": "这是一个模拟卡片", "suggestion": "请在设置中配置 API Key", "color": "blue"}]
            }, ensure_ascii=False)
            add_status(session, "rep2", "simulated")
            session.status = "completed"
            save_latest_report(session)
            return

        add_status(session, "api", "active")
        add_log(session, "api", f"正在请求 AI 初步诊断 (模型: {active_settings['model']})...")
        initial_resp = await call_ai(active_settings, cleaned_texts, {"anomalies": anomaly_summary.get("aiPromptData", {})})
        initial_report = initial_resp["choices"][0]["message"]["content"]
        add_log(session, "api", "初诊报告已生成")
        add_status(session, "api", "success")
        ensure_not_stopped(session)

        # 7) Output
        add_status(session, "output", "active")
        add_log(session, "output", f"报告内容: {len(initial_report)} 字符")
        add_status(session, "output", "success")
        ensure_not_stopped(session)

        # 8) Error Review
        add_status(session, "review", "active")
        add_log(session, "review", "启动逻辑审计复核...")
        review_resp = await review_error(active_settings, initial_report, cleaned_texts)
        review_text = review_resp["choices"][0]["message"]["content"] if isinstance(review_resp, dict) else str(review_resp)
        add_log(session, "review", "审计复核完成")
        add_status(session, "review", "success")
        ensure_not_stopped(session)

        # 9) Fusion
        add_status(session, "fusion", "active")
        add_log(session, "fusion", "数据融合：合并初级报告、错误评审、异常日志...")
        anomaly_logs_text = json.dumps(anomaly_summary.get("sortedAlerts", []), ensure_ascii=False)
        fused_context = (
            f"【初级报告】\n{initial_report}\n\n"
            f"【审计意见】\n{review_text}\n\n"
            f"【算法日志】\n{anomaly_logs_text}"
        )
        add_status(session, "fusion", "success")
        ensure_not_stopped(session)

        # 10) Detailed
        add_status(session, "rep1", "active")
        add_log(session, "rep1", "正在融合审计意见生成深度报告...")
        detailed_resp = await call_detailed_ai(active_settings, fused_context)
        session.full_result = detailed_resp["choices"][0]["message"]["content"]
        add_log(session, "rep1", "深度报告生成成功")
        add_status(session, "rep1", "success")
        ensure_not_stopped(session)

        # 11) Simplified
        add_status(session, "rep2", "active")
        add_log(session, "rep2", "生成精简老板视图...")
        simplified_resp = await call_simplified_ai(active_settings, session.full_result)
        session.result = simplified_resp["choices"][0]["message"]["content"]
        add_log(session, "rep2", "任务全部完成！")
        add_status(session, "rep2", "success")

        with session.runtime_lock:
            session.status = "completed"
        save_latest_report(session)

    except TaskAbortedError as e:
        with session.runtime_lock:
            session.status = "aborted"
            session.error_message = str(e)
        add_log(session, "system", f"⚠️ {str(e)}")

    except Exception as e:
        error_msg = "任务执行失败，请在后台监控流查看 system 节点日志"
        add_log(session, "system", f"发生错误: {str(e)}")
        with session.runtime_lock:
            session.error_message = error_msg
            session.status = "error"


async def run_multifile_analysis(session: SessionState, decoded_files: list):
    """新多文件管线：支持 JSON / Excel / CSV 多文件分析"""
    import json as _json

    with session.runtime_lock:
        session.status = "running"
        session.force_stop = False
        session.error_message = ""
        session.result = None
        session.full_result = None
    reset_events(session)

    active_settings = get_llm_preset(session.config.get("reasoningEffort", "medium"))

    try:
        # 1) Input Adapter
        add_status(session, "input", "active")
        add_log(session, "input", f"多文件管线: 收到 {len(decoded_files)} 个文件")
        for f in decoded_files:
            source = infer_source_type(f.get("name", ""))
            add_log(session, "input", f"  [{source}] {f.get('name', '?')} ({len(f.get('bytes', b''))} bytes)")
        dataset_bundle = parse_uploaded_files(decoded_files)
        add_log(session, "input", f"解析完成: {dataset_bundle['source_type']}, {len(dataset_bundle['tables'])} 张表")
        add_status(session, "input", "success")
        ensure_not_stopped(session)

        # 2) Data Profiler
        add_status(session, "profile", "active")
        add_log(session, "profile", "开始数据画像...")
        profiles = profile_dataset(dataset_bundle)
        add_log(session, "profile", f"画像完成: {len(profiles)} 个字段")
        add_status(session, "profile", "success")
        ensure_not_stopped(session)

        # 3) Scene Classifier
        add_status(session, "scene", "active")
        add_log(session, "scene", "开始场景识别...")
        scene = classify_scene(profiles)
        add_log(session, "scene", f"识别结果: {scene.get('industry')}/{scene.get('business_model')} (conf={scene.get('confidence')})")
        add_status(session, "scene", "success")
        ensure_not_stopped(session)

        # 4) Semantic Mapper
        add_status(session, "mapping", "active")
        add_log(session, "mapping", "开始字段语义映射（场景感知）...")
        add_log(session, "mapping", f"共 {len(profiles)} 个字段待映射, 场景: {scene.get('industry')}")
        mappings = map_profiles(profiles, scene)
        mapped_count = sum(1 for m in mappings if m.get("semantic_field") != "unknown")
        confirm_count = sum(1 for m in mappings if m.get("need_confirm"))
        add_log(session, "mapping", f"结果: {mapped_count}/{len(mappings)} 已识别, {confirm_count} 需确认")
        for m in mappings:
            sf = m.get("semantic_field", "unknown")
            conf = m.get("confidence", 0)
            mark = " [需确认]" if m.get("need_confirm") else ""
            table = m.get("table", "?")
            add_log(session, "mapping", f"  `{m['raw_field']}` → `{sf}` (table={table}, conf={conf}){mark}")
        if confirm_count > 0:
            add_log(session, "mapping", f"低置信字段: {[m['raw_field'] for m in mappings if m.get('need_confirm')]}")
        add_status(session, "mapping", "success")

        # 完善 data_scope（基于语义映射结果）
        scene["data_scope"] = classify_data_scope(mappings)
        add_log(session, "scene", f"数据范围: {scene.get('data_scope')}")
        ensure_not_stopped(session)

        # 5) Canonical Dataset
        add_status(session, "canonical", "active")
        add_log(session, "canonical", "构建标准语义数据层...")
        canonical = build_canonical_dataset(dataset_bundle, mappings, scene)
        table_names = list(canonical.get("tables", {}).keys())
        total_rows = sum(len(rows) for rows in canonical.get("tables", {}).values())
        add_log(session, "canonical", f"标准数据集: {table_names}, 共 {total_rows} 行")
        add_status(session, "canonical", "success")
        ensure_not_stopped(session)

        # 6) Metric Registry
        add_status(session, "registry", "active")
        add_log(session, "registry", "匹配可计算指标...")
        metric_defs = match_metrics(canonical)
        available = [m for m in metric_defs if m["available"]]
        unavailable = [m for m in metric_defs if not m["available"]]
        add_log(session, "registry", f"可计算: {len(available)} 项 - {[m['metric_id'] for m in available]}")
        if unavailable:
            add_log(session, "registry", f"不可计算: {len(unavailable)} 项 - {[m['metric_id'] for m in unavailable]}")
        add_status(session, "registry", "success")
        ensure_not_stopped(session)

        # 7) Metric Engine
        add_status(session, "engine", "active")
        add_log(session, "engine", f"开始计算 {len(available)} 项指标...")
        metric_results = run_metrics(available, canonical)
        pass_count = sum(1 for r in metric_results if r.get("status") == "pass")
        unc_count = sum(1 for r in metric_results if r.get("status") == "uncountable")
        for r in metric_results:
            val = r.get("value")
            val_str = _json.dumps(val, ensure_ascii=False) if isinstance(val, (dict, list)) else str(val)
            add_log(session, "engine", f"  {r.get('name', '?')} → {r.get('status')} | {val_str[:80]}")
        add_log(session, "engine", f"完成: {pass_count} pass, {unc_count} uncountable")
        add_status(session, "engine", "success")
        ensure_not_stopped(session)

        # 8) Threshold Resolver
        add_status(session, "threshold", "active")
        add_log(session, "threshold", "场景化健康判断...")
        metric_results = resolve_all_statuses(metric_results, scene)
        tally = {"pass": 0, "attention": 0, "warning": 0, "uncountable": 0}
        for r in metric_results:
            s = r.get("status", "uncountable")
            tally[s] = tally.get(s, 0) + 1
        add_log(session, "threshold", f"🟢 pass: {tally['pass']}, 🟡 attention: {tally['attention']}, 🔴 warning: {tally['warning']}, ⚪ uncountable: {tally['uncountable']}")
        add_status(session, "threshold", "success")
        ensure_not_stopped(session)

        # 9) Evidence Builder
        add_status(session, "evidence", "active")
        add_log(session, "evidence", "构建证据包...")
        evidence = build_evidence_bundle(metric_results, canonical)
        add_log(session, "evidence", f"证据包: {len(evidence['items'])} 条证据")
        add_status(session, "evidence", "success")
        ensure_not_stopped(session)

        # 10) AI Report (如果无 API Key 则生成摘要)
        if not active_settings.get("apiKey"):
            add_status(session, "report", "active")
            add_log(session, "report", "未检测到 API Key，生成算法摘要...")

            lines = [f"# {scene.get('industry', '未知')} 经营分析摘要"]
            lines.append("")
            lines.append(f"## 场景识别")
            lines.append(f"- 行业: {scene.get('industry')}")
            lines.append(f"- 业态: {scene.get('business_model')}")
            lines.append(f"- 数据范围: {', '.join(scene.get('data_scope', []))}")
            lines.append("")
            lines.append(f"## 指标结果 ({len(metric_results)} 项)")
            lines.append("")
            for r in metric_results:
                icon = STATUS_ICON_MAP.get(r.get("status"), "⚪")
                val = r.get("value")
                val_str = _json.dumps(val, ensure_ascii=False) if isinstance(val, (dict, list)) else str(val)
                lines.append(f"- {icon} **{r.get('name')}**: {val_str}")
                if r.get("reason"):
                    lines.append(f"  - {r.get('reason')}")
            lines.append("")
            lines.append("## 字段映射")
            for m in mappings:
                sf = m.get("semantic_field", "unknown")
                conf = m.get("confidence", 0)
                lines.append(f"- `{m.get('raw_field')}` → `{sf}` (conf: {conf})")

            report_md = "\n".join(lines)
            session.full_result = report_md
            session.result = _json.dumps({
                "health_status": f"{tally['warning']}项报警/{tally['attention']}项关注",
                "overview_text": f"{scene.get('industry')}场景分析完成，共{tally['warning'] + tally['attention']}项需关注。",
                "cards": [
                    {"title": r.get("name", ""), "explanation": r.get("reason", ""),
                     "suggestion": "", "color": "red" if r.get("status") == "warning" else "yellow" if r.get("status") == "attention" else "green"}
                    for r in metric_results if r.get("status") in ("warning", "attention")
                ]
            }, ensure_ascii=False)
            add_status(session, "report", "simulated")
            session.status = "completed"
            save_latest_report(session)
            return

        # AI-1: 初诊报告 (新管线 — 格式化输入)
        add_status(session, "report", "active")
        add_log(session, "report", f"调用 AI 生成报告 (模型: {active_settings['model']})...")
        initial_resp = await call_ai_new(active_settings, scene, metric_results, evidence, mappings)
        initial_report = initial_resp["choices"][0]["message"]["content"]
        add_log(session, "report", f"初诊报告已生成 ({len(initial_report)} 字符)")
        ensure_not_stopped(session)

        # AI-2: 审计复核 (新管线 — 证据包对照)
        add_status(session, "review", "active")
        add_log(session, "review", "启动逻辑审计复核...")
        review_resp = await review_error_new(active_settings, scene, initial_report, evidence)
        review_text = review_resp["choices"][0]["message"]["content"] if isinstance(review_resp, dict) else str(review_resp)
        add_log(session, "review", f"审计复核完成 ({len(review_text)} 字符)")
        add_status(session, "review", "success")
        ensure_not_stopped(session)

        # 融合
        add_status(session, "fusion", "active")
        add_log(session, "fusion", "数据融合：合并初级报告、错误评审、证据数据...")
        fused_context = (
            f"【初级报告】\n{initial_report}\n\n"
            f"【审计意见】\n{review_text}\n\n"
            f"【证据包】\n{_json.dumps(evidence.get('items', [])[:10], ensure_ascii=False)}"
        )
        add_log(session, "fusion", f"融合上下文: {len(fused_context)} 字符")
        add_status(session, "fusion", "success")
        ensure_not_stopped(session)

        # AI-3: 深度报告 (新管线)
        add_status(session, "rep1", "active")
        add_log(session, "rep1", "正在融合审计意见生成深度报告...")
        detailed_resp = await call_detailed_ai_new(active_settings, scene, fused_context)
        session.full_result = detailed_resp["choices"][0]["message"]["content"]
        add_log(session, "rep1", f"深度报告生成成功 ({len(session.full_result)} 字符)")
        add_status(session, "rep1", "success")
        ensure_not_stopped(session)

        # AI-4: 精简报告
        add_status(session, "rep2", "active")
        add_log(session, "rep2", "生成精简老板视图...")
        simplified_resp = await call_simplified_ai(active_settings, session.full_result)
        session.result = simplified_resp["choices"][0]["message"]["content"]
        add_log(session, "rep2", f"任务全部完成！(精简报告 {len(session.result)} 字符)")
        add_status(session, "rep2", "success")

        with session.runtime_lock:
            session.status = "completed"
        save_latest_report(session)

    except TaskAbortedError as e:
        with session.runtime_lock:
            session.status = "aborted"
            session.error_message = str(e)
        add_log(session, "system", f"⚠️ {str(e)}")

    except Exception as e:
        error_msg = "多文件分析失败，请在后台监控流查看 system 节点日志"
        add_log(session, "system", f"发生错误: {str(e)}")
        with session.runtime_lock:
            session.error_message = error_msg
            session.status = "error"


@app.post("/api/run")
async def run(request: Request, background_tasks: BackgroundTasks, x_fzt_key: Optional[str] = Header(default=None)):
    session = resolve_session(x_fzt_key)
    data = await request.json()
    files = data.get("files", [])
    user_settings = data.get("settings")

    with session.runtime_lock:
        if session.status == "running":
            raise HTTPException(status_code=400, detail="任务正在运行中")

    decoded_files = []

    for item in files:
        name = item.get("name", "unnamed")
        b64_str = item.get("base64", "")
        if not b64_str:
            continue
        try:
            decoded_bytes = base64.b64decode(b64_str)
        except Exception as e:
            add_log(session, "system", f"Base64 解码失败 ({name}): {str(e)}")
            continue

        decoded_files.append({"name": name, "bytes": decoded_bytes})

    if not decoded_files:
        raise HTTPException(status_code=400, detail="未提供有效的文件内容")

    save_current_uploads(session, decoded_files)
    background_tasks.add_task(run_multifile_analysis, session, decoded_files)
    return {"status": "started"}


@app.post("/api/analyze")
async def analyze(
    background_tasks: BackgroundTasks,
    files: List[UploadFile] = File(...),
    x_fzt_key: Optional[str] = Header(default=None)
):
    session = resolve_session(x_fzt_key)
    with session.runtime_lock:
        if session.status == "running":
            raise HTTPException(status_code=400, detail="任务正在运行中")

    parsed_files = []
    decoded_files = []
    has_non_json = False

    for i, uploaded_file in enumerate(files):
        filename = uploaded_file.filename or "unnamed"

        try:
            raw = await uploaded_file.read()
            if len(raw) > MAX_UPLOAD_FILE_SIZE:
                raise HTTPException(status_code=400, detail=f"文件过大(>{MAX_UPLOAD_FILE_SIZE_LABEL}): {filename}")

            source_type = infer_source_type(filename)
            if source_type != "json":
                has_non_json = True

            # 先尝试 JSON 解析（兼容旧流程）
            try:
                parsed = json.loads(raw.decode("utf-8-sig"))
                parsed_files.append(parsed)
            except (json.JSONDecodeError, UnicodeDecodeError):
                if source_type == "json":
                    raise HTTPException(status_code=400, detail=f"JSON 解析失败: {filename}")
                # 非 JSON 文件（Excel/CSV），保留原始字节供新管线处理

            decoded_files.append({"name": filename, "bytes": raw})
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"文件读取失败: {filename} - {str(e)}")

    save_current_uploads(session, decoded_files)

    # 统一走新管线（input_adapter 已支持旧药店 JSON 解包）
    background_tasks.add_task(run_multifile_analysis, session, decoded_files)

    return {"status": "started", "pipeline": "multifile"}


@app.post("/api/stop")
def stop(x_fzt_key: Optional[str] = Header(default=None)):
    session = resolve_session(x_fzt_key)
    with session.runtime_lock:
        if session.status == "running":
            session.force_stop = True
            session.status = "aborted"
            session.error_message = "任务被用户强行终止。"
            add_log(session, "system", "⚠️ 用户强制终止了任务！")
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
