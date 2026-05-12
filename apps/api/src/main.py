import os
import json
import asyncio
import time
import shutil
import re
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
from packages.ai.ai_caller import call_ai, call_detailed_ai, call_simplified_ai
from packages.ai.error_reviewer import review_error
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
DEFAULT_TALLY = {"pass": 0, "attention": 0, "warning": 0, "uncountable": 0}
STATUS_ICON_MAP = {"warning": "🔴", "attention": "🟡", "uncountable": "⚪", "pass": "🟢"}
SAFE_UPLOAD_FILENAME = re.compile(r"^[A-Za-z0-9._\-\u4e00-\u9fff]+$")
MAX_UPLOAD_FILE_SIZE = 5 * 1024 * 1024
MAX_UPLOAD_FILE_SIZE_LABEL = f"{MAX_UPLOAD_FILE_SIZE // (1024 * 1024)}MB"
GLOBAL_API_KEY = os.getenv("OPENAI_API_KEY", "")
GLOBAL_API_KEY_LOCK = Lock()


class TaskAbortedError(Exception):
    pass


def get_global_api_key() -> str:
    with GLOBAL_API_KEY_LOCK:
        return GLOBAL_API_KEY


def set_global_api_key(raw_key: str):
    global GLOBAL_API_KEY
    with GLOBAL_API_KEY_LOCK:
        GLOBAL_API_KEY = (raw_key or "").strip()


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
            "baseUrl": os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1"),
            "model": os.getenv("OPENAI_MODEL", "gpt-5.4-mini")
        }

    def _load_profile(self, account_dir: Path) -> dict:
        profile_path = account_dir / "profile.json"
        cfg = self._default_config()
        if profile_path.exists():
            try:
                profile = json.loads(profile_path.read_text(encoding="utf-8"))
                if isinstance(profile, dict):
                    for key in ["baseUrl", "model"]:
                        if key in profile:
                            cfg[key] = profile[key]
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
                "baseUrl": session.config.get("baseUrl", ""),
                "model": session.config.get("model", "")
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
    log_entry = emit_event(session, "log", {"nodeId": node_id, "message": message})
    hash_prefix = session.key_hash[:8]
    logger.info("[%s] %s %s: %s", log_entry["time"], hash_prefix, node_id, message)


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


def save_current_uploads(session: SessionState, files_data: List[dict], filenames: Optional[List[str]] = None):
    _ensure_session_dirs(session)
    current_dir = session.upload_dir / "current"
    tmp_dir = session.upload_dir / ".current_tmp"

    with session.upload_lock:
        if tmp_dir.exists():
            shutil.rmtree(tmp_dir)
        tmp_dir.mkdir(parents=True, exist_ok=True)

        for i, payload in enumerate(files_data):
            source_name = filenames[i] if filenames and i < len(filenames) else None
            if source_name is not None:
                sanitize_upload_filename(source_name, i)
            output_path = tmp_dir / f"file_{i + 1:02d}.json"
            try:
                output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
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
    safe = {}
    for key in ["baseUrl", "apiKey", "model"]:
        value = raw.get(key)
        if isinstance(value, str):
            safe[key] = value.strip()
    return safe


def resolve_session(x_fzt_key: Optional[str], require_key: bool = False) -> SessionState:
    key = (x_fzt_key or "").strip()
    if key:
        try:
            return session_manager.get_session(key, create_if_missing=False)
        except KeyError:
            raise HTTPException(status_code=401, detail="Invalid or expired key")
    if require_key:
        raise HTTPException(status_code=401, detail="Authentication required")
    return session_manager.get_legacy_session()


@app.get("/api/health")
def health():
    return {"status": "ok"}


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
    session = resolve_session(x_fzt_key, require_key=True)
    return {
        "userKey": mask_user_key(session.user_key),
        "config": {
            "baseUrl": session.config.get("baseUrl", ""),
            "model": session.config.get("model", ""),
            "hasKey": bool(get_global_api_key())
        }
    }


@app.post("/api/auth/verify")
def auth_verify(x_fzt_key: Optional[str] = Header(default=None)):
    session = resolve_session(x_fzt_key, require_key=True)
    return {
        "status": "ok",
        "userKey": mask_user_key(session.user_key)
    }


@app.get("/api/config")
def get_config(x_fzt_key: Optional[str] = Header(default=None)):
    session = resolve_session(x_fzt_key, require_key=False)
    api_key = get_global_api_key()
    return {
        "baseUrl": session.config["baseUrl"],
        "apiKey": api_key,
        "model": session.config["model"],
        "hasKey": bool(api_key)
    }


@app.post("/api/config")
async def save_config(request: Request, x_fzt_key: Optional[str] = Header(default=None)):
    session = resolve_session(x_fzt_key, require_key=False)
    data = sanitize_settings(await request.json())
    incoming_api_key = data.pop("apiKey", None)
    if incoming_api_key is not None:
        set_global_api_key(incoming_api_key)
    session.config.update(data)
    session_manager.save_profile(session)
    return {"status": "ok"}


@app.get("/api/status")
def get_status(x_fzt_key: Optional[str] = Header(default=None)):
    session = resolve_session(x_fzt_key, require_key=False)
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
    session = resolve_session(x_fzt_key, require_key=False)
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
    session = resolve_session(x_fzt_key, require_key=False)
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
    incoming_api_key = updates.pop("apiKey", None)
    if incoming_api_key is not None:
        set_global_api_key(incoming_api_key)
    session.config.update(updates)
    active_settings = session.config.copy()
    active_settings["apiKey"] = get_global_api_key()
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


@app.post("/api/run")
async def run(request: Request, background_tasks: BackgroundTasks, x_fzt_key: Optional[str] = Header(default=None)):
    session = resolve_session(x_fzt_key, require_key=False)
    data = await request.json()
    files = data.get("files", [])
    filenames = data.get("filenames")
    user_settings = data.get("settings")

    with session.runtime_lock:
        if session.status == "running":
            raise HTTPException(status_code=400, detail="任务正在运行中")

    save_current_uploads(session, files, filenames if isinstance(filenames, list) else None)
    background_tasks.add_task(run_analysis_task, session, files, user_settings)
    return {"status": "started"}


@app.post("/api/analyze")
async def analyze(
    background_tasks: BackgroundTasks,
    files: List[UploadFile] = File(...),
    x_fzt_key: Optional[str] = Header(default=None)
):
    session = resolve_session(x_fzt_key, require_key=False)
    with session.runtime_lock:
        if session.status == "running":
            raise HTTPException(status_code=400, detail="任务正在运行中")

    parsed_files = []
    filenames = []
    for i, uploaded_file in enumerate(files):
        filename = uploaded_file.filename or "unnamed.json"
        safe_name = sanitize_upload_filename(filename, i)

        try:
            raw = await uploaded_file.read()
            if len(raw) > MAX_UPLOAD_FILE_SIZE:
                raise HTTPException(status_code=400, detail=f"文件过大(>{MAX_UPLOAD_FILE_SIZE_LABEL}): {safe_name}")
            parsed = json.loads(raw.decode("utf-8-sig"))
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"JSON 解析失败: {safe_name} - {str(e)}")

        parsed_files.append(parsed)
        filenames.append(safe_name)

    save_current_uploads(session, parsed_files, filenames)
    background_tasks.add_task(run_analysis_task, session, parsed_files, None)
    return {"status": "started"}


@app.post("/api/stop")
def stop(x_fzt_key: Optional[str] = Header(default=None)):
    session = resolve_session(x_fzt_key, require_key=False)
    with session.runtime_lock:
        if session.status == "running":
            session.force_stop = True
            session.status = "aborted"
            session.error_message = "任务被用户强行终止。"
            add_log(session, "system", "⚠️ 用户强制终止了任务！")
    return {"status": "ok"}


@app.get("/api/examples")
def get_examples():
    """读取案例文件"""
    example_dir = ROOT_DIR / "data" / "samples"
    files_content = []
    if example_dir.exists():
        for f_path in example_dir.glob("*.json"):
            try:
                with open(f_path, 'r', encoding='utf-8') as f:
                    files_content.append(json.load(f))
            except Exception as e:
                logger.warning("案例文件加载失败: %s (%s)", f_path, str(e))

    if not files_content:
        return {"error": "未找到案例文件，请检查目录结构"}
    return {"files": files_content}


# 预创建 legacy 账号目录，确保兼容旧前端流程
legacy_session = session_manager.get_legacy_session()
_ensure_session_dirs(legacy_session)
session_manager.save_profile(legacy_session)

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
