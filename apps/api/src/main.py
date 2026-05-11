import os
import json
import asyncio
import time
from typing import List, Optional
from fastapi import FastAPI, Request, BackgroundTasks, HTTPException
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

load_dotenv()

app = FastAPI(title="福州门店 AI 分析系统")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# 全局状态存储（简化版）
class AppState:
    def __init__(self):
        self.status = "idle"  # idle, running, completed, error
        self.error_message = ""
        self.logs = []
        self.result = None      # 精简报告 (JSON string)
        self.full_result = None # 完整报告 (Markdown string)
        self.config = {
            "baseUrl": os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1"),
            "apiKey": os.getenv("OPENAI_API_KEY", ""),
            "model": os.getenv("OPENAI_MODEL", "gpt-5.4-mini")
        }
        self.force_stop = False

state = AppState()
DEFAULT_TALLY = {"pass": 0, "attention": 0, "warning": 0, "uncountable": 0}
STATUS_ICON_MAP = {"warning": "🔴", "attention": "🟡", "uncountable": "⚪"}

class TaskAbortedError(Exception):
    pass


def _now_time():
    return time.strftime("%H:%M:%S")


def emit_event(event_type: str, payload: dict):
    event = {"type": event_type, "time": _now_time(), **payload}
    state.logs.append(event)
    return event


def reset_events():
    state.logs = []
    emit_event("reset", {})


def add_status(node_id: str, status: str):
    emit_event("status", {"nodeId": node_id, "status": status})


def add_log(node_id: str, message: str):
    log_entry = emit_event("log", {"nodeId": node_id, "message": message})
    print(f"[{log_entry['time']}] {node_id}: {message}")


def add_progress(node_id: str, current: int, total: int):
    emit_event("progress", {"nodeId": node_id, "current": current, "total": total})


def add_tally(node_id: str, tally: dict):
    emit_event("tally", {"nodeId": node_id, "tally": tally})


def ensure_not_stopped():
    if state.force_stop or state.status == "aborted":
        raise TaskAbortedError("任务被用户强制终止。")

@app.get("/api/health")
def health():
    return {"status": "ok"}

@app.get("/api/config")
def get_config():
    return {
        "baseUrl": state.config["baseUrl"],
        "apiKey": state.config["apiKey"],
        "model": state.config["model"],
        "hasKey": bool(state.config["apiKey"])
    }

@app.post("/api/config")
async def save_config(request: Request):
    data = await request.json()
    state.config.update(data)
    return {"status": "ok"}

@app.get("/api/status")
def get_status():
    return {
        "status": state.status,
        "errorMessage": state.error_message,
        "result": state.result,
        "fullResult": state.full_result
    }

async def sse_generator():
    """SSE 日志推送，适配前端 EventSource('/api/stream')"""
    last_idx = 0
    yield f"data: {json.dumps({'type': 'reset', 'time': _now_time()})}\n\n"
    
    while True:
        if last_idx < len(state.logs):
            for i in range(last_idx, len(state.logs)):
                yield f"data: {json.dumps(state.logs[i], ensure_ascii=False)}\n\n"
            last_idx = len(state.logs)
        
        if state.status in ["completed", "error", "idle"] and last_idx >= len(state.logs):
            # 这里的逻辑可以让连接保持一会儿或者结束
            # 前端并没有处理关闭，所以我们可以继续循环或等待
            pass
            
        await asyncio.sleep(0.5)

@app.get("/api/stream")
async def stream():
    return StreamingResponse(sse_generator(), media_type="text/event-stream")

async def run_analysis_task(files_data: List[dict], user_settings: Optional[dict]):
    state.status = "running"
    state.force_stop = False
    state.error_message = ""
    state.result = None
    state.full_result = None
    reset_events()
    
    settings = user_settings or state.config
    
    try:
        # 1) Input
        add_status("input", "active")
        add_log("input", f"收到 {len(files_data)} 个 JSON 文件")
        for i, f in enumerate(files_data):
            page = f.get("page", {})
            add_log("input", f"  [{i}] {page.get('module', '?')} - {page.get('title', '')}")
        add_status("input", "success")
        ensure_not_stopped()

        # 2) Clean
        add_status("clean", "active")
        add_log("clean", "开始清洗数据...")
        cleaned_list = []
        for f in files_data:
            ensure_not_stopped()
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
        add_log("clean", f"完成清洗，共 {len(cleaned_list)} 个有效模块")
        add_status("clean", "success")
        ensure_not_stopped()

        # 3) Alg1
        add_status("alg1", "active")
        add_log("alg1", "启动算法引擎，加载并整理数据源...")
        overview_day = next((f for f in files_data if f.get("page", {}).get("module") == "business_overview" and f.get("page", {}).get("viewType") == "day"), None)
        o2o_day = next((f for f in files_data if f.get("page", {}).get("module") == "o2o_business_summary"), None)

        rows = normalize_overview_rows(overview_day) if overview_day else []
        source_distribution = overview_day.get("sourceDistribution", {}) if overview_day else {}
        o2o_rows = (o2o_day or {}).get("businessTable", {}).get("rows", [{}]) if o2o_day else [{}]
        o2o_rev = o2o_rows[0].get("total_revenue", 0)
        overview_revenue = rows[0].get("revenue") if rows else None

        add_log("alg1", f"数据源就绪: 概览日={'是' if overview_day else '否'} / O2O日={'是' if o2o_day else '否'}")
        add_status("alg1", "success")
        ensure_not_stopped()

        # 4) Alg2
        add_status("alg2", "active")
        add_log("alg2", "开始逐项计算指标...")
        metric_tasks = [
            ("calcRevenueChange", lambda: calc_revenue_change(rows)),
            ("prepareGrowthDecomposition", lambda: prepare_growth_decomposition(rows)),
            ("calcChannelMix", lambda: calc_channel_mix(source_distribution)),
            ("calcO2OvsTotal", lambda: calc_o2o_vs_total(o2o_rev, overview_revenue)),
        ]
        m_results = {}
        total = len(metric_tasks)
        for i, (name, fn) in enumerate(metric_tasks, start=1):
            ensure_not_stopped()
            result = fn()
            m_results[name] = result
            icon = STATUS_ICON_MAP.get(result.get("status"), "🟢")
            add_log("alg2", f"  [{i}/{total}] {icon} {name} -> {result.get('status', 'unknown')}")
            add_progress("alg2", i, total)
            await asyncio.sleep(0.05)

        add_log("alg2", f"完成: {total} 个指标已计算")
        add_status("alg2", "success")
        ensure_not_stopped()

        # 5) Alg3
        add_status("alg3", "active")
        add_log("alg3", "汇总异常检测结果...")
        anomaly_summary = prepare_anomaly_summary(m_results)
        tally = (anomaly_summary.get("aiPromptData") or {}).get("tally") or DEFAULT_TALLY
        add_tally("alg3", tally)
        add_log("alg3", f"  🟢 pass: {tally.get('pass', 0)}")
        add_log("alg3", f"  🟡 attention: {tally.get('attention', 0)}")
        add_log("alg3", f"  🔴 warning: {tally.get('warning', 0)}")
        if tally.get("uncountable", 0) > 0:
            add_log("alg3", f"  ⚪ uncountable: {tally.get('uncountable', 0)}")
        add_status("alg3", "success")
        ensure_not_stopped()

        # 6) AI Initial Call
        if not settings.get("apiKey"):
            add_status("api", "active")
            add_log("api", "未检测到 API Key，进入模拟模式...")
            await asyncio.sleep(0.3)
            add_status("api", "simulated")

            add_status("output", "active")
            add_log("output", "使用模拟报告")
            add_status("output", "simulated")

            add_status("review", "active")
            add_log("review", "模拟模式：跳过错误评审")
            add_status("review", "simulated")

            add_status("fusion", "active")
            add_log("fusion", "模拟模式：融合默认报告与异常摘要")
            add_status("fusion", "success")

            add_status("rep1", "active")
            state.full_result = "# 模拟诊断报告\n\n这是一个模拟生成的报告，因为没有配置 API Key。"
            add_status("rep1", "simulated")

            add_status("rep2", "active")
            state.result = json.dumps({
                "health_status": "模拟运行",
                "overview_text": "系统处于模拟测试模式。",
                "cards": [{"title": "演示问题", "explanation": "这是一个模拟卡片", "suggestion": "请在设置中配置 API Key", "color": "blue"}]
            }, ensure_ascii=False)
            add_status("rep2", "simulated")
            state.status = "completed"
            return

        add_status("api", "active")
        add_log("api", f"正在请求 AI 初步诊断 (模型: {settings['model']})...")
        initial_resp = await call_ai(settings, cleaned_texts, {"anomalies": anomaly_summary.get("aiPromptData")})
        initial_report = initial_resp["choices"][0]["message"]["content"]
        add_log("api", "初诊报告已生成")
        add_status("api", "success")
        ensure_not_stopped()

        # 7) Output
        add_status("output", "active")
        add_log("output", f"报告内容: {len(initial_report)} 字符")
        add_status("output", "success")
        ensure_not_stopped()

        # 8) Error Review
        add_status("review", "active")
        add_log("review", "启动逻辑审计复核...")
        review_resp = await review_error(settings, initial_report, cleaned_texts)
        review_text = review_resp["choices"][0]["message"]["content"] if isinstance(review_resp, dict) else str(review_resp)
        add_log("review", "审计复核完成")
        add_status("review", "success")
        ensure_not_stopped()

        # 9) Fusion
        add_status("fusion", "active")
        add_log("fusion", "数据融合：合并初级报告、错误评审、异常日志...")
        anomaly_logs_text = json.dumps(anomaly_summary["sortedAlerts"], ensure_ascii=False)
        fused_context = (
            f"【初级报告】\n{initial_report}\n\n"
            f"【审计意见】\n{review_text}\n\n"
            f"【算法日志】\n{anomaly_logs_text}"
        )
        add_status("fusion", "success")
        ensure_not_stopped()

        # 10) Detailed
        add_status("rep1", "active")
        add_log("rep1", "正在融合审计意见生成深度报告...")
        detailed_resp = await call_detailed_ai(settings, fused_context)
        state.full_result = detailed_resp["choices"][0]["message"]["content"]
        add_log("rep1", "深度报告生成成功")
        add_status("rep1", "success")
        ensure_not_stopped()

        # 11) Simplified
        add_status("rep2", "active")
        add_log("rep2", "生成精简老板视图...")
        simplified_resp = await call_simplified_ai(settings, state.full_result)
        state.result = simplified_resp["choices"][0]["message"]["content"]
        add_log("rep2", "任务全部完成！")
        add_status("rep2", "success")

        state.status = "completed"

    except TaskAbortedError as e:
        state.status = "aborted"
        state.error_message = str(e)
        add_log("fusion", f"⚠️ {str(e)}")

    except Exception as e:
        error_msg = "任务执行失败，请在后台监控流查看 system 节点日志"
        add_log("system", f"发生错误: {str(e)}")
        state.error_message = error_msg
        state.status = "error"

@app.post("/api/run")
async def run(request: Request, background_tasks: BackgroundTasks):
    data = await request.json()
    files = data.get("files", [])
    user_settings = data.get("settings")
    
    if state.status == "running":
        raise HTTPException(status_code=400, detail="任务正在运行中")
        
    background_tasks.add_task(run_analysis_task, files, user_settings)
    return {"status": "started"}

@app.post("/api/stop")
def stop():
    if state.status == "running":
        state.force_stop = True
        state.status = "aborted"
        state.error_message = "任务被用户强行终止。"
        add_log("fusion", "⚠️ 用户强制终止了任务！")
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
            except: pass
    
    if not files_content:
        return {"error": "未找到案例文件，请检查目录结构"}
    return {"files": files_content}

# 存储与目录配置
STORAGE_DIR = ROOT_DIR / "storage"
UPLOAD_DIR = STORAGE_DIR / "uploads"
CACHE_DIR = STORAGE_DIR / "cache"

# 确保目录存在
for d in [STORAGE_DIR, UPLOAD_DIR, CACHE_DIR]:
    d.mkdir(parents=True, exist_ok=True)

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
