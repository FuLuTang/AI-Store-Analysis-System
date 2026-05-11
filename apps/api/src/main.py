import os
import json
import asyncio
import time
from typing import List, Optional
from fastapi import FastAPI, Request, BackgroundTasks, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from dotenv import load_dotenv
from pathlib import Path
import sys

# 路径处理
ROOT_DIR = Path(__file__).parent.parent.parent.parent
sys.path.append(str(ROOT_DIR))

from packages.core.cleaner import clean_data, merge_hot_products, merge_hot_top500
from packages.core.metrics import (
    normalize_overview_rows, normalize_hot_products,
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

state = AppState()

def add_log(node_id: str, message: str, status: str = "processing"):
    log_entry = {
        "type": "log",
        "nodeId": node_id,
        "message": message,
        "status": status,
        "time": time.strftime("%H:%M:%S")
    }
    state.logs.append(log_entry)
    print(f"[{log_entry['time']}] {node_id}: {message}")

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
    # 发送重置信号
    yield f"data: {json.dumps({'type': 'reset'})}\n\n"
    
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
    state.logs = []
    state.error_message = ""
    state.result = None
    state.full_result = None
    
    settings = user_settings or state.config
    
    try:
        # 1. Cleaner
        add_log("clean", "开始清洗数据...")
        cleaned_list = []
        for f in files_data:
            c = clean_data(f)
            if c: cleaned_list.append(c)
        
        # 合并处理
        hot_merged = merge_hot_products(files_data)
        top500_merged = merge_hot_top500(files_data)
        if hot_merged: cleaned_list.append(hot_merged)
        if top500_merged: cleaned_list.append(top500_merged)
        
        cleaned_texts = [json.dumps(c, ensure_ascii=False, indent=2) for c in cleaned_list]
        add_log("clean", f"完成清洗，共 {len(cleaned_list)} 个有效模块", "success")

        # 2. Metrics
        add_log("alg1", "启动算法引擎计算核心指标...")
        overview_day = next((f for f in files_data if f.get("page", {}).get("module") == "business_overview" and f.get("page", {}).get("viewType") == "day"), None)
        o2o_day = next((f for f in files_data if f.get("page", {}).get("module") == "o2o_business_summary"), None)
        
        m_results = {}
        if overview_day:
            rows = normalize_overview_rows(overview_day)
            m_results["calcRevenueChange"] = calc_revenue_change(rows)
            m_results["prepareGrowthDecomposition"] = prepare_growth_decomposition(rows)
            m_results["calcChannelMix"] = calc_channel_mix(overview_day.get("sourceDistribution", {}))
            
            if o2o_day:
                o2o_rev = o2o_day.get("businessTable", {}).get("rows", [{}])[0].get("total_revenue", 0)
                m_results["calcO2OvsTotal"] = calc_o2o_vs_total(o2o_rev, rows[0]["revenue"] if rows else 0)

        anomaly_summary = prepare_anomaly_summary(m_results)
        add_log("alg1", f"算法计算完成，发现 {anomaly_summary['totalAlerts']} 项异常", "success")

        # 3. AI Initial Call
        if not settings.get("apiKey"):
            add_log("api", "未检测到 API Key，进入模拟模式...", "simulated")
            await asyncio.sleep(2)
            state.full_result = "# 模拟诊断报告\n\n这是一个模拟生成的报告，因为没有配置 API Key。"
            state.result = json.dumps({
                "health_status": "模拟运行",
                "overview_text": "系统处于模拟测试模式。",
                "cards": [{"title": "演示问题", "explanation": "这是一个模拟卡片", "suggestion": "请在设置中配置 API Key", "color": "blue"}]
            })
            state.status = "completed"
            return

        add_log("api", f"正在请求 AI 初步诊断 (模型: {settings['model']})...")
        initial_resp = await call_ai(settings, cleaned_texts, {"anomalies": anomaly_summary})
        initial_report = initial_resp["choices"][0]["message"]["content"]
        add_log("api", "初诊报告已生成", "success")

        # 4. Error Review
        add_log("review", "启动逻辑审计复核...")
        review_resp = await review_error(settings, initial_report, cleaned_texts)
        review_text = review_resp["choices"][0]["message"]["content"] if isinstance(review_resp, dict) else str(review_resp)
        add_log("review", "审计复核完成", "success")

        # 5. Fusion & Final Report
        add_log("rep1", "正在融合审计意见生成深度报告...")
        fused_context = f"【初级报告】\n{initial_report}\n\n【审计意见】\n{review_text}\n\n【算法日志】\n{json.dumps(anomaly_summary['sortedAlerts'], ensure_ascii=False)}"
        detailed_resp = await call_detailed_ai(settings, fused_context)
        state.full_result = detailed_resp["choices"][0]["message"]["content"]
        add_log("rep1", "深度报告生成成功", "success")

        # 6. Simplified Report
        add_log("rep2", "生成精简老板视图...")
        simplified_resp = await call_simplified_ai(settings, state.full_result)
        state.result = simplified_resp["choices"][0]["message"]["content"]
        add_log("rep2", "任务全部完成！", "success")

        state.status = "completed"

    except Exception as e:
        import traceback
        error_msg = f"发生错误: {str(e)}"
        print(traceback.format_exc())
        add_log("system", error_msg, "error")
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
    state.status = "idle"
    add_log("system", "任务已被用户手动终止", "error")
    return {"status": "ok"}

@app.get("/api/examples")
def get_examples():
    """读取案例文件"""
    example_dir = ROOT_DIR / "简化v1_26.5.7" / "data_cache"
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
