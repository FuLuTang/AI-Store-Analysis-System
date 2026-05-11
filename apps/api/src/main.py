import os
import json
import asyncio
from typing import List
from fastapi import FastAPI, UploadFile, File, Request, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from dotenv import load_dotenv

# 导入我们迁移的 Python 模块
import sys
from pathlib import Path
# 将项目根目录添加到路径，以便导入 packages
sys.path.append(str(Path(__file__).parent.parent.parent.parent))

from packages.core.cleaner import clean_data, merge_hot_products, merge_hot_top500
from packages.core.metrics import (
    normalize_overview_rows, normalize_hot_products,
    calc_channel_mix, calc_revenue_change, calc_o2o_vs_total,
    calc_consecutive_change, calc_gross_margin_trend,
    calc_product_stability, calc_high_rank_stockout_alert,
    prepare_growth_decomposition, prepare_sales_quality_check,
    prepare_member_health_check, prepare_stockout_loss_estimate,
    prepare_channel_risk_assessment, prepare_anomaly_summary
)
from packages.ai.ai_caller import call_ai, call_detailed_ai, call_simplified_ai
from packages.ai.error_reviewer import review_error

load_dotenv()

app = FastAPI(title="福州门店 AI 分析系统 API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

STORAGE_DIR = Path("storage")
STORAGE_DIR.mkdir(exist_ok=True)
UPLOAD_DIR = STORAGE_DIR / "uploads"
UPLOAD_DIR.mkdir(exist_ok=True)

# 内存中存储任务状态（生产环境建议使用 Redis）
jobs = {}

@app.get("/api/health")
def health_check():
    return {"status": "ok", "engine": "FastAPI/Python"}

@app.get("/api/config")
def get_config():
    return {
        "hasKey": bool(os.getenv("OPENAI_API_KEY")),
        "baseUrl": os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1"),
        "model": os.getenv("OPENAI_MODEL", "gpt-4-turbo")
    }

async def log_streamer(job_id: str):
    """SSE 日志流"""
    if job_id not in jobs:
        yield f"data: {json.dumps({'error': 'Job not found'})}\n\n"
        return

    job = jobs[job_id]
    last_idx = 0
    while True:
        if last_idx < len(job["logs"]):
            for i in range(last_idx, len(job["logs"])):
                yield f"data: {json.dumps(job['logs'][i], ensure_ascii=False)}\n\n"
            last_idx = len(job["logs"])
        
        if job["status"] in ["completed", "failed"]:
            # 发送最后一条完成消息
            yield f"data: {json.dumps({'type': 'done', 'report': job.get('report')}, ensure_ascii=False)}\n\n"
            break
        await asyncio.sleep(0.5)

@app.get("/api/stream/{job_id}")
async def stream_logs(job_id: str):
    return StreamingResponse(log_streamer(job_id), media_type="text/event-stream")

async def run_analysis_task(job_id: str, files_content: List[dict]):
    job = jobs[job_id]
    def add_log(node, msg, status="processing"):
        job["logs"].append({"node": node, "message": msg, "status": status, "time": asyncio.get_event_loop().time()})

    try:
        # 1. 数据清洗
        add_log("cleaner", "开始清洗上传的 JSON 数据...")
        raw_jsons = [json.loads(f["content"]) for f in files_content]
        cleaned_list = []
        for r in raw_jsons:
            c = clean_data(r)
            if c: cleaned_list.append(c)
        
        # 特殊处理合并
        hot_merged = merge_hot_products(raw_jsons)
        top500_merged = merge_hot_top500(raw_jsons)
        
        if hot_merged: cleaned_list.append(hot_merged)
        if top500_merged: cleaned_list.append(top500_merged)
        
        cleaned_texts = [json.dumps(c, ensure_ascii=False, indent=2) for c in cleaned_list]
        add_log("cleaner", f"成功清洗 {len(cleaned_list)} 个数据模块", "success")

        # 2. 算法指标计算
        add_log("metrics", "启动算法引擎计算核心指标...")
        # 寻找必要数据源
        overview_day = next((r for r in raw_jsons if r.get("page", {}).get("module") == "business_overview" and r.get("page", {}).get("viewType") == "day"), None)
        overview_month = next((r for r in raw_jsons if r.get("page", {}).get("module") == "business_overview" and r.get("page", {}).get("viewType") == "month"), None)
        o2o_day = next((r for r in raw_jsons if r.get("page", {}).get("module") == "o2o_business_summary"), None)
        
        day_rows = normalize_overview_rows(overview_day) if overview_day else []
        
        # 计算一组指标
        m_results = {}
        if overview_day:
            m_results["calcChannelMix"] = calc_channel_mix(overview_day.get("sourceDistribution", {}))
            m_results["calcRevenueChange"] = calc_revenue_change(day_rows)
            m_results["prepareGrowthDecomposition"] = prepare_growth_decomposition(day_rows)
        
        if o2o_day and overview_day:
            o2o_rev = o2o_day.get("businessTable", {}).get("rows", [{}])[0].get("total_revenue", 0)
            m_results["calcO2OvsTotal"] = calc_o2o_vs_total(o2o_rev, day_rows[0]["revenue"] if day_rows else 0)

        anomaly_summary = prepare_anomaly_summary(m_results)
        add_log("metrics", f"指标计算完成，检测到 {anomaly_summary['totalAlerts']} 个异常项", "success")

        # 3. AI 初诊
        add_log("ai_caller", "请求 AI 进行初步经营诊断...")
        ai_settings = {
            "apiKey": os.getenv("OPENAI_API_KEY"),
            "baseUrl": os.getenv("OPENAI_BASE_URL"),
            "model": os.getenv("OPENAI_MODEL")
        }
        initial_resp = await call_ai(ai_settings, cleaned_texts, {"anomalies": anomaly_summary})
        initial_report = initial_resp["choices"][0]["message"]["content"]
        add_log("ai_caller", "初级诊断报告生成成功", "success")

        # 4. 错误评审
        add_log("error_reviewer", "启动数据审计专家进行逻辑复核...")
        review_resp = await review_error(ai_settings, initial_report, cleaned_texts)
        review_text = review_resp["choices"][0]["message"]["content"] if isinstance(review_resp, dict) else str(review_resp)
        add_log("error_reviewer", "错误评审完成", "success")

        # 5. 详细报告重写
        add_log("fusion", "正在融合所有维度信息生成深度报告...")
        fused_context = f"【初级报告】\n{initial_report}\n\n【评审意见】\n{review_text}\n\n【指标异常日志】\n{json.dumps(anomaly_summary['sortedAlerts'], ensure_ascii=False)}"
        detailed_resp = await call_detailed_ai(ai_settings, fused_context)
        final_report = detailed_resp["choices"][0]["message"]["content"]
        add_log("fusion", "深度诊断报告已就绪", "success")

        # 6. 生成老板视图
        add_log("simplified", "正在生成精简老板视图...")
        simplified_resp = await call_simplified_ai(ai_settings, final_report)
        simplified_data = json.loads(simplified_resp["choices"][0]["message"]["content"])
        add_log("simplified", "精简视图生成成功", "success")

        job["report"] = {
            "full": final_report,
            "simplified": simplified_data,
            "metrics": m_results,
            "anomalies": anomaly_summary
        }
        job["status"] = "completed"

    except Exception as e:
        import traceback
        error_msg = f"任务执行出错: {str(e)}\n{traceback.format_exc()}"
        print(error_msg)
        add_log("system", error_msg, "failed")
        job["status"] = "failed"

@app.post("/api/run")
async def run_analysis(request: Request, background_tasks: BackgroundTasks):
    data = await request.json()
    job_id = f"job_{int(asyncio.get_event_loop().time())}"
    jobs[job_id] = {"status": "running", "logs": [], "report": None}
    
    background_tasks.add_task(run_analysis_task, job_id, data.get("files", []))
    return {"jobId": job_id}

@app.post("/api/stop/{job_id}")
def stop_job(job_id: str):
    if job_id in jobs:
        jobs[job_id]["status"] = "failed"
        jobs[job_id]["logs"].append({"node": "system", "message": "用户手动终止任务", "status": "failed"})
    return {"status": "ok"}

# 挂载前端静态文件（生产环境下，或开发时预览）
# app.mount("/", StaticFiles(directory="apps/web/public", html=True), name="static")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=3001)
