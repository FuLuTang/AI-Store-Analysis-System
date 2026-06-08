"""
商品价格推荐功能 (ai_analyse/launch_price_recommendation)

功能描述：
针对特定的商品，分析其门店销售历史数据及竞品/市场价格，进行 AI 价格定位分析与销量拟合，产出价格调整建议。

参数说明：
- file_names (list[str]): 必须。相关的销售数据、市场竞品价格表或属性配置文件列表（通常路径为 "chatbot/files/[stored_name]"，可以直接传入相对路径）。
- product_name (str): 必须。需要进行价格推荐的目标商品名称。
- candidate_count (int): 可选。推荐的价格候选数量。默认值为 3。
- reasoning_effort (str): 可选。AI 的推理力度。可选值为 "low", "medium", "high"，默认值为 "high"。

示例参数：
{
  "file_names": ["chatbot/files/sales_data.xlsx"],
  "product_name": "感康",
  "candidate_count": 3,
  "reasoning_effort": "high"
}
"""

import uuid
import json
from pathlib import Path
from packages.price_recommendation.service import run_price_workflow

def run(ws, params: dict, llm_preset: dict) -> str:
    # 1. 解析参数
    file_names = params.get("file_names", [])
    if not file_names:
        return "错误：未指定需要分析的文件名列表（file_names）。"
        
    product_name = params.get("product_name", "")
    if not product_name:
        return "错误：未指定目标商品名称（product_name）。"
        
    candidate_count = params.get("candidate_count", 3)
    reasoning_effort = params.get("reasoning_effort", "high")

    # 复用/覆盖 llm_preset 配置
    active_preset = llm_preset.copy()
    active_preset["reasoningEffort"] = reasoning_effort
    if "call" in active_preset:
        active_preset["call"] = active_preset["call"].copy()
        active_preset["call"]["reasoningEffort"] = reasoning_effort

    decoded_files = []
    for f in file_names:
        try:
            # 优先通过 workspace.resolve_read 找文件
            file_path = ws.resolve_read(f)
        except Exception:
            file_path = ws.write_root / f
        
        if not file_path.exists():
            return f"错误：未找到文件 '{f}'。请确认文件已上传且路径正确。"
        
        # 价格推荐底层期望的结构为 [{"name": str, "bytes": bytes}]，与诊断稍有差异
        decoded_files.append({
            "name": file_path.name,
            "bytes": file_path.read_bytes()
        })

    # 2. 为本次价格推荐运行创建独立的隔离工作区
    run_id = uuid.uuid4().hex[:12]
    price_ws_dir = ws.write_root / "runs" / "price_recommendation" / run_id
    price_ws_dir.mkdir(parents=True, exist_ok=True)

    # 3. 执行价格推荐 workflow (同步方法，无需 asyncio 循环)
    try:
        print(f"开始执行商品价格推荐任务，run_id={run_id}，商品={product_name} ...")
        result, summary = run_price_workflow(
            decoded_files=decoded_files,
            product_name=product_name,
            candidate_count=candidate_count,
            workspace_dir=price_ws_dir,
            llm_preset=active_preset,
            emit_log=lambda nid, payload: print(f"[price-{run_id}] [{nid}] {payload}"),
            check_aborted=None
        )
    except Exception as e:
        return f"错误：价格推荐任务执行过程中发生异常: {str(e)}"

    # 4. 返回结果
    if summary:
        return f"价格推荐分析完成。报告已生成，以下是推荐内容：\n\n{summary}"
    else:
        return f"价格推荐分析完成。推荐结果：\n{json.dumps(result, ensure_ascii=False, indent=2)}"
