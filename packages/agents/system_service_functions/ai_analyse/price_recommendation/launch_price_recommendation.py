"""
商品价格推荐功能 (ai_analyse/price_recommendation/launch_price_recommendation)

功能描述：
针对特定的商品，分析其门店销售历史数据及竞品/市场价格，进行 AI 价格定位分析与销量拟合，产出价格调整建议。

参数说明：
- file_names (list[str]): 必须。相关的销售数据、市场竞品价格表或属性配置文件列表（通常路径为 "chatbot/files/[stored_name]"，可以直接传入相对路径）。
- service_token (str): 必须。用户确认授权后由 get_user_service_token 返回的客服 token。
- product_name (str): 必须。需要进行价格推荐的目标商品名称。
- candidate_count (int): 可选。推荐的价格候选数量。默认值为 3。
- reasoning_effort (str): 可选。AI 的推理力度。可选值为 "low", "medium", "high"，默认值为 "high"。
- api_base_url (str): 可选。后端 API 地址，默认 "http://localhost:3000"。

示例参数：
{
  "file_names": ["chatbot/files/sales_data.xlsx"],
  "service_token": "serv_xxx_xxxxxx_random",
  "product_name": "感康",
  "candidate_count": 3,
  "reasoning_effort": "high"
}
"""

import json

import httpx

from packages.agents.system_service_functions.ai_analyse._api_client import api_error_message


DEFAULT_API_BASE_URL = "http://localhost:3000"


def run(ws, params: dict, llm_preset: dict) -> str:
    file_names = params.get("file_names", [])
    if not file_names:
        return "错误：未指定需要分析的文件名列表（file_names）。"

    service_token = str(params.get("service_token") or params.get("token") or "").strip()
    if not service_token:
        return "错误：缺少 service_token。请先调用 get_user_service_token 获取用户授权。"

    product_name = params.get("product_name", "")
    if not product_name:
        return "错误：未指定目标商品名称（product_name）。"

    candidate_count = params.get("candidate_count", 3)
    reasoning_effort = params.get("reasoning_effort", "high")
    base_url = str(params.get("api_base_url") or DEFAULT_API_BASE_URL).rstrip("/")

    upload_files = []
    for f in file_names:
        try:
            file_path = ws.resolve_read(f)
        except Exception:
            file_path = ws.write_root / f

        if not file_path.exists():
            return f"错误：未找到文件 '{f}'。请确认文件已上传且路径正确。"

        upload_files.append(("files", (file_path.name, file_path.read_bytes(), "application/octet-stream")))

    data = {
        "productName": product_name,
        "candidateCount": str(candidate_count),
        "reasoningEffort": reasoning_effort,
    }

    try:
        with httpx.Client(timeout=httpx.Timeout(180.0)) as client:
            response = client.post(
                f"{base_url}/api/price-recommendations",
                headers={"X-Auth-Token": service_token},
                data=data,
                files=upload_files,
            )
    except Exception as e:
        return f"错误：请求价格推荐 API 失败: {str(e)}"

    if response.status_code >= 400:
        return f"错误：价格推荐 API 返回失败，HTTP {response.status_code}: {api_error_message(response)}"

    try:
        payload = response.json()
    except Exception:
        payload = {"raw": response.text}

    return (
        "价格推荐任务已通过正式 API 提交。\n"
        f"提交结果：{json.dumps(payload, ensure_ascii=False, indent=2)}\n"
        "后续可使用同一个客服 token 查询 /api/price-recommendations/status、/api/price-recommendations/logs 或 /api/price-recommendations/stream 查看进度。"
    )
