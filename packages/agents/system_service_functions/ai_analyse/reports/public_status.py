"""
查询公开分享报告的状态。

功能说明：
- 调用 `/api/reports/public/status`
- 返回公开报告的状态、结果和账户展示名

参数说明：
- run_id (str): 必须。公开分享的运行批次 ID。
- sign (str): 必须。公开分享签名。
- api_base_url (str): 可选。后端地址，默认 "http://localhost:3000"。
"""

import httpx

from packages.agents.system_service_functions.ai_analyse._api_client import api_base_url, api_error_message, pretty_json, response_payload


def run(ws, params: dict, llm_preset: dict) -> str:
    run_id = str(params.get("run_id") or "").strip()
    sign = str(params.get("sign") or "").strip()
    if not run_id or not sign:
        return "错误：缺少 run_id 或 sign。"

    try:
        with httpx.Client(timeout=httpx.Timeout(30.0)) as client:
            response = client.get(
                f"{api_base_url(params)}/api/reports/public/status",
                params={"run_id": run_id, "sign": sign},
            )
    except Exception as e:
        return f"错误：请求 /api/reports/public/status 失败: {str(e)}"

    if response.status_code >= 400:
        return f"错误：/api/reports/public/status 返回失败，HTTP {response.status_code}: {api_error_message(response)}"

    return pretty_json(response_payload(response))
