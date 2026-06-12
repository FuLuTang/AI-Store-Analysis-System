"""
更新当前用户保存的分析参数约束

功能说明：
- 调用 `/api/analysis-params`
- 写入用户自定义的分析约束文本

参数说明：
- service_token (str): 必须。客服 token。
- analysis_params (str): 必须。要保存的分析约束文本。
- api_base_url (str): 可选。后端地址，默认 "http://localhost:3000"。
"""

import httpx

from packages.agents.system_service_functions.ai_analyse._api_client import (
    api_base_url,
    api_error_message,
    auth_headers,
    pretty_json,
    response_payload,
)


def run(ws, params: dict, llm_preset: dict) -> str:
    raw = str(params.get("analysis_params") or "").strip()
    if not raw:
        return "错误：缺少 analysis_params。"

    try:
        headers = auth_headers(params)
    except ValueError as e:
        return f"错误：{str(e)}"

    try:
        with httpx.Client(timeout=httpx.Timeout(30.0)) as client:
            response = client.put(
                f"{api_base_url(params)}/api/analysis-params",
                headers=headers,
                json={"analysis_params": raw},
            )
    except Exception as e:
        return f"错误：请求 /api/analysis-params 失败: {str(e)}"

    if response.status_code >= 400:
        return f"错误：/api/analysis-params 返回失败，HTTP {response.status_code}: {api_error_message(response)}"

    return pretty_json(response_payload(response))
