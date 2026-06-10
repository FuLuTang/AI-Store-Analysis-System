"""
获取当前用户的系统基础 LLM 预设配置状态

功能说明：
- 读取 `/api/config`
- 返回当前 reasoningEffort、可用档位、基础模型地址和是否已配置密钥

参数说明：
- service_token (str): 必须。用户授权后可用于调用用户侧接口的客服 token。
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
    try:
        headers = auth_headers(params)
    except ValueError as e:
        return f"错误：{str(e)}"

    try:
        with httpx.Client(timeout=httpx.Timeout(30.0)) as client:
            response = client.get(f"{api_base_url(params)}/api/config", headers=headers)
    except Exception as e:
        return f"错误：请求 /api/config 失败: {str(e)}"

    if response.status_code >= 400:
        return f"错误：/api/config 返回失败，HTTP {response.status_code}: {api_error_message(response)}"

    payload = response_payload(response)
    return pretty_json(payload)
