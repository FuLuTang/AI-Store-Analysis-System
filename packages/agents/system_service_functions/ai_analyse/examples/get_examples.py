"""
读取系统提供的示例数据文件包。

功能说明：
- 调用 `/api/examples`
- 返回系统可直接用于体验的示例文件列表

参数说明：
- api_base_url (str): 可选。后端地址，默认 "http://localhost:3000"。
"""

import httpx

from packages.agents.system_service_functions.ai_analyse._api_client import (
    api_base_url,
    api_error_message,
    pretty_json,
    response_payload,
)


def run(ws, params: dict, llm_preset: dict) -> str:
    try:
        with httpx.Client(timeout=httpx.Timeout(30.0)) as client:
            response = client.get(f"{api_base_url(params)}/api/examples")
    except Exception as e:
        return f"错误：请求 /api/examples 失败: {str(e)}"

    if response.status_code >= 400:
        return f"错误：/api/examples 返回失败，HTTP {response.status_code}: {api_error_message(response)}"

    return pretty_json(response_payload(response))
