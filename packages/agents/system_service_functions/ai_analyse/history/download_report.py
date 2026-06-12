"""
下载指定运行批次的输出产物 ZIP

功能说明：
- 调用 `/api/reports/download`
- 把返回的 ZIP 保存到 chatbot 工作区，供后续查看或转发

参数说明：
- service_token (str): 必须。客服 token。
- run_id (str): 必须。要下载的运行批次 ID。
- api_base_url (str): 可选。后端地址，默认 "http://localhost:3000"。
"""

import httpx

from packages.agents.system_service_functions.ai_analyse._api_client import (
    api_base_url,
    api_error_message,
    auth_headers,
    save_download_to_workspace,
)


def run(ws, params: dict, llm_preset: dict) -> str:
    run_id = str(params.get("run_id") or "").strip()
    if not run_id:
        return "错误：缺少 run_id。"

    try:
        headers = auth_headers(params)
    except ValueError as e:
        return f"错误：{str(e)}"

    try:
        with httpx.Client(timeout=httpx.Timeout(180.0)) as client:
            response = client.get(
                f"{api_base_url(params)}/api/reports/download",
                headers=headers,
                params={"run_id": run_id},
            )
    except Exception as e:
        return f"错误：请求 /api/reports/download 失败: {str(e)}"

    if response.status_code >= 400:
        return f"错误：/api/reports/download 返回失败，HTTP {response.status_code}: {api_error_message(response)}"

    target = save_download_to_workspace(ws, response, f"report_{run_id}.zip")
    return f"已下载报告压缩包到 {target.as_posix()}"
