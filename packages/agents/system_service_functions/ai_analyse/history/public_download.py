"""
下载公开分享报告 ZIP

功能说明：
- 调用 `/api/reports/public/download`
- 把公开分享 ZIP 保存到 chatbot 工作区

参数说明：
- run_id (str): 必须。公开分享的运行批次 ID。
- sign (str): 必须。公开分享签名。
- api_base_url (str): 可选。后端地址，默认 "http://localhost:3000"。
"""

import httpx

from packages.agents.system_service_functions.ai_analyse._api_client import (
    api_base_url,
    api_error_message,
    save_download_to_workspace,
)


def run(ws, params: dict, llm_preset: dict) -> str:
    run_id = str(params.get("run_id") or "").strip()
    sign = str(params.get("sign") or "").strip()
    if not run_id or not sign:
        return "错误：缺少 run_id 或 sign。"

    try:
        with httpx.Client(timeout=httpx.Timeout(180.0)) as client:
            response = client.get(
                f"{api_base_url(params)}/api/reports/public/download",
                params={"run_id": run_id, "sign": sign},
            )
    except Exception as e:
        return f"错误：请求 /api/reports/public/download 失败: {str(e)}"

    if response.status_code >= 400:
        return f"错误：/api/reports/public/download 返回失败，HTTP {response.status_code}: {api_error_message(response)}"

    target = save_download_to_workspace(ws, response, f"public_report_{run_id}.zip")
    return f"已下载公开报告压缩包到 {target.as_posix()}"
