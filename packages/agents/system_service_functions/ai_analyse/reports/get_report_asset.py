"""
获取历史运行报告中的单个资源附件。

功能说明：
- 调用 `/api/reports/{run_id}/assets/{filename}`
- 把资源文件保存到 chatbot 工作区

参数说明：
- service_token (str): 必须。客服 token。
- run_id (str): 必须。运行批次 ID。
- filename (str): 必须。资源文件名，例如图表图片名。
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
    filename = str(params.get("filename") or "").strip()
    if not run_id or not filename:
        return "错误：缺少 run_id 或 filename。"

    try:
        headers = auth_headers(params)
    except ValueError as e:
        return f"错误：{str(e)}"

    try:
        with httpx.Client(timeout=httpx.Timeout(180.0)) as client:
            response = client.get(
                f"{api_base_url(params)}/api/reports/{run_id}/assets/{filename}",
                headers=headers,
            )
    except Exception as e:
        return f"错误：请求 /api/reports/{run_id}/assets/{filename} 失败: {str(e)}"

    if response.status_code >= 400:
        return f"错误：/api/reports/{run_id}/assets/{filename} 返回失败，HTTP {response.status_code}: {api_error_message(response)}"

    target = save_download_to_workspace(ws, response, filename)
    return f"已保存资源文件到 {target.as_posix()}"
