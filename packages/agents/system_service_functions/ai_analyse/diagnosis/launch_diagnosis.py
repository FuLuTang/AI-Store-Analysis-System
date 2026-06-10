"""
门店经营诊断

参数说明：
- file_names (list[str]): 必须。需要诊断的文件名称列表。文件必须已经上传到 Chatbot 系统（通常路径为 "chatbot/files/[stored_name]"，可以直接传入相对路径）。
- service_token (str): 必须。用户确认授权后由 get_user_service_token 返回的客服 token。
- reasoning_effort (str): 可选。AI 的推理力度。可选值为 "low", "medium", "high"，默认值为 "medium"。
- analysis_params (str): 可选。额外的分析指令或参数约束。
- api_base_url (str): 可选。后端 API 地址，默认 "http://localhost:3000"。

示例参数：
{
  "file_names": ["chatbot/files/sales_data.xlsx"],
  "service_token": "serv_xxx_xxxxxx_random",
  "reasoning_effort": "high"
}
"""

import json

import httpx

from packages.agents.system_service_functions.ai_analyse._api_client import api_base_url, api_error_message


DEFAULT_API_BASE_URL = "http://localhost:3000"


def _api_payload_message(response: httpx.Response) -> str:
    try:
        payload = response.json()
    except Exception:
        payload = response.text
    return json.dumps(payload, ensure_ascii=False) if not isinstance(payload, str) else payload


def run(ws, params: dict, llm_preset: dict) -> str:
    file_names = params.get("file_names", [])
    if not file_names:
        return "错误：未指定需要分析的文件名列表（file_names）。"

    service_token = str(params.get("service_token") or params.get("token") or "").strip()
    if not service_token:
        return "错误：缺少 service_token。请先调用 get_user_service_token 获取用户授权。"

    reasoning_effort = params.get("reasoning_effort", "medium")
    analysis_params = params.get("analysis_params", "")
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

    headers = {"X-Auth-Token": service_token}

    try:
        with httpx.Client(timeout=httpx.Timeout(180.0)) as client:
            if analysis_params:
                params_response = client.put(
                    f"{base_url}/api/analysis-params",
                    headers=headers,
                    json={"analysis_params": analysis_params},
                )
                if params_response.status_code >= 400:
                    return f"错误：设置分析参数失败，HTTP {params_response.status_code}: {api_error_message(params_response)}"

            response = client.post(
                f"{base_url}/api/analyze",
                headers=headers,
                data={"reasoningEffort": reasoning_effort},
                files=upload_files,
            )
    except Exception as e:
        return f"错误：请求系统诊断 API 失败: {str(e)}"

    if response.status_code >= 400:
        return f"错误：系统诊断 API 返回失败，HTTP {response.status_code}: {api_error_message(response)}"

    try:
        payload = response.json()
    except Exception:
        payload = {"raw": response.text}

    return (
        "系统诊断任务已通过正式 API 提交。\n"
        f"提交结果：{json.dumps(payload, ensure_ascii=False, indent=2)}\n"
        "后续可使用同一个客服 token 查询 /api/status、/api/logs 或 /api/stream 查看进度。"
    )
