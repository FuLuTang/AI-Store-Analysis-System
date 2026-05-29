import json
import logging
import sys
import time
from pathlib import Path
from typing import Callable, Optional

import httpx
from fastapi import APIRouter, Header, HTTPException, Request
from fastapi.responses import StreamingResponse

from packages.ai.ai_caller import _get_model_settings

logger = logging.getLogger("app.chat_bridge")
if not logger.handlers:
    _handler = logging.StreamHandler(sys.stdout)
    _handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
    logger.addHandler(_handler)
logger.setLevel(logging.INFO)
logger.propagate = False


class ChatBridgeStreamError(Exception):
    def __init__(self, message: str):
        super().__init__(message)
        self.message = message

CHAT_SYSTEM_PROMPT = """你是福州门店 AI 分析系统里的追问助手。

你正在基于一次已经完成的分析产物回答用户的后续问题。

回答规则：
1. 优先依据提供的 summary_short.json 和 summary.md 回答。
2. 如果产物里没有足够信息，就明确说信息不足，不要编造。
3. 默认使用中文，回答直接、清楚、可执行。
4. 如果用户追问某张卡片、某个结论或某个建议，就结合现有产物解释原因、证据和限制。"""


def _preview_text(text: str, limit: int = 180) -> str:
    compact = " ".join(str(text or "").split())
    if len(compact) <= limit:
        return compact
    return compact[:limit] + "..."


def _messages_brief(messages: list[dict]) -> list[dict]:
    return [
        {
            "role": item["role"],
            "chars": len(item["content"]),
            "preview": _preview_text(item["content"]),
        }
        for item in messages
    ]


def _normalize_messages(raw_messages) -> list[dict]:
    if not isinstance(raw_messages, list):
        raise HTTPException(status_code=400, detail="messages 必须是数组")

    messages = []
    for item in raw_messages:
        if not isinstance(item, dict):
            continue
        role = str(item.get("role", "")).strip().lower()
        content = item.get("content", "")
        if role not in {"system", "user", "assistant"}:
            continue
        if not isinstance(content, str):
            content = str(content)
        text = content.strip()
        if not text:
            continue
        messages.append({"role": role, "content": text})

    if not messages:
        raise HTTPException(status_code=400, detail="messages 不能为空")
    return messages


def _resolve_run_dir(account_dir: Path, task_type: str, run_id: str) -> Path:
    primary = account_dir / "runs" / task_type / run_id
    if primary.exists():
        return primary
    fallback = account_dir / "runs" / run_id
    if fallback.exists():
        return fallback
    raise HTTPException(status_code=404, detail="未找到对应的分析批次")


def _read_run_summaries(run_dir: Path) -> tuple[str, str]:
    output_dir = run_dir / "workspace" / "output"
    short_text = ""
    full_text = ""

    short_path = output_dir / "summary_short.json"
    full_path = output_dir / "summary.md"

    if short_path.exists():
        short_text = short_path.read_text(encoding="utf-8").strip()
    if full_path.exists():
        full_text = full_path.read_text(encoding="utf-8").strip()

    if not short_text and not full_text:
        raise HTTPException(status_code=404, detail="当前 run 尚未生成 summary.md 或 summary_short.json")
    return short_text, full_text


def _build_system_content(run_id: str, short_text: str, full_text: str, messages: list[dict]) -> str:
    client_system_texts = [m["content"] for m in messages if m["role"] == "system"]
    parts = [
        CHAT_SYSTEM_PROMPT,
        f"【当前分析批次】\n- run_id: {run_id}",
    ]

    if short_text:
        parts.append(f"【summary_short.json】\n```json\n{short_text}\n```")
    if full_text:
        parts.append(f"【summary.md】\n{full_text}")
    if client_system_texts:
        parts.append("【前端额外系统提示】\n" + "\n\n".join(client_system_texts))

    return "\n\n".join(parts)


async def _stream_chat_completion(base_url: str, api_key: str, payload: dict):
    url = base_url.rstrip("/") + "/chat/completions"
    buffer = ""

    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(180.0)) as client:
            async with client.stream(
                "POST",
                url,
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {api_key}",
                },
                json=payload,
            ) as response:
                if response.status_code != 200:
                    error_bytes = await response.aread()
                    message = error_bytes.decode("utf-8", errors="ignore")[:500]
                    raise ChatBridgeStreamError(f"AI 对话调用失败: {message}")

                async for chunk in response.aiter_text():
                    buffer += chunk
                    lines = buffer.split("\n")
                    buffer = lines.pop()
                    for line in lines:
                        raw = line.strip()
                        if not raw.startswith("data:"):
                            continue
                        data_str = raw[5:].strip()
                        if not data_str or data_str == "[DONE]":
                            continue
                        try:
                            data_obj = json.loads(data_str)
                        except json.JSONDecodeError:
                            continue
                        delta = data_obj.get("choices", [{}])[0].get("delta", {})
                        content = delta.get("content")
                        if content:
                            yield content.encode("utf-8")
    except ChatBridgeStreamError:
        raise
    except Exception as exc:
        raise ChatBridgeStreamError(f"对话流转发失败: {str(exc)}")


def register_chat_routes(
    app,
    *,
    resolve_session: Callable,
    get_llm_preset: Callable,
    normalize_reasoning_effort: Callable,
):
    router = APIRouter()

    @router.post("/api/chat")
    async def chat_stream(request: Request, x_fzt_key: Optional[str] = Header(default=None)):
        t0 = time.perf_counter()
        session = resolve_session(x_fzt_key)
        payload = await request.json()
        if not isinstance(payload, dict):
            raise HTTPException(status_code=400, detail="请求体必须是 JSON 对象")

        run_id = str(payload.get("runId") or session.run_id or "").strip()
        if not run_id:
            raise HTTPException(status_code=400, detail="缺少 runId")

        messages = _normalize_messages(payload.get("messages"))
        reasoning_effort = "medium"
        request_id = f"{run_id}:{int(time.time() * 1000)}"
        logger.info(
            "[chat] stage=web_to_server_received request_id=%s run_id=%s remote=%s payload_keys=%s forced_effort=%s",
            request_id,
            run_id,
            getattr(request.client, "host", "-"),
            sorted(payload.keys()),
            reasoning_effort,
        )
        preset = get_llm_preset(reasoning_effort)
        base_url, api_key, model, upstream_effort = _get_model_settings(preset, "call")
        if not base_url or not api_key or not model:
            raise HTTPException(status_code=500, detail="LLM 配置不完整")

        run_dir = _resolve_run_dir(session.account_dir, session.task_type, run_id)
        short_text, full_text = _read_run_summaries(run_dir)
        system_content = _build_system_content(run_id, short_text, full_text, messages)

        logger.info(
            "[chat] request_id=%s run_id=%s effort=%s model=%s messages=%d short_chars=%d full_chars=%d",
            request_id,
            run_id,
            reasoning_effort,
            model,
            len(messages),
            len(short_text),
            len(full_text),
        )
        logger.info(
            "[chat] request_id=%s message_brief=%s",
            request_id,
            json.dumps(_messages_brief(messages), ensure_ascii=False),
        )
        logger.info(
            "[chat] request_id=%s system_chars=%d system_preview=%s",
            request_id,
            len(system_content),
            _preview_text(system_content, 260),
        )

        upstream_messages = [{"role": "system", "content": system_content}]
        upstream_messages.extend(m for m in messages if m["role"] != "system")

        upstream_payload = {
            "model": model,
            "messages": upstream_messages,
            "stream": True,
        }
        if upstream_effort:
            upstream_payload["reasoning_effort"] = upstream_effort

        logger.info(
            "[chat] stage=server_to_llm_prepare request_id=%s run_id=%s model=%s effort=%s upstream_messages=%d base_url=%s",
            request_id,
            run_id,
            model,
            upstream_effort or reasoning_effort,
            len(upstream_messages),
            base_url,
        )

        async def _logged_stream():
            output_chars = 0
            first_chunk_sent = False
            try:
                logger.info(
                    "[chat] stage=server_to_llm_send request_id=%s run_id=%s",
                    request_id,
                    run_id,
                )
                async for chunk in _stream_chat_completion(base_url, api_key, upstream_payload):
                    if output_chars == 0:
                        logger.info(
                            "[chat] stage=llm_to_server_first_chunk request_id=%s run_id=%s",
                            request_id,
                            run_id,
                        )
                    try:
                        output_chars += len(chunk.decode("utf-8", errors="ignore"))
                    except Exception:
                        pass
                    if not first_chunk_sent:
                        first_chunk_sent = True
                        logger.info(
                            "[chat] stage=server_to_web_first_chunk request_id=%s run_id=%s",
                            request_id,
                            run_id,
                        )
                    yield chunk
                elapsed_ms = round((time.perf_counter() - t0) * 1000, 1)
                logger.info(
                    "[chat] stage=llm_to_server_done request_id=%s run_id=%s elapsed_ms=%s output_chars=%d",
                    request_id,
                    run_id,
                    elapsed_ms,
                    output_chars,
                )
                logger.info(
                    "[chat] stage=server_to_web_done request_id=%s run_id=%s elapsed_ms=%s output_chars=%d",
                    request_id,
                    run_id,
                    elapsed_ms,
                    output_chars,
                )
                logger.info(json.dumps({
                    "kind": "chat_bridge",
                    "request_id": request_id,
                    "run_id": run_id,
                    "reasoning_effort": reasoning_effort,
                    "model": model,
                    "input_messages": _messages_brief(messages),
                    "system_chars": len(system_content),
                    "summary_short_chars": len(short_text),
                    "summary_md_chars": len(full_text),
                    "output_chars": output_chars,
                    "elapsed_ms": elapsed_ms,
                }, ensure_ascii=False))
            except ChatBridgeStreamError as exc:
                elapsed_ms = round((time.perf_counter() - t0) * 1000, 1)
                logger.exception(
                    "[chat] stage=llm_or_bridge_failed request_id=%s run_id=%s elapsed_ms=%s error=%s",
                    request_id,
                    run_id,
                    elapsed_ms,
                    exc.message,
                )
                error_text = f"\n[对话失败] {exc.message}"
                logger.info(
                    "[chat] stage=server_to_web_error_text request_id=%s run_id=%s error_preview=%s",
                    request_id,
                    run_id,
                    _preview_text(error_text),
                )
                yield error_text.encode("utf-8")
            except Exception as exc:
                elapsed_ms = round((time.perf_counter() - t0) * 1000, 1)
                logger.exception(
                    "[chat] stage=unexpected_failed request_id=%s run_id=%s elapsed_ms=%s error=%s",
                    request_id,
                    run_id,
                    elapsed_ms,
                    str(exc),
                )
                error_text = f"\n[对话失败] {str(exc)}"
                logger.info(
                    "[chat] stage=server_to_web_error_text request_id=%s run_id=%s error_preview=%s",
                    request_id,
                    run_id,
                    _preview_text(error_text),
                )
                yield error_text.encode("utf-8")

        return StreamingResponse(
            _logged_stream(),
            media_type="text/plain; charset=utf-8",
        )

    app.include_router(router)
