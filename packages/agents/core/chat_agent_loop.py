"""chat_agent_loop.py — 账号级聊天 Agent 循环。

核心原则：
  1. 由上游 service/runner 注入完整初始消息，不拼 plan / bootstrap
  2. tool_calls 走原生 OpenAI function calling 格式
  3. reasoning_content 按 DeepSeek 规则处理（有 tool_calls 保留，无则丢弃）
  4. 没有 tool_calls 时自然结束
"""

import json
import logging
import re
import sys
import time
from openai import OpenAI

from .workspace import Workspace
from .tool_converter import available_tool_call_for_agent, build_tool_map
from .agent_loop import _is_retryable, _tool_target, _StreamResult

logger = logging.getLogger("agent.chatbot")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    _handler = logging.StreamHandler(sys.stdout)
    _handler.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
    logger.addHandler(_handler)


class ChatAgentLoop:
    """薄封装：OpenAI SDK 客户端 + messages 管理 + tool 执行。"""

    def __init__(
        self,
        client: OpenAI,
        ws: Workspace,
        llm_preset: dict,
        initial_messages: list[dict],
        emit_log=None,
        emit_status=None,
        check_aborted=None,
    ):
        self.ws = ws
        self._check_aborted = check_aborted

        call_cfg = llm_preset.get("call", {}) if isinstance(llm_preset, dict) else {}
        self.model = call_cfg.get("model") or llm_preset.get("model", "deepseek-chat")
        self.reasoning_effort = call_cfg.get("reasoningEffort") or llm_preset.get("reasoningEffort", "medium")

        call_base = call_cfg.get("baseUrl") or llm_preset.get("baseUrl", "https://api.deepseek.com")
        call_key = call_cfg.get("apiKey") or llm_preset.get("apiKey", "")

        self.client = client or OpenAI(api_key=call_key, base_url=call_base, max_retries=0)

        self._total_input = 0
        self._total_output = 0
        self._total_cache_hit = 0
        self._total_cache_miss = 0
        self.messages: list[dict] = list(initial_messages or [])
        self.tools = available_tool_call_for_agent(ws, task_type="chatbot")
        self._emit_log = emit_log or (lambda nid, msg: None)
        self._emit_status = emit_status or (lambda nid, st: None)
        self._round = 0

        self.tool_map = build_tool_map(
            ws,
            task_type="chatbot",
            emit_log=self._emit_log,
            emit_status=self._emit_status,
            on_finish=None,
        )

    def run(self) -> dict:
        if not self.messages:
            raise ValueError("ChatAgentLoop 需要 initial_messages")

        self._emit_status("chatbot_agent", "active")
        self._emit_log(
            "chatbot_agent",
            {"level": "info", "message": f"🚀 启动 ChatAgent 循环, model={self.model}, tools={len(self.tools)} 个"},
        )
        logger.info("[chat-agent] start model=%s tools=%d", self.model, len(self.tools))

        max_rounds = 50
        try:
            for self._round in range(max_rounds):
                sr = self._call_api()

                if self._check_aborted:
                    try:
                        self._check_aborted()
                    except Exception:
                        self._emit_log("chatbot_agent", {"level": "error", "message": "⛔ 用户已强制停止"})
                        raise

                self.messages.append(self._normalize_assistant_message(sr))

                if sr.usage:
                    u = sr.usage
                    inp = getattr(u, "prompt_tokens", 0) or 0
                    out = getattr(u, "completion_tokens", 0) or 0
                    hit = getattr(u, "prompt_cache_hit_tokens", 0) or 0
                    miss = getattr(u, "prompt_cache_miss_tokens", 0) or 0
                    self._total_input += inp
                    self._total_output += out
                    self._total_cache_hit += hit
                    self._total_cache_miss += miss
                    logger.info(
                        "llm_usage %s",
                        json.dumps(
                            {
                                "report_id": self.ws.report_id,
                                "pipeline": "chatbot",
                                "phase": f"round_{self._round}",
                                "model": self.model,
                                "input_tokens": inp,
                                "output_tokens": out,
                                "cached_input_tokens": hit,
                                "cache_miss_tokens": miss,
                                "cache_hit_ratio": round(hit / max(inp, 1), 3),
                                "reasoning_chars": len(sr.reasoning_content),
                                "tool_calls": len(sr.tool_calls or []),
                            },
                            ensure_ascii=False,
                        ),
                    )
                    self.ws.save_trace(
                        {
                            "step": f"round_{self._round}",
                            "input_tokens": inp,
                            "output_tokens": out,
                            "cached_tokens": hit,
                            "cache_miss_tokens": miss,
                        }
                    )

                if not sr.tool_calls and sr.finish_reason != "tool_calls":
                    self._emit_log("chatbot_agent", {"level": "info", "message": f"✅ 对话结束，最终回答 {len(sr.content)} 字"})
                    self._emit_status("chatbot_agent", "success")
                    return self._with_usage(self._parse_final_output(sr.content))

                if sr.tool_calls:
                    for tc in sr.tool_calls:
                        target = _tool_target(tc["name"], tc["arguments"])
                        self._emit_log("chatbot_agent", {"level": "info", "message": f"🔧 {target}"})

                        result = self._execute_tool(tc)
                        self.messages.append(
                            {
                                "role": "tool",
                                "tool_call_id": tc["id"],
                                "name": tc["name"],
                                "content": result,
                            }
                        )

                self.messages = list(self.messages)

        except Exception as e:
            if "aborted" in type(e).__name__.lower() or "强制" in str(e):
                raise
            logger.exception("[chat-agent] 循环异常")
            self._emit_log("chatbot_agent", {"level": "error", "message": f"❌ ChatAgent 循环异常: {str(e)[:300]}", "error_details": str(e)})
            self._emit_status("chatbot_agent", "error")
            raise e

        self._emit_log("chatbot_agent", {"level": "error", "message": f"⚠️ 达到最大轮次 {max_rounds}"})
        self._emit_status("chatbot_agent", "max_rounds")
        return self._with_usage(self._parse_final_output(""))

    def _call_api(self):
        kwargs = {
            "model": self.model,
            "messages": self.messages,
            "tools": self.tools,
            "stream": True,
        }
        if self.reasoning_effort:
            kwargs["reasoning_effort"] = self.reasoning_effort
        if "deepseek" in self.model.lower():
            kwargs["extra_body"] = {"thinking": {"type": "enabled"}}

        logger.info("[round_%d] → 发送 LLM 请求 (messages 共 %d 条):", self._round, len(self.messages))
        if self._round == 0 and self.tools:
            logger.info("[round_0] 发送的 Tools 列表 Schema:\n%s", json.dumps(self.tools, ensure_ascii=False, indent=2))
        for idx, msg in enumerate(self.messages):
            role = msg.get("role", "")
            text = msg.get("content") or ""
            if not isinstance(text, str):
                text = str(text)
            preview = text.strip().replace("\n", " ")
            if len(preview) > 150:
                preview = preview[:150] + "..."
            tc_info = ""
            if "tool_calls" in msg and msg["tool_calls"]:
                tc_info = f" [tool_calls={len(msg['tool_calls'])}]"
            if role == "tool":
                tc_id = msg.get("tool_call_id", "")[:8]
                logger.info("  [%d] role=%s (id=%s): %s", idx, role, tc_id, preview)
            else:
                logger.info("  [%d] role=%s%s: %s", idx, role, tc_info, preview)

        t0 = time.time()
        last_exc = None
        for attempt in range(3):
            try:
                stream = self.client.chat.completions.create(**kwargs, timeout=300)
                last_exc = None
                break
            except Exception as e:
                last_exc = e
                logger.error("[round_%d] API 调用失败 (attempt %d/3): %s", self._round, attempt + 1, str(e))
                if attempt < 2 and _is_retryable(e):
                    self._emit_log("chatbot_agent", {"level": "info", "message": f"⏳ API 调用失败: {str(e)[:100]}，15 秒后重试 ({attempt + 1}/2)，可点强制停止中断..."})
                    for _ in range(15):
                        if self._check_aborted:
                            try:
                                self._check_aborted()
                            except Exception:
                                self._emit_log("chatbot_agent", {"level": "error", "message": "⛔ 用户已强制停止"})
                                raise
                        time.sleep(1)
                else:
                    self._emit_log("chatbot_agent", {"level": "error", "message": f"❌ API 调用失败: {str(e)[:300]}", "error_details": str(e)})
                    raise

        if last_exc:
            raise last_exc

        reasoning_parts: list[str] = []
        content_parts: list[str] = []
        tool_call_buffers: dict[int, dict] = {}
        usage = None
        finish = "stop"

        for _chunk_count, chunk in enumerate(stream):
            if not chunk.choices:
                continue
            delta = chunk.choices[0].delta
            if chunk.choices[0].finish_reason:
                finish = chunk.choices[0].finish_reason

            if getattr(delta, "reasoning_content", None):
                reasoning_parts.append(delta.reasoning_content)

            if delta.content:
                content_parts.append(delta.content)

            if delta.tool_calls:
                for tc_delta in delta.tool_calls:
                    idx = tc_delta.index
                    if idx not in tool_call_buffers:
                        tool_call_buffers[idx] = {"id": "", "name": "", "arguments": ""}
                    if tc_delta.id:
                        tool_call_buffers[idx]["id"] = tc_delta.id
                    if tc_delta.function:
                        if tc_delta.function.name:
                            tool_call_buffers[idx]["name"] = tc_delta.function.name
                        if tc_delta.function.arguments:
                            tool_call_buffers[idx]["arguments"] += tc_delta.function.arguments

            if chunk.usage:
                usage = chunk.usage

            if self._check_aborted and _chunk_count % 20 == 0:
                try:
                    self._check_aborted()
                except Exception:
                    raise

        elapsed = (time.time() - t0) * 1000
        reasoning = "".join(reasoning_parts)
        content = "".join(content_parts)
        tool_calls = [tool_call_buffers[i] for i in sorted(tool_call_buffers.keys())] if tool_call_buffers else None

        logger.info("[round_%d] ← LLM 响应:", self._round)
        if reasoning:
            logger.info("  🧠 思考:\n%s", reasoning)
        if content:
            logger.info("  🤖 回复:\n%s", content)
        if tool_calls:
            logger.info("  🔧 工具调用: %s", json.dumps(tool_calls, ensure_ascii=False))

        u = usage
        logger.info(
            "api_json %s",
            json.dumps(
                {
                    "round": self._round,
                    "model": self.model,
                    "elapsed_ms": round(elapsed, 1),
                    "request_last_message": self.messages[-1] if self.messages else None,
                    "response": {
                        "content_len": len(content),
                        "reasoning_len": len(reasoning),
                        "content_first_200": content[:200],
                        "reasoning_first_200": reasoning[:200],
                        "tool_calls": tool_calls,
                        "finish_reason": finish,
                    },
                    "usage": {
                        "prompt_tokens": getattr(u, "prompt_tokens", 0) if u else 0,
                        "completion_tokens": getattr(u, "completion_tokens", 0) if u else 0,
                        "total_tokens": getattr(u, "total_tokens", 0) if u else 0,
                        "prompt_cache_hit_tokens": getattr(u, "prompt_cache_hit_tokens", 0) if u else 0,
                        "prompt_cache_miss_tokens": getattr(u, "prompt_cache_miss_tokens", 0) if u else 0,
                    }
                    if u
                    else {},
                },
                ensure_ascii=False,
                default=str,
            ),
        )

        if reasoning and reasoning.strip():
            r_preview = reasoning.strip().replace("\n", " ")
            if len(r_preview) > 200:
                r_preview = r_preview[:200] + "..."
            self._emit_log("chatbot_agent", {"level": "info", "message": f"🧠 思考: {r_preview}"})

        if content and content.strip():
            c_preview = content.strip().replace("\n", " ")
            if len(c_preview) > 200:
                c_preview = c_preview[:200] + "..."
            self._emit_log("chatbot_agent", {"level": "info", "message": f"🤖 回复: {c_preview}"})

        return _StreamResult(
            content=content,
            reasoning_content=reasoning,
            tool_calls=tool_calls,
            finish_reason=finish,
            usage=usage,
        )

    def _normalize_assistant_message(self, sr) -> dict:
        content = sr.content
        reasoning = sr.reasoning_content

        if not reasoning and "<think>" in content:
            m = re.match(r"<think>(.*?)</think>\s*", content, re.DOTALL)
            if m:
                reasoning = m.group(1).strip()
                content = content[m.end():].strip()

        saved = {"role": "assistant", "content": content}
        if reasoning:
            saved["reasoning_content"] = reasoning

        if sr.tool_calls:
            saved["tool_calls"] = [
                {"id": tc["id"], "type": "function", "function": {"name": tc["name"], "arguments": tc["arguments"]}}
                for tc in sr.tool_calls
            ]
            saved["reasoning_content"] = reasoning or ""

        return saved

    def _execute_tool(self, tc: dict) -> str:
        name = tc["name"]
        try:
            args = json.loads(tc["arguments"])
        except json.JSONDecodeError:
            args = {}

        if name not in self.tool_map:
            return json.dumps({"error": f"未知工具: {name}"}, ensure_ascii=False)

        try:
            t0 = time.time()
            result = self.tool_map[name](**args)
            elapsed = (time.time() - t0) * 1000
            logger.info("tool_exec name=%s elapsed=%.0fms args=%s", name, elapsed, json.dumps(args, ensure_ascii=False, default=str)[:200].replace("\n", " "))
            res_preview = str(result).strip().replace("\n", " ")
            if len(res_preview) > 500:
                res_preview = res_preview[:500] + "..."
            logger.info("  ↳ tool_result: %s", res_preview)

            if isinstance(result, (dict, list)):
                return json.dumps(result, ensure_ascii=False, default=str)
            return str(result)
        except Exception as e:
            logger.error("tool_exec_error tool=%s error=%s", name, e)
            return json.dumps({"error": str(e)}, ensure_ascii=False)

    def _with_usage(self, result: dict) -> dict:
        total = self._total_input + self._total_output
        result["_token_usage"] = {
            "input_tokens": self._total_input,
            "output_tokens": self._total_output,
            "cache_hit_tokens": self._total_cache_hit,
            "cache_miss_tokens": self._total_cache_miss,
            "total_tokens": total,
        }
        logger.info(
            "[chat-agent] round=%d input=%d output=%d cache_hit=%d cache_miss=%d total=%d",
            self._round,
            self._total_input,
            self._total_output,
            self._total_cache_hit,
            self._total_cache_miss,
            total,
        )
        return result

    def _parse_final_output(self, content: str) -> dict:
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            pass
        m = re.search(r"```(?:json)?\s*\n?(.*?)\n?```", content, re.DOTALL)
        if m:
            try:
                return json.loads(m.group(1))
            except json.JSONDecodeError:
                pass
        return {"full_report": content, "cards": [], "metrics": [], "mapping": [], "warnings": []}
