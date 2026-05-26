"""agent_loop.py — 薄 OpenAI SDK Agent 循环。

核心原则：
  1. messages 原样 append，不改写不重组不摘要
  2. tool_calls 走原生 OpenAI function calling 格式
  3. reasoning_content 按 DeepSeek 规则处理（有 tool_calls 保留，无则丢弃）
  4. 每次请求打印缓存命中日志
"""

import json
import logging
import re
import sys
import time
from openai import OpenAI

from .workspace import Workspace
from .prompt_builder import build_system_content, build_user_content
from .tool_converter import available_tool_call_for_agent, build_tool_map

logger = logging.getLogger("agent.custom")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    _handler = logging.StreamHandler(sys.stdout)
    _handler.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
    logger.addHandler(_handler)


class AgentLoop:
    """薄封装：OpenAI SDK 客户端 + messages 管理 + tool 执行。"""

    def __init__(self, client: OpenAI, ws: Workspace, llm_preset: dict, emit_log=None, emit_status=None, analysis_params: str = "", check_aborted=None):
        self.client = client
        self.ws = ws
        self.model = llm_preset.get("model", "deepseek-chat")
        self.reasoning_effort = llm_preset.get("reasoningEffort", "medium")
        self.analysis_params = analysis_params
        self._check_aborted = check_aborted
        self._total_input = 0
        self._total_output = 0
        self._total_cache_hit = 0
        self._total_cache_miss = 0
        self.messages: list[dict] = []
        self.tools = available_tool_call_for_agent(ws)
        self.tool_map = build_tool_map(ws)
        self._emit_log = emit_log or (lambda nid, msg: None)
        self._emit_status = emit_status or (lambda nid, st: None)
        self._round = 0

    def run(self) -> dict:
        """主循环：构建初始 messages → 发请求 → 处理 tool_calls → 循环直到收到最终回答。"""
        self.messages = [
            {"role": "system", "content": build_system_content()},
            {"role": "user", "content": build_user_content(self.ws, self.analysis_params)},
        ]

        self._emit_status("custom_agent", "active")
        self._emit_log("custom_agent", f"启动 Agent 循环, model={self.model}, tools={len(self.tools)} 个")
        logger.info("[agent] start model=%s tools=%d", self.model, len(self.tools))

        max_rounds = 50
        try:
            for self._round in range(max_rounds):
                sr = self._call_api()

                # ── 流式返回后立即检查：用户可能在 API 调用期间点了强制停止 ──
                if self._check_aborted:
                    try:
                        self._check_aborted()
                    except Exception:
                        self._emit_log("custom_agent", "⛔ 用户已强制停止")
                        raise

                # ── 保存 assistant message ──
                self.messages.append(self._normalize_assistant_message(sr))

                # ── 缓存 & token 日志（仅文件日志，不推 SSE）──
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
                    logger.info("llm_usage %s",
                        json.dumps({
                            "report_id": self.ws.report_id, "pipeline": "custom",
                            "phase": f"round_{self._round}", "model": self.model,
                            "input_tokens": inp, "output_tokens": out,
                            "cached_input_tokens": hit, "cache_miss_tokens": miss,
                            "cache_hit_ratio": round(hit / max(inp, 1), 3),
                            "reasoning_chars": len(sr.reasoning_content),
                            "tool_calls": len(sr.tool_calls or []),
                        }, ensure_ascii=False))
                    self.ws.save_trace({"step": f"round_{self._round}", "input_tokens": inp,
                                        "output_tokens": out, "cached_tokens": hit, "cache_miss_tokens": miss})

                # ── 没有 tool_calls → 最终回答 ──
                if not sr.tool_calls and sr.finish_reason != "tool_calls":
                    self._emit_log("custom_agent", f"✅ 分析完成，最终回答 {len(sr.content)} 字")
                    self._emit_status("custom_agent", "success")
                    return self._with_usage(self._parse_final_output(sr.content))

                # ── 有 tool_calls → 执行工具 ──
                if sr.tool_calls:
                    for tc in sr.tool_calls:
                        target = _tool_target(tc["name"], tc["arguments"])
                        self._emit_log("custom_agent", f"🔧 {target}")
                        result = self._execute_tool(tc)
                        self.messages.append({
                            "role": "tool",
                            "tool_call_id": tc["id"],
                            "content": result,
                        })

                    # plan 全部完成后，注入收尾请求，不继续普通循环
                    if _is_plan_done(self.ws):
                        self._emit_log("custom_agent", "📋 所有步骤已完成，生成最终总结")
                        self.messages.append({
                            "role": "user",
                            "content": "所有计划步骤已全部完成。请提供一份简洁的最终总结（约200字），包含健康状态、核心结论和交付物清单。不要调用任何工具。"
                        })
                        self._round += 1
                        sr = self._call_api()
                        self.messages.append(self._normalize_assistant_message(sr))
                        self._emit_log("custom_agent", f"✅ 分析完成，最终回答 {len(sr.content)} 字")
                        self._emit_status("custom_agent", "success")
                        return self._with_usage(self._parse_final_output(sr.content))

        except Exception as e:
            # 用户强制停止 → 让 PipelineAbortedError 透传
            if "aborted" in type(e).__name__.lower() or "强制" in str(e):
                raise
            logger.exception("[agent] 循环异常")
            self._emit_log("custom_agent", f"❌ Agent 循环异常: {str(e)[:300]}")
            self._emit_status("custom_agent", "error")
            return self._with_usage({"full_report": f"Agent 执行异常: {e}", "cards": [], "metrics": [],
                                      "mapping": [], "warnings": [str(e)]})

        self._emit_log("custom_agent", f"达到最大轮次 {max_rounds}")
        self._emit_status("custom_agent", "max_rounds")
        return self._with_usage(self._parse_final_output(""))

    # ── 内部 ──

    def _call_api(self):
        """流式调用 API，实时推送思考和回复到 SSE，返回结构化结果。"""
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

        t0 = time.time()
        last_exc = None
        for attempt in range(3):  # 首次 + 最多 2 次重试
            try:
                stream = self.client.chat.completions.create(**kwargs, timeout=300)
                last_exc = None
                break
            except Exception as e:
                last_exc = e
                logger.error("[round_%d] API 调用失败 (attempt %d/3): %s", self._round, attempt + 1, str(e))
                if attempt < 2 and _is_retryable(e):
                    self._emit_log("custom_agent", f"⏳ API 调用失败: {str(e)[:100]}，15 秒后重试 ({attempt + 1}/2)，可点强制停止中断...")
                    for _ in range(15):
                        if self._check_aborted:
                            try:
                                self._check_aborted()
                            except Exception:
                                self._emit_log("custom_agent", "⛔ 用户已强制停止")
                                raise
                        time.sleep(1)
                else:
                    self._emit_log("custom_agent", f"❌ API 调用失败: {str(e)[:300]}")
                    raise

        if last_exc:
            raise last_exc

        # 实时收集
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

            # tool_calls — 收集
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

            # usage 通常在最后一个 chunk
            if chunk.usage:
                usage = chunk.usage

            # 流式传输中定期检查强制停止
            if self._check_aborted and _chunk_count % 20 == 0:
                try:
                    self._check_aborted()
                except Exception:
                    raise

        elapsed = (time.time() - t0) * 1000
        reasoning = "".join(reasoning_parts)
        content = "".join(content_parts)
        tool_calls = [tool_call_buffers[i] for i in sorted(tool_call_buffers.keys())] if tool_call_buffers else None

        # 文件日志：content 保留 \n
        if reasoning:
            logger.info("[round_%d] 🧠 思考:\n%s", self._round, reasoning)
        if content:
            logger.info("[round_%d] ← 回复:\n%s", self._round, content)

        u = usage
        logger.info("api_json %s", json.dumps({
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
            } if u else {},
        }, ensure_ascii=False, default=str))

        # 推送汇总
        tag = _plan_step_tag(self.ws)
        self._emit_log("custom_agent", f"\n{tag}🤖: {content[:500]}")

        return _StreamResult(
            content=content,
            reasoning_content=reasoning,
            tool_calls=tool_calls,
            finish_reason=finish,
            usage=usage,
        )

    def _normalize_assistant_message(self, sr) -> dict:
        """统一 assistant message 格式，处理 reasoning_content / think 标签。

        规则（DeepSeek 文档）：
          - 有 tool_calls：reasoning_content 必须保留
          - 无 tool_calls：reasoning_content 丢弃
          - content 中如果有 <think>...</think>，解析到 reasoning_content 字段
        """
        content = sr.content
        reasoning = sr.reasoning_content

        if not reasoning and "<think>" in content:
            m = re.match(r"<think>(.*?)</think>\s*", content, re.DOTALL)
            if m:
                reasoning = m.group(1).strip()
                content = content[m.end():].strip()

        saved = {"role": "assistant", "content": content}

        if sr.tool_calls:
            saved["tool_calls"] = [
                {"id": tc["id"], "type": "function",
                 "function": {"name": tc["name"], "arguments": tc["arguments"]}}
                for tc in sr.tool_calls
            ]
            # 有 tool_calls 时必须传 reasoning_content，即使为空
            saved["reasoning_content"] = reasoning or ""

        return saved

    def _execute_tool(self, tc: dict) -> str:
        """执行工具调用，返回 JSON 字符串。tc 是 dict 格式：{id, name, arguments}。"""
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
            logger.info("tool_exec name=%s elapsed=%.0fms args=%s", name, elapsed,
                        json.dumps(args, ensure_ascii=False, default=str)[:200].replace('\n', ' '))
            if isinstance(result, (dict, list)):
                return json.dumps(result, ensure_ascii=False, default=str)
            return str(result)
        except Exception as e:
            logger.error("tool_exec_error tool=%s error=%s", name, e)
            return json.dumps({"error": str(e)}, ensure_ascii=False)

    def _with_usage(self, result: dict) -> dict:
        """注入累计 token 统计到返回结果中。"""
        total = self._total_input + self._total_output
        result["_token_usage"] = {
            "input_tokens": self._total_input,
            "output_tokens": self._total_output,
            "cache_hit_tokens": self._total_cache_hit,
            "cache_miss_tokens": self._total_cache_miss,
            "total_tokens": total,
        }
        logger.info("[agent] round=%d input=%d output=%d cache_hit=%d cache_miss=%d total=%d",
                    self._round, self._total_input, self._total_output,
                    self._total_cache_hit, self._total_cache_miss, total)
        return result

    def _parse_final_output(self, content: str) -> dict:
        """从最终回答中解析 AgentResult JSON。"""
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


# ── helpers ──

def _is_plan_done(ws) -> bool:
    """检查 plan 中所有步骤是否都已完成。"""
    try:
        plan_path = ws.resolve("output/plan.json")
        if not plan_path.exists():
            return False
        plan = json.loads(plan_path.read_text(encoding="utf-8"))
        return all(s.get("status") == "success" for s in plan)
    except Exception:
        return False


def _plan_step_tag(ws) -> str:
    """读取 plan.json，返回短标签，如 '[步骤3/6: 展平数据] '。"""
    try:
        plan_path = ws.resolve("output/plan.json")
        if not plan_path.exists():
            return ""
        plan = json.loads(plan_path.read_text(encoding="utf-8"))
        total = len(plan)
        for i, step in enumerate(plan):
            if step.get("status") == "in_progress":
                return f"[步骤{i + 1}/{total}: {step['title']}] "
        return ""
    except Exception:
        return ""


def _is_retryable(e: Exception) -> bool:
    """429 / 5xx / APIConnectionError 可重试，其他直接报错。"""
    msg = str(e).lower()
    status = getattr(e, "status_code", 0) or getattr(e, "code", 0)
    # openai.RateLimitError / openai.APIStatusError
    if hasattr(e, "status_code"):
        status = e.status_code
    elif hasattr(e, "response"):
        status = getattr(e.response, "status_code", 0)
    if status in (429, 500, 502, 503):
        return True
    if "负载" in msg or "rate limit" in msg:
        return True
    if "timeout" in msg or "timed out" in msg:
        return True
    if "closed connection" in msg or "incomplete chunked" in msg:
        return True
    if "connection error" in msg or "connection refused" in msg:
        return True
    # openai.APIConnectionError 类名匹配
    if "APIConnectionError" in type(e).__name__:
        return True
    return False


def _tool_target(name: str, args_json: str) -> str:
    """从工具调用参数中提取有意义的"目标"，用于日志展示。"""
    try:
        args = json.loads(args_json)
    except json.JSONDecodeError:
        return args_json[:60]

    # 按工具名返回可读的描述
    labels = {
        "read_document": lambda a: f"读取 {a.get('path', '?')}",
        "read_file": lambda a: f"读取 {a.get('path', '?')}",
        "write_file": lambda a: f"写入 {a.get('path', '?')}",
        "list_files": lambda a: f"列出 {a.get('subdir', '根目录')}/ 目录",
        "run_python": lambda a: f"执行 {a.get('script_path', '?')}",
        "duckdb_register_parquet": lambda a: f"注册表 {a.get('table_name', '?')}",
        "list_tables": lambda a: "列出所有表",
        "read_context": lambda a: f"读取上下文: {a.get('topic', '?')}",
        "read_plan": lambda a: "读取任务计划",
        "check_plan": lambda a: f"检查步骤 {a.get('step_index', '?')} 是否完成",
    }
    fn = labels.get(name)
    if fn:
        return fn(args)

    # 兜底：取 path / sql 等关键字段
    for key in ("path", "script_path", "parquet_path", "table_name", "doc_name", "subdir"):
        if key in args:
            return str(args[key])[:80]
    if "sql" in args:
        return args["sql"][:80].replace("\n", " ")
    if "content" in args:
        c = args["content"]
        return c[:60] + ("…" if len(c) > 60 else "")
    for v in args.values():
        if isinstance(v, str):
            return v[:80]
    return args_json[:80]


class _StreamResult:
    """流式 API 调用的聚合结果。"""
    __slots__ = ("content", "reasoning_content", "tool_calls", "finish_reason", "usage")

    def __init__(self, content: str, reasoning_content: str, tool_calls, finish_reason, usage):
        self.content = content
        self.reasoning_content = reasoning_content
        self.tool_calls = tool_calls
        self.finish_reason = finish_reason
        self.usage = usage
