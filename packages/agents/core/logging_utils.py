"""统一 LLM usage logging — Pydantic / Custom 线共用。

每次 LLM 调用后打一条 JSON 日志，记录 token / cache / reasoning 数据。
兼容 OpenAI / GPT 和 DeepSeek 的 usage 字段差异。
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any

logger = logging.getLogger("agent.llm_usage")


def log_llm_usage(
    report_id: str,
    pipeline: str,
    phase: str,
    attempt: int,
    model: str,
    usage: Any,
    raw_usage: dict | None = None,
    reasoning_content: str | None = None,
    latency_ms: float = 0,
) -> dict:
    """打一条结构化 LLM usage 日志，返回规范化后的 dict。"""
    record = _build_usage_record(
        report_id=report_id,
        pipeline=pipeline,
        phase=phase,
        attempt=attempt,
        model=model,
        usage=usage,
        raw_usage=raw_usage,
        reasoning_content=reasoning_content,
        latency_ms=latency_ms,
    )
    logger.info(json.dumps(record, ensure_ascii=False, default=str))
    return record


def _build_usage_record(
    report_id: str,
    pipeline: str,
    phase: str,
    attempt: int,
    model: str,
    usage: Any,
    raw_usage: dict | None,
    reasoning_content: str | None,
    latency_ms: float,
) -> dict:
    # 从 usage 对象提取数值
    if hasattr(usage, "input_tokens"):
        input_tokens = usage.input_tokens or 0
        output_tokens = usage.output_tokens or 0
        total_tokens = usage.total_tokens or 0

        # Pydantic AI RunUsage
        cached_input = getattr(usage, "cache_read_tokens", 0) or 0
        cache_miss = max(0, input_tokens - cached_input)
        reasoning_tokens = _extract_reasoning_tokens(usage, raw_usage)
        tool_calls = getattr(usage, "tool_calls", 0) or 0
        requests = getattr(usage, "requests", 0) or 0

        # 有 raw_usage 时优先从 raw_usage 取 DeepSeek 专属字段覆盖
        if raw_usage:
            if raw_usage.get("prompt_cache_hit_tokens") is not None:
                cached_input = raw_usage["prompt_cache_hit_tokens"]
            if raw_usage.get("prompt_cache_miss_tokens") is not None:
                cache_miss = raw_usage["prompt_cache_miss_tokens"]
    elif isinstance(usage, dict):
        # OpenAI / DeepSeek 原生 dict
        input_tokens = usage.get("prompt_tokens") or usage.get("input_tokens") or 0
        output_tokens = usage.get("completion_tokens") or usage.get("output_tokens") or 0
        total_tokens = usage.get("total_tokens") or (input_tokens + output_tokens)

        cached_input = (
            _deep_get(usage, "prompt_tokens_details", "cached_tokens")
            or _deep_get(usage, "input_tokens_details", "cached_tokens")
            or usage.get("prompt_cache_hit_tokens")
            or 0
        )
        cache_miss = (
            usage.get("prompt_cache_miss_tokens")
            or max(0, input_tokens - cached_input)
        )
        reasoning_tokens = (
            _deep_get(usage, "completion_tokens_details", "reasoning_tokens")
            or _deep_get(usage, "output_tokens_details", "reasoning_tokens")
            or 0
        )
        tool_calls = 0
        requests = 1
    else:
        input_tokens = output_tokens = total_tokens = 0
        cached_input = cache_miss = reasoning_tokens = 0
        tool_calls = requests = 0

    cache_hit_ratio = cached_input / max(input_tokens, 1)

    reason_chars = len(reasoning_content) if reasoning_content else 0

    record = {
        "ts": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "report_id": report_id,
        "pipeline": pipeline,
        "phase": phase,
        "attempt": attempt,
        "model": model,
        "latency_ms": round(latency_ms, 1),

        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "total_tokens": total_tokens,

        "cached_input_tokens": cached_input,
        "cache_miss_tokens": cache_miss,
        "cache_hit_ratio": round(cache_hit_ratio, 4),

        "reasoning_tokens": reasoning_tokens,
        "reasoning_content_present": reason_chars > 0,
        "reasoning_content_chars": reason_chars,

        "tool_calls": tool_calls,
        "requests": requests,
        "raw_usage": raw_usage or _usage_to_dict(usage),
    }
    return record


def _extract_reasoning_tokens(usage: Any, raw_usage: dict | None) -> int:
    """提取 reasoning_tokens，兼容 Pydantic AI RunUsage 和原生 dict。"""
    # 1. 从 raw_usage 提取
    if raw_usage:
        return (
            _deep_get(raw_usage, "completion_tokens_details", "reasoning_tokens")
            or _deep_get(raw_usage, "output_tokens_details", "reasoning_tokens")
            or 0
        )
    # 2. 尝试 Pydantic AI usage
    try:
        return getattr(usage, "reasoning_tokens", 0) or 0
    except Exception:
        return 0


def _deep_get(d: dict, *keys: str) -> Any:
    """深度取值 d[key1][key2]...，任一环节缺失返回 None。"""
    for k in keys:
        if not isinstance(d, dict):
            return None
        d = d.get(k)
    return d


def _usage_to_dict(usage: Any) -> dict:
    """尝试将 usage 对象转为 dict。"""
    if hasattr(usage, "model_dump"):
        try:
            return usage.model_dump(mode="json")
        except Exception:
            pass
    if hasattr(usage, "dict"):
        try:
            return usage.dict()
        except Exception:
            pass
    if hasattr(usage, "__dict__"):
        return {k: v for k, v in usage.__dict__.items() if not k.startswith("_")}
    if isinstance(usage, dict):
        return usage
    return {"type": type(usage).__name__}
