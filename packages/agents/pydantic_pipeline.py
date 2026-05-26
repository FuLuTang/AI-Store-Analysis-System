"""Pydantic AI 管线：staged pipeline — LLM 出策略 → 程序执行和验收。

三个阶段：
1. Flatten Phase: Agent 输出 FlattenPlan → 程序展平 + 写 parquet
2. Mapping Phase: Agent 输出 SemanticMapping[] → 程序记录
3. SQL Phase: Agent 输出 SqlPlan → 程序校验 + 执行

每阶段允许多轮 retry，程序判断成功/部分成功/失败。
记录 token 用量和 DeepSeek KV Cache 命中情况。

init 流程:
  1. 创建 Workspace → 目录 + 分区 + duckdb
  2. 写原始数据为 parquet
  3. 注入上下文文档
  4. 注册 DuckDB 视图
  5. 创建 Agent + 注入 deps
"""

from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path
from typing import Any, Callable

from pydantic import ValidationError

from packages.agents.base import AgentPipeline
from packages.agents.models import (
    AgentResult,
    DatasetBundle,
    FlattenPlan,
    MetricResult,
    MetricStatus,
    PhaseResult,
    ReportCard,
    SemanticMapping,
    SqlPlan,
    TableMeta,
)
from packages.agents.workspace import Workspace
from packages.agents.workspace import _quote_ident as _q
from packages.agents.logging_utils import log_llm_usage

# pydantic-ai 必须在模块级导入，否则 tool 函数的 RunContext[Workspace] 标注 get_type_hints() 解析失败
try:
    from pydantic_ai import RunContext  # noqa: F811
except ImportError:
    RunContext = None  # type: ignore[assignment]

logger = logging.getLogger("agent.pydantic")

MAX_RETRIES = 3


def _extract_reasoning(result) -> str | None:
    """从 AgentRunResult 的 messages 中提取 reasoning_content。"""
    try:
        for msg in result.all_messages():
            parts = getattr(msg, "parts", [])
            for part in parts:
                if getattr(part, "part_kind", "") == "thinking":
                    return getattr(part, "content", None)
    except Exception:
        pass
    return None


def _usage_to_phase(usage_dict: dict) -> dict:
    """将 usage dict 转为 PhaseResult 字段。"""
    inp = usage_dict.get("input_tokens", 0)
    cached = usage_dict.get("cached_input_tokens", 0)
    return {
        "input_tokens": inp,
        "output_tokens": usage_dict.get("output_tokens", 0),
        "cache_hit_tokens": cached,
        "cache_miss_tokens": max(0, inp - cached),
        "requests": usage_dict.get("requests", 0),
        "tool_calls": usage_dict.get("tool_calls", 0),
    }


def _strip_json_fence(text: str) -> str:
    """去除 LLM 返回的 Markdown JSON code fence（```json ... ```），返回纯 JSON 字符串。"""
    s = text.strip()
    if s.startswith("```"):
        first_nl = s.find("\n")
        if first_nl != -1:
            s = s[first_nl + 1:]
        if s.endswith("```"):
            s = s[:-3]
        elif s.endswith("``` "):
            s = s[:-4]
    return s.strip()


def _unwrap_agent_json(data):
    """如果 LLM 输出被包裹在单 key dict 中（如 {"FlattenPlan": {...}}），解包返回内层值。"""
    if isinstance(data, dict) and len(data) == 1:
        k, v = next(iter(data.items()))
        if k in ("tables", "metrics"):
            return data
        if isinstance(v, (dict, list)):
            return v
    return data


class PydanticPipeline(AgentPipeline):
    name = "pydantic"

    def __init__(
        self,
        model: str | None = None,
        base_url: str | None = None,
        api_key: str | None = None,
        llm_preset: dict | None = None,
        check_aborted: Callable[[], None] | None = None,
        workspace_dir: Path | None = None,
        analysis_params: str = "",
    ):
        super().__init__(workspace_dir=workspace_dir, analysis_params=analysis_params)
        self._llm_preset = llm_preset or {}
        self._check_aborted = check_aborted
        self.model = model or os.getenv("AGENT_MODEL", "deepseek-v4-pro")
        self.base_url = base_url or os.getenv("OPENAI_BASE_URL", "https://api.deepseek.com")
        self.api_key = api_key or os.getenv("OPENAI_API_KEY", "")

    def _ensure_not_stopped(self):
        if self._check_aborted:
            self._check_aborted()

    def _log_messages(self, phase: str, attempt: int, messages):
        """将 agent 一次 run 的所有消息逐条 emit 到 SSE 日志。"""
        for i, msg in enumerate(messages):
            for part in getattr(msg, "parts", []):
                kind = getattr(part, "part_kind", "unknown")
                try:
                    if kind == "system-prompt":
                        continue
                    elif kind == "user-prompt":
                        content = getattr(part, "content", "") or ""
                        self._emit_log(f"pydantic_{phase}", f"[#{attempt}.{i}] → 提问: {content[:200]}")
                    elif kind == "text":
                        content = getattr(part, "content", "") or ""
                        self._emit_log(f"pydantic_{phase}", f"[#{attempt}.{i}] ← 模型回答: {content[:300]}")
                    elif kind == "thinking":
                        content = getattr(part, "content", "") or ""
                        self._emit_log(f"pydantic_{phase}", f"[#{attempt}.{i}] 🧠 推理: {content[:300]}")
                    elif kind == "tool-call":
                        tool_name = getattr(part, "tool_name", "?")
                        raw_args = getattr(part, "args", "")
                        if isinstance(raw_args, dict):
                            args_str = json.dumps(raw_args, ensure_ascii=False)
                        else:
                            args_str = str(raw_args)
                        self._emit_log(f"pydantic_{phase}", f"[#{attempt}.{i}] 🔧 工具调用: {tool_name}({args_str[:200]})")
                    elif kind == "tool-return":
                        content = getattr(part, "content", "")
                        if isinstance(content, (dict, list)):
                            content_str = json.dumps(content, ensure_ascii=False)
                        else:
                            content_str = str(content)
                        self._emit_log(f"pydantic_{phase}", f"[#{attempt}.{i}] 📦 工具返回: {content_str[:200]}")
                    elif kind == "retry-prompt":
                        content = getattr(part, "content", "") or ""
                        self._emit_log(f"pydantic_{phase}", f"[#{attempt}.{i}] 🔄 重试提示: {content[:200]}")
                except Exception:
                    pass  # 单条日志失败不影响流程

    # ================================================================
    # 入口
    # ================================================================

    async def run(self, bundle: DatasetBundle) -> AgentResult:
        t0 = time.time()
        ws = Workspace(base_dir=self._workspace_dir) if self._workspace_dir else Workspace()
        all_warnings: list[str] = []
        all_phases: list[PhaseResult] = []
        total_tokens = input_tokens_total = cache_hit_total = 0

        self._emit_log("pydantic_init", f"启动 Pydantic 管线，{len(bundle.tables)} 张表")
        self._emit_status("pydantic_init", "active")
        logger.info(f"[run] report_id={ws.report_id} tables={len(bundle.tables)} model={self.model}")

        raw_metas = ws.write_raw_parquet(bundle.tables)
        logger.info(f"[run] raw parquet: {len(raw_metas)} 张, workspace={ws.report_id}")
        ws.init_duckdb()
        self._emit_log("pydantic_init", f"已写入 {len(raw_metas)} 张 parquet，DuckDB 已初始化")
        self._emit_status("pydantic_init", "success")

        self._ensure_not_stopped()

        # Phase 1: Flatten
        self._emit_status("pydantic_flatten", "active")
        self._emit_log("pydantic_flatten", "开始展平阶段...")
        flat_result = await self._run_flatten_phase(ws, raw_metas)
        all_phases.append(flat_result)
        flat_metas = flat_result.output if flat_result.status != "failed" else raw_metas
        all_warnings.extend(flat_result.warnings)
        total_tokens += flat_result.input_tokens + flat_result.output_tokens
        input_tokens_total += flat_result.input_tokens
        cache_hit_total += flat_result.cache_hit_tokens
        ws.init_duckdb()
        self._emit_log("pydantic_flatten", f"展平完成: {len(flat_metas)} 张表, status={flat_result.status}, attempts={flat_result.attempts}")
        if flat_result.errors:
            self._emit_log("pydantic_flatten", f"展平错误: {'; '.join(flat_result.errors[:5])}")
        self._emit_status("pydantic_flatten", flat_result.status)
        self._ensure_not_stopped()

        # Phase 2: Mapping
        self._emit_status("pydantic_mapping", "active")
        self._emit_log("pydantic_mapping", "开始语义映射...")
        mapping_result = await self._run_mapping_phase(ws, flat_metas)
        all_phases.append(mapping_result)
        mappings = mapping_result.output if mapping_result.status != "failed" else []
        all_warnings.extend(mapping_result.warnings)
        total_tokens += mapping_result.input_tokens + mapping_result.output_tokens
        input_tokens_total += mapping_result.input_tokens
        cache_hit_total += mapping_result.cache_hit_tokens
        self._emit_log("pydantic_mapping", f"映射完成: {len(mappings)} 个字段, status={mapping_result.status}, attempts={mapping_result.attempts}")
        if mapping_result.errors:
            self._emit_log("pydantic_mapping", f"映射错误: {'; '.join(mapping_result.errors[:5])}")
        self._emit_status("pydantic_mapping", mapping_result.status)
        self._ensure_not_stopped()

        # Phase 3: SQL
        self._emit_status("pydantic_sql", "active")
        self._emit_log("pydantic_sql", "开始指标计算...")
        sql_result = await self._run_sql_phase(ws, flat_metas, mappings)
        all_phases.append(sql_result)
        metrics = sql_result.output  # 始终保留，含 UNCOUNTABLE
        all_warnings.extend(sql_result.warnings)
        total_tokens += sql_result.input_tokens + sql_result.output_tokens
        input_tokens_total += sql_result.input_tokens
        cache_hit_total += sql_result.cache_hit_tokens
        self._emit_log("pydantic_sql", f"指标计算完成: {len(metrics)} 项, status={sql_result.status}, attempts={sql_result.attempts}")
        if sql_result.errors:
            self._emit_log("pydantic_sql", f"指标计算错误: {'; '.join(sql_result.errors[:5])}")
        self._emit_status("pydantic_sql", sql_result.status)
        self._ensure_not_stopped()

        elapsed = (time.time() - t0) * 1000
        token_summary = f"耗时 {elapsed:.0f}ms, tokens: {total_tokens} (input={input_tokens_total}, cache_hit={cache_hit_total})"
        self._emit_log("pydantic_init", f"管线完成: {token_summary}")
        logger.info(
            f"[run] 完成 elapsed={elapsed:.0f}ms total_tokens={total_tokens} "
            f"input_tokens={input_tokens_total} cache_hit={cache_hit_total} "
            f"ratio={cache_hit_total / max(input_tokens_total, 1) * 100:.1f}%"
        )

        full_report, cards = self._build_report(bundle, flat_metas, mappings, metrics, all_phases, elapsed, total_tokens)
        self._write_summary_files(full_report, cards)

        return AgentResult(
            report_id=ws.report_id,
            tables=flat_metas,
            mapping=mappings,
            metrics=metrics,
            warnings=all_warnings,
            pipeline=self.name,
            elapsed_ms=elapsed,
            total_tokens=total_tokens,
            input_tokens=input_tokens_total,
            cache_hit_tokens=cache_hit_total,
            phases=all_phases,
            full_report=full_report,
            cards=cards,
        )

    # ================================================================
    # Report Builder
    # ================================================================

    def _build_report(
        self,
        bundle: DatasetBundle,
        flat_metas: list[TableMeta],
        mappings: list[SemanticMapping],
        metrics: list[MetricResult],
        all_phases: list[PhaseResult],
        elapsed_ms: float,
        total_tokens: int,
    ) -> tuple[str, list[ReportCard]]:
        lines: list[str] = []
        lines.append(f"# {bundle.source_type.upper()} 数据分析报告")
        lines.append("")
        lines.append(f"pipeline: {self.name}")
        lines.append(f"耗时: {elapsed_ms:.0f}ms")
        lines.append(f"Token 总数: {total_tokens}")
        lines.append("")
        lines.append(f"---")
        lines.append("")

        lines.append("## 数据表")
        for m in flat_metas:
            lines.append(f"- **{m.name}**: {m.row_count} 行, {len(m.columns)} 列")
        lines.append("")

        lines.append("## 字段映射")
        for mp in mappings:
            lines.append(f"- `{mp.table}.{mp.raw_field}` → `{mp.semantic_field}` (conf={mp.confidence})")
        lines.append("")

        lines.append("## 指标结果")
        status_icon = {
            MetricStatus.PASS: "✅",
            MetricStatus.ATTENTION: "⚠️",
            MetricStatus.WARNING: "🔶",
            MetricStatus.UNCOUNTABLE: "⛔",
        }
        for m in metrics:
            icon = status_icon.get(m.status, "⚪")
            val_str = json.dumps(m.value, ensure_ascii=False) if m.value is not None else "-"
            if len(val_str) > 300:
                val_str = val_str[:300] + "..."
            lines.append(f"- {icon} **{m.name}** (`{m.metric_id}`): {val_str}")
            if m.reason:
                lines.append(f"  - {m.reason}")
        lines.append("")

        lines.append("## 管线阶段")
        for p in all_phases:
            phase_tokens = p.input_tokens + p.output_tokens
            lines.append(f"- **{p.phase}**: status=`{p.status}`, attempts={p.attempts}, tokens={phase_tokens}")
            if p.errors:
                for e in p.errors[:3]:
                    lines.append(f"  - {e}")

        cards: list[ReportCard] = []
        for m in metrics:
            evidence_str = ""
            if m.value is not None:
                evidence_str = json.dumps(m.value, ensure_ascii=False) if not isinstance(m.value, str) else m.value
                if len(evidence_str) > 200:
                    evidence_str = evidence_str[:200] + "..."
            if m.status == MetricStatus.WARNING:
                cards.append(ReportCard(title=m.name, explanation=m.reason or "需关注", suggestion="", evidence=evidence_str, color="red"))
            elif m.status == MetricStatus.ATTENTION:
                cards.append(ReportCard(title=m.name, explanation=m.reason or "需关注", suggestion="", evidence=evidence_str, color="yellow"))
            elif m.status == MetricStatus.UNCOUNTABLE:
                cards.append(ReportCard(title=m.name, explanation=f"无法计算: {m.reason}", suggestion="", evidence="", color="pink"))

        return "\n".join(lines), cards

    def _write_summary_files(self, full_report: str, cards: list[ReportCard]):
        ws_dir = self._workspace_dir
        if not ws_dir:
            return
        ws_dir.mkdir(parents=True, exist_ok=True)
        (ws_dir / "summary.md").write_text(full_report, encoding="utf-8")

        health = "分析完成"
        for c in cards:
            if c.color == "red":
                health = "存在异常"
                break
            elif c.color == "yellow":
                health = "部分指标异常"

        (ws_dir / "summary_short.json").write_text(
            json.dumps({
                "health_status": health,
                "overview_text": f"共 {len(cards)} 项待关注",
                "cards": [{"title": c.title, "explanation": c.explanation, "suggestion": c.suggestion, "color": c.color} for c in cards],
            }, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    # ================================================================
    # Phase 1: Flatten
    # ================================================================

    async def _run_flatten_phase(
        self, ws: Workspace, raw_metas: list[TableMeta]
    ) -> PhaseResult:
        errors: list[str] = []
        msg_history = None
        usage_sum = {"input_tokens": 0, "output_tokens": 0, "cached_input_tokens": 0, "requests": 0, "tool_calls": 0}

        logger.info(f"[flatten] 开始, 原始表 {len(raw_metas)} 张")

        for attempt in range(1, MAX_RETRIES + 1):
            if attempt > 1:
                self._emit_log("pydantic_flatten", f"第 {attempt} 次重试，前一次错误: {'; '.join(errors[:3])}")
            self._ensure_not_stopped()
            agent = self._build_phase_agent(ws, "flatten")
            agent = self._register_phase_tools(agent, ws, "flatten")

            if attempt == 1:
                prompt = self._build_flatten_prompt(raw_metas)
            else:
                prompt = (
                    f"上一次展平失败，错误: {'; '.join(errors)}。请修正 FlattenPlan 后重试。"
                )

            try:
                t_call = time.time()
                result = await agent.run(prompt, deps=ws, message_history=msg_history)
                self._log_messages("flatten", attempt, result.all_messages())
                latency_ms = (time.time() - t_call) * 1000
                raw_text = result.output
                data = json.loads(_strip_json_fence(raw_text))
                data = _unwrap_agent_json(data)
                plan = FlattenPlan.model_validate(data)
                msg_history = result.all_messages()
                reasoning = _extract_reasoning(result)

                u_rec = log_llm_usage(
                    report_id=ws.report_id, pipeline=self.name, phase="flatten",
                    attempt=attempt, model=self.model,
                    usage=result.usage,
                    reasoning_content=reasoning, latency_ms=latency_ms,
                )
                for k in ("input_tokens", "output_tokens", "cached_input_tokens", "tool_calls"):
                    usage_sum[k] += u_rec.get(k, 0)
                inp = u_rec.get("input_tokens", 0)
                cache_hit = u_rec.get("cached_input_tokens", 0)
                ratio_str = f"{cache_hit / max(inp, 1) * 100:.0f}%" if inp else "N/A"
                self._emit_log("pydantic_flatten", f"LLM#{attempt}: 耗时{latency_ms/1000:.1f}s, tokens: {inp}+{u_rec.get('output_tokens',0)}={inp+u_rec.get('output_tokens',0)}, cache命中 {ratio_str}")
            except (json.JSONDecodeError, ValidationError) as e:
                errors.append(f"Agent 输出解析失败: {e}")
                continue
            except Exception as e:
                logger.error(f"[flatten#{attempt}] Agent 调用失败: {e}")
                errors.append(f"Agent 调用失败: {e}")
                continue

            flat_metas, exec_errors = self._execute_flatten(ws, plan, raw_metas)
            if not exec_errors:
                self._emit_log("pydantic_flatten", f"展平成功: {len(flat_metas)} 张, strategy_count={len(plan.tables)}")
                logger.info(f"[flatten#{attempt}] 成功, 产出 {len(flat_metas)} 张 flat table")
                return PhaseResult(
                    phase="flatten", status="success",
                    attempts=attempt, output=flat_metas,
                    **_usage_to_phase(usage_sum),
                )
            errors = exec_errors
            logger.warning(f"[flatten#{attempt}] 失败: {'; '.join(errors)}")

        self._emit_log("pydantic_flatten", f"展平全部 {MAX_RETRIES} 次失败: {'; '.join(errors[:5])}")
        logger.error(f"[flatten] 全部 {MAX_RETRIES} 次失败")
        return PhaseResult(
            phase="flatten", status="failed", attempts=MAX_RETRIES,
            errors=errors, output=raw_metas,
            warnings=["Flatten 阶段失败，使用原始表继续"],
            **_usage_to_phase(usage_sum),
        )

    def _build_flatten_prompt(self, metas: list[TableMeta]) -> str:
        lines = []
        for m in metas:
            cols = ", ".join(c.name for c in m.columns[:20])
            lines.append(f"  - {m.name}: {m.row_count} 行, [{cols}]")
        prompt = (
            "你需要分析以下原始表的结构，制定展平策略。\n\n"
            f"{chr(10).join(lines)}\n\n"
            "输出 FlattenPlan JSON，包含 tables 列表，每项含:\n"
            "- source_table: 原始表名\n"
            "- strategy: pass / explode_array / unfold_object / pivot\n"
            "- target_name: 展平后的表名\n"
            "- columns: 要保留的字段列表\n\n"
            "只输出 FlattenPlan，程序会执行展平。\n\n"
            "只输出纯 JSON，不要 Markdown 代码块，不要额外说明。"
        )
        if self._analysis_params:
            prompt = f"【用户分析参数】\n{self._analysis_params}\n\n" + prompt
        return prompt

    def _execute_flatten(
        self, ws: Workspace, plan: FlattenPlan, raw_metas: list[TableMeta]
    ) -> tuple[list[TableMeta], list[str]]:
        errors: list[str] = []
        flat_metas: list[TableMeta] = []

        for tp in plan.tables:
            if tp.strategy == "pass":
                meta = next((m for m in raw_metas if m.name == tp.source_table), None)
                if meta:
                    flat_metas.append(meta)
                else:
                    errors.append(f"pass 策略找不到源表: {tp.source_table}")
            elif tp.strategy == "explode_array":
                try:
                    df = ws.read_parquet(tp.source_table)
                    exploded = df.explode(tp.columns[0]) if tp.columns else df
                    meta = ws.write_parquet(tp.target_name, exploded)
                    flat_metas.append(meta)
                except Exception as e:
                    errors.append(f"explode_array 失败 ({tp.source_table}): {e}")
            elif tp.strategy == "unfold_object":
                try:
                    df = ws.read_parquet(tp.source_table)
                    from pandas import json_normalize
                    normalized = json_normalize(df[tp.columns].to_dict("records"))
                    meta = ws.write_parquet(tp.target_name, normalized)
                    flat_metas.append(meta)
                except Exception as e:
                    errors.append(f"unfold_object 失败 ({tp.source_table}): {e}")
            elif tp.strategy == "pivot":
                errors.append(f"pivot 策略暂未实现: {tp.source_table}")
            else:
                errors.append(f"未知策略: {tp.strategy}")

        return flat_metas, errors

    # ================================================================
    # Phase 2: Mapping
    # ================================================================

    async def _run_mapping_phase(
        self, ws: Workspace, flat_metas: list[TableMeta]
    ) -> PhaseResult:
        errors: list[str] = []
        msg_history = None
        mappings: list[SemanticMapping] = []
        usage_sum = {"input_tokens": 0, "output_tokens": 0, "cached_input_tokens": 0, "requests": 0, "tool_calls": 0}

        logger.info(f"[mapping] 开始, flat table {len(flat_metas)} 张")

        for attempt in range(1, MAX_RETRIES + 1):
            if attempt > 1:
                self._emit_log("pydantic_mapping", f"第 {attempt} 次重试，前一次错误: {'; '.join(errors[:3])}")
            self._ensure_not_stopped()
            agent = self._build_phase_agent(ws, "mapping")
            agent = self._register_phase_tools(agent, ws, "mapping")

            if attempt == 1:
                prompt = self._build_mapping_prompt(flat_metas)
            else:
                prompt = (
                    f"上一次字段映射失败，错误: {'; '.join(errors)}。请修正后重试。"
                )

            try:
                t_call = time.time()
                result = await agent.run(prompt, deps=ws, message_history=msg_history)
                self._log_messages("mapping", attempt, result.all_messages())
                latency_ms = (time.time() - t_call) * 1000
                raw_text = result.output
                data = json.loads(_strip_json_fence(raw_text))
                data = _unwrap_agent_json(data)
                if isinstance(data, dict):
                    data = [data]
                mappings = [SemanticMapping.model_validate(d) for d in data]
                msg_history = result.all_messages()
                reasoning = _extract_reasoning(result)

                u_rec = log_llm_usage(
                    report_id=ws.report_id, pipeline=self.name, phase="mapping",
                    attempt=attempt, model=self.model,
                    usage=result.usage,
                    reasoning_content=reasoning, latency_ms=latency_ms,
                )
                for k in ("input_tokens", "output_tokens", "cached_input_tokens", "tool_calls"):
                    usage_sum[k] += u_rec.get(k, 0)
                inp = u_rec.get("input_tokens", 0)
                cache_hit = u_rec.get("cached_input_tokens", 0)
                ratio_str = f"{cache_hit / max(inp, 1) * 100:.0f}%" if inp else "N/A"
                self._emit_log("pydantic_mapping", f"LLM#{attempt}: 耗时{latency_ms/1000:.1f}s, tokens: {inp}+{u_rec.get('output_tokens',0)}={inp+u_rec.get('output_tokens',0)}, cache命中 {ratio_str}")
            except (json.JSONDecodeError, ValidationError) as e:
                errors.append(f"Agent 输出解析失败: {e}")
                continue
            except Exception as e:
                logger.error(f"[mapping#{attempt}] Agent 调用失败: {e}")
                errors.append(f"Agent 调用失败: {e}")
                continue

            map_errors = self._validate_mappings(mappings, flat_metas)
            if not map_errors:
                self._emit_log("pydantic_mapping", f"映射成功: {len(mappings)} 条, attempt={attempt}")
                logger.info(f"[mapping#{attempt}] 成功, {len(mappings)} 条映射")
                return PhaseResult(
                    phase="mapping", status="success",
                    attempts=attempt, output=mappings,
                    **_usage_to_phase(usage_sum),
                )
            errors = map_errors
            self._emit_log("pydantic_mapping", f"映射校验失败: {'; '.join(map_errors[:3])}")
            logger.warning(f"[mapping#{attempt}] 失败: {'; '.join(errors)}")

        self._emit_log("pydantic_mapping", f"映射全部 {MAX_RETRIES} 次失败: {'; '.join(errors[:5])}")
        logger.error(f"[mapping] 全部 {MAX_RETRIES} 次失败")
        return PhaseResult(
            phase="mapping", status="failed", attempts=MAX_RETRIES,
            errors=errors, output=mappings,
            **_usage_to_phase(usage_sum),
        )

    def _build_mapping_prompt(self, metas: list[TableMeta]) -> str:
        lines = []
        for m in metas:
            samples = ""
            if m.sample_rows:
                samples = f", 样本行: {m.sample_rows[:1]}"
            cols = ", ".join(c.name for c in m.columns[:20])
            lines.append(f"  - {m.name}: [{cols}]{samples}")
        prompt = (
            "你需要根据表结构和样本数据，将原始字段映射为标准语义字段。\n\n"
            "先用 read_context_tool('fields') 读取标准字段定义。\n\n"
            "表信息:\n" + "\n".join(lines) + "\n\n"
            "输出 SemanticMapping[] JSON 列表，每项含:\n"
            "- raw_field: 原始字段名\n"
            "- table: 所属表名\n"
            "- semantic_field: 标准字段名\n"
            "- confidence: 0~1 置信度\n"
            "- reason: 映射理由\n"
            "- need_confirm: confidence < 0.75 时设为 true\n\n"
            "只输出 SemanticMapping 列表，程序会记录映射。\n\n"
            "只输出纯 JSON，不要 Markdown 代码块，不要额外说明。"
        )
        if self._analysis_params:
            prompt = f"【用户分析参数】\n{self._analysis_params}\n\n" + prompt
        return prompt

    def _validate_mappings(
        self, mappings: list[SemanticMapping], metas: list[TableMeta]
    ) -> list[str]:
        errors: list[str] = []
        if not mappings:
            errors.append("映射列表为空")
            return errors

        valid_columns = {
            col.name
            for m in metas
            for col in m.columns
        }
        for mp in mappings:
            if mp.raw_field not in valid_columns:
                errors.append(f"字段 {mp.raw_field} 不存在于任何表中")
            if not mp.semantic_field:
                errors.append(f"字段 {mp.raw_field} 缺少 semantic_field")
            if mp.confidence < 0 or mp.confidence > 1:
                errors.append(f"字段 {mp.raw_field} confidence 超出 0~1 范围")
        return errors

    # ================================================================
    # Phase 3: SQL
    # ================================================================

    async def _run_sql_phase(
        self,
        ws: Workspace,
        flat_metas: list[TableMeta],
        mappings: list[SemanticMapping],
    ) -> PhaseResult:
        errors: list[str] = []
        msg_history = None
        last_metrics: list = []
        usage_sum = {"input_tokens": 0, "output_tokens": 0, "cached_input_tokens": 0, "requests": 0, "tool_calls": 0}

        logger.info(f"[sql] 开始, {len(flat_metas)} 张表, {len(mappings)} 条映射")

        for attempt in range(1, MAX_RETRIES + 1):
            if attempt > 1:
                self._emit_log("pydantic_sql", f"第 {attempt} 次重试，前一次错误: {'; '.join(errors[:3])}")
            self._ensure_not_stopped()
            agent = self._build_phase_agent(ws, "sql")
            agent = self._register_phase_tools(agent, ws, "sql")

            if attempt == 1:
                prompt = self._build_sql_prompt(flat_metas, mappings)
            else:
                prompt = (
                    f"上一次 SQL 生成失败，错误: {'; '.join(errors)}。请修正后重试。"
                )

            try:
                t_call = time.time()
                result = await agent.run(prompt, deps=ws, message_history=msg_history)
                self._log_messages("sql", attempt, result.all_messages())
                latency_ms = (time.time() - t_call) * 1000
                raw_text = result.output
                data = json.loads(_strip_json_fence(raw_text))
                data = _unwrap_agent_json(data)
                plan = SqlPlan.model_validate(data)
                msg_history = result.all_messages()
                reasoning = _extract_reasoning(result)

                u_rec = log_llm_usage(
                    report_id=ws.report_id, pipeline=self.name, phase="sql",
                    attempt=attempt, model=self.model,
                    usage=result.usage,
                    reasoning_content=reasoning, latency_ms=latency_ms,
                )
                for k in ("input_tokens", "output_tokens", "cached_input_tokens", "tool_calls"):
                    usage_sum[k] += u_rec.get(k, 0)
                inp = u_rec.get("input_tokens", 0)
                cache_hit = u_rec.get("cached_input_tokens", 0)
                ratio_str = f"{cache_hit / max(inp, 1) * 100:.0f}%" if inp else "N/A"
                self._emit_log("pydantic_sql", f"LLM#{attempt}: 耗时{latency_ms/1000:.1f}s, tokens: {inp}+{u_rec.get('output_tokens',0)}={inp+u_rec.get('output_tokens',0)}, cache命中 {ratio_str}")
            except (json.JSONDecodeError, ValidationError) as e:
                errors.append(f"Agent 输出解析失败: {e}")
                continue
            except Exception as e:
                logger.error(f"[sql#{attempt}] Agent 调用失败: {e}")
                errors.append(f"Agent 调用失败: {e}")
                continue

            val_errors = self._validate_sql_plan(plan, flat_metas)
            if val_errors:
                errors = val_errors
                self._emit_log("pydantic_sql", f"SQL 校验失败: {'; '.join(val_errors[:3])}")
                logger.warning(f"[sql#{attempt}] 校验失败: {'; '.join(val_errors)}")
                continue

            metrics, exec_errors = self._execute_sql(ws, plan, flat_metas)
            last_metrics = metrics

            if not exec_errors:
                self._emit_log("pydantic_sql", f"SQL 执行成功: {len(metrics)} 项指标, attempt={attempt}")
                logger.info(f"[sql#{attempt}] 成功, {len(metrics)} 条指标")
                return PhaseResult(
                    phase="sql", status="success",
                    attempts=attempt, output=metrics,
                    **_usage_to_phase(usage_sum),
                )

            errors = exec_errors
            pass_count = len(metrics) - len(exec_errors)
            self._emit_log("pydantic_sql", f"SQL 执行部分成功: {pass_count}/{len(metrics)} 项, 错误: {'; '.join(exec_errors[:3])}")

        self._emit_log("pydantic_sql", f"SQL 全部 {MAX_RETRIES} 次后部分完成: {len(last_metrics)} 项指标")
        logger.error(f"[sql] 全部 {MAX_RETRIES} 次失败, 保留 {len(last_metrics)} 条指标")
        return PhaseResult(
            phase="sql", status="partial", attempts=MAX_RETRIES,
            errors=errors, output=last_metrics,
            **_usage_to_phase(usage_sum),
        )

    def _build_sql_prompt(
        self, metas: list[TableMeta], mappings: list[SemanticMapping]
    ) -> str:
        map_text = "\n".join(
            f"  - {m.table}.{m.raw_field} → {m.semantic_field} (confidence={m.confidence})"
            for m in mappings
        )
        table_text = "\n".join(
            f"  - {m.name} [SQL表名: {m.duckdb_name}]: {m.row_count} 行, [{', '.join(c.name for c in m.columns[:15])}]"
            for m in metas
        )
        prompt = (
            "你需要根据字段映射结果和表结构，生成 SQL 查询计划来计算指标。\n\n"
            "先用 read_context_tool('metrics') 读取指标定义和公式。\n\n"
            "表结构:\n" + table_text + "\n\n"
            "字段映射:\n" + map_text + "\n\n"
            "输出 SqlPlan JSON，包含 metrics 列表，每项含:\n"
            "- metric_id: 指标ID\n"
            "- name: 指标名\n"
            "- sql: 要在 DuckDB 上执行的 SELECT 语句\n"
            "- required_fields: 需要的标准字段列表\n\n"
            "SQL 规则:\n"
            "- 只允许 SELECT/WITH 查询\n"
            "- 表名请用双引号包裹，且必须用 SQL表名（不是原始表名）\n"
            "- ratio 类: SUM(numerator) / NULLIF(SUM(denominator), 0)\n"
            "- period_change 类: 用 LAG() 窗口函数\n"
            "- share_by_dimension 类: GROUP BY + SUM\n"
            "- concentration 类: SUM(column) / SUM(SUM(column)) OVER()\n\n"
            "只输出 SqlPlan，程序会校验并执行 SQL。\n\n"
            "只输出纯 JSON，不要 Markdown 代码块，不要额外说明。"
        )
        if self._analysis_params:
            prompt = f"【用户分析参数】\n{self._analysis_params}\n\n" + prompt
        return prompt

    def _validate_sql_plan(
        self, plan: SqlPlan, metas: list[TableMeta]
    ) -> list[str]:
        errors: list[str] = []
        if not plan.metrics:
            errors.append("SqlPlan 中 metrics 为空")
            return errors

        for ms in plan.metrics:
            if not ms.metric_id:
                errors.append("metric_id 为空")
            if not ms.sql:
                errors.append(f"{ms.metric_id}: sql 为空")
                continue

            upper = ms.sql.strip().upper()
            if not (upper.startswith("SELECT") or upper.startswith("WITH")):
                errors.append(f"{ms.metric_id}: SQL 只允许 SELECT/WITH，当前: {ms.sql[:50]}")

        return errors

    def _execute_sql(
        self, ws: Workspace, plan: SqlPlan, metas: list[TableMeta]
    ) -> tuple[list, list[str]]:
        import duckdb

        name_to_duckdb = {m.name: m.duckdb_name for m in metas if m.name != m.duckdb_name}
        errors: list[str] = []
        metrics: list[MetricResult] = []
        db_path = ws.duckdb_path
        conn = duckdb.connect(str(db_path))

        try:
            for ms in plan.metrics:
                try:
                    sql = ms.sql
                    for orig, safe in name_to_duckdb.items():
                        sql = sql.replace(f'"{orig}"', f'"{safe}"')
                    result = conn.execute(sql).fetchdf()
                    value = result.to_dict(orient="records") if not result.empty else None
                    metrics.append(MetricResult(
                        metric_id=ms.metric_id,
                        name=ms.name,
                        value=value,
                        status=MetricStatus.PASS,
                        required_fields=ms.required_fields,
                    ))
                    logger.debug(f"[sql] {ms.metric_id} OK → {json.dumps(value, ensure_ascii=False)[:100]}")
                except Exception as e:
                    errors.append(f"{ms.metric_id}: SQL 执行失败: {e}")
                    metrics.append(MetricResult(
                        metric_id=ms.metric_id,
                        name=ms.name,
                        status=MetricStatus.UNCOUNTABLE,
                        reason=str(e),
                        required_fields=ms.required_fields,
                        missing_fields=ms.required_fields,
                    ))
                    logger.warning(f"[sql] {ms.metric_id} FAILED → {e}")
        finally:
            conn.close()

        return metrics, errors

    # ================================================================
    # Agent 构建
    # ================================================================

    def _build_phase_agent(self, ws: Workspace, phase: str):
        from pydantic_ai import Agent
        from pydantic_ai.models.openai import OpenAIChatModel
        from pydantic_ai.providers.openai import OpenAIProvider
        from pydantic_ai.settings import ModelSettings

        if self._llm_preset:
            model_name = self._llm_preset.get("model", self.model)
            base_url = self._llm_preset.get("baseUrl", self.base_url)
            api_key = self._llm_preset.get("apiKey", self.api_key)
        else:
            model_name = self.model
            base_url = self.base_url
            api_key = self.api_key

        model = OpenAIChatModel(
            model_name,
            provider=OpenAIProvider(base_url=base_url, api_key=api_key),
        )

        logger.debug(f"[{phase}] 创建 Agent, model={self.model}")

        return Agent(
            model,
            deps_type=Workspace,
            model_settings=ModelSettings(temperature=0.0),
        )

    def _register_phase_tools(self, agent, ws: Workspace, phase: str):
        # ---- 公共工具 ----

        @agent.tool
        async def profile_table_tool(ctx: RunContext[Workspace], table_name: str) -> TableMeta:
            """查看指定表的结构、列名、类型、样本值和行数。"""
            w = ctx.deps
            meta = (
                w.manifest.tables_by_name.get(table_name) or
                w.manifest.tables_by_duckdb_name.get(table_name)
            )
            if not meta:
                w.load_manifest()
                meta = (
                    w.manifest.tables_by_name.get(table_name) or
                    w.manifest.tables_by_duckdb_name.get(table_name)
                )
            if not meta:
                raise ValueError(f"表 {table_name!r} 不在 workspace 中（请用 SQL 表名如 '{table_name.replace('/', '_')}' 尝试）")
            logger.debug(f"[{phase}] profile_table_tool: {table_name}")
            return meta

        @agent.tool
        async def read_context_tool(ctx: RunContext[Workspace], topic: str) -> str:
            """读取上下文文档。topic: metrics/fields/pharmacy/restaurant/hr/common。"""
            from pathlib import Path
            root = Path(__file__).resolve().parent.parent.parent
            doc_map = {
                "metrics": root / "docs" / "指标计算文档.md",
                "fields": root / "docs" / "指标计算文档.md",
                "pharmacy": root / "packages" / "domain_packs" / "pharmacy.yaml",
                "restaurant": root / "packages" / "domain_packs" / "restaurant.yaml",
                "hr": root / "packages" / "domain_packs" / "hr.yaml",
                "common": root / "packages" / "domain_packs" / "common.yaml",
            }
            path = doc_map.get(topic)
            content = path.read_text(encoding="utf-8") if path and path.is_file() else f"未找到文档: topic={topic}"
            logger.debug(f"[{phase}] read_context_tool: {topic} → {len(content)} chars")
            return content

        @agent.tool
        async def list_tables_tool(ctx: RunContext[Workspace]) -> list[dict]:
            """列出 workspace 中所有可用表的名称、SQL表名、行数和列名。"""
            w = ctx.deps
            w.load_manifest()
            result = []
            for t in w.manifest.tables:
                result.append({
                    "name": t.name,
                    "duckdb_name": t.duckdb_name,
                    "row_count": t.row_count,
                    "columns": [c.name for c in t.columns],
                })
            logger.debug(f"[{phase}] list_tables_tool: {len(result)} 张表")
            return result

        # ---- Flatten 阶段工具 ----

        if phase == "flatten":
            @agent.tool
            async def write_workspace_file(ctx: RunContext[Workspace], filename: str, content: str) -> str:
                """将内容写入 workspace 文件。"""
                p = ctx.deps.write_file(filename, content)
                logger.debug(f"[{phase}] write_file: {filename} ({len(content)} chars)")
                return f"已写入: {p}"

            @agent.tool
            async def run_python_tool(ctx: RunContext[Workspace], script_filename: str) -> str:
                """在 workspace 沙箱内执行指定 Python 脚本，返回 stdout。"""
                import subprocess
                import sys
                w = ctx.deps
                script_path = w.dir / script_filename
                if not script_path.is_file():
                    return f"脚本不存在: {script_filename}"
                try:
                    r = subprocess.run(
                        [sys.executable, str(script_path)],
                        capture_output=True, text=True, timeout=30,
                    )
                    logger.debug(f"[{phase}] run_python: {script_filename} → {len(r.stdout)} chars")
                    return r.stdout or r.stderr or "(no output)"
                except subprocess.TimeoutExpired:
                    return "脚本执行超时 (30s)"

        # ---- Mapping 阶段工具 ----

        if phase == "mapping":
            @agent.tool
            async def execute_duckdb_sql(ctx: RunContext[Workspace], sql: str) -> list[dict]:
                """在 DuckDB 上执行 SELECT 查询并返回结果。"""
                import duckdb
                w = ctx.deps
                conn = duckdb.connect(w.duckdb_path)
                try:
                    df = conn.execute(sql).fetchdf()
                    logger.debug(f"[{phase}] duckdb_sql: {sql[:80]} → {len(df)} 行")
                    return df.head(20).to_dict(orient="records")
                finally:
                    conn.close()

        # ---- SQL 阶段工具 ----

        if phase == "sql":
            @agent.tool
            async def execute_duckdb_sql(ctx: RunContext[Workspace], sql: str) -> list[dict]:
                """在 DuckDB 上执行 SELECT 查询并返回结果。"""
                import duckdb
                w = ctx.deps
                conn = duckdb.connect(w.duckdb_path)
                try:
                    df = conn.execute(sql).fetchdf()
                    logger.debug(f"[{phase}] duckdb_sql: {sql[:80]} → {len(df)} 行")
                    return df.head(20).to_dict(orient="records")
                finally:
                    conn.close()

            @agent.tool
            async def register_parquet_tool(
                ctx: RunContext[Workspace], table_name: str, parquet_filename: str
            ) -> str:
                """将 workspace 中的 parquet 文件注册为 DuckDB 视图。"""
                import duckdb
                w = ctx.deps
                parquet_path = w.dir / parquet_filename
                if not parquet_path.is_file():
                    return f"parquet 文件不存在: {parquet_filename}"
                qname = _q(table_name)
                conn = duckdb.connect(w.duckdb_path)
                try:
                    conn.execute(
                        f"CREATE OR REPLACE VIEW {qname} AS SELECT * FROM '{parquet_path}'"
                    )
                    logger.debug(f"[{phase}] register_parquet: {table_name} ← {parquet_filename}")
                    return f"视图 {qname} 已注册"
                finally:
                    conn.close()

            @agent.tool
            async def list_workspace_files(ctx: RunContext[Workspace]) -> list[str]:
                """列出 workspace 目录下所有文件。"""
                files = ctx.deps.list_files()
                logger.debug(f"[{phase}] list_files: {files}")
                return files

            @agent.tool
            async def validate_result_tool(ctx: RunContext[Workspace], result_json: str) -> str:
                """校验 AgentResult JSON 的完整性。"""
                errors_local = []
                try:
                    data = json.loads(result_json)
                    for field in ["mapping", "metrics", "warnings", "tables"]:
                        if not isinstance(data.get(field), list):
                            errors_local.append(f"{field} 必须是数组")
                except json.JSONDecodeError as e:
                    errors_local.append(f"JSON 解析失败: {e}")
                msg = "校验通过" if not errors_local else "校验失败: " + "; ".join(errors_local)
                logger.debug(f"[{phase}] validate_result: {msg}")
                return msg

        return agent
