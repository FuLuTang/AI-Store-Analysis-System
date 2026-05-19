"""Smolagents CodeAgent 管线（方法2）：写代码 → 沙箱执行。

编排器职责：创建 workspace → 写 parquet → 初始化 DuckDB → 加载 tools → 启动 CodeAgent → 收集结果
内部步骤由 Agent 自行决定，编排器不写死步骤顺序。
工具通过 build_smol_tools(ws) 闭包注入，消除全局 get_workspace()。

init 流程:
  1. 创建 Workspace → 目录 + duckdb
  2. 写原始数据为 parquet
  3. 注入上下文文档
  4. 注册 DuckDB 视图
  5. 构建 tools + 创建 CodeAgent
"""
import asyncio
import json
import logging
import os
import re
import time
from pathlib import Path

from .base import AgentPipeline
from .models import AgentResult, DatasetBundle
from .workspace import Workspace

logger = logging.getLogger(__name__)

AUTHORIZED_IMPORTS = ["json", "pandas", "duckdb", "pathlib", "os", "glob", "re"]

PLAN_TEMPLATE = [
    {"title": "查看输入文件",
     "detail": "列出 input/ 下所有文件了解格式结构。推荐: Python open()/os.listdir()。检查: 确认文件数 > 0 且可读。",
     "status": "pending"},
    {"title": "展平并输出 parquet",
     "detail": "写 Python 递归展平嵌套数据为二维表，推荐用 pandas.to_parquet 输出到 tables/。检查: pd.read_parquet 读回验证 row_count > 0。",
     "status": "pending"},
    {"title": "入库，用 duckdb_register_parquet 注册表",
     "detail": "推荐用 duckdb_register_parquet(name, path) 注册所有 parquet，再用 duckdb_query('SELECT COUNT(*) FROM ...') 逐表验证行数匹配。",
     "status": "pending"},
    {"title": "画像，推荐用 profile_table 或 duckdb_query 探索字段",
     "detail": "用 profile_table(path) 获取列名/类型/样本/空值率。也可 duckdb_query('DESCRIBE ...')。检查: 确认核心字段（金额/数量/日期类）已识别。",
     "status": "pending"},
    {"title": "读文档，read_context('指标计算文档.md')",
     "detail": "推荐用 read_context('指标计算文档.md') 获取标准字段定义和指标公式。可用工具: read_context。检查: 确认理解了 revenue/order_count/channel 等核心字段含义。",
     "status": "pending"},
    {"title": "映射，原始字段→标准字段",
     "detail": "根据文档中的标准字段定义，推荐将原始字段映射到 semantic_field。检查: 每个核心指标所需的 required_fields 都有对应映射，无遗漏。",
     "status": "pending"},
    {"title": "计算指标",
     "detail": "推荐用 duckdb_query 写 SQL 计算: revenue_change/avg_order_value/gross_margin_rate/channel_share/top_contribution 等。检查: 每个指标 value 不为 None，status 合理。",
     "status": "pending"},
    {"title": "输出 AgentResult",
     "detail": "整理 scene/mapping/metrics/warnings，推荐调用 validate_result(json)，通过后用 Python open().write() 写入 output/result.json。检查: validate_result 返回 valid=true。",
     "status": "pending"},
    {"title": "清理大文件",
     "detail": "推荐调用 cleanup_workspace('large') 删除 parquet + duckdb。检查: 确认 output/result.json 已保存。",
     "status": "pending"},
]


class SmolPipeline(AgentPipeline):

    class _PlanInjectModel:
        """Model wrapper：每次 LLM 调用前注入 plan，调用后打 usage 日志。"""
        def __init__(self, model, ws: Workspace, pipeline_name: str = "smol"):
            self._model = model
            self._ws = ws
            self._pipeline = pipeline_name
            self._call_count = 0

        def __call__(self, messages: list, **kwargs):
            from .tools.impl.setup_impl import read_plan_short_impl
            plan_text = read_plan_short_impl(self._ws)
            messages = list(messages)
            for i in range(len(messages) - 1, -1, -1):
                if messages[i]["role"] == "user":
                    plan_block = f"<current_plan>\n{plan_text}\n</current_plan>\n\n"
                    messages[i] = {**messages[i], "content": plan_block + messages[i]["content"]}
                    break

            t_start = time.time()
            result = self._model(messages, **kwargs)
            latency_ms = (time.time() - t_start) * 1000
            self._call_count += 1

            usage_log = _extract_usage(result, self._call_count, self._pipeline, self._ws.report_id, latency_ms)
            logger.info("llm_usage %s", json.dumps(usage_log, ensure_ascii=False))
            self._ws.save_trace({"step": "llm_call", **usage_log})

            return result

        def __getattr__(self, name):
            return getattr(self._model, name)

    name = "smol"

    def __init__(self, model=None, max_rounds: int = 15):
        self.model = model
        self.max_rounds = max_rounds

    async def run(self, bundle: DatasetBundle) -> AgentResult:
        t0 = time.time()
        ws = Workspace(label="smol")

        try:
            ws.write_raw_parquet(bundle.tables)
            self._stage_context(ws)
            ws.init_duckdb()
            ws.save_trace({"step": "init", "tables": len(bundle.tables)})

            self._write_plan(ws)
            tools = self._make_tools(ws)
            agent = self._make_agent(tools, ws)
            prompt = self._build_prompt(ws)

            ws.save_trace({"step": "agent_start", "tools": len(tools)})
            raw_output = await asyncio.to_thread(agent.run, prompt)
            ws.save_trace({"step": "agent_done"})

            return self._collect_result(raw_output, ws, t0)
        finally:
            ws.cleanup_large_files()

    # ── staging ──

    def _stage_context(self, ws: Workspace):
        ROOT = Path(__file__).parent.parent.parent
        docs_dir = ROOT / "docs"
        for name in ["指标计算文档.md"]:
            doc = docs_dir / name
            if doc.exists():
                ws.write_context(name, doc.read_text(encoding="utf-8"))

    def _write_plan(self, ws: Workspace):
        from .tools.impl.setup_impl import design_plan_impl
        import json as _json
        design_plan_impl(ws, _json.dumps(PLAN_TEMPLATE, ensure_ascii=False))
        plan_path = ws.resolve("output/plan.json")
        plan = _json.loads(plan_path.read_text(encoding="utf-8"))
        if plan and plan[0]["status"] == "pending":
            plan[0]["status"] = "in_progress"
            plan_path.write_text(_json.dumps(plan, ensure_ascii=False, indent=2), encoding="utf-8")

    # ── tools ──

    def _make_tools(self, ws: Workspace) -> list:
        from .adapters.smol_tools import build_smol_tools
        return build_smol_tools(ws)

    # ── agent ──

    def _make_agent(self, tools: list, ws: Workspace):
        from smolagents import CodeAgent
        model = self._resolve_model()
        model = self._PlanInjectModel(model, ws)
        return CodeAgent(
            tools=tools,
            model=model,
            max_iterations=self.max_rounds,
            additional_authorized_imports=AUTHORIZED_IMPORTS,
        )

    def _resolve_model(self):
        if self.model is not None:
            return self.model
        from smolagents import LiteLLMModel
        model_id = os.getenv("SMOL_MODEL_ID", "deepseek/deepseek-chat")
        api_key = os.getenv("DEEPSEEK_API_KEY") or os.getenv("OPENAI_API_KEY", "")
        api_base = os.getenv("SMOL_API_BASE", "https://api.deepseek.com/v1")
        return LiteLLMModel(model_id=model_id, api_key=api_key, api_base=api_base)

    def _build_prompt(self, ws: Workspace) -> str:
        prompt_file = Path(__file__).parent / "prompts" / "smol.md"
        base = prompt_file.read_text(encoding="utf-8")
        task = (
            f"\n\n## 当前任务\n"
            f"- workspace: {ws.dir}\n"
            f"- input 文件: {ws.list_inputs()}\n"
            f"- 上下文文档: context/ 目录\n"
            f"\n按 plan 逐项推进，完成后用 Python 写入 output/result.json。"
        )
        return base + task

    # ── collect ──

    def _collect_result(self, raw_output: str, ws: Workspace, t0: float) -> AgentResult:
        elapsed_ms = (time.time() - t0) * 1000
        data = ws.read_output_json("result.json")

        if not data:
            data = self._extract_json(raw_output)

        try:
            return AgentResult(
                report_id=ws.report_id,
                pipeline=self.name,
                elapsed_ms=elapsed_ms,
                raw_output=raw_output[:2000],
                **({"scene": data.get("scene"), "mapping": data.get("mapping", []),
                    "metrics": data.get("metrics", []), "warnings": data.get("warnings", []),
                    "cards": data.get("cards", []), "full_report": data.get("full_report", "")}
                   if isinstance(data, dict) else {}),
            )
        except Exception:
            return AgentResult(
                report_id=ws.report_id,
                pipeline=self.name,
                elapsed_ms=elapsed_ms,
                raw_output=raw_output[:2000],
            )

    def _extract_json(self, raw: str) -> dict | None:
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            pass
        m = re.search(r"```(?:json)?\s*\n?(.*?)\n?```", raw, re.DOTALL)
        if m:
            try:
                return json.loads(m.group(1))
            except json.JSONDecodeError:
                pass
        return None


# ── usage logging ──

def _extract_usage(result, call_index: int, pipeline: str, report_id: str, latency_ms: float) -> dict:
    """从 smolagents ChatMessage / raw_response 中提取 usage 信息。"""
    log = {
        "report_id": report_id,
        "pipeline": pipeline,
        "phase": f"agent_step_{call_index}",
        "attempt": 1,
        "model": "",
        "provider": "",
        "latency_ms": round(latency_ms, 1),
        "input_tokens": 0,
        "output_tokens": 0,
        "total_tokens": 0,
        "cached_input_tokens": 0,
        "cache_miss_tokens": 0,
        "cache_hit_ratio": 0.0,
        "reasoning_tokens": 0,
        "reasoning_content_present": False,
        "reasoning_content_chars": 0,
        "tool_calls": 0,
        "raw_usage": {},
    }

    # 1) model info
    if hasattr(result, "model"):
        log["model"] = str(result.model) or ""
    elif hasattr(result, "raw_response") and hasattr(result.raw_response, "model"):
        log["model"] = str(result.raw_response.model) or ""

    # 2) reasoning_content
    rc = _get_attr(result, "reasoning_content", "")
    if rc:
        log["reasoning_content_present"] = True
        log["reasoning_content_chars"] = len(str(rc))

    # 3) tool_calls count
    tc = _get_attr(result, "tool_calls", None)
    if tc:
        log["tool_calls"] = len(tc) if isinstance(tc, list) else 1

    # 4) usage — try raw_response first, then top-level attributes
    usage = None
    raw = _get_attr(result, "raw_response", None)
    if raw is not None:
        usage = _get_usage_from_response(raw)

    if not usage:
        usage = _get_usage_from_response(result)

    if usage:
        log["raw_usage"] = _safe_dict(usage)
        log["input_tokens"] = int(_nz(usage, "prompt_tokens", "input_tokens"))
        log["output_tokens"] = int(_nz(usage, "completion_tokens", "output_tokens"))
        log["total_tokens"] = int(_nz(usage, "total_tokens"))

        # DeepSeek 缓存字段
        log["cached_input_tokens"] = int(_nz(usage, "prompt_cache_hit_tokens"))
        cache_miss = _nz(usage, "prompt_cache_miss_tokens")
        if cache_miss:
            log["cache_miss_tokens"] = int(cache_miss)
        elif log["input_tokens"] > log["cached_input_tokens"]:
            log["cache_miss_tokens"] = log["input_tokens"] - log["cached_input_tokens"]

        # OpenAI/Anthropic 缓存字段
        if not log["cached_input_tokens"]:
            details = usage.get("prompt_tokens_details") or usage.get("input_tokens_details") or {}
            log["cached_input_tokens"] = int(_nz(details, "cached_tokens"))
            if not log["cache_miss_tokens"] and log["input_tokens"] > log["cached_input_tokens"]:
                log["cache_miss_tokens"] = log["input_tokens"] - log["cached_input_tokens"]

        if log["input_tokens"] > 0:
            log["cache_hit_ratio"] = round(log["cached_input_tokens"] / log["input_tokens"], 3)

        # reasoning_tokens
        details = usage.get("completion_tokens_details") or usage.get("output_tokens_details") or {}
        log["reasoning_tokens"] = int(_nz(details, "reasoning_tokens"))

    return log


def _get_usage_from_response(obj) -> dict | None:
    """从对象中提取 usage 字典。"""
    if hasattr(obj, "usage") and obj.usage is not None:
        return _safe_dict(obj.usage)
    if isinstance(obj, dict) and "usage" in obj:
        return obj["usage"]
    return None


def _get_attr(obj, name, default=None):
    for attr in (name, f"_{name}", f"__{name}__"):
        if hasattr(obj, attr):
            return getattr(obj, attr)
    if isinstance(obj, dict) and name in obj:
        return obj[name]
    return default


def _nz(obj, *keys):
    """取第一个非零值。"""
    for k in keys:
        if isinstance(obj, dict):
            v = obj.get(k, 0)
        elif hasattr(obj, k):
            v = getattr(obj, k, 0)
        else:
            continue
        if v:
            return v
    return 0


def _safe_dict(obj) -> dict:
    if isinstance(obj, dict):
        return obj
    if hasattr(obj, "__dict__"):
        d = {}
        for k, v in obj.__dict__.items():
            if not k.startswith("_"):
                try:
                    json.dumps(v)
                    d[k] = v
                except (TypeError, ValueError):
                    d[k] = str(v)
        return d
    return {}
