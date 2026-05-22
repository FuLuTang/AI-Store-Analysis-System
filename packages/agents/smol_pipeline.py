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
import sys
import time
from pathlib import Path

from .base import AgentPipeline
from .models import AgentResult, DatasetBundle
from .workspace import Workspace
from .adapters import build_smol_tools

logger = logging.getLogger(__name__)
if not logger.handlers:
    logger.setLevel(logging.INFO)
    _handler = logging.StreamHandler(sys.stdout)
    _handler.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
    logger.addHandler(_handler)

AUTHORIZED_IMPORTS = ["json", "pandas", "duckdb", "pathlib", "os", "glob", "re"]

PLAN_TEMPLATE = [
    {"title": "展平数据、数据入库",
     "detail": (
         "原始数据在 input/*.json，管线已预处理了一份 parquet 注册进 DuckDB，但可能没展平。"
         "先调 list_tables() 和 duckdb_query('SELECT * FROM \"表名\" LIMIT 5') 看看预注册的表能不能直接用。"
         "如果字段里有嵌套的 dict/list，就用 read_file 看原始 JSON，自己写 Python 展平成二维表，"
         "再用 write_file 写成新的 parquet，然后 duckdb_register_parquet 注册。"
         "如果数据本身就是扁平的，跳过展平直接用就行。"
         "确认每张表都能查、行数 > 0，这步结束数据链路就通了。"
     ),
     "status": "pending",
     "check": (
         "# 检查每张表行数 > 0\n"
         "import duckdb\n"
         "con = duckdb.connect('analysis.duckdb')\n"
         "tables = con.execute(\"SELECT table_name FROM information_schema.tables WHERE table_schema='main'\").fetchall()\n"
         "assert tables, 'DuckDB 中没有注册任何表'\n"
         "for (t,) in tables:\n"
         "    cnt = con.execute(f'SELECT COUNT(*) FROM \"{t}\"').fetchone()[0]\n"
         "    assert cnt > 0, f'{t} 行数为 0'\n"
     ),
     "errors": []},
    {"title": "算指标，写入 output/指标.json",
     "detail": (
         "先调 read_context('指标计算文档.md') 了解标准字段定义和指标公式。"
         "把原始字段映射到标准字段（如 零售金额→revenue，来客数→customer_count）。"
         "然后用 duckdb_query 写 SQL 尽可能算出各种指标——营收变化、毛利率、客单价、渠道占比等。"
         "可以一批一批算，每算几个就用 write_file 追加到 output/指标.json 里存着。"
         "每条 metric 要有 metric_id/name/value/unit/status/reason/evidence，"
         "status 只能是 pass/attention/warning/uncountable 四种。"
     ),
     "status": "pending",
     "check": (
         "# 检查 output/指标.json 存在\n"
         "import os\n"
         "assert os.path.exists('output/指标.json'), '指标.json 还没写'\n"
     ),
     "errors": []},
    {"title": "分析深层原因，写诊断报告 → summary.md",
     "detail": (
         "根据算出来的指标，分析背后的原因——哪些指标异常？为什么？有什么趋势？"
         "结合行业常识和数据证据，写一篇完整的 Markdown 经营诊断报告，用 write_file 保存到 summary.md。"
         "报告格式分两大部分："
         "# 第一部分：现状诊断报告 — "
         "1) 核心经营判断（涨跌稳定波动，可能原因，带emoji如📈📉）；"
         "2) 核心指标逐项解读（优先关注 attention/warning 项）；"
         "3) 关联分析（多指标交叉解读，如营收涨但毛利跌说明什么）；"
         "4) 风险预警。"
         "# 第二部分：优化行动方案 — "
         "1) 紧急事项（高风险指标对应的动作）；"
         "2) 中期改善（结构性问题的优化方向）；"
         "3) 基于行业经验的其他建议。"
         "要求：用 # 现状诊断报告 ... --- ... # 优化行动方案 分隔；"
         "全文约1000字；bullet 不超45字；最多4个核心问题；"
         "优先使用证据中的数据，禁止编造数值；禁止结尾客套话。"
     ),
     "status": "pending",
     "check": (
         "# 检查 summary.md 存在\n"
         "import os\n"
         "assert os.path.exists('summary.md'), 'summary.md 还没写'\n"
     ),
     "errors": []},
    {"title": "输出精简视图 + 最终产物",
     "detail": (
         "做两件事："
         "1) 写 summary_short.json — 给管理层看的精简视图，严格 JSON 格式："
         '{\"health_status\": \"1-2词整体状态\", \"overview_text\": \"一句大白话总结当前经营状况\", '
         '\"cards\": [{\"title\": \"问题标题\", \"explanation\": \"大白话说怎么回事\", '
         '\"suggestion\": \"咋办\", \"evidence\": \"数据证据（优先 Markdown 迷你表格）\", '
         '\"color\": \"red/yellow/green/blue/pink\"}]}。'
         "cards 最多 7 个；color: red=报警 yellow=关注 green=正常 blue=信息 pink=数据口径不一致。"
         "2) 写 output/result.json — 完整的 AgentResult JSON，顶级字段: "
         "scene（对象: industry/business_model/data_scope/analysis_goal）、"
         "mapping（列表: raw_field/table/semantic_field/confidence/reason）、"
         "metrics（列表: metric_id/name/value/unit/status/reason/evidence）、"
         "warnings（字符串列表）、cards（同上）、full_report（summary.md 的完整内容）。"
         "先调 validate_result(json_str) 校验格式，通过后再 write_file 写入。"
     ),
     "status": "pending",
     "check": (
         "# 检查三个产物都存在\n"
         "import os\n"
         "assert os.path.exists('summary.md'), 'summary.md 不存在'\n"
         "assert os.path.exists('summary_short.json'), 'summary_short.json 不存在'\n"
         "assert os.path.exists('output/result.json'), 'result.json 不存在'\n"
     ),
     "errors": []},
    {"title": "清理大文件",
     "detail": (
         "确认产物都保存好了，调 cleanup_workspace('large') 删掉 parquet 和 duckdb 省空间。"
     ),
     "status": "pending",
     "check": (
         "# 确认产物已保存\n"
         "import os\n"
         "assert os.path.exists('output/result.json'), 'result.json 不存在'\n"
     ),
     "errors": []},
]


class SmolPipeline(AgentPipeline):

    class _PlanInjectModel:
        """Model wrapper: 每次 LLM 调用前注入 plan + 检查 abort，调用后打 usage 日志和原始回复。"""
        def __init__(self, model, ws: Workspace, pipeline_name: str = "smol", emit_log=None, check_aborted=None):
            self._model = model
            self._ws = ws
            self._pipeline = pipeline_name
            self._call_count = 0
            self._emit_log = emit_log or (lambda nid, msg: None)
            self._check_aborted = check_aborted

        def __call__(self, messages: list, **kwargs):
            return self.generate(messages, **kwargs)

        def __getattr__(self, name):
            return getattr(self._model, name)

        def generate(self, messages: list, **kwargs):
            if self._check_aborted:
                self._check_aborted()

            from .tools.impl.setup_impl import read_plan_short_impl
            from smolagents.models import ChatMessage, MessageRole
            plan_text = read_plan_short_impl(self._ws)
            messages = list(messages)
            messages.append(ChatMessage(role=MessageRole.USER, content=[{"type": "text", "text": f"<current_plan>\n{plan_text}\n</current_plan>"}]))

            step_label = f"Step {self._call_count}"
            logger.info("[%s] → 请求: %s", step_label, str(messages))

            t_start = time.time()
            result = self._model.generate(messages, **kwargs)
            latency_ms = (time.time() - t_start) * 1000
            self._call_count += 1

            usage_log = _extract_usage(result, self._call_count, self._pipeline, self._ws.report_id, latency_ms)
            logger.info("llm_usage %s", json.dumps(usage_log, ensure_ascii=False))
            self._ws.save_trace({"step": "llm_call", **usage_log})

            inp = usage_log.get("input_tokens", 0)
            cache_hit = usage_log.get("cached_input_tokens", 0)
            ratio_str = f"{usage_log.get('cache_hit_ratio', 0) * 100:.0f}%" if inp else "N/A"
            self._emit_log("smol_agent", f"[{step_label}] tokens: {inp}+{usage_log.get('output_tokens',0)}={usage_log.get('total_tokens',0)}, cache命中 {ratio_str}, tool_calls={usage_log.get('tool_calls',0)}, 耗时{latency_ms/1000:.1f}s")

            thinking = _get_attr(result, "reasoning_content", "")
            content = _get_attr(result, "content", "")
            if not content and hasattr(result, "choices") and result.choices:
                msg = result.choices[0].message
                thinking = _get_attr(msg, "reasoning_content", "") or thinking
                content = _get_attr(msg, "content", "") or content
            if not content and isinstance(result, dict):
                thinking = result.get("reasoning_content", "")
                content = result.get("content", "")
            if not content and isinstance(result, str):
                content = result

            if thinking:
                self._emit_log("smol_agent", f"[{step_label}] 🧠 思考: {thinking[:500]}")
            if content:
                logger.info("[%s] ← 回复: %s", step_label, content)
                self._emit_log("smol_agent", f"[{step_label}] ← 回复: {content[:500]}")

            return result

    name = "smol"

    def __init__(self, model=None, max_rounds: int = 30, llm_preset=None, check_aborted=None, workspace_dir=None, analysis_params: str = ""):
        super().__init__(workspace_dir=workspace_dir, analysis_params=analysis_params)
        self.model = model
        self.max_rounds = max_rounds
        self._llm_preset = llm_preset or {}
        self._check_aborted = check_aborted

    def _ensure_not_stopped(self):
        if self._check_aborted:
            self._check_aborted()

    async def run(self, bundle: DatasetBundle) -> AgentResult:
        t0 = time.time()
        ws = Workspace(base_dir=self._workspace_dir) if self._workspace_dir else Workspace(label="smol")

        try:
            self._emit_log("smol_init", f"启动 Smolagent 管线，{len(bundle.tables)} 张表")
            self._emit_status("smol_init", "active")
            # 原始 JSON 写到 input/ 让 Agent 能看到原始结构
            for t in bundle.tables:
                file_stem = t.name.replace(" ", "_").replace("/", "_")
                ws.write_input_json(f"{file_stem}.json", {"name": t.name, "rows": t.rows})
            # 预处理一份 parquet + DuckDB（Agent 可以直接用，也可以自己展平后重新注册）
            ws.write_raw_parquet(bundle.tables)
            self._stage_context(ws)
            ws.init_duckdb()
            ws.save_trace({"step": "init", "tables": len(bundle.tables)})
            self._emit_log("smol_init", "环境初始化完毕: input JSON + parquet + DuckDB + 上下文")
            self._emit_status("smol_init", "success")

            self._emit_status("smol_plan", "active")
            self._emit_log("smol_plan", "制定执行计划...")
            self._write_plan(ws)
            self._emit_log("smol_plan", "计划已写入")
            self._emit_status("smol_plan", "success")
            self._ensure_not_stopped()

            tools = self._make_tools(ws)
            agent = self._make_agent(tools, ws)
            prompt = self._build_prompt(ws)
            self._emit_status("smol_agent", "active")
            self._emit_log("smol_agent", f"启动 CodeAgent ({len(tools)} 个工具)...")

            ws.save_trace({"step": "agent_start", "tools": len(tools)})
            raw_output = await asyncio.to_thread(agent.run, prompt)
            ws.save_trace({"step": "agent_done"})
            self._emit_log("smol_agent", "Agent 执行完毕")
            self._emit_status("smol_agent", "success")
            self._ensure_not_stopped()

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
        return build_smol_tools(ws, emit_log=self._emit_log)

    # ── agent ──

    def _make_agent(self, tools: list, ws: Workspace):
        from smolagents import CodeAgent
        model = self._resolve_model()
        model = self._PlanInjectModel(model, ws, emit_log=self._emit_log, check_aborted=self._check_aborted)
        return CodeAgent(
            tools=tools,
            model=model,
            max_steps=self.max_rounds,
            additional_authorized_imports=AUTHORIZED_IMPORTS,
        )

    def _resolve_model(self):
        if self.model is not None:
            return self.model
        if self._llm_preset:
            model_id = self._llm_preset.get("model", "deepseek/deepseek-chat")
            api_key = self._llm_preset.get("apiKey", "")
            api_base = self._llm_preset.get("baseUrl", "https://api.deepseek.com/v1")
            if "/" not in model_id:
                model_id = f"openai/{model_id}"
            from smolagents import LiteLLMModel
            return LiteLLMModel(model_id=model_id, api_key=api_key, api_base=api_base)
        from smolagents import LiteLLMModel
        model_id = os.getenv("SMOL_MODEL_ID", "deepseek/deepseek-chat")
        api_key = os.getenv("DEEPSEEK_API_KEY") or os.getenv("OPENAI_API_KEY", "")
        api_base = os.getenv("SMOL_API_BASE", "https://api.deepseek.com/v1")
        if "/" not in model_id:
            model_id = f"openai/{model_id}"
        return LiteLLMModel(model_id=model_id, api_key=api_key, api_base=api_base)

    def _build_prompt(self, ws: Workspace) -> str:
        prompt_file = Path(__file__).parent / "prompts" / "smol.md"
        base = prompt_file.read_text(encoding="utf-8")
        task = (
            f"\n\n## 当前任务\n"
            f"- workspace: {ws.dir}\n"
            f"- input 文件: {ws.list_inputs()}\n"
            f"- 上下文文档: context/ 目录\n"
            f"\n按 plan 逐项推进，最终产物: summary.md + summary_short.json + output/result.json。"
        )
        if self._analysis_params:
            task += f"\n【用户分析参数】\n{self._analysis_params}\n"
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

    def _write_summary_files(self, ws: Workspace, full_report: str, cards: list[dict]):
        if not self._workspace_dir:
            return
        self._workspace_dir.mkdir(parents=True, exist_ok=True)
        if full_report:
            (self._workspace_dir / "summary.md").write_text(full_report, encoding="utf-8")

        health = "分析完成"
        for c in (cards or []):
            color = c.get("color", "") if isinstance(c, dict) else getattr(c, "color", "")
            if color == "red":
                health = "存在异常"
                break
            elif color == "yellow":
                health = "部分指标异常"

        cards_list = []
        for c in (cards or []):
            if isinstance(c, dict):
                cards_list.append({"title": c.get("title", ""), "explanation": c.get("explanation", ""), "suggestion": c.get("suggestion", ""), "evidence": c.get("evidence", ""), "color": c.get("color", "green")})
            else:
                cards_list.append({"title": getattr(c, "title", ""), "explanation": getattr(c, "explanation", ""), "suggestion": getattr(c, "suggestion", ""), "evidence": getattr(c, "evidence", ""), "color": getattr(c, "color", "green")})
        (self._workspace_dir / "summary_short.json").write_text(
            json.dumps({
                "health_status": health,
                "overview_text": f"共 {len(cards_list)} 项待关注",
                "cards": cards_list,
            }, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )


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
    elif hasattr(result, "raw") and hasattr(result.raw, "model"):
        log["model"] = str(result.raw.model) or ""

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
    raw = _get_attr(result, "raw_response", None) or _get_attr(result, "raw", None)
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
    if hasattr(obj, "token_usage") and obj.token_usage is not None:
        return _safe_dict(obj.token_usage)
    if isinstance(obj, dict) and "usage" in obj:
        return obj["usage"]
    if hasattr(obj, "raw") and obj.raw is not None:
        raw = obj.raw
        if hasattr(raw, "usage") and raw.usage is not None:
            return _safe_dict(raw.usage)
        if isinstance(raw, dict) and "usage" in raw:
            return raw["usage"]
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
