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
    {"title": "查看输入文件", "detail": "列出 input/ 下所有文件，了解数据格式和结构", "status": "pending"},
    {"title": "展平并输出 parquet", "detail": "写 Python 递归展平嵌套数据为二维表，用 pandas 输出 parquet 到 tables/", "status": "pending"},
    {"title": "入库，用 duckdb_register_parquet 注册表", "detail": "把展平后的结构化数据注册到 DuckDB，通过写 Python 检查所有数据都入库了才标记完成（出错的噪声数据可以除外）", "status": "pending"},
    {"title": "画像，用 profile_table 或 duckdb_query 探索字段", "detail": "获取每个表的列名、类型、样本、空值率", "status": "pending"},
    {"title": "读文档，read_context('指标计算文档.md')", "detail": "了解标准字段定义和指标计算公式", "status": "pending"},
    {"title": "映射，原始字段→标准字段", "detail": "根据上下文文档将原始字段映射到标准语义字段", "status": "pending"},
    {"title": "计算指标", "detail": "用 duckdb_query 写 SQL 计算各项指标", "status": "pending"},
    {"title": "输出 AgentResult", "detail": "整理结果，validate_result 校验，写入 output/result.json", "status": "pending"},
    {"title": "清理大文件", "detail": "调用 cleanup_workspace('large')", "status": "pending"},
]


class SmolPipeline(AgentPipeline):
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
            agent = self._make_agent(tools)
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
        design_plan_impl(ws, json.dumps(PLAN_TEMPLATE, ensure_ascii=False))

    # ── tools ──

    def _make_tools(self, ws: Workspace) -> list:
        from .adapters.smol_tools import build_smol_tools
        return build_smol_tools(ws)

    # ── agent ──

    def _make_agent(self, tools: list):
        from smolagents import CodeAgent
        model = self._resolve_model()
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
            f"\n完成后调用 `submit_final_result` 提交。"
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
                    "metrics": data.get("metrics", []), "warnings": data.get("warnings", [])}
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
