"""Pydantic AI 管线：LLM 出策略 → 程序执行。

特点：
- LLM 不接触行数据，只通过 TableMeta 看结构
- LLM 输出 Pydantic structured output（FlattenPlan / SemanticMapping / SqlPlan）
- 程序执行展平、DuckDB 入库、SQL 校验与运行
- 工具通过 ctx.deps.workspace 注入，禁止全局 get_workspace()

init 流程（已可实现）:
  1. 创建 Workspace → 目录 + 分区 + duckdb
  2. 写原始数据为 parquet
  3. 注入上下文文档
  4. 注册 DuckDB 视图
  5. 创建 Agent + 注入 deps
"""

import time

from .base import AgentPipeline
from .models import AgentResult, DatasetBundle
from .workspace import Workspace
from .adapters import AgentDeps, register_pydantic_tools


class PydanticPipeline(AgentPipeline):
    name = "pydantic"

    def __init__(self, model: str = "deepseek-chat", max_rounds: int = 12):
        self.model = model
        self.max_rounds = max_rounds

    async def run(self, bundle: DatasetBundle) -> AgentResult:
        t0 = time.time()

        # ── Phase 1: Init workspace ──
        ws = Workspace(label="pydantic")
        ws.write_raw_parquet(bundle.tables)
        self._stage_context(ws)
        ws.init_duckdb()
        ws.save_trace({"step": "init", "tables": len(bundle.tables)})

        # ── Phase 2: Create Agent + register tools ──
        deps = AgentDeps(workspace=ws, context_docs={})
        # from pydantic_ai import Agent as PydanticAgent
        # agent = PydanticAgent(self.model, deps_type=AgentDeps, output_type=AgentResult)
        # register_pydantic_tools(agent)
        # result = await agent.run(self._prompt(ws), deps=deps)

        raise NotImplementedError("PydanticPipeline: Agent 创建待 pydantic_ai 安装后实现")

        # ── Phase 3: Return ──
        return AgentResult(
            report_id=ws.report_id,
            pipeline=self.name,
            elapsed_ms=(time.time() - t0) * 1000,
        )

    def _stage_context(self, ws: Workspace):
        import os
        docs_dir = os.path.join(os.path.dirname(__file__), "..", "..", "docs")
        for doc_name in ["指标计算文档.md"]:
            doc_path = os.path.join(docs_dir, doc_name)
            if os.path.exists(doc_path):
                with open(doc_path) as f:
                    ws.write_context(doc_name, f.read())

    def _prompt(self, ws: Workspace) -> str:
        import os
        prompt_path = os.path.join(os.path.dirname(__file__), "prompts", "pydantic.md")
        if os.path.exists(prompt_path):
            with open(prompt_path) as f:
                return f.read()
        return "请根据 workspace 中的数据进行经营诊断分析。"
