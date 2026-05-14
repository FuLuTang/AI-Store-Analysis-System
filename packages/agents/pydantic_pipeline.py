"""Pydantic AI 管线：LLM 出策略 → 程序执行。

特点：
- LLM 不接触行数据，只通过 TableMeta 看结构
- LLM 输出 Pydantic structured output（FlattenPlan / SemanticMapping / SqlPlan）
- 程序执行展平、DuckDB 入库、SQL 校验与运行
- 工具通过 ctx.deps.workspace 注入，禁止全局 get_workspace()
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
        ws = Workspace(label="pydantic")
        try:
            # 1. 写原始 parquet
            ws.write_raw_parquet(bundle.tables)
            # 2. 注入上下文文档
            self._stage_context(ws)
            # 3. 创建 AgentDeps
            deps = AgentDeps(workspace=ws, context_docs={})
            # TODO: 创建 Pydantic AI Agent 实例
            # from pydantic_ai import Agent as PydanticAgent
            # agent = PydanticAgent(self.model, deps_type=AgentDeps, output_type=AgentResult)
            # register_pydantic_tools(agent)  ← ctx.deps.workspace 模式
            # result = await agent.run(prompt, deps=deps)
            # return result
            raise NotImplementedError("PydanticPipeline not yet implemented")
        finally:
            # 生产阶段不自动清，保留 audit trail
            pass

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
