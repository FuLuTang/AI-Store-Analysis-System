"""
pydantic_pipeline.py — Pydantic AI 管线（方法3）

LLM 出策略，程序执行。LLM 输出 FlattenPlan / SemanticMapping / SqlPlan，
编排器按计划逐步调用工具执行，不依赖 LLM 写代码。
"""
import logging
from .base import AgentPipeline
from .models import DatasetBundle, AgentResult
from .workspace import AgentWorkspace

logger = logging.getLogger(__name__)


class PydanticPipeline(AgentPipeline):

    def __init__(self, model_id: str = "deepseek-chat"):
        self.model_id = model_id

    async def run(self, bundle: DatasetBundle) -> AgentResult:
        ws = AgentWorkspace(label="pydantic")
        try:
            self._stage_inputs(bundle, ws)
            self._stage_context(ws)
            plan = await self._plan_steps(ws)
            await self._execute_plan(plan, ws)
            raw = self._collect_result(ws)
            return AgentResult.model_validate(raw)
        finally:
            ws.cleanup()

    def _stage_inputs(self, bundle: DatasetBundle, ws: AgentWorkspace):
        for table in bundle.tables:
            ws.write_input_json(f"{table.name}.json", {
                "name": table.name,
                "columns": table.columns,
                "rows": table.rows,
            })

    def _stage_context(self, ws: AgentWorkspace):
        import os
        docs_dir = os.path.join(os.path.dirname(__file__), "..", "..", "docs")
        for doc_name in ["指标计算文档.md"]:
            doc_path = os.path.join(docs_dir, doc_name)
            if os.path.exists(doc_path):
                with open(doc_path) as f:
                    ws.write_context(doc_name, f.read())

    async def _plan_steps(self, ws: AgentWorkspace) -> list:
        """LLM 输出执行计划: [FlattenPlan, SemanticMapping, SqlPlan, ...]"""
        # TODO: LLM 调用，返回结构化 plan
        return []

    async def _execute_plan(self, plan: list, ws: AgentWorkspace):
        """按 plan 逐步用共享 tools 执行"""
        # TODO: 遍历 plan，调用对应 tool
        pass

    def _collect_result(self, ws: AgentWorkspace) -> dict:
        """从 workspace 收集最终产物"""
        return {}
