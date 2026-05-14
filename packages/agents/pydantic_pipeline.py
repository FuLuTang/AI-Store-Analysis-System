"""Pydantic AI 管线：LLM 出策略 → 程序执行。

特点：
- LLM 不接触行数据，只通过 TableMeta 看结构
- LLM 输出 Pydantic structured output（FlattenPlan / SemanticMapping / SqlPlan）
- 程序执行展平、DuckDB 入库、SQL 校验与运行
- 编排器不写死步骤，控制轮数上限，允许 Agent 多轮修复
"""

from __future__ import annotations

import time

from packages.agents.base import AgentPipeline
from packages.agents.models import AgentResult, DatasetBundle
from packages.agents.workspace import Workspace


class PydanticPipeline(AgentPipeline):
    name = "pydantic"

    def __init__(self, model: str = "deepseek-chat", max_rounds: int = 12):
        self.model = model
        self.max_rounds = max_rounds

    async def run(self, bundle: DatasetBundle) -> AgentResult:
        t0 = time.time()
        ws = Workspace()

        try:
            # TODO: 完整实现
            # 1. 写入原始 parquet (ws.write_raw_parquet)
            # 2. 创建 Pydantic AI Agent，注册 tools
            # 3. 循环调用 agent.run，轮数限制 max_rounds
            # 4. Agent 输出 FlattenPlan → 程序执行展平
            # 5. 程序 DuckDB 入库
            # 6. Agent 输出 SemanticMapping[]
            # 7. Agent 输出 SqlPlan → 程序校验+执行 SQL
            # 8. 组装 AgentResult
            # 9. ws.cleanup()
            raise NotImplementedError("PydanticPipeline not yet implemented")
        finally:
            ws.cleanup()

        return AgentResult(
            report_id=ws.report_id,
            pipeline=self.name,
            elapsed_ms=(time.time() - t0) * 1000,
        )
