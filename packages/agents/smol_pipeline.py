"""Smolagents 管线：CodeAgent 写代码 → 沙箱执行。

特点：
- CodeAgent 在 workspace 中写 Python 脚本执行展平
- 所有代码执行限制在 workspace 沙箱内
- 最终输出与 Pydantic 线一致的 AgentResult
"""

from __future__ import annotations

import time

from packages.agents.base import AgentPipeline
from packages.agents.models import AgentResult, DatasetBundle
from packages.agents.workspace import Workspace


class SmolPipeline(AgentPipeline):
    name = "smol"

    def __init__(self, model: str | None = None, max_rounds: int = 8):
        self.model = model
        self.max_rounds = max_rounds

    async def run(self, bundle: DatasetBundle) -> AgentResult:
        t0 = time.time()
        ws = Workspace()

        try:
            # TODO: 完整实现
            # 1. 写入原始 parquet
            # 2. 创建 smolagents CodeAgent，注册 tools
            # 3. CodeAgent 多轮写代码→执行→查 DuckDB→修复
            # 4. 组装 AgentResult
            raise NotImplementedError("SmolPipeline not yet implemented")
        finally:
            ws.cleanup()

        return AgentResult(
            report_id=ws.report_id,
            pipeline=self.name,
            elapsed_ms=(time.time() - t0) * 1000,
        )
