"""Smolagents CodeAgent 管线：写代码 → 沙箱执行。

编排器职责：创建 workspace → 注入上下文/tools → 启动 CodeAgent → 限制轮数 → 校验输出 → 清理
内部步骤由 Agent 自行决定，编排器不写死步骤顺序。
"""

import time

from .base import AgentPipeline
from .models import AgentResult, DatasetBundle
from .workspace import Workspace


class SmolPipeline(AgentPipeline):
    name = "smol"

    def __init__(self, model: str | None = None, max_rounds: int = 15):
        self.model = model
        self.max_rounds = max_rounds

    async def run(self, bundle: DatasetBundle) -> AgentResult:
        t0 = time.time()
        ws = Workspace(label="smol")

        try:
            # TODO: 完整实现
            # 1. 写入原始 parquet (ws.write_raw_parquet)
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
