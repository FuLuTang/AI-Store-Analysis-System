"""Smolagents CodeAgent 管线：写代码 → 沙箱执行。

编排器职责：创建 workspace → 注入上下文/tools → 启动 CodeAgent → 限制轮数 → 校验输出
内部步骤由 Agent 自行决定，编排器不写死步骤顺序。
工具通过 build_smol_tools(ws) 闭包注入，禁止全局 get_workspace()。
"""

import time

from .base import AgentPipeline
from .models import AgentResult, DatasetBundle
from .workspace import Workspace
from .adapters import build_smol_tools


class SmolPipeline(AgentPipeline):
    name = "smol"

    def __init__(self, model: str | None = None, max_rounds: int = 15):
        self.model = model or "deepseek-chat"
        self.max_rounds = max_rounds

    async def run(self, bundle: DatasetBundle) -> AgentResult:
        t0 = time.time()
        ws = Workspace(label="smol")
        try:
            # 1. 写原始 parquet
            ws.write_raw_parquet(bundle.tables)
            # 2. 注入上下文文档
            self._stage_context(ws)
            # 3. 构建 Smol 专用工具（闭包注入 workspace）
            tools = build_smol_tools(ws)
            # TODO: 创建 smolagents CodeAgent
            # from smolagents import CodeAgent, HfApiModel
            # agent = CodeAgent(
            #     tools=tools,
            #     model=HfApiModel(self.model),
            #     max_iterations=self.max_rounds,
            # )
            # result = agent.run(prompt)
            # return result
            raise NotImplementedError("SmolPipeline not yet implemented")
        finally:
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
