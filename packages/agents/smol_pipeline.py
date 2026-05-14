"""Smolagents CodeAgent 管线：写代码 → 沙箱执行。

编排器职责：创建 workspace → 注入上下文/tools → 启动 CodeAgent → 限制轮数 → 校验输出
内部步骤由 Agent 自行决定，编排器不写死步骤顺序。
工具通过 build_smol_tools(ws) 闭包注入，禁止全局 get_workspace()。

init 流程（已可实现）:
  1. 创建 Workspace → 目录 + 分区 + duckdb
  2. 写原始数据为 parquet
  3. 注入上下文文档
  4. 注册 DuckDB 视图
  5. 构建 tools + 创建 CodeAgent
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

        # ── Phase 1: Init workspace ──
        ws = Workspace(label="smol")
        ws.write_raw_parquet(bundle.tables)
        self._stage_context(ws)
        ws.init_duckdb()
        ws.save_trace({"step": "init", "tables": len(bundle.tables)})

        # ── Phase 2: Build tools + create CodeAgent ──
        try:
            tools = build_smol_tools(ws)
        except ImportError as e:
            raise NotImplementedError(f"SmolPipeline: {e}")
        # from smolagents import CodeAgent, HfApiModel
        # agent = CodeAgent(
        #     tools=tools,
        #     model=HfApiModel(self.model),
        #     max_iterations=self.max_rounds,
        # )
        # result = agent.run(self._prompt())
        # return self._parse_result(result, ws)

        raise NotImplementedError("SmolPipeline: Agent 创建待 smolagents 安装后实现")

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

    def _prompt(self) -> str:
        import os
        prompt_path = os.path.join(os.path.dirname(__file__), "prompts", "smol.md")
        if os.path.exists(prompt_path):
            with open(prompt_path) as f:
                return f.read()
        return "请根据 workspace 中的数据进行经营诊断分析。"

    def _parse_result(self, raw: str, ws: Workspace) -> AgentResult:
        try:
            import json
            data = json.loads(raw) if isinstance(raw, str) else raw
            return AgentResult(**data)
        except Exception:
            return AgentResult(
                report_id=ws.report_id,
                pipeline=self.name,
                raw_output=str(raw),
            )
