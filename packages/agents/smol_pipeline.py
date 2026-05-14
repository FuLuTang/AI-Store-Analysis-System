"""
smol_pipeline.py — Smolagents CodeAgent 管线（方法2）

编排器职责：创建 workspace → 注入上下文/tools → 启动 CodeAgent → 限制轮数 → 校验输出 → 清理
内部步骤由 Agent 自行决定，编排器不写死步骤顺序。
"""
import json
import logging
from .base import AgentPipeline
from .models import DatasetBundle, AgentResult
from .workspace import AgentWorkspace

logger = logging.getLogger(__name__)


class SmolPipeline(AgentPipeline):

    def __init__(self, model_id: str = "deepseek-chat", max_rounds: int = 15):
        self.model_id = model_id
        self.max_rounds = max_rounds

    async def run(self, bundle: DatasetBundle) -> AgentResult:
        ws = AgentWorkspace(label="smol")
        try:
            self._stage_inputs(bundle, ws)
            self._stage_context(ws)
            tool_list = self._load_tools(ws)
            agent = self._create_agent(tool_list)
            raw_output = await self._execute_agent(agent, ws)
            validated = self._validate_output(raw_output)
            return validated
        finally:
            ws.cleanup()

    def _stage_inputs(self, bundle: DatasetBundle, ws: AgentWorkspace):
        """写入上传文件到 workspace input/"""
        for table in bundle.tables:
            ws.write_input_json(f"{table.name}.json", {
                "name": table.name,
                "columns": table.columns,
                "rows": table.rows,
            })

    def _stage_context(self, ws: AgentWorkspace):
        """注入上下文文档到 workspace context/"""
        # 第一阶段：注入指标文档
        import os
        docs_dir = os.path.join(os.path.dirname(__file__), "..", "..", "docs")
        for doc_name in ["指标计算文档.md"]:
            doc_path = os.path.join(docs_dir, doc_name)
            if os.path.exists(doc_path):
                with open(doc_path) as f:
                    ws.write_context(doc_name, f.read())

    def _load_tools(self, ws: AgentWorkspace):
        """加载共享工具，注入 workspace 引用"""
        from .tools import (
            read_workspace_file, write_workspace_file, list_workspace_files,
            duckdb_query, duckdb_register_parquet,
            run_python_script, read_context, validate_result, profile_table,
        )
        # TODO: 注入 workspace 到每个 tool
        return []

    def _create_agent(self, tool_list: list):
        """创建 smolagents CodeAgent"""
        # TODO: from smolagents import CodeAgent, HfApiModel
        pass

    async def _execute_agent(self, agent, ws: AgentWorkspace) -> dict:
        """启动 Agent，限制轮数，收集输出"""
        # TODO: agent.run(prompt)
        pass

    def _validate_output(self, raw: dict) -> AgentResult:
        """校验输出结构"""
        try:
            return AgentResult.model_validate(raw)
        except Exception:
            return AgentResult(raw_output=str(raw))
