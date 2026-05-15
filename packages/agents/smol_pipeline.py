"""smol_pipeline.py — Smolagents CodeAgent 管线（方法2）
编排器职责：创建 workspace → 注入上下文/tools → 启动 CodeAgent → 限制轮数 → 校验输出 → 清理
内部步骤由 Agent 自行决定，编排器不写死步骤顺序。"""
import asyncio
import json
import logging
import re
from pathlib import Path
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

            tools = self._make_tools(ws)
            agent = self._make_agent(tools)
            prompt = self._build_prompt(ws)

            raw_output = await asyncio.to_thread(agent.run, prompt)
            return self._collect_result(raw_output, ws)
        finally:
            ws.cleanup()

    # ── staging ──

    def _stage_inputs(self, bundle: DatasetBundle, ws: AgentWorkspace):
        for table in bundle.tables:
            ws.write_input_json(f"{table.name}.json", {
                "name": table.name,
                "columns": table.columns,
                "rows": table.rows,
            })

    def _stage_context(self, ws: AgentWorkspace):
        ROOT = Path(__file__).parent.parent.parent
        docs_dir = ROOT / "docs"
        for name in ["指标计算文档.md"]:
            doc = docs_dir / name
            if doc.exists():
                ws.write_context(name, doc.read_text(encoding="utf-8"))

    # ── tools ──

    def _make_tools(self, ws: AgentWorkspace) -> list:
        from .adapters.smol_tool_adapter import create_smol_tools
        return create_smol_tools(ws)

    # ── agent ──

    def _make_agent(self, tools: list):
        from smolagents import CodeAgent, HfApiModel
        return CodeAgent(
            tools=tools,
            model=HfApiModel(self.model_id),
            max_iterations=self.max_rounds,
        )

    def _build_prompt(self, ws: AgentWorkspace) -> str:
        prompt_file = Path(__file__).parent / "prompts" / "smol.md"
        base = prompt_file.read_text(encoding="utf-8")
        task = (
            f"\n\n## 当前任务\n"
            f"- workspace: {ws.dir}\n"
            f"- input 目录文件: {ws.list_inputs()}\n"
            f"- 上下文文档: context/ 目录\n"
            f"\n请按流程完成展平→入库→画像→映射→计算→输出，最终调用 `submit_final_result` 提交。"
        )
        return base + task

    # ── collect ──

    def _collect_result(self, raw_output: str, ws: AgentWorkspace) -> AgentResult:
        # 优先从 agent 写的 output/result.json 取
        data = ws.read_output_json("result.json")
        if data:
            try:
                return AgentResult.model_validate(data)
            except Exception:
                pass

        # 其次从 agent.run() 返回值里提取 JSON
        try:
            data = json.loads(raw_output)
            return AgentResult.model_validate(data)
        except (json.JSONDecodeError, Exception):
            pass

        # 最后尝试提取 markdown code block
        m = re.search(r"```(?:json)?\s*\n?(.*?)\n?```", raw_output, re.DOTALL)
        if m:
            try:
                data = json.loads(m.group(1))
                return AgentResult.model_validate(data)
            except (json.JSONDecodeError, Exception):
                pass

        return AgentResult(raw_output=raw_output[:2000])
