"""Pydantic AI 管线：LLM 出策略 → 程序执行。

编排器负责：
- 创建 workspace、写原始 parquet
- 创建 Pydantic AI Agent、注册 tools
- 控制轮数上限、校验输出、组装 AgentResult
- 不写死步骤，Agent 可多轮写代码→执行→查 DB→修复

最终产物固定：AgentResult，步骤次数不固定。
"""

from __future__ import annotations

import os
import time
import logging

from packages.agents.base import AgentPipeline
from packages.agents.models import (
    AgentResult,
    DatasetBundle,
    FlattenPlan,
    SemanticMapping,
    SqlPlan,
    TableMeta,
    MetricResult,
    MetricStatus,
)
from packages.agents.workspace import Workspace

logger = logging.getLogger(__name__)


class PydanticPipeline(AgentPipeline):
    name = "pydantic"

    def __init__(
        self,
        model: str | None = None,
        max_rounds: int = 12,
        base_url: str | None = None,
        api_key: str | None = None,
    ):
        self.model = model or os.getenv("AGENT_MODEL", "deepseek-chat")
        self.max_rounds = max_rounds
        self.base_url = base_url or os.getenv("OPENAI_BASE_URL", "https://api.deepseek.com")
        self.api_key = api_key or os.getenv("OPENAI_API_KEY", "")

    async def run(self, bundle: DatasetBundle) -> AgentResult:
        t0 = time.time()
        ws = Workspace()
        try:
            raw_metas = ws.write_raw_parquet(bundle.tables)
            self._register_duckdb(ws, raw_metas)

            agent = self._build_agent(ws)
            result = await self._run_agent(agent, ws, raw_metas)
            result.report_id = ws.report_id
            result.pipeline = self.name
            result.elapsed_ms = (time.time() - t0) * 1000
            return result
        finally:
            pass  # keep workspace for inspection/debug

    # ---- Agent 构建 ----

    def _build_agent(self, ws: Workspace):
        try:
            from pydantic_ai import Agent
            from pydantic_ai.models.openai import OpenAIChatModel
            from pydantic_ai.providers.openai import OpenAIProvider
        except ImportError:
            raise ImportError(
                "pydantic-ai 未安装，请执行: pip install pydantic-ai"
            )

        model = OpenAIChatModel(
            self.model,
            provider=OpenAIProvider(
                base_url=self.base_url,
                api_key=self.api_key,
            ),
        )

        agent = Agent(
            model,
            deps_type=Workspace,
            system_prompt=self._load_prompt(),
        )

        self._register_tools(agent)
        return agent

    def _load_prompt(self) -> str:
        from pathlib import Path
        prompt_path = Path(__file__).parent / "prompts" / "pydantic.md"
        if prompt_path.is_file():
            return prompt_path.read_text(encoding="utf-8")

        prompt_path = Path(__file__).resolve().parent.parent.parent / "docs" / "agent-poc" / "pydantic-ai-agent" / "PydanticAI官方文档_llms.txt"
        if prompt_path.is_file():
            return prompt_path.read_text(encoding="utf-8")

        return (
            "你是一位数据分析管线 Agent。"
            "用工具查看数据结构、读上下文文档、生成展平策略/字段映射/SQL plan。"
            "程序负责执行，你负责策略。"
        )

    # ---- Tool 注册 ----

    def _register_tools(self, agent):
        from pydantic_ai import RunContext

        @agent.tool
        async def profile_table_tool(ctx: RunContext[Workspace], table_name: str) -> TableMeta:
            """查看指定表的结构、列名、类型、样本值和行数。"""
            ws = ctx.deps
            meta = ws.manifest.tables_by_name.get(table_name)
            if not meta:
                ws.load_manifest()
                meta = ws.manifest.tables_by_name.get(table_name)
            if not meta:
                raise ValueError(f"表 {table_name!r} 不在 workspace 中")
            return meta

        @agent.tool
        async def profile_all_tables(ctx: RunContext[Workspace]) -> list[TableMeta]:
            """列出 workspace 中所有表的 metadata。"""
            ws = ctx.deps
            ws.load_manifest()
            return ws.manifest.tables

        @agent.tool
        async def read_context_tool(ctx: RunContext[Workspace], topic: str) -> str:
            """读取上下文文档。

            topic 可选值:
            - "metrics"  : 指标计算文档
            - "fields"   : 标准字段定义
            - "pharmacy" : 药店行业包
            - "restaurant": 餐饮行业包
            - "hr"       : HR行业包
            - "common"   : 通用经营包
            """
            from pathlib import Path
            root = Path(__file__).parent.parent.parent.parent  # packages/agents -> packages -> project root -> parent = root
            # Actually: __file__=packages/agents/pydantic_pipeline.py
            # .parent = packages/agents, .parent.parent = packages, 
            # .parent.parent.parent = project root
            root = Path(__file__).resolve().parent.parent.parent
            doc_map = {
                "metrics": root / "docs" / "指标计算文档.md",
                "fields": root / "docs" / "指标计算文档.md",
                "pharmacy": root / "packages" / "domain_packs" / "pharmacy.yaml",
                "restaurant": root / "packages" / "domain_packs" / "restaurant.yaml",
                "hr": root / "packages" / "domain_packs" / "hr.yaml",
                "common": root / "packages" / "domain_packs" / "common.yaml",
            }
            path = doc_map.get(topic)
            if not path or not path.is_file():
                return f"未找到文档: topic={topic}"
            return path.read_text(encoding="utf-8")

        @agent.tool
        async def execute_duckdb_sql(ctx: RunContext[Workspace], sql: str) -> list[dict]:
            """在 workspace 的 DuckDB 上执行 SELECT 查询并返回结果。"""
            import duckdb
            ws = ctx.deps
            db_path = ws.dir / "data.ddb"
            conn = duckdb.connect(str(db_path))
            try:
                result = conn.execute(sql).fetchdf()
                return result.head(20).to_dict(orient="records")
            finally:
                conn.close()

        @agent.tool
        async def write_flatten_script(ctx: RunContext[Workspace], script: str) -> str:
            """将 Python 展平脚本写入 workspace 文件，供程序执行。"""
            ws = ctx.deps
            path = ws.write_file("flatten_script.py", script)
            return f"脚本已写入: {path}"

        @agent.tool
        async def run_python_tool(ctx: RunContext[Workspace], script_filename: str) -> str:
            """在 workspace 沙箱内执行指定 Python 脚本，返回 stdout。"""
            import subprocess
            ws = ctx.deps
            script_path = ws.dir / script_filename
            if not script_path.is_file():
                return f"脚本不存在: {script_filename}"
            try:
                result = subprocess.run(
                    ["python", str(script_path)],
                    capture_output=True, text=True,
                    timeout=30,
                )
                return result.stdout or result.stderr or "(no output)"
            except subprocess.TimeoutExpired:
                return "脚本执行超时 (30s)"

        @agent.tool
        async def register_parquet_tool(
            ctx: RunContext[Workspace], table_name: str, parquet_filename: str
        ) -> str:
            """将 workspace 中的 parquet 文件注册为 DuckDB 视图。"""
            import duckdb
            ws = ctx.deps
            parquet_path = ws.dir / parquet_filename
            if not parquet_path.is_file():
                return f"parquet 文件不存在: {parquet_filename}"
            db_path = ws.dir / "data.ddb"
            conn = duckdb.connect(str(db_path))
            try:
                conn.execute(
                    f"CREATE OR REPLACE VIEW {table_name} AS "
                    f"SELECT * FROM '{parquet_path}'"
                )
                return f"视图 {table_name} 已注册 (来自 {parquet_filename})"
            finally:
                conn.close()

        @agent.tool
        async def read_workspace_file(ctx: RunContext[Workspace], filename: str) -> str:
            """读取 workspace 中指定文件的内容。"""
            ws = ctx.deps
            try:
                return ws.read_file(filename)
            except FileNotFoundError:
                return f"文件不存在: {filename}"

        @agent.tool
        async def write_workspace_file(ctx: RunContext[Workspace], filename: str, content: str) -> str:
            """将内容写入 workspace 文件（脚本/manifest/中间结果）。"""
            ws = ctx.deps
            path = ws.write_file(filename, content)
            return f"已写入: {path}"

        @agent.tool
        async def list_workspace_files(ctx: RunContext[Workspace]) -> list[str]:
            """列出 workspace 目录下所有文件。"""
            return ctx.deps.list_files()

        @agent.tool
        async def validate_result_tool(ctx: RunContext[Workspace], result_json: str) -> str:
            """校验 AgentResult JSON 的完整性（tables/mapping/metrics/warnings）。"""
            import json
            errors = []
            try:
                data = json.loads(result_json)
                if not isinstance(data.get("mapping"), list):
                    errors.append("mapping 必须是数组")
                if not isinstance(data.get("metrics"), list):
                    errors.append("metrics 必须是数组")
                if not isinstance(data.get("warnings"), list):
                    errors.append("warnings 必须是数组（可为空）")
                if not isinstance(data.get("tables"), list):
                    errors.append("tables 必须是数组")
            except json.JSONDecodeError as e:
                errors.append(f"JSON 解析失败: {e}")
            return "校验通过" if not errors else "校验失败: " + "; ".join(errors)

        @agent.tool
        async def submit_final_result_tool(ctx: RunContext[Workspace], result_json: str) -> str:
            """强制提交标准 AgentResult JSON，写入 workspace 的 result.json。"""
            ws = ctx.deps
            ws.write_file("result.json", result_json)
            return f"结果已提交至: {ws.dir / 'result.json'}"

    # ---- 内部执行 ----

    def _register_duckdb(self, ws: Workspace, metas: list[TableMeta]) -> None:
        """将原始 parquet 注册为 DuckDB 视图。"""
        import duckdb
        db_path = ws.dir / "data.ddb"
        conn = duckdb.connect(str(db_path))
        try:
            for meta in metas:
                conn.execute(
                    f"CREATE OR REPLACE VIEW {meta.name} AS "
                    f"SELECT * FROM '{meta.path}'"
                )
        finally:
            conn.close()

    async def _run_agent(
        self, agent, ws: Workspace, metas: list[TableMeta]
    ) -> AgentResult:
        from pydantic_ai import UsageLimits

        table_names = [m.name for m in metas]
        table_info = "\n".join(
            f"- {m.name} ({m.row_count} 行, {len(m.columns)} 列: "
            f"{', '.join(c.name for c in m.columns[:10])}"
            f"{'...' if len(m.columns) > 10 else ''})"
            for m in metas
        )

        prompt = f"""以下是上传的数据表概览：

{ws.dir}

{len(metas)} 张表:
{table_info}

请按以下步骤完成分析：
1. 用 profile_table_tool 查看每张表的结构
2. 用 read_context_tool 读取标准字段定义
3. 如果需要展平，写 flatten 脚本并执行
4. 用 register_parquet_tool 注册展平后的 parquet
5. 做字段映射，输出为 JSON（包含 rawField, semanticField, confidence）
6. 写 SQL 计算指标并执行
7. 最终汇总为 AgentResult JSON：
   {{
     "tables": [...],
     "mapping": [...],
     "metrics": [...],
     "warnings": [...]
   }}
"""

        try:
            result = await agent.run(
                prompt,
                deps=ws,
                usage_limits=UsageLimits(request_limit=self.max_rounds),
            )
            output = result.output
        except Exception as e:
            logger.exception("Agent 执行失败")
            return AgentResult(
                report_id=ws.report_id,
                warnings=[f"Agent 执行异常: {str(e)}"],
                pipeline=self.name,
            )

        return self._parse_agent_output(ws, output, metas)

    def _parse_agent_output(
        self, ws: Workspace, output, metas: list[TableMeta]
    ) -> AgentResult:
        """从 Agent 输出中提取 AgentResult。"""
        import json
        text = str(output) if not isinstance(output, str) else output

        warnings: list[str] = []

        # 尝试从输出中提取 JSON
        try:
            # 可能包裹在 ```json ... ``` 中
            if "```json" in text:
                text = text.split("```json")[1].split("```")[0]
            elif "```" in text:
                text = text.split("```")[1].split("```")[0]

            data = json.loads(text.strip())
            result = AgentResult(
                report_id=ws.report_id,
                tables=metas,
                mapping=data.get("mapping", []),
                metrics=data.get("metrics", []),
                warnings=data.get("warnings", warnings),
                pipeline=self.name,
            )
        except (json.JSONDecodeError, IndexError):
            result = AgentResult(
                report_id=ws.report_id,
                tables=metas,
                warnings=[*warnings, f"Agent 输出非标准 JSON，原文: {text[:500]}"],
                pipeline=self.name,
            )

        return result
