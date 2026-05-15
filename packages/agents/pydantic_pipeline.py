"""Pydantic AI 管线：staged pipeline — LLM 出策略 → 程序执行和验收。

三个阶段：
1. Flatten Phase: Agent 输出 FlattenPlan → 程序展平 + 写 parquet
2. Mapping Phase: Agent 输出 SemanticMapping[] → 程序记录
3. SQL Phase: Agent 输出 SqlPlan → 程序校验 + 执行

每阶段允许多轮 retry，程序判断成功/部分成功/失败。
"""

from __future__ import annotations

import logging
import os
import time

from packages.agents.base import AgentPipeline
from packages.agents.models import (
    AgentResult,
    DatasetBundle,
    FlattenPlan,
    PhaseResult,
    SemanticMapping,
    SqlPlan,
    TableMeta,
)
from packages.agents.workspace import Workspace
from packages.agents.workspace import _quote_ident as _q

logger = logging.getLogger(__name__)

MAX_RETRIES = 3


class PydanticPipeline(AgentPipeline):
    name = "pydantic"

    def __init__(
        self,
        model: str | None = None,
        base_url: str | None = None,
        api_key: str | None = None,
    ):
        self.model = model or os.getenv("AGENT_MODEL", "deepseek-chat")
        self.base_url = base_url or os.getenv("OPENAI_BASE_URL", "https://api.deepseek.com")
        self.api_key = api_key or os.getenv("OPENAI_API_KEY", "")

    # ================================================================
    # 入口
    # ================================================================

    async def run(self, bundle: DatasetBundle) -> AgentResult:
        t0 = time.time()
        ws = Workspace()
        all_warnings: list[str] = []
        all_phases: list[PhaseResult] = []

        raw_metas = ws.write_raw_parquet(bundle.tables)
        ws.init_duckdb()

        flat_result = await self._run_flatten_phase(ws, raw_metas)
        all_phases.append(flat_result)
        flat_metas = flat_result.output if flat_result.status != "failed" else raw_metas
        all_warnings.extend(flat_result.warnings)
        ws.init_duckdb()  # 重新扫描，注册展平后新产生的 parquet

        mapping_result = await self._run_mapping_phase(ws, flat_metas)
        all_phases.append(mapping_result)
        mappings = mapping_result.output if mapping_result.status != "failed" else []
        all_warnings.extend(mapping_result.warnings)

        sql_result = await self._run_sql_phase(ws, flat_metas, mappings)
        all_phases.append(sql_result)
        metrics = sql_result.output if sql_result.status != "failed" else []
        all_warnings.extend(sql_result.warnings)

        return AgentResult(
            report_id=ws.report_id,
            tables=flat_metas,
            mapping=mappings,
            metrics=metrics,
            warnings=all_warnings,
            pipeline=self.name,
            elapsed_ms=(time.time() - t0) * 1000,
        )

    # ================================================================
    # Phase 1: Flatten
    # ================================================================

    async def _run_flatten_phase(
        self, ws: Workspace, raw_metas: list[TableMeta]
    ) -> PhaseResult:
        errors: list[str] = []
        msg_history = None

        for attempt in range(1, MAX_RETRIES + 1):
            agent = self._build_phase_agent(ws, FlattenPlan, "flatten")
            agent = self._register_phase_tools(agent, ws, "flatten")

            if attempt == 1:
                prompt = self._build_flatten_prompt(raw_metas)
            else:
                prompt = (
                    f"上一次展平失败，错误: {'; '.join(errors)}。请修正 FlattenPlan 后重试。"
                )

            try:
                result = await agent.run(prompt, deps=ws, message_history=msg_history)
                plan: FlattenPlan = result.output
                msg_history = result.all_messages()
            except Exception as e:
                errors.append(f"Agent 调用失败: {e}")
                continue

            flat_metas, exec_errors = self._execute_flatten(ws, plan, raw_metas)
            if not exec_errors:
                return PhaseResult(
                    phase="flatten", status="success",
                    attempts=attempt, output=flat_metas,
                )
            errors = exec_errors

        return PhaseResult(
            phase="flatten", status="failed", attempts=MAX_RETRIES,
            errors=errors, output=raw_metas,
            warnings=["Flatten 阶段失败，使用原始表继续"],
        )

    def _build_flatten_prompt(self, metas: list[TableMeta]) -> str:
        lines = []
        for m in metas:
            cols = ", ".join(c.name for c in m.columns[:20])
            lines.append(f"  - {m.name}: {m.row_count} 行, [{cols}]")
        return (
            "你需要分析以下原始表的结构，制定展平策略。\n\n"
            f"{chr(10).join(lines)}\n\n"
            "输出 FlattenPlan JSON，包含 tables 列表，每项含:\n"
            "- source_table: 原始表名\n"
            "- strategy: pass / explode_array / unfold_object / pivot\n"
            "- target_name: 展平后的表名\n"
            "- columns: 要保留的字段列表\n\n"
            "只输出 FlattenPlan，程序会执行展平。"
        )

    def _execute_flatten(
        self, ws: Workspace, plan: FlattenPlan, raw_metas: list[TableMeta]
    ) -> tuple[list[TableMeta], list[str]]:
        errors: list[str] = []
        flat_metas: list[TableMeta] = []

        for tp in plan.tables:
            if tp.strategy == "pass":
                meta = next((m for m in raw_metas if m.name == tp.source_table), None)
                if meta:
                    flat_metas.append(meta)
                else:
                    errors.append(f"pass 策略找不到源表: {tp.source_table}")
            elif tp.strategy == "explode_array":
                try:
                    df = ws.read_parquet(tp.source_table)
                    exploded = df.explode(tp.columns[0]) if tp.columns else df
                    meta = ws.write_parquet(tp.target_name, exploded)
                    flat_metas.append(meta)
                except Exception as e:
                    errors.append(f"explode_array 失败 ({tp.source_table}): {e}")
            elif tp.strategy == "unfold_object":
                try:
                    df = ws.read_parquet(tp.source_table)
                    from pandas import json_normalize
                    normalized = json_normalize(df[tp.columns].to_dict("records"))
                    meta = ws.write_parquet(tp.target_name, normalized)
                    flat_metas.append(meta)
                except Exception as e:
                    errors.append(f"unfold_object 失败 ({tp.source_table}): {e}")
            elif tp.strategy == "pivot":
                errors.append(f"pivot 策略暂未实现: {tp.source_table}")
            else:
                errors.append(f"未知策略: {tp.strategy}")

        return flat_metas, errors

    # ================================================================
    # Phase 2: Mapping
    # ================================================================

    async def _run_mapping_phase(
        self, ws: Workspace, flat_metas: list[TableMeta]
    ) -> PhaseResult:
        errors: list[str] = []
        msg_history = None
        mappings: list[SemanticMapping] = []

        for attempt in range(1, MAX_RETRIES + 1):
            agent = self._build_phase_agent(ws, list[SemanticMapping], "mapping")
            agent = self._register_phase_tools(agent, ws, "mapping")

            if attempt == 1:
                prompt = self._build_mapping_prompt(flat_metas)
            else:
                prompt = (
                    f"上一次字段映射失败，错误: {'; '.join(errors)}。请修正后重试。"
                )

            try:
                result = await agent.run(prompt, deps=ws, message_history=msg_history)
                mappings = result.output
                msg_history = result.all_messages()
            except Exception as e:
                errors.append(f"Agent 调用失败: {e}")
                continue

            map_errors = self._validate_mappings(mappings, flat_metas)
            if not map_errors:
                return PhaseResult(
                    phase="mapping", status="success",
                    attempts=attempt, output=mappings,
                )
            errors = map_errors

        return PhaseResult(
            phase="mapping", status="failed", attempts=MAX_RETRIES,
            errors=errors, output=mappings,
        )

    def _build_mapping_prompt(self, metas: list[TableMeta]) -> str:
        lines = []
        for m in metas:
            samples = ""
            if m.sample_rows:
                samples = f", 样本行: {m.sample_rows[:1]}"
            cols = ", ".join(c.name for c in m.columns[:20])
            lines.append(f"  - {m.name}: [{cols}]{samples}")
        return (
            "你需要根据表结构和样本数据，将原始字段映射为标准语义字段。\n\n"
            "先用 read_context_tool('fields') 读取标准字段定义。\n\n"
            "表信息:\n" + "\n".join(lines) + "\n\n"
            "输出 SemanticMapping[] JSON 列表，每项含:\n"
            "- raw_field: 原始字段名\n"
            "- table: 所属表名\n"
            "- semantic_field: 标准字段名\n"
            "- confidence: 0~1 置信度\n"
            "- reason: 映射理由\n"
            "- need_confirm: confidence < 0.75 时设为 true\n\n"
            "只输出 SemanticMapping 列表，程序会记录映射。"
        )

    def _validate_mappings(
        self, mappings: list[SemanticMapping], metas: list[TableMeta]
    ) -> list[str]:
        errors: list[str] = []
        if not mappings:
            errors.append("映射列表为空")
            return errors

        valid_columns = {
            col.name
            for m in metas
            for col in m.columns
        }
        for mp in mappings:
            if mp.raw_field not in valid_columns:
                errors.append(f"字段 {mp.raw_field} 不存在于任何表中")
            if not mp.semantic_field:
                errors.append(f"字段 {mp.raw_field} 缺少 semantic_field")
            if mp.confidence < 0 or mp.confidence > 1:
                errors.append(f"字段 {mp.raw_field} confidence 超出 0~1 范围")
        return errors

    # ================================================================
    # Phase 3: SQL
    # ================================================================

    async def _run_sql_phase(
        self,
        ws: Workspace,
        flat_metas: list[TableMeta],
        mappings: list[SemanticMapping],
    ) -> PhaseResult:
        errors: list[str] = []
        msg_history = None

        for attempt in range(1, MAX_RETRIES + 1):
            agent = self._build_phase_agent(ws, SqlPlan, "sql")
            agent = self._register_phase_tools(agent, ws, "sql")

            if attempt == 1:
                prompt = self._build_sql_prompt(flat_metas, mappings)
            else:
                prompt = (
                    f"上一次 SQL 生成失败，错误: {'; '.join(errors)}。请修正后重试。"
                )

            try:
                result = await agent.run(prompt, deps=ws, message_history=msg_history)
                plan: SqlPlan = result.output
                msg_history = result.all_messages()
            except Exception as e:
                errors.append(f"Agent 调用失败: {e}")
                continue

            sql_errors = self._validate_sql_plan(plan, flat_metas)
            if sql_errors:
                errors = sql_errors
                continue

            metrics, exec_errors = self._execute_sql(ws, plan)
            if not exec_errors:
                return PhaseResult(
                    phase="sql", status="success",
                    attempts=attempt, output=metrics,
                )
            errors = exec_errors

        return PhaseResult(
            phase="sql", status="failed", attempts=MAX_RETRIES,
            errors=errors, output=[],
        )

    def _build_sql_prompt(
        self, metas: list[TableMeta], mappings: list[SemanticMapping]
    ) -> str:
        map_text = "\n".join(
            f"  - {m.table}.{m.raw_field} → {m.semantic_field} (confidence={m.confidence})"
            for m in mappings
        )
        table_text = "\n".join(
            f"  - {m.name}: {m.row_count} 行, [{', '.join(c.name for c in m.columns[:15])}]"
            for m in metas
        )
        return (
            "你需要根据字段映射结果和表结构，生成 SQL 查询计划来计算指标。\n\n"
            "先用 read_context_tool('metrics') 读取指标定义和公式。\n\n"
            "表结构:\n" + table_text + "\n\n"
            "字段映射:\n" + map_text + "\n\n"
            "输出 SqlPlan JSON，包含 metrics 列表，每项含:\n"
            "- metric_id: 指标ID\n"
            "- name: 指标名\n"
            "- sql: 要在 DuckDB 上执行的 SELECT 语句\n"
            "- required_fields: 需要的标准字段列表\n\n"
            "SQL 规则:\n"
            "- 只允许 SELECT/WITH 查询\n"
            "- 表名请用双引号包裹\n"
            "- ratio 类: SUM(numerator) / NULLIF(SUM(denominator), 0)\n"
            "- period_change 类: 用 LAG() 窗口函数\n"
            "- share_by_dimension 类: GROUP BY + SUM\n"
            "- concentration 类: SUM(column) / SUM(SUM(column)) OVER()\n\n"
            "只输出 SqlPlan，程序会校验并执行 SQL。"
        )

    def _validate_sql_plan(
        self, plan: SqlPlan, metas: list[TableMeta]
    ) -> list[str]:
        errors: list[str] = []
        if not plan.metrics:
            errors.append("SqlPlan 中 metrics 为空")
            return errors

        valid_tables = {m.name for m in metas}
        valid_columns = {
            (m.name, col.name)
            for m in metas
            for col in m.columns
        }

        for ms in plan.metrics:
            if not ms.metric_id:
                errors.append("metric_id 为空")
            if not ms.sql:
                errors.append(f"{ms.metric_id}: sql 为空")
                continue

            upper = ms.sql.strip().upper()
            if not (upper.startswith("SELECT") or upper.startswith("WITH")):
                errors.append(f"{ms.metric_id}: SQL 只允许 SELECT/WITH，当前: {ms.sql[:50]}")

        return errors

    def _execute_sql(
        self, ws: Workspace, plan: SqlPlan
    ) -> tuple[list, list[str]]:
        import duckdb
        from packages.agents.models import MetricResult, MetricStatus

        errors: list[str] = []
        metrics: list[MetricResult] = []
        db_path = ws.duckdb_path
        conn = duckdb.connect(str(db_path))

        try:
            for ms in plan.metrics:
                try:
                    result = conn.execute(ms.sql).fetchdf()
                    value = result.to_dict(orient="records") if not result.empty else None
                    metrics.append(MetricResult(
                        metric_id=ms.metric_id,
                        name=ms.name,
                        value=value,
                        status=MetricStatus.PASS,
                        required_fields=ms.required_fields,
                    ))
                except Exception as e:
                    errors.append(f"{ms.metric_id}: SQL 执行失败: {e}")
                    metrics.append(MetricResult(
                        metric_id=ms.metric_id,
                        name=ms.name,
                        status=MetricStatus.UNCOUNTABLE,
                        reason=str(e),
                        required_fields=ms.required_fields,
                        missing_fields=ms.required_fields,
                    ))
        finally:
            conn.close()

        return metrics, errors

    # ================================================================
    # Agent 构建
    # ================================================================

    def _build_phase_agent(self, ws: Workspace, output_type, phase: str):
        from pydantic_ai import Agent
        from pydantic_ai.models.openai import OpenAIChatModel
        from pydantic_ai.providers.openai import OpenAIProvider
        from pydantic_ai.settings import ModelSettings

        model = OpenAIChatModel(
            self.model,
            provider=OpenAIProvider(base_url=self.base_url, api_key=self.api_key),
        )

        return Agent(
            model,
            deps_type=Workspace,
            output_type=output_type,
            model_settings=ModelSettings(temperature=0.0),
            tool_retries=3,
            output_retries=3,
        )

    def _register_phase_tools(self, agent, ws: Workspace, phase: str):
        from pydantic_ai import RunContext

        # ---- 公共工具 ----

        @agent.tool
        async def profile_table_tool(ctx: RunContext[Workspace], table_name: str) -> TableMeta:
            """查看指定表的结构、列名、类型、样本值和行数。"""
            w = ctx.deps
            meta = w.manifest.tables_by_name.get(table_name)
            if not meta:
                w.load_manifest()
                meta = w.manifest.tables_by_name.get(table_name)
            if not meta:
                raise ValueError(f"表 {table_name!r} 不在 workspace 中")
            return meta

        @agent.tool
        async def read_context_tool(ctx: RunContext[Workspace], topic: str) -> str:
            """读取上下文文档。topic: metrics/fields/pharmacy/restaurant/hr/common。"""
            from pathlib import Path
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

        # ---- Flatten 阶段工具 ----

        if phase == "flatten":
            @agent.tool
            async def write_workspace_file(ctx: RunContext[Workspace], filename: str, content: str) -> str:
                """将内容写入 workspace 文件。"""
                p = ctx.deps.write_file(filename, content)
                return f"已写入: {p}"

            @agent.tool
            async def run_python_tool(ctx: RunContext[Workspace], script_filename: str) -> str:
                """在 workspace 沙箱内执行指定 Python 脚本，返回 stdout。"""
                import subprocess
                w = ctx.deps
                script_path = w.dir / script_filename
                if not script_path.is_file():
                    return f"脚本不存在: {script_filename}"
                try:
                    r = subprocess.run(
                        ["python", str(script_path)],
                        capture_output=True, text=True, timeout=30,
                    )
                    return r.stdout or r.stderr or "(no output)"
                except subprocess.TimeoutExpired:
                    return "脚本执行超时 (30s)"

        # ---- Mapping 阶段工具 ----

        if phase == "mapping":
            @agent.tool
            async def execute_duckdb_sql(ctx: RunContext[Workspace], sql: str) -> list[dict]:
                """在 DuckDB 上执行 SELECT 查询并返回结果。"""
                import duckdb
                w = ctx.deps
                db_path = w.duckdb_path
                conn = duckdb.connect(str(db_path))
                try:
                    df = conn.execute(sql).fetchdf()
                    return df.head(20).to_dict(orient="records")
                finally:
                    conn.close()

        # ---- SQL 阶段工具 ----

        if phase == "sql":
            @agent.tool
            async def execute_duckdb_sql(ctx: RunContext[Workspace], sql: str) -> list[dict]:
                """在 DuckDB 上执行 SELECT 查询并返回结果。"""
                import duckdb
                w = ctx.deps
                db_path = w.duckdb_path
                conn = duckdb.connect(str(db_path))
                try:
                    df = conn.execute(sql).fetchdf()
                    return df.head(20).to_dict(orient="records")
                finally:
                    conn.close()

            @agent.tool
            async def register_parquet_tool(
                ctx: RunContext[Workspace], table_name: str, parquet_filename: str
            ) -> str:
                """将 workspace 中的 parquet 文件注册为 DuckDB 视图。"""
                import duckdb
                w = ctx.deps
                parquet_path = w.dir / parquet_filename
                if not parquet_path.is_file():
                    return f"parquet 文件不存在: {parquet_filename}"
                qname = _q(table_name)
                db_path = w.duckdb_path
                conn = duckdb.connect(str(db_path))
                try:
                    conn.execute(
                        f"CREATE OR REPLACE VIEW {qname} AS SELECT * FROM '{parquet_path}'"
                    )
                    return f"视图 {qname} 已注册"
                finally:
                    conn.close()

            @agent.tool
            async def list_workspace_files(ctx: RunContext[Workspace]) -> list[str]:
                """列出 workspace 目录下所有文件。"""
                return ctx.deps.list_files()

            @agent.tool
            async def validate_result_tool(ctx: RunContext[Workspace], result_json: str) -> str:
                """校验 AgentResult JSON 的完整性。"""
                import json
                errors = []
                try:
                    data = json.loads(result_json)
                    for field in ["mapping", "metrics", "warnings", "tables"]:
                        if not isinstance(data.get(field), list):
                            errors.append(f"{field} 必须是数组")
                except json.JSONDecodeError as e:
                    errors.append(f"JSON 解析失败: {e}")
                return "校验通过" if not errors else "校验失败: " + "; ".join(errors)

        return agent

    # ================================================================
    # DuckDB 通过 workspace.init_duckdb() 统一管理，无需单独方法
    # ================================================================
