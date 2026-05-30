"""custom_pipeline.py — 自定义 AgentPipeline，使用薄 OpenAI SDK Agent 循环。
"""

import json
import logging
import time
from pathlib import Path

from openai import OpenAI

from .base import AgentPipeline
from .models import AgentResult, DatasetBundle, TableMeta
from .workspace import Workspace
from .adapters.agent_loop import AgentLoop

logger = logging.getLogger("agent.custom")


class CustomPipeline(AgentPipeline):
    name = "custom"

    def __init__(self, model=None, llm_preset=None, check_aborted=None, workspace_dir=None, analysis_params=""):
        super().__init__(workspace_dir=workspace_dir, analysis_params=analysis_params)
        self._llm_preset = llm_preset or {}
        self._check_aborted = check_aborted

    async def run(self, bundle: DatasetBundle) -> AgentResult:
        import asyncio
        t0 = time.time()

        ws = Workspace(base_dir=self._workspace_dir) if self._workspace_dir else Workspace(label="custom")

        try:
            # ── 初始化 workspace ──
            self._emit_log("custom_init", f"启动 Custom 管线，{len(bundle.tables)} 张表, {len(bundle.raw_files)} 个原始文件")
            self._emit_status("custom_init", "active")

            # 保存原始文件
            for rf in bundle.raw_files:
                ws.write_input(rf.name, rf.data)
            # 原始 JSON 写到 input/
            for t in bundle.tables:
                file_stem = t.name.replace(" ", "_").replace("/", "_")
                ws.write_input_json(f"{file_stem}.json", {"name": t.name, "rows": t.rows})
            # 预写 parquet（不做 DuckDB 注册，由 Agent 自行注册）
            ws.write_raw_parquet(bundle.tables)
            self._stage_context(ws)
            self._emit_log("custom_init", "环境初始化完毕")
            self._emit_status("custom_init", "success")

            # ── 写入 plan ──
            self._emit_status("custom_plan", "active")
            self._write_plan(ws)
            self._emit_log("custom_plan", "计划已写入")
            self._emit_status("custom_plan", "success")

            # ── 构建 client ──
            preset = self._llm_preset
            api_key = preset.get("apiKey", "")
            base_url = preset.get("baseUrl", "https://api.deepseek.com")
            client = OpenAI(api_key=api_key, base_url=base_url, max_retries=0)

            # ── 运行 Agent Loop ──
            loop = AgentLoop(
                client=client,
                ws=ws,
                llm_preset=preset,
                analysis_params=self._analysis_params,
                emit_log=self._emit_log,
                emit_status=self._emit_status,
            )
            output = await asyncio.to_thread(loop.run)

            elapsed_ms = (time.time() - t0) * 1000

            # ── Agent 已将产物写入 workspace，API 直接读文件 ──
            cards, full_report = self._read_agent_outputs(ws)
            tables = [
                TableMeta(
                    name=t.name,
                    duckdb_name=t.name,
                    path=f"input/{t.name.replace(' ', '_').replace('/', '_')}.json",
                    row_count=len(t.rows),
                    columns=[],
                )
                for t in bundle.tables
            ]
            return AgentResult(
                report_id=ws.report_id,
                pipeline=self.name,
                elapsed_ms=elapsed_ms,
                scene=output.get("scene"),
                mapping=output.get("mapping", []),
                metrics=output.get("metrics", []),
                warnings=output.get("warnings", []),
                cards=cards,
                full_report=full_report,
                tables=tables,
            )

        finally:
            ws.cleanup_large_files()

    # ── staging ──

    def _stage_context(self, ws: Workspace):
        ROOT = Path(__file__).parent.parent.parent
        docs_dir = ROOT / "docs"
        for name in ["指标计算文档.md"]:
            doc = docs_dir / name
            if doc.exists():
                ws.write_context(name, doc.read_text(encoding="utf-8"))

    def _write_plan(self, ws: Workspace):
        from .tools.impl.setup_impl import design_plan_impl
        from .plan_template import PLAN_TEMPLATE
        design_plan_impl(ws, json.dumps(PLAN_TEMPLATE, ensure_ascii=False))
        plan_path = ws.resolve("output/plan.json")
        plan = json.loads(plan_path.read_text(encoding="utf-8"))
        if plan and plan[0]["status"] == "pending":
            plan[0]["status"] = "in_progress"
            plan_path.write_text(json.dumps(plan, ensure_ascii=False, indent=2), encoding="utf-8")

    def _read_agent_outputs(self, ws: Workspace) -> tuple[list, str]:
        """从 workspace 文件读取 Agent 的最终产物。
        cards 优先从 output/result.json（Agent validate 校验过的文件）读取，
        其次从 summary_short.json 读取。
        """
        cards = []
        full_report = ""

        try:
            result_path = ws.resolve("output/result.json")
            if result_path.exists():
                result_data = json.loads(result_path.read_text(encoding="utf-8"))
                if isinstance(result_data, dict):
                    cards = result_data.get("cards", [])
                    full_report = result_data.get("full_report", "")
        except Exception:
            pass

        if not cards:
            try:
                short_path = ws.resolve("summary_short.json")
                if short_path.exists():
                    short = json.loads(short_path.read_text(encoding="utf-8"))
                    if isinstance(short, dict):
                        cards = short.get("cards", [])
            except Exception:
                pass

        if not full_report:
            try:
                summary_path = ws.resolve("summary.md")
                if summary_path.exists():
                    full_report = summary_path.read_text(encoding="utf-8")
            except Exception:
                pass

        return cards, full_report
