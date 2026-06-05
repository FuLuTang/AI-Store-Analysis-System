"""custom_pipeline.py — 自定义 AgentPipeline，使用薄 OpenAI SDK Agent 循环。
"""

import json
import logging
import time
from pathlib import Path

from openai import OpenAI

from ..core.base import AgentPipeline
from ..core.models import AgentResult, DatasetBundle
from ..core.workspace import Workspace
from ..core.agent_loop import AgentLoop

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
            self._emit_log("custom_init", {"level": "info", "message": f"🚀 启动 Custom 管线，{len(bundle.tables)} 张表, {len(bundle.raw_files)} 个原始文件", "progress": 3})
            self._emit_status("custom_init", "active")

            # 保存原始文件
            for rf in bundle.raw_files:
                ws.write_input(rf.name, rf.data)
            ws.unpack_archives()
            # 原始 JSON 写到 input/
            for t in bundle.tables:
                file_stem = t.name.replace(" ", "_").replace("/", "_")
                ws.write_input_json(f"{file_stem}.json", {"name": t.name, "rows": t.rows})
            # 预写 parquet（不做 DuckDB 注册，由 Agent 自行注册）
            ws.write_raw_parquet(bundle.tables)
            self._stage_context(ws)
            self._emit_log("custom_init", {"level": "info", "message": "✅ 环境初始化完毕", "progress": 8})
            self._emit_status("custom_init", "success")

            # ── 写入 plan ──
            self._emit_status("custom_plan", "active")
            self._write_plan(ws)
            self._emit_log("custom_plan", {"level": "info", "message": "📋 计划已写入", "progress": 12})
            self._emit_status("custom_plan", "success")

            # ── 构建 client ──
            preset = self._llm_preset
            api_key = preset.get("apiKey", "")
            base_url = preset.get("baseUrl", "https://api.deepseek.com")
            client = OpenAI(api_key=api_key, base_url=base_url, max_retries=0)

            # ── Bootstrap 注入 ──
            bootstrap_messages = self._build_bootstrap_messages(ws)

            # ── 运行 Agent Loop ──
            loop = AgentLoop(
                client=client,
                ws=ws,
                llm_preset=preset,
                analysis_params=self._analysis_params,
                emit_log=self._emit_log,
                emit_status=self._emit_status,
                check_aborted=self._check_aborted,
                bootstrap_messages=bootstrap_messages,
            )

            # 标记第一个步骤节点为 active
            self._emit_status("custom_step0", "active")
            self._emit_log("custom_step0", {
                "level": "status",
                "message": "[步骤 1/4] 数据展平 开始执行...",
                "step": {"index": 0, "title": "数据展平"},
                "progress": 15
            })

            output = await asyncio.to_thread(loop.run)

            # ── 检查 plan 是否全部完成 ──
            if not self._is_plan_done(ws):
                logger.error("Agent plan not fully completed")
                self._emit_log("custom_done", {"level": "error", "message": "Agent 诊断步骤未全部完成", "error_details": "Agent 计划中存在未成功完成的步骤。"})
                raise RuntimeError("Agent 诊断步骤未全部完成")

            # ── 收尾与产物输出 ──
            self._emit_status("custom_output", "active")
            self._emit_log("custom_output", {"level": "status", "message": "📦 正在整理产物...", "progress": 95})

            elapsed_ms = (time.time() - t0) * 1000

            # ── Agent 已将产物写入 workspace，API 直接读文件 ──
            token_usage = output.get("_token_usage", {})
            cards, full_report, metrics = self._read_agent_outputs(ws, token_usage)

            self._emit_log("custom_output", {"level": "info", "message": f"✅ 产物输出完毕，耗时 {elapsed_ms/1000:.1f}s", "progress": 100})
            self._emit_status("custom_output", "success")

            return AgentResult(
                report_id=ws.report_id,
                pipeline=self.name,
                elapsed_ms=elapsed_ms,
                metrics=metrics,
                cards=cards,
                full_report=full_report,
                total_tokens=token_usage.get("total_tokens", 0),
                input_tokens=token_usage.get("input_tokens", 0),
                cache_hit_tokens=token_usage.get("cache_hit_tokens", 0),
                cache_miss_tokens=token_usage.get("cache_miss_tokens", 0),
            )

        finally:
            ws.cleanup_large_files()

    # ── staging ──

    def _stage_context(self, ws: Workspace):
        ROOT = Path(__file__).parent.parent.parent.parent
        docs_dir = ROOT / "docs"
        doc_map = {
            "指标计算文档.md": docs_dir / "经营分析" / "指标计算文档.md",
        }
        for name, doc in doc_map.items():
            if doc.exists():
                ws.write_context(name, doc.read_text(encoding="utf-8"))

    def _write_plan(self, ws: Workspace):
        from ..core.tools.impl.setup_impl import design_plan_impl
        from .plan_template import PLAN_TEMPLATE
        import copy
        
        plan = copy.deepcopy(PLAN_TEMPLATE)
        params_str = self._analysis_params
        
        for step in plan:
            if "get_param" in step["detail"]:
                params_str = (self._analysis_params or "").strip() or "none"
                step["detail"] = step["detail"].replace("get_param", params_str)
                    
        design_plan_impl(ws, json.dumps(plan, ensure_ascii=False))
        plan_path = ws.resolve("plan.json")
        plan = json.loads(plan_path.read_text(encoding="utf-8"))
        if plan and plan[0]["status"] == "pending":
            plan[0]["status"] = "in_progress"
            plan_path.write_text(json.dumps(plan, ensure_ascii=False, indent=2), encoding="utf-8")

    def _is_plan_done(self, ws: Workspace) -> bool:
        try:
            plan_path = ws.resolve("plan.json")
            if not plan_path.exists():
                return False
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            return all(s.get("status") == "success" for s in plan)
        except Exception:
            return False

    def _build_bootstrap_messages(self, ws: Workspace) -> list[dict]:
        from ..core.tools.impl.setup_impl import read_plan_short_impl
        from ..core.tools.impl.file_impl import list_files_impl
        from ..core.tools.impl.doc_impl import read_document_structure_impl

        def tool_pair(call_id: str, name: str, arguments: dict, content: str) -> list[dict]:
            return [
                {
                    "role": "assistant",
                    "content": None,
                    "reasoning_content": "",
                    "tool_calls": [
                        {
                            "id": call_id,
                            "type": "function",
                            "function": {
                                "name": name,
                                "arguments": json.dumps(arguments, ensure_ascii=False),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": call_id,
                    "name": name,
                    "content": content,
                },
            ]

        messages: list[dict] = []
        
        # 1. list_files
        list_files_json = json.dumps(list_files_impl(ws, ""), ensure_ascii=False)
        messages.extend(tool_pair("call_bootstrap_list_files", "list_files", {"subdir": ""}, list_files_json))

        # 2. read_plan_short
        plan_short_text = read_plan_short_impl(ws)
        if plan_short_text and plan_short_text.strip():
            messages.extend(tool_pair("call_bootstrap_read_plan_short", "read_plan_short", {}, plan_short_text))

        # 3. read_document_structure
        for idx, rel_path in enumerate(ws.list_inputs()):
            try:
                struct_text = read_document_structure_impl(ws, f"input/{rel_path}")
                messages.extend(tool_pair(
                    f"call_bootstrap_read_document_structure_{idx}",
                    "read_document_structure",
                    {"path": f"input/{rel_path}"},
                    struct_text,
                ))
            except Exception:
                continue

        # 4. old session scripts
        old_scripts_dir = ws.resolve("scripts/old_session_scripts")
        if old_scripts_dir.is_dir():
            for session_dir in sorted(old_scripts_dir.iterdir()):
                if not session_dir.is_dir():
                    continue
                for script_file in sorted(session_dir.iterdir()):
                    if script_file.suffix != ".py":
                        continue
                    try:
                        rel = str(script_file.relative_to(ws.dir))
                        content = script_file.read_text(encoding="utf-8")
                        call_id = f"call_bootstrap_old_script_{script_file.stem}"
                        messages.extend(tool_pair(call_id, "read_file", {"path": rel}, content))
                    except Exception:
                        continue

        return messages

    def _read_agent_outputs(self, ws: Workspace, token_usage: dict | None = None) -> tuple[list, str, list]:
        """从 workspace 文件组装 Agent 最终产物，程序化写入 output/result.json。"""
        cards = []
        full_report = ""
        metrics = []

        try:
            short_path = ws.resolve("output/summary_short.json")
            if short_path.exists():
                short = json.loads(short_path.read_text(encoding="utf-8"))
                if isinstance(short, dict):
                    cards = short.get("cards", [])
        except Exception:
            pass

        try:
            summary_path = ws.resolve("output/summary.md")
            if summary_path.exists():
                full_report = summary_path.read_text(encoding="utf-8")
        except Exception:
            pass

        try:
            metrics_path = ws.resolve("指标.json")
            if metrics_path.exists():
                data = json.loads(metrics_path.read_text(encoding="utf-8"))
                if isinstance(data, list):
                    metrics = data
                elif isinstance(data, dict):
                    metrics = data.get("metrics", data.get("indicators", []))
        except Exception:
            pass

        result = {"cards": cards, "full_report": full_report, "metrics": metrics}
        if token_usage:
            result["token_usage"] = token_usage
        try:
            result_path = ws.resolve("output/result.json")
            result_path.parent.mkdir(parents=True, exist_ok=True)
            result_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            pass

        return cards, full_report, metrics
