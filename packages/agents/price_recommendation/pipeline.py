"""pipeline.py — PriceRecommendation AgentPipeline."""

import json
import logging
import time
import copy
from pathlib import Path
from typing import Any

from openai import OpenAI

from ..core.base import AgentPipeline
from ..core.models import AgentResult, DatasetBundle
from ..core.workspace import Workspace
from ..core.agent_loop import AgentLoop
from ..core.tools.impl.setup_impl import design_plan_impl
from .plan_template import PRICE_PLAN_TEMPLATE
from packages.price_recommendation.data_fitting import run_data_fitting

logger = logging.getLogger("price_recommendation")


class PricePipeline(AgentPipeline):
    name = "price_recommendation"

    def __init__(self, *, llm_preset=None, check_aborted=None, workspace_dir=None, product_name: str = "", candidate_count: int = 2):
        super().__init__(workspace_dir=workspace_dir)
        self._llm_preset = llm_preset or {}
        self._check_aborted = check_aborted
        self.product_name = product_name
        self.candidate_count = candidate_count

    async def run(self, bundle: DatasetBundle) -> AgentResult:
        import asyncio
        t0 = time.time()
        
        ws = Workspace(base_dir=self._workspace_dir) if self._workspace_dir else Workspace(label="price")

        def checkpoint():
            if self._check_aborted:
                self._check_aborted()

        self._emit_log("price_init", {"level": "info", "message": "价格推荐任务初始化完成", "progress": 5})
        checkpoint()

        # Save files and unpack
        for rf in bundle.raw_files:
            ws.write_input(rf.name, rf.data)
        ws.unpack_archives()
        checkpoint()

        # ── 写入 plan ──
        plan = copy.deepcopy(PRICE_PLAN_TEMPLATE)
        design_plan_impl(ws, json.dumps(plan, ensure_ascii=False))
        
        plan_path = ws.resolve("plan.json")
        plan = json.loads(plan_path.read_text(encoding="utf-8"))
        if plan and plan[0]["status"] == "pending":
            plan[0]["status"] = "in_progress"
            plan_path.write_text(json.dumps(plan, ensure_ascii=False, indent=2), encoding="utf-8")
        checkpoint()

        # ── 构建 client ──
        preset = self._llm_preset
        api_key = preset.get("apiKey", "")
        base_url = preset.get("baseUrl", "https://api.deepseek.com")
        client = OpenAI(api_key=api_key, base_url=base_url, max_retries=0)

        # ── Bootstrap 注入 ──
        bootstrap_messages = self._build_bootstrap_messages(ws)

        # Log initial parse
        self._emit_log("price_parse", {"level": "status", "message": "正在解析上传文件...", "progress": 18})

        # ── Log Proxy ──
        from packages.agents.core.tool_converter import get_plan_progress_info

        def mapped_emit_log(node_id: str, message: Any):
            curr_idx, total_steps = get_plan_progress_info(ws)
            target_node = node_id
            if node_id == "custom_agent":
                if curr_idx == 0:
                    progress = 15
                    if isinstance(message, dict):
                        progress = message.get("progress", 15)
                    if progress < 20:
                        target_node = "price_parse"
                    elif progress < 30:
                        target_node = "price_product_match"
                    else:
                        target_node = "price_field_mapping"
                elif curr_idx == 1:
                    target_node = "price_db"
                elif curr_idx == 2:
                    target_node = "price_normalize"
                else:
                    target_node = "price_normalize"
            elif node_id == "plan":
                if curr_idx == 1:
                    target_node = "price_db"
                elif curr_idx == 2:
                    target_node = "price_normalize"

            # Forward the log
            self._emit_log(target_node, message)

        # ── 运行 Agent Loop ──
        loop = AgentLoop(
            client=client,
            ws=ws,
            llm_preset=preset,
            emit_log=mapped_emit_log,
            emit_status=None,
            check_aborted=self._check_aborted,
            bootstrap_messages=bootstrap_messages,
            task_type="price_recommendation",
            product_name=self.product_name,
            candidate_count=self.candidate_count,
        )

        try:
            output = await asyncio.to_thread(loop.run)
        except Exception as e:
            logger.error("Agent loop failed: %s", str(e), exc_info=True)
            self._emit_log("price_done", {"level": "error", "message": f"价格推荐任务失败: {str(e)}", "error_details": str(e)})
            raise e

        checkpoint()

        # ── 检查 plan 是否全部完成 ──
        if not self._is_plan_done(ws):
            logger.error("Agent plan not fully completed")
            self._emit_log("price_done", {"level": "error", "message": "Agent 价格推荐清洗与归一化步骤未全部完成", "error_details": "Agent 计划中存在未成功完成的步骤。"})
            raise RuntimeError("Agent 价格推荐清洗与归一化步骤未全部完成")

        logger.info("Agent plan completed, starting data fitting")
        
        # ── 开始数据拟合与连续搜索 ──
        self._emit_log("price_curve_fit", {
            "level": "status",
            "message": "正在进行数据拟合与连续曲线生成...",
            "progress": 86,
        })
        
        norm_path = ws.resolve("output/normalized_price_points.json")
        if not norm_path.exists():
            logger.error("normalized_price_points.json not found at %s", norm_path)
            raise FileNotFoundError("未找到归一化价格点文件 (output/normalized_price_points.json)")
        
        normalized_payload = json.loads(norm_path.read_text(encoding="utf-8"))
        
        raw_path = ws.resolve("output/raw_price_points.json")
        raw_points = []
        if raw_path.exists():
            try:
                raw_data = json.loads(raw_path.read_text(encoding="utf-8"))
                if isinstance(raw_data, dict):
                    raw_points = raw_data.get("rawPoints", raw_data.get("points", []))
                elif isinstance(raw_data, list):
                    raw_points = raw_data
            except Exception:
                pass

        evidence = {
            "matchedRows": len(raw_points),
            "rawPointCount": len(raw_points),
            "storeField": "shop",
            "sourceTables": ws.list_inputs(),
            "notes": ["基于 Agent 清洗和归一化的价格点集进行数据拟合"],
        }

        self._emit_log("price_recommend", {
            "level": "status",
            "message": "正在分析最优价格推荐...",
            "progress": 90,
        })
        
        logger.info("Running data fitting with %d raw price points", len(raw_points))
        result = run_data_fitting(
            normalized_payload=normalized_payload,
            evidence=evidence,
            product_name=self.product_name,
            candidate_count=self.candidate_count,
            workspace_dir=ws.dir,
        )
        logger.info("Data fitting completed, best_price=%s, recommendations=%d",
                    result.get("bestPrice"), len(result.get("recommendations", [])))
        checkpoint()

        self._emit_log("price_validate", {
            "level": "status",
            "message": "正在校验推荐结果 JSON...",
            "progress": 94,
        })
        self._validate_result(result)
        
        # ── 写入最终产物 ──
        # 统一输出路径约定到 output/ 目录
        output_result_path = ws.output_dir / "price_recommendation.json"
        self._write_json(output_result_path, result)
        
        summary = self._build_summary(result)
        # 将 summary.md 统一写入到 workspace/output/ 目录
        output_summary_path = ws.output_dir / "summary.md"
        output_summary_path.write_text(summary, encoding="utf-8")
        
        self._emit_log("price_done", {"level": "info", "message": "价格推荐任务完成", "progress": 100})
        logger.info("Price recommendation workflow completed successfully")
        
        elapsed_ms = (time.time() - t0) * 1000
        token_usage = output.get("_token_usage", {})

        return AgentResult(
            report_id=ws.report_id,
            pipeline=self.name,
            elapsed_ms=elapsed_ms,
            metrics=result.get("recommendations", []),
            cards=[],
            full_report=summary,
            total_tokens=token_usage.get("total_tokens", 0),
            input_tokens=token_usage.get("input_tokens", 0),
            cache_hit_tokens=token_usage.get("cache_hit_tokens", 0),
            cache_miss_tokens=token_usage.get("cache_miss_tokens", 0),
        )

    def _is_plan_done(self, ws: Workspace) -> bool:
        try:
            plan_path = ws.resolve("plan.json")
            if not plan_path.exists():
                return False
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            return all(s.get("status") == "success" for s in plan)
        except Exception:
            return False

    def _validate_result(self, result: dict):
        if result.get("taskType") != "price_recommendation":
            raise ValueError("结果 taskType 必须为 price_recommendation")
        recommendations = result.get("recommendations")
        if not isinstance(recommendations, list) or not recommendations:
            raise ValueError("结果缺少 recommendations")
        normalized_points = result.get("normalizedPoints")
        if not isinstance(normalized_points, list) or not normalized_points:
            raise ValueError("结果缺少 normalizedPoints")
        rendered_final_charts = result.get("renderedFinalCharts")
        if not isinstance(rendered_final_charts, list) or not rendered_final_charts:
            raise ValueError("结果缺少 renderedFinalCharts")
        for item in recommendations:
            if not isinstance(item.get("price"), (int, float)) or item["price"] <= 0:
                raise ValueError("推荐价格必须为正数")

    def _build_summary(self, result: dict) -> str:
        lines = [
            "# 最优价格推荐结果",
            "",
            f"商品名称：{result.get('productName', '')}",
            "",
            "## 推荐价格",
        ]
        for item in result.get("recommendations", []):
            lines.append(f"- 第 {item.get('rank')} 推荐：{item.get('price')} {item.get('unit', '元')}")
        evidence = result.get("evidence", {})
        lines.extend([
            "",
            "## 数据证据",
            f"- 命中行数：{evidence.get('matchedRows', 0)}",
            f"- 归一化价格点：{len(result.get('normalizedPoints', []))}",
            f"- 时间颗粒度：{result.get('timeGranularity', result.get('dataFitting', {}).get('timeGranularity', '未指定'))}",
            f"- 渲染图表数：{len(result.get('renderedFinalCharts', []))}",
            f"- 最优售价：{result.get('bestPrice', '')}",
            "",
            "当前版本先完成归一化点集，再接数据拟合层输出渲染图表和推荐售价。",
        ])
        return "\n".join(lines)

    def _write_json(self, path: Path, data):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

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
