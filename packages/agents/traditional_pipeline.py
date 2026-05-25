"""TraditionalPipeline — 传统多文件分析管线。

包装 run_multifile_analysis 逻辑，与 PydanticPipeline / SmolPipeline 共用 AgentPipeline 接口。
"""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from typing import Any, Callable, Optional

from .base import AgentPipeline
from .models import AgentResult, DatasetBundle, MetricResult, SceneContext, SemanticMapping, ReportCard, TableMeta, Manifest
from .workspace import Workspace

from packages.core.profiler import profile_dataset
from packages.core.semantic_mapper import llm_map_profiles
from packages.core.scene_classifier import llm_classify_scene, classify_data_scope
from packages.core.canonical import build_canonical_dataset
from packages.core.metric_registry import match_metrics
from packages.core.metric_engine import run_metrics
from packages.core.threshold_resolver import resolve_all_statuses
from packages.core.evidence_builder import build_evidence_bundle
from packages.ai.ai_caller import call_ai_early, call_detailed_ai_new, call_simplified_ai, _build_data_context_text
from packages.ai.error_reviewer import review_error_new

STATUS_ICON_MAP = {"warning": "🔴", "attention": "🟡", "uncountable": "⚪", "pass": "🟢"}


class PipelineAbortedError(Exception):
    pass


class TraditionalPipeline(AgentPipeline):
    name = "traditional"

    def __init__(self, llm_preset: Optional[dict] = None, check_aborted: Optional[Callable[[], None]] = None, workspace_dir: Optional[Path] = None, analysis_params: str = ""):
        super().__init__(workspace_dir=workspace_dir, analysis_params=analysis_params)
        self._llm_preset = llm_preset or {}
        self._check_aborted = check_aborted  # can be None

    def _ensure_not_stopped(self):
        if self._check_aborted:
            self._check_aborted()

    async def run(self, bundle: DatasetBundle) -> AgentResult:
        t0 = time.time()
        active_settings = self._llm_preset
        ws = Workspace(base_dir=self._workspace_dir) if self._workspace_dir else Workspace(label="traditional")

        try:
            # 1) Input Adapter — 直接从 DatasetBundle 构建 core 模块所需 dict
            self._emit_status("input", "active")
            self._emit_log("input", f"传统管线: 收到 {len(bundle.tables)} 张表")
            for t in bundle.tables:
                self._emit_log("input", f"  [{bundle.source_type}] {t.name} ({len(t.rows)} 行)")
            ds_bundle = {
                "source_type": bundle.source_type,
                "tables": [{"name": t.name, "rows": t.rows} for t in bundle.tables],
                "received_at": bundle.received_at.isoformat(),
            }
            self._emit_log("input", f"解析完成: {ds_bundle['source_type']}, {len(ds_bundle['tables'])} 张表")
            self._emit_status("input", "success")
            self._ensure_not_stopped()

            # 2) Data Profiler
            self._emit_status("profile", "active")
            self._emit_log("profile", "开始数据画像...")
            profiles = profile_dataset(ds_bundle)
            self._emit_log("profile", f"画像完成: {len(profiles)} 个字段")
            self._emit_status("profile", "success")
            self._ensure_not_stopped()

            # 并行: 早路 AI 初诊 + 审计 / 晚路 scene→mapping→metrics→threshold→evidence
            async def report_flow():
                if not active_settings.get("apiKey"):
                    return None, None, None
                dummy_scene = {"industry": "generic", "business_model": "unknown"}
                data_context = _build_data_context_text(profiles, None, dummy_scene)
                self._emit_log("report", "并行: 调用 AI 生成初诊报告（基于数据字段结构）...")
                self._emit_status("report", "active")
                try:
                    early_resp = await call_ai_early(active_settings, data_context, dummy_scene, self._analysis_params)
                    initial_report = early_resp["choices"][0]["message"]["content"]
                    self._emit_log("report", f"初诊报告已生成 ({len(initial_report)} 字符)")

                    self._emit_status("review", "active")
                    self._emit_log("review", "并行: 启动逻辑审计复核...")
                    review_resp = await review_error_new(active_settings, dummy_scene, initial_report, {"items": []}, self._analysis_params)
                    review_text = review_resp["choices"][0]["message"]["content"] if isinstance(review_resp, dict) else str(review_resp)
                    self._emit_log("review", f"审计复核完成 ({len(review_text)} 字符)")
                    self._emit_status("review", "success")

                    return initial_report, review_text, None
                except Exception as e:
                    self._emit_log("report", f"并行初诊报告失败: {e}")
                    return None, None, str(e)

            async def metrics_flow():
                # 3) Scene Classifier (AI 优先)
                self._emit_status("scene", "active")
                self._emit_log("scene", "开始场景识别...")
                scene = await llm_classify_scene(profiles, active_settings, self._analysis_params)
                self._emit_log("scene", f"识别结果: {scene.get('industry')}/{scene.get('business_model')} (conf={scene.get('confidence')})")
                if scene.get("llm_reason"):
                    self._emit_log("scene", f"  AI判断: {scene['llm_reason']}")
                self._emit_status("scene", "success")
                self._ensure_not_stopped()

                # 4) Semantic Mapper (AI 优先)
                self._emit_status("mapping", "active")
                self._emit_log("mapping", "开始字段语义映射（场景感知）...")
                self._emit_log("mapping", f"共 {len(profiles)} 个字段待映射, 场景: {scene.get('industry')}")
                mappings = await llm_map_profiles(profiles, active_settings, scene, self._analysis_params)
                mapped_count = sum(1 for m in mappings if m.get("semantic_field") not in ("unknown", "ignore"))
                ignored_count = sum(1 for m in mappings if m.get("semantic_field") == "ignore")
                confirm_count = sum(1 for m in mappings if m.get("need_confirm"))
                self._emit_log("mapping", f"结果: {mapped_count}/{len(mappings)} 已识别, {ignored_count} 忽略, {confirm_count} 需确认")
                for m in mappings:
                    sf = m.get("semantic_field", "unknown")
                    conf = m.get("confidence", 0)
                    mark = " [需确认]" if m.get("need_confirm") else ""
                    table = m.get("table", "?")
                    if sf == "ignore":
                        self._emit_log("mapping", f"  `{m['raw_field']}` → ⛔忽略 ({table})")
                    else:
                        self._emit_log("mapping", f"  `{m['raw_field']}` → `{sf}` (table={table}, conf={conf}){mark}")
                if confirm_count > 0:
                    self._emit_log("mapping", f"低置信字段: {[m['raw_field'] for m in mappings if m.get('need_confirm')]}")
                self._emit_status("mapping", "success")

                scene["data_scope"] = classify_data_scope(mappings)
                self._emit_log("scene", f"数据范围: {scene.get('data_scope')}")
                self._ensure_not_stopped()

                # 5) Canonical
                self._emit_status("canonical", "active")
                self._emit_log("canonical", "构建标准语义数据层...")
                canonical = build_canonical_dataset(ds_bundle, mappings, scene)
                table_names = list(canonical.get("tables", {}).keys())
                total_rows = sum(len(rows) for rows in canonical.get("tables", {}).values())
                self._emit_log("canonical", f"标准数据集: {table_names}, 共 {total_rows} 行")
                self._emit_status("canonical", "success")
                self._ensure_not_stopped()

                # 6) Metric Registry
                self._emit_status("registry", "active")
                self._emit_log("registry", "匹配可计算指标...")
                metric_defs = match_metrics(canonical)
                available = [m for m in metric_defs if m["available"]]
                unavailable = [m for m in metric_defs if not m["available"]]
                self._emit_log("registry", f"可计算: {len(available)} 项 - {[m['metric_id'] for m in available]}")
                if unavailable:
                    self._emit_log("registry", f"不可计算: {len(unavailable)} 项 - {[m['metric_id'] for m in unavailable]}")
                self._emit_status("registry", "success")
                self._ensure_not_stopped()

                # 7) Metric Engine
                self._emit_status("engine", "active")
                self._emit_log("engine", f"开始计算 {len(available)} 项指标...")
                metric_results = run_metrics(available, canonical)
                pass_count = sum(1 for r in metric_results if r.get("status") == "pass")
                unc_count = sum(1 for r in metric_results if r.get("status") == "uncountable")
                for i, r in enumerate(metric_results):
                    val = r.get("value")
                    val_str = json.dumps(val, ensure_ascii=False) if isinstance(val, (dict, list)) else str(val)
                    self._emit_log("engine", f"  {r.get('name', '?')} → {r.get('status')} | {val_str[:80]}")
                    self._emit_progress("engine", i + 1, len(metric_results))
                self._emit_log("engine", f"完成: {pass_count} pass, {unc_count} uncountable")
                self._emit_status("engine", "success")
                self._ensure_not_stopped()

                # 8) Threshold Resolver
                self._emit_status("threshold", "active")
                self._emit_log("threshold", "场景化健康判断...")
                metric_results = resolve_all_statuses(metric_results, scene)
                tally = {"pass": 0, "attention": 0, "warning": 0, "uncountable": 0}
                for r in metric_results:
                    s = r.get("status", "uncountable")
                    tally[s] = tally.get(s, 0) + 1
                self._emit_log("threshold", f"🟢 pass: {tally['pass']}, 🟡 attention: {tally['attention']}, 🔴 warning: {tally['warning']}, ⚪ uncountable: {tally['uncountable']}")
                self._emit_tally("threshold", tally)
                self._emit_status("threshold", "success")
                self._ensure_not_stopped()

                # 9) Evidence Builder
                self._emit_status("evidence", "active")
                self._emit_log("evidence", "构建证据包...")
                evidence = build_evidence_bundle(metric_results, canonical)
                self._emit_log("evidence", f"证据包: {len(evidence['items'])} 条证据")
                self._emit_status("evidence", "success")
                self._ensure_not_stopped()

                return scene, mappings, metric_results, evidence, tally

            # 并行执行
            report_task = asyncio.create_task(report_flow())
            metrics_task = asyncio.create_task(metrics_flow())
            tasks = [report_task, metrics_task]
            try:
                (initial_report, review_text, _early_error), (scene, mappings, metric_results, evidence, tally) = \
                    await asyncio.gather(*tasks)
            except PipelineAbortedError:
                for t in tasks:
                    if not t.done():
                        t.cancel()
                raise

            # 无 API Key → 生成算法摘要
            if not active_settings.get("apiKey"):
                self._emit_log("report", "未检测到 API Key，生成算法摘要...")
                lines = [f"# {scene.get('industry', '未知')} 经营分析摘要", ""]
                lines.append("## 场景识别")
                lines.append(f"- 行业: {scene.get('industry')}")
                lines.append(f"- 业态: {scene.get('business_model')}")
                lines.append(f"- 数据范围: {', '.join(scene.get('data_scope', []))}")
                lines.append("")
                lines.append(f"## 指标结果 ({len(metric_results)} 项)")
                lines.append("")
                for r in metric_results:
                    icon = STATUS_ICON_MAP.get(r.get("status"), "⚪")
                    val = r.get("value")
                    val_str = json.dumps(val, ensure_ascii=False) if isinstance(val, (dict, list)) else str(val)
                    lines.append(f"- {icon} **{r.get('name')}**: {val_str}")
                    if r.get("reason"):
                        lines.append(f"  - {r.get('reason')}")
                lines.append("")
                lines.append("## 字段映射")
                for m in mappings:
                    sf = m.get("semantic_field", "unknown")
                    conf = m.get("confidence", 0)
                    lines.append(f"- `{m.get('raw_field')}` → `{sf}` (conf: {conf})")

                full_report = "\n".join(lines)
                cards = [
                    {"title": r.get("name", ""), "explanation": r.get("reason", ""), "suggestion": "", "color": "red" if r.get("status") == "warning" else "yellow" if r.get("status") == "attention" else "green"}
                    for r in metric_results if r.get("status") in ("warning", "attention")
                ]
                self._emit_status("report", "simulated")

                self._write_summary_files(scene, cards, full_report)

                return AgentResult(
                    report_id=ws.report_id,
                    pipeline=self.name,
                    elapsed_ms=(time.time() - t0) * 1000,
                    full_report=full_report,
                    cards=[ReportCard(**c) for c in cards],
                    scene=SceneContext(industry=scene.get("industry", "generic"), business_model=scene.get("business_model", "unknown")),
                    mapping=[SemanticMapping(**m) for m in mappings],
                    metrics=[MetricResult(metric_id=r.get("metric_id", ""), name=r.get("name", ""), value=r.get("value"), status=r.get("status", "pass")) for r in metric_results],
                )

            # 有 API Key → 完整报告流
            self._emit_status("fusion", "active")
            self._emit_log("fusion", "数据融合：合并初级报告、错误评审、证据数据...")
            fused_context = (
                f"【初级报告】\n{initial_report or '(初诊未生成)'}\n\n"
                f"【审计意见】\n{review_text or '(审计未完成)'}\n\n"
                f"【指标证据包】\n{json.dumps(evidence.get('items', [])[:10], ensure_ascii=False)}"
            )
            self._emit_log("fusion", f"融合上下文: {len(fused_context)} 字符")
            self._emit_status("fusion", "success")
            self._ensure_not_stopped()

            # AI-3: 深度报告
            self._emit_status("rep1", "active")
            self._emit_log("rep1", "正在融合审计意见生成深度报告...")
            detailed_resp = await call_detailed_ai_new(active_settings, scene, fused_context, self._analysis_params)
            full_report = detailed_resp["choices"][0]["message"]["content"]
            self._emit_log("rep1", f"深度报告生成成功 ({len(full_report)} 字符)")
            self._emit_status("rep1", "success")
            self._ensure_not_stopped()

            # AI-4: 精简报告
            self._emit_status("rep2", "active")
            self._emit_log("rep2", "生成精简老板视图...")
            simplified_resp = await call_simplified_ai(active_settings, full_report, self._analysis_params)
            simplified_text = simplified_resp["choices"][0]["message"]["content"]
            self._emit_log("rep2", f"任务全部完成！(精简报告 {len(simplified_text)} 字符)")
            self._emit_status("rep2", "success")

            cards_list = [
                {"title": r.get("name", ""), "explanation": r.get("reason", ""), "suggestion": "", "evidence": "", "color": "red" if r.get("status") == "warning" else "yellow" if r.get("status") == "attention" else "green"}
                for r in metric_results if r.get("status") in ("warning", "attention")
            ]
            self._write_summary_files(scene, cards_list, full_report, simplified_text)

            elapsed = (time.time() - t0) * 1000
            return AgentResult(
                report_id=ws.report_id,
                pipeline=self.name,
                elapsed_ms=elapsed,
                full_report=full_report,
                scene=SceneContext(industry=scene.get("industry", "generic"), business_model=scene.get("business_model", "unknown")),
                mapping=[SemanticMapping(**m) for m in mappings],
                metrics=[MetricResult(metric_id=r.get("metric_id", ""), name=r.get("name", ""), value=r.get("value"), status=r.get("status", "pass")) for r in metric_results],
                cards=[
                    ReportCard(**{"title": r.get("name", ""), "explanation": r.get("reason", ""), "suggestion": "", "evidence": "", "color": "red" if r.get("status") == "warning" else "yellow" if r.get("status") == "attention" else "green"})
                    for r in metric_results if r.get("status") in ("warning", "attention")
                ],
            )

        except PipelineAbortedError:
            raise
        except Exception:
            raise

    # ── helpers ──

    def _write_summary_files(self, scene: dict, cards: list[dict], full_report: str, simplified_text: str = ""):
        ws_dir = self._workspace_dir
        if not ws_dir:
            return
        ws_dir.mkdir(parents=True, exist_ok=True)

        (ws_dir / "summary.md").write_text(full_report, encoding="utf-8")

        if simplified_text:
            short_path = ws_dir / "summary_short.json"
            try:
                json.loads(simplified_text)
                short_path.write_text(simplified_text, encoding="utf-8")
            except json.JSONDecodeError:
                short_path.write_text(json.dumps({
                    "health_status": "分析完成",
                    "overview_text": "AI 精简报告生成异常，退化为指标摘要",
                    "cards": cards or [],
                }, ensure_ascii=False, indent=2), encoding="utf-8")
        else:
            health = "分析完成"
            if cards:
                colors = [c.get("color") for c in cards]
                if "red" in colors:
                    health = "存在异常"
                elif "yellow" in colors:
                    health = "部分指标异常"
            (ws_dir / "summary_short.json").write_text(json.dumps({
                "health_status": health,
                "overview_text": f"行业: {scene.get('industry', '未知')} / {scene.get('business_model', '未知')}，共 {len(cards)} 项待关注",
                "cards": cards or [],
            }, ensure_ascii=False, indent=2), encoding="utf-8")
