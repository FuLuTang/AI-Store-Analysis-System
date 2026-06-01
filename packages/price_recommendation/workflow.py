"""Programmatic workflow for the first price recommendation implementation."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Callable, Union

from .precheck import (
    build_price_point_artifacts,
    inspect_uploaded_files,
)
from .data_fitting import run_data_fitting


LogCallback = Callable[[str, dict], None]
AbortCallback = Callable[[], None]


def run_price_recommendation_workflow(
    *,
    decoded_files: list[dict],
    product_name: str,
    candidate_count: int,
    workspace_dir: Path,
    llm_preset: dict,
    emit_log: LogCallback,
    check_aborted: AbortCallback | None = None,
) -> tuple[dict, str]:
    """Run the price workflow with Agent and write final fitted artifacts."""
    import copy
    from openai import OpenAI
    from packages.agents.core.workspace import Workspace
    from packages.agents.core.agent_loop import AgentLoop
    from packages.agents.core.tools.impl.setup_impl import design_plan_impl
    from packages.agents.price_recommendation.plan_template import PRICE_PLAN_TEMPLATE
    from packages.agents.core.tool_converter import get_plan_progress_info

    ws = Workspace(base_dir=workspace_dir)

    def checkpoint():
        if check_aborted:
            check_aborted()

    emit_log("price_init", {"level": "info", "message": "价格推荐任务初始化完成", "progress": 5})
    checkpoint()

    # Save original files and unpack
    for item in decoded_files:
        filename = _safe_filename(item.get("name") or "unnamed")
        ws.write_input(filename, item.get("bytes") or b"")
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

    # Log initial parse
    emit_log("price_parse", {"level": "status", "message": "正在解析上传文件...", "progress": 18})

    # ── 构建 client ──
    preset = llm_preset or {}
    api_key = preset.get("apiKey", "")
    base_url = preset.get("baseUrl", "https://api.deepseek.com")
    client = OpenAI(api_key=api_key, base_url=base_url, max_retries=0)

    # ── Log Proxy ──
    def mapped_emit_log(node_id: str, message: Union[str, dict]):
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

        # Forward the log to standard callback
        emit_log(target_node, message)

    # ── 运行 Agent Loop ──
    loop = AgentLoop(
        client=client,
        ws=ws,
        llm_preset=preset,
        emit_log=mapped_emit_log,
        emit_status=None,
        check_aborted=check_aborted,
        task_type="price_recommendation",
        product_name=product_name,
        candidate_count=candidate_count,
    )

    try:
        loop.run()
    except Exception as e:
        emit_log("price_done", {"level": "error", "message": f"价格推荐任务失败: {str(e)}", "error_details": str(e)})
        raise e

    checkpoint()

    if not _is_plan_done(ws):
        raise RuntimeError("Agent 价格推荐清洗与归一化步骤未全部完成")

    # ── 开始数据拟合与连续搜索 ──
    emit_log("price_curve_fit", {
        "level": "status",
        "message": "正在进行数据拟合与连续曲线生成...",
        "progress": 86,
    })
    
    norm_path = ws.resolve("output/normalized_price_points.json")
    if not norm_path.exists():
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
        "priceField": "price",
        "salesField": "qty",
        "timeField": "date",
        "storeField": "shop",
        "sourceTables": ws.list_inputs(),
        "notes": ["基于 Agent 清洗和归一化的价格点集进行数据拟合"],
    }

    emit_log("price_recommend", {
        "level": "status",
        "message": "正在分析最优价格推荐...",
        "progress": 90,
    })
    
    result = run_data_fitting(
        normalized_payload=normalized_payload,
        evidence=evidence,
        product_name=product_name,
        candidate_count=candidate_count,
        workspace_dir=ws.dir,
    )
    checkpoint()

    emit_log("price_validate", {
        "level": "status",
        "message": "正在校验推荐结果 JSON...",
        "progress": 94,
    })
    _validate_result(result)
    
    _write_json(ws.output_dir / "price_recommendation.json", result)
    summary = _build_summary(result)
    (ws.dir / "summary.md").write_text(summary, encoding="utf-8")
    
    emit_log("price_done", {"level": "info", "message": "价格推荐任务完成", "progress": 100})
    return result, summary


def _is_plan_done(ws: Workspace) -> bool:
    try:
        plan_path = ws.resolve("plan.json")
        if not plan_path.exists():
            return False
        plan = json.loads(plan_path.read_text(encoding="utf-8"))
        return all(s.get("status") == "success" for s in plan)
    except Exception:
        return False


def _validate_result(result: dict):
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


def _build_summary(result: dict) -> str:
    lines = [
        "# 最优价格推荐结果",
        "",
        f"商品名称：{result.get('productName', '')}",
        "",
        "## 推荐价格",
    ]
    for item in result.get("recommendations", []):
        lines.append(f"- 第 {item.get('rank')} 推荐：{item.get('price')} {item.get('unit', '元')}，置信度 {item.get('confidence')}")
    evidence = result.get("evidence", {})
    lines.extend([
        "",
        "## 数据证据",
        f"- 命中行数：{evidence.get('matchedRows', 0)}",
        f"- 价格字段：{evidence.get('priceField', '')}",
        f"- 销售字段：{evidence.get('salesField', '')}",
        f"- 归一化价格点：{len(result.get('normalizedPoints', []))}",
        f"- 渲染图表数：{len(result.get('renderedFinalCharts', []))}",
        f"- 最优售价：{result.get('bestPrice', '')}",
        "",
        "当前版本先完成归一化点集，再接数据拟合层输出渲染图表和推荐售价。",
    ])
    return "\n".join(lines)


def _write_json(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _safe_filename(name: str) -> str:
    cleaned = re.sub(r"[\\/]+", "_", name).strip()
    return cleaned or "unnamed"


def _write_analysis_duckdb(path: Path, raw_points: list[dict], normalized_points: list[dict]):
    import duckdb

    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(path))
    try:
        con.execute("DROP TABLE IF EXISTS raw_price_points")
        con.execute(
            """
            CREATE TABLE raw_price_points (
              price DOUBLE,
              raw_qty DOUBLE,
              source_shop VARCHAR,
              source_table VARCHAR,
              source_date VARCHAR
            )
            """
        )
        if raw_points:
            con.executemany(
                "INSERT INTO raw_price_points VALUES (?, ?, ?, ?, ?)",
                [
                    (
                        item.get("price"),
                        item.get("rawQty"),
                        item.get("sourceShop"),
                        item.get("sourceTable"),
                        item.get("sourceDate"),
                    )
                    for item in raw_points
                ],
            )

        con.execute("DROP TABLE IF EXISTS normalized_price_points")
        con.execute(
            """
            CREATE TABLE normalized_price_points (
              price DOUBLE,
              raw_qty DOUBLE,
              normalized_qty DOUBLE,
              sample_count INTEGER,
              avg_factor DOUBLE,
              source_shops_json VARCHAR
            )
            """
        )
        if normalized_points:
            con.executemany(
                "INSERT INTO normalized_price_points VALUES (?, ?, ?, ?, ?, ?)",
                [
                    (
                        item.get("price"),
                        item.get("rawQty"),
                        item.get("normalizedQty"),
                        item.get("sampleCount"),
                        item.get("avgFactor"),
                        json.dumps(item.get("sourceShops", []), ensure_ascii=False),
                    )
                    for item in normalized_points
                ],
            )
    finally:
        con.close()
