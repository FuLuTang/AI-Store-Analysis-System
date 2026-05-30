"""Programmatic workflow for the first price recommendation implementation."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Callable

from .precheck import (
    build_price_point_artifacts,
    build_recommendation_from_points,
    inspect_uploaded_files,
)


LogCallback = Callable[[str, dict], None]
AbortCallback = Callable[[], None]


def run_price_recommendation_workflow(
    *,
    decoded_files: list[dict],
    product_name: str,
    candidate_count: int,
    workspace_dir: Path,
    emit_log: LogCallback,
    check_aborted: AbortCallback | None = None,
) -> tuple[dict, str]:
    """Run the first price workflow and write the V1 artifacts."""
    workspace_dir.mkdir(parents=True, exist_ok=True)
    input_dir = workspace_dir / "input"
    output_dir = workspace_dir / "output"
    input_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    def checkpoint():
        if check_aborted:
            check_aborted()

    emit_log("price_init", {"level": "info", "message": "价格推荐任务初始化完成", "progress": 5})
    checkpoint()

    for item in decoded_files:
        filename = _safe_filename(item.get("name") or "unnamed")
        (input_dir / filename).write_bytes(item.get("bytes") or b"")

    emit_log("price_parse", {"level": "status", "message": "正在解析上传文件...", "progress": 18, "step": "price_parse"})
    inspection = inspect_uploaded_files(decoded_files)
    checkpoint()

    emit_log("price_product_match", {
        "level": "info",
        "message": "目标商品记录定位完成",
        "progress": 30,
        "step": "price_product_match",
    })
    checkpoint()

    emit_log("price_db", {
        "level": "status",
        "message": "正在整理结构化数据并写入 analysis.duckdb...",
        "progress": 42,
        "step": "price_db",
    })
    artifacts = build_price_point_artifacts(inspection, product_name)
    raw_payload = artifacts["raw"]
    normalized_payload = dict(artifacts["normalized"])
    normalized_payload["productName"] = product_name
    _write_analysis_duckdb(workspace_dir / "analysis.duckdb", raw_payload["points"], normalized_payload.get("points", []))
    checkpoint()

    emit_log("price_raw_points", {
        "level": "status",
        "message": "正在生成归一前价格点...",
        "progress": 56,
        "step": "price_raw_points",
    })
    _write_json(output_dir / "raw_price_points.json", raw_payload)
    checkpoint()

    emit_log("price_normalize", {
        "level": "status",
        "message": "正在生成归一化价格点...",
        "progress": 72,
        "step": "price_normalize",
    })
    _write_json(output_dir / "normalized_price_points.json", normalized_payload)
    checkpoint()

    emit_log("price_recommend", {
        "level": "status",
        "message": "正在生成最终推荐结果...",
        "progress": 86,
        "step": "price_recommend",
    })
    result = build_recommendation_from_points(
        product_name=product_name,
        normalized_points=normalized_payload.get("points", []),
        evidence=artifacts["evidence"],
        candidate_count=candidate_count,
    )
    checkpoint()

    emit_log("price_validate", {
        "level": "status",
        "message": "正在校验推荐结果 JSON...",
        "progress": 94,
        "step": "price_validate",
    })
    _validate_result(result)
    _write_json(output_dir / "price_recommendation.json", result)
    summary = _build_summary(result)
    (workspace_dir / "summary.md").write_text(summary, encoding="utf-8")
    emit_log("price_done", {"level": "info", "message": "价格推荐任务完成", "progress": 100, "step": "price_done"})
    return result, summary


def _validate_result(result: dict):
    if result.get("taskType") != "price_recommendation":
        raise ValueError("结果 taskType 必须为 price_recommendation")
    recommendations = result.get("recommendations")
    if not isinstance(recommendations, list) or not recommendations:
        raise ValueError("结果缺少 recommendations")
    normalized_points = result.get("normalizedPoints")
    if not isinstance(normalized_points, list) or not normalized_points:
        raise ValueError("结果缺少 normalizedPoints")
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
        "",
        "当前版本直接使用归一化后的离散点集输出结果。",
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
