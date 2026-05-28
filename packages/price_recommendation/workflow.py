"""Programmatic workflow for the first price recommendation implementation."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Callable

from .precheck import build_basic_recommendation, inspect_uploaded_files, run_precheck


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
    """Run deterministic MVP workflow and write price recommendation artifacts."""
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
    precheck = run_precheck(decoded_files, product_name)
    _write_json(output_dir / "precheck.json", precheck)
    checkpoint()

    if not precheck.get("valid"):
        raise ValueError(f"价格推荐预检未通过: {precheck.get('issues', [])}")

    emit_log("price_product_match", {
        "level": "info",
        "message": f"目标商品匹配完成，命中 {precheck['productDataMatch']['matchedRows']} 行",
        "progress": 35,
        "step": "price_product_match",
    })
    checkpoint()

    emit_log("price_field_mapping", {
        "level": "status",
        "message": "正在整理价格、销量、时间字段...",
        "progress": 50,
        "step": "price_field_mapping",
    })
    field_mapping = precheck.get("detectedFields", {})
    _write_json(output_dir / "field_mapping.json", field_mapping)
    checkpoint()

    emit_log("price_recommend", {
        "level": "status",
        "message": "正在生成基准推荐价格...",
        "progress": 72,
        "step": "price_recommend",
    })
    result = build_basic_recommendation(inspection, product_name, candidate_count=candidate_count)
    _write_json(output_dir / "price_candidates.json", result.get("recommendations", []))
    checkpoint()

    emit_log("price_validate", {
        "level": "status",
        "message": "正在校验推荐结果 JSON...",
        "progress": 90,
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
        "",
        "当前版本为确定性基准推荐，后续可替换为价格弹性或曲线拟合模型。",
    ])
    return "\n".join(lines)


def _write_json(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _safe_filename(name: str) -> str:
    cleaned = re.sub(r"[\\/]+", "_", name).strip()
    return cleaned or "unnamed"
