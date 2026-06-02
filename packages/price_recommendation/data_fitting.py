"""Deterministic data fitting for rendered price charts and final recommendations."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger("price_recommendation")


def run_data_fitting(
    *,
    normalized_payload: dict,
    evidence: dict[str, Any],
    product_name: str,
    candidate_count: int,
    workspace_dir: Path | None = None,
) -> dict:
    normalized_points = _merge_normalized_points(normalized_payload)
    logger.info("Data fitting: product=%s, candidate_count=%d, normalized_points=%d",
                product_name, candidate_count, len(normalized_points))
    purchase_price = _infer_purchase_price(normalized_payload, evidence)
    time_granularity = _infer_time_granularity(normalized_payload, evidence)
    rendered_final_charts = _build_rendered_final_charts(normalized_points, purchase_price)
    primary_metric = "profit" if purchase_price is not None else "sales_revenue"
    scored_points = _score_points(normalized_points, purchase_price)
    recommendations = _build_recommendations(
        product_name=product_name,
        scored_points=scored_points,
        evidence=evidence,
        candidate_count=candidate_count,
        purchase_price=purchase_price,
    )
    best = recommendations[0] if recommendations else None
    best_price = best["price"] if best else None
    best_reason = best["reason"] if best else ""
    best_value = best.get("metricValue") if best else None

    result = {
        "taskType": "price_recommendation",
        "productName": product_name,
        "normalizedPoints": normalized_points,
        "renderedFinalCharts": rendered_final_charts,
        "timeGranularity": time_granularity,
        "recommendations": recommendations,
        "bestPrice": best_price,
        "bestPriceMetric": primary_metric,
        "bestPriceValue": best_value,
        "bestPriceReason": best_reason,
        "validPriceRange": _build_price_range(normalized_points),
        "dataFitting": {
            "mode": "rendered_points",
            "primaryMetric": primary_metric,
            "hasPurchasePrice": purchase_price is not None,
            "purchasePrice": purchase_price,
            "chartCount": len(rendered_final_charts),
            "timeGranularity": time_granularity,
        },
        "evidence": {
            **dict(evidence or {}),
            "timeGranularity": time_granularity,
            "hasPurchasePrice": purchase_price is not None,
            "purchasePrice": purchase_price,
        },
        "warnings": [],
    }

    if workspace_dir is not None:
        output_dir = workspace_dir / "output"
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "rendered_final_charts.json").write_text(
            json.dumps(rendered_final_charts, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    logger.info("Data fitting result: best_price=%s, primary_metric=%s, charts=%d",
                result.get("bestPrice"), result.get("bestPriceMetric"),
                len(result.get("renderedFinalCharts", [])))
    return result


def _normalize_payload_format(payload: dict) -> dict:
    if not isinstance(payload, dict):
        return {"points": []}

    raw_points = payload.get("points")
    if not isinstance(raw_points, list):
        return payload
        
    flattened_points = []
    is_nested_store_format = False
    for p in raw_points:
        if isinstance(p, dict) and "price&quantity" in p:
            is_nested_store_format = True
            break
            
    if is_nested_store_format:
        for p in raw_points:
            if not isinstance(p, dict):
                continue
            store = p.get("store") or p.get("shop") or "unknown"
            price_qty_list = p.get("price&quantity")
            if isinstance(price_qty_list, list):
                from collections import defaultdict
                price_groups = defaultdict(list)
                for item in price_qty_list:
                    if isinstance(item, list) and len(item) >= 2:
                        price_val = _to_number(item[0])
                        qty_val = _to_number(item[1])
                        if price_val is not None and qty_val is not None:
                            price_groups[round(price_val, 2)].append(qty_val)

                for price, qties in price_groups.items():
                    if qties:
                        final_qty = qties[-1]  # Choose normalized quantity
                        flattened_points.append({
                            "price": price,
                            "normalizedQty": final_qty,
                            "rawQty": final_qty,
                            "sampleCount": 1,
                            "avgFactor": 1.0,
                            "sourceShops": [store]
                        })
        new_payload = dict(payload)
        new_payload["points"] = flattened_points
        return new_payload

    return payload


def _merge_normalized_points(normalized_payload: dict) -> list[dict[str, Any]]:
    normalized_payload = _normalize_payload_format(normalized_payload)
    raw_points = normalized_payload.get("points") if isinstance(normalized_payload, dict) else normalized_payload
    if not isinstance(raw_points, list):
        return []

    grouped: dict[float, dict[str, Any]] = {}
    for point in raw_points:
        price = _to_number(point.get("price"))
        qty = _to_number(point.get("normalizedQty"))
        if price is None or qty is None:
            continue
        raw_qty = _to_number(point.get("rawQty")) or 0.0
        sample_count = int(_to_number(point.get("sampleCount")) or 1)
        avg_factor = _to_number(point.get("avgFactor"))
        source_shops = point.get("sourceShops") or []
        bucket = grouped.setdefault(round(price, 2), {
            "price": round(price, 2),
            "rawQty": 0.0,
            "normalizedQty": 0.0,
            "sampleCount": 0,
            "avgFactorSum": 0.0,
            "avgFactorCount": 0,
            "sourceShops": set(),
        })
        bucket["rawQty"] += raw_qty
        bucket["normalizedQty"] += qty
        bucket["sampleCount"] += sample_count
        if avg_factor is not None:
            bucket["avgFactorSum"] += avg_factor
            bucket["avgFactorCount"] += 1
        for shop in source_shops:
            if shop:
                bucket["sourceShops"].add(str(shop))

    merged: list[dict[str, Any]] = []
    for price in sorted(grouped):
        bucket = grouped[price]
        avg_factor_count = bucket["avgFactorCount"] or max(bucket["sampleCount"], 1)
        count = max(bucket["sampleCount"], 1)
        merged.append({
            "price": round(bucket["price"], 2),
            "rawQty": round(bucket["rawQty"] / count, 4),
            "normalizedQty": round(bucket["normalizedQty"] / count, 4),
            "sampleCount": int(bucket["sampleCount"]),
            "avgFactor": round(bucket["avgFactorSum"] / max(avg_factor_count, 1), 6) if bucket["avgFactorSum"] else 1.0,
            "sourceShops": sorted(bucket["sourceShops"]),
        })
    return merged


def _infer_purchase_price(normalized_payload: dict, evidence: dict[str, Any]) -> float | None:
    candidates = [
        normalized_payload.get("purchasePrice"),
        normalized_payload.get("costPrice"),
        normalized_payload.get("cost"),
        evidence.get("purchasePrice"),
        evidence.get("costPrice"),
        evidence.get("cost"),
    ]
    for candidate in candidates:
        value = _to_number(candidate)
        if value is not None and value > 0:
            return value
    return None


def _infer_time_granularity(normalized_payload: dict, evidence: dict[str, Any]) -> str:
    candidates = [
        normalized_payload.get("timeGranularity"),
        normalized_payload.get("normalization", {}).get("timeGranularity"),
        evidence.get("timeGranularity"),
    ]
    for candidate in candidates:
        if candidate:
            return str(candidate)
    return "未指定"


def _build_rendered_final_charts(points: list[dict[str, Any]], purchase_price: float | None) -> list[dict[str, Any]]:
    if not points:
        return []

    dimensions = ["售价", "销量", "销售额"]
    if purchase_price is not None:
        dimensions.append("利润")

    grouped: dict[float, dict[str, float]] = {}
    for point in points:
        price = _to_number(point.get("price"))
        qty = _to_number(point.get("normalizedQty"))
        if price is None or qty is None:
            continue
        bucket_price = round(price, 2)
        bucket = grouped.setdefault(bucket_price, {"qty": 0.0, "revenue": 0.0, "profit": 0.0})
        bucket["qty"] += qty
        bucket["revenue"] += price * qty
        if purchase_price is not None:
            bucket["profit"] += (price - purchase_price) * qty

    source = []
    for price in sorted(grouped):
        bucket = grouped[price]
        row = [round(price, 2), round(bucket["qty"], 4), round(bucket["revenue"], 4)]
        if purchase_price is not None:
            row.append(round(bucket["profit"], 4))
        source.append(row)

    return [
        {
            "name": "售价综合分析",
            "xLabel": "售价",
            "yAxisLeft": {"name": "销量", "unit": "件"},
            "yAxisRight": {"name": "金额", "unit": "元"},
            "dimensions": dimensions,
            "source": source,
        }
    ]


def _score_points(points: list[dict[str, Any]], purchase_price: float | None) -> list[dict[str, Any]]:
    scored: list[dict[str, Any]] = []
    for point in points:
        price = _to_number(point.get("price"))
        qty = _to_number(point.get("normalizedQty"))
        if price is None or qty is None:
            continue
        revenue = round(price * qty, 4)
        profit = round((price - purchase_price) * qty, 4) if purchase_price is not None else revenue
        scored.append({
            "price": round(price, 2),
            "normalizedQty": round(qty, 4),
            "salesRevenue": revenue,
            "profit": profit,
            "sampleCount": int(_to_number(point.get("sampleCount")) or 0),
        })
    return scored


def _build_recommendations(
    *,
    product_name: str,
    scored_points: list[dict[str, Any]],
    evidence: dict[str, Any],
    candidate_count: int,
    purchase_price: float | None,
) -> list[dict[str, Any]]:
    if not scored_points:
        return []
    metric_key = "profit" if purchase_price is not None else "salesRevenue"
    scored_points = sorted(scored_points, key=lambda item: (-item[metric_key], item["price"]))
    recommendations: list[dict[str, Any]] = []
    for rank, item in enumerate(scored_points[:max(1, candidate_count)], start=1):
        metric_value = item[metric_key]
        if rank == 1:
            reason = (
                f"利润最高，利润值 {metric_value:.2f}"
                if purchase_price is not None
                else f"进货价缺失，按销售额最高选取，销售额 {metric_value:.2f}"
            )
        else:
            reason = (
                f"利润第 {rank} 高，利润值 {metric_value:.2f}"
                if purchase_price is not None
                else f"进货价缺失，按销售额第 {rank} 高选取，销售额 {metric_value:.2f}"
            )
        recommendations.append({
            "rank": rank,
            "price": item["price"],
            "unit": "元",
            "reason": reason,
            "metric": metric_key,
            "metricValue": round(metric_value, 4),
        })
    return recommendations


def _build_price_range(points: list[dict[str, Any]]) -> dict[str, Any]:
    if not points:
        return {"min": 0, "max": 0, "unit": "元"}
    prices = [float(point["price"]) for point in points if _to_number(point.get("price")) is not None]
    if not prices:
        return {"min": 0, "max": 0, "unit": "元"}
    return {"min": round(min(prices), 2), "max": round(max(prices), 2), "unit": "元"}




def _to_number(value: Any) -> float | None:
    import re
    if isinstance(value, (int, float)):
        return float(value)
    if value is None:
        return None
    text = str(value).replace(",", "")
    match = re.search(r"-?\d+(?:\.\d+)?", text)
    if not match:
        return None
    try:
        return float(match.group(0))
    except ValueError:
        return None
