"""Chart builder for price recommendation charts."""

from __future__ import annotations

import re
from typing import Any


def _to_number(value: Any) -> float | None:
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


def build_rendered_final_charts(points: list[dict[str, Any]], purchase_price: float | None) -> list[dict[str, Any]]:
    # 流程A - 点群A - 无优惠点的坐标
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
