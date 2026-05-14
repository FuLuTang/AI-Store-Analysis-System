"""
metric_engine.py — 通用指标计算引擎
只运行系统内置计算器，禁止生产环境让 AI 生成代码执行
"""
import math
from typing import List, Optional


def is_valid(v):
    return isinstance(v, (int, float)) and math.isfinite(v)


def round_to(v, d=1):
    if not is_valid(v):
        return 0
    return round(v, d)


# ── 通用计算器 ──

def _ratio(numerator, denominator):
    """比率计算"""
    if not is_valid(numerator) or not is_valid(denominator) or denominator == 0:
        return None
    return numerator / denominator


def _period_change(rows: list, field: str, period_field: str = "date"):
    """环比变化"""
    if len(rows) < 2:
        return None
    current = rows[0].get(field)
    previous = rows[1].get(field)
    if not is_valid(current) or not is_valid(previous) or previous == 0:
        return None
    return (current - previous) / previous


def _share_by_dimension(rows: list, value_field: str, dim_field: str) -> dict:
    """按维度计算占比"""
    groups = {}
    total = 0
    for r in rows:
        v = r.get(value_field, 0) or 0
        if not is_valid(v):
            continue
        dim = r.get(dim_field, "未知")
        groups[dim] = groups.get(dim, 0) + v
        total += v

    if total <= 0:
        return {"total": 0, "groups": [], "dominant": None}

    items = sorted(
        [{"name": k, "value": round_to(v, 2), "pct": round_to(v / total * 100)} for k, v in groups.items()],
        key=lambda x: x["pct"], reverse=True
    )
    return {
        "total": round_to(total, 2),
        "groups": items,
        "dominant": items[0]["name"] if items else None
    }


def _concentration(rows: list, value_field: str, dim_field: str) -> dict:
    """集中度 = 最大值 / 总值"""
    share = _share_by_dimension(rows, value_field, dim_field)
    if not share["groups"]:
        return {"concentration": 0, "dominant": None}
    max_pct = share["groups"][0]["pct"]
    return {
        "concentration": max_pct,
        "dominant": share["groups"][0]["name"]
    }


def _top_contribution(rows: list, value_field: str, dim_field: str, top_n: int = 3) -> dict:
    """TOP N 贡献度"""
    share = _share_by_dimension(rows, value_field, dim_field)
    top_pct = sum(g["pct"] for g in share["groups"][:top_n])
    return {
        "top_n": top_n,
        "top_contribution": round_to(top_pct),
        "top_items": share["groups"][:top_n]
    }


def _trend_slope(rows: list, field: str) -> dict:
    """趋势斜率（简单线性回归）"""
    vals = [r.get(field) for r in rows if is_valid(r.get(field))]
    if len(vals) < 2:
        return {"slope": 0, "direction": "stable"}

    rev = list(vals)
    n = len(rev)
    sx = sum(range(n))
    sy = sum(rev)
    sxy = sum(i * rev[i] for i in range(n))
    sx2 = sum(i * i for i in range(n))

    denom = n * sx2 - sx * sx
    if denom == 0:
        return {"slope": 0, "direction": "stable"}

    slope = (n * sxy - sx * sy) / denom
    direction = "rising" if slope > 0.01 else ("falling" if slope < -0.01 else "stable")

    return {"slope": round_to(slope, 4), "direction": direction}


def _volatility(rows: list, field: str) -> dict:
    """波动率 CV = std / mean"""
    vals = [r.get(field) for r in rows if is_valid(r.get(field))]
    if len(vals) < 2:
        return {"cv": 0, "mean": 0, "std": 0}

    mean = sum(vals) / len(vals)
    variance = sum((v - mean) ** 2 for v in vals) / len(vals)
    std = math.sqrt(variance)
    cv = std / mean if mean != 0 else 0

    return {"cv": round_to(cv, 3), "mean": round_to(mean, 2), "std": round_to(std, 2)}


def _anomaly_detect(rows: list, field: str, method: str = "zscore") -> dict:
    """异常点检测"""
    vals = [(i, r.get(field)) for i, r in enumerate(rows) if is_valid(r.get(field))]
    if len(vals) < 3:
        return {"anomalies": [], "count": 0}

    numbers = [v[1] for v in vals]
    mean = sum(numbers) / len(numbers)
    variance = sum((v - mean) ** 2 for v in numbers) / len(numbers)
    std = math.sqrt(variance) if variance > 0 else 0

    anomalies = []
    if method == "zscore" and std > 0:
        for idx, val in vals:
            z = abs(val - mean) / std
            if z > 2:
                anomalies.append({"index": idx, "value": val, "z_score": round_to(z, 2)})
    elif method == "threshold":
        # 环比 >50% 视为异常（简化版）
        for i in range(1, len(rows)):
            cur = rows[i].get(field)
            prev = rows[i - 1].get(field)
            if is_valid(cur) and is_valid(prev) and prev != 0:
                pct = abs((cur - prev) / prev)
                if pct > 0.5:
                    anomalies.append({"index": i, "value": cur, "change_pct": round_to(pct * 100)})

    return {"anomalies": anomalies, "count": len(anomalies)}


def _threshold_rate(rows: list, field: str, op: str, threshold: float) -> dict:
    """阈值占比"""
    total = len(rows)
    if total == 0:
        return {"rate": 0, "total": 0, "over_threshold": 0}

    if op == ">":
        count = sum(1 for r in rows if is_valid(r.get(field)) and r.get(field) > threshold)
    elif op == "<":
        count = sum(1 for r in rows if is_valid(r.get(field)) and r.get(field) < threshold)
    elif op == ">=":
        count = sum(1 for r in rows if is_valid(r.get(field)) and r.get(field) >= threshold)
    else:
        count = 0

    return {
        "rate": round_to(count / total * 100),
        "total": total,
        "over_threshold": count
    }


def _data_quality(rows: list, profiles: Optional[list] = None) -> dict:
    """数据完整度"""
    if not rows:
        return {"completeness": 0, "null_rate": 0, "duplicate_rows": 0, "issues": ["无数据"]}

    null_total = 0
    cell_total = 0
    for r in rows:
        if isinstance(r, dict):
            vals = list(r.values())
            null_total += sum(1 for v in vals if v is None or v == "")
            cell_total += len(vals)

    null_rate = round_to(null_total / cell_total * 100) if cell_total > 0 else 0
    completeness = 100 - null_rate

    issues = []
    if null_rate > 20:
        issues.append(f"空值率偏高({null_rate}%)")
    if len(rows) < 3:
        issues.append(f"数据行数过少({len(rows)}行)")

    return {
        "completeness": round_to(completeness),
        "null_rate": round_to(null_rate),
        "total_rows": len(rows),
        "issues": issues
    }


def _consecutive_change(rows: list, field: str = "revenue") -> dict:
    """连续涨跌：判断最近几期连续上升/下降"""
    vals = [(r.get(field), r.get("date")) for r in rows if is_valid(r.get(field))]
    if len(vals) < 2:
        return {"direction": "stable", "consecutive_days": 0, "change_pct": 0}

    changes = []
    for i in range(1, len(vals)):
        prev = vals[i - 1][0]
        cur = vals[i][0]
        if prev != 0:
            pct = (cur - prev) / prev
            changes.append({"direction": "up" if pct > 0 else "down", "pct": round_to(pct * 100, 1)})

    if not changes:
        return {"direction": "stable", "consecutive_days": 0, "change_pct": 0}

    # 从最近一期往前数连续同向
    count = 0
    ref_dir = changes[-1]["direction"]
    for c in reversed(changes):
        if c["direction"] == ref_dir:
            count += 1
        else:
            break

    return {
        "direction": ref_dir,
        "consecutive_days": count,
        "change_pct": changes[-1]["pct"]
    }


def _gross_margin_trend(rows: list) -> dict:
    """毛利率趋势：每期毛利率 + 整体趋势方向"""
    pairs = []
    for r in rows:
        rev = r.get("revenue")
        gp = r.get("gross_profit")
        if is_valid(rev) and is_valid(gp) and rev != 0:
            pairs.append(gp / rev)

    if len(pairs) < 2:
        return {"direction": "stable", "avg_margin": round_to(pairs[0] * 100) if pairs else 0}

    current = pairs[-1]
    previous = pairs[-2]
    direction = "rising" if current > previous else ("falling" if current < previous else "stable")
    avg_margin = sum(pairs) / len(pairs)

    return {
        "direction": direction,
        "current_margin": round_to(current * 100),
        "previous_margin": round_to(previous * 100),
        "avg_margin": round_to(avg_margin * 100),
        "change_pct": round_to((current - previous) * 100)
    }


# ── 计算器调度表 ──

CALCULATOR_MAP = {
    "sum": None,
    "ratio": _ratio,
    "period_change": _period_change,
    "share_by_dimension": _share_by_dimension,
    "concentration": _concentration,
    "trend_slope": _trend_slope,
    "anomaly_detect": _anomaly_detect,
    "top_contribution": _top_contribution,
    "volatility": _volatility,
    "threshold_rate": _threshold_rate,
    "data_quality": _data_quality,
    "consecutive_change": _consecutive_change,
    "gross_margin_trend": _gross_margin_trend,
}


def compute_metric(metric_def: dict, canonical_dataset: dict) -> dict:
    """
    执行单个指标计算

    输入: MetricDefinition + CanonicalDataset
    输出: MetricResult
    """
    metric_id = metric_def.get("metric_id", "")
    calculator = metric_def.get("calculator", "")
    params = metric_def.get("params", {})

    if calculator not in CALCULATOR_MAP:
        return {
            "metric_id": metric_id,
            "name": metric_def.get("name", ""),
            "value": None,
            "status": "uncountable",
            "reason": f"未知计算器: {calculator}",
            "required_fields": metric_def.get("required_fields", []),
            "missing_fields": metric_def.get("missing_fields", []),
            "confidence": 0
        }

    # 如果指标不可用
    if not metric_def.get("available", True):
        return {
            "metric_id": metric_id,
            "name": metric_def.get("name", ""),
            "value": None,
            "status": "uncountable",
            "reason": f"缺字段: {metric_def.get('missing_fields', [])}",
            "required_fields": metric_def.get("required_fields", []),
            "missing_fields": metric_def.get("missing_fields", []),
            "confidence": 0
        }

    # 取主要数据表（优先 sales 表）
    tables = canonical_dataset.get("tables", {})
    if "sales" in tables:
        rows = tables["sales"]
    else:
        rows = next(iter(tables.values()), []) if tables else []

    try:
        calc_fn = CALCULATOR_MAP[calculator]

        if calculator == "ratio":
            numerator_field = params.get("numerator", "")
            denominator_field = params.get("denominator", "")
            # 找第一个同时包含 numerator 和 denominator 的同行
            row = None
            for r in rows:
                if is_valid(r.get(numerator_field)) and is_valid(r.get(denominator_field)):
                    row = r
                    break
            if row is None:
                value = None
            else:
                value = _ratio(row[numerator_field], row[denominator_field])

        elif calculator == "period_change":
            value = _period_change(rows, params.get("field", "revenue"), params.get("period_field", "date"))

        elif calculator == "share_by_dimension":
            value = _share_by_dimension(rows, params.get("value_field", "revenue"), params.get("dim_field", "channel"))

        elif calculator == "concentration":
            value = _concentration(rows, params.get("value_field", "revenue"), params.get("dim_field", "channel"))

        elif calculator == "top_contribution":
            value = _top_contribution(rows, params.get("value_field", "revenue"), params.get("dim_field", "product_name"), params.get("top_n", 3))

        elif calculator == "trend_slope":
            value = _trend_slope(rows, params.get("field", "revenue"))

        elif calculator == "volatility":
            value = _volatility(rows, params.get("field", "revenue"))

        elif calculator == "anomaly_detect":
            value = _anomaly_detect(rows, params.get("field", "revenue"), params.get("method", "zscore"))

        elif calculator == "threshold_rate":
            value = _threshold_rate(rows, params.get("field", ""), params.get("op", ">"), params.get("threshold", 0))

        elif calculator == "data_quality":
            value = _data_quality(rows)

        elif calculator == "consecutive_change":
            value = _consecutive_change(rows, params.get("field", "revenue"))

        elif calculator == "gross_margin_trend":
            value = _gross_margin_trend(rows)

        else:
            value = None

        if value is None:
            return {
                "metric_id": metric_id,
                "name": metric_def.get("name", ""),
                "value": None,
                "status": "uncountable",
                "reason": "计算缺失必要数值",
                "required_fields": metric_def.get("required_fields", []),
                "confidence": 0
            }

        return {
            "metric_id": metric_id,
            "name": metric_def.get("name", ""),
            "value": value,
            "status": "pass",  # 健康判断由 threshold_resolver 完成
            "required_fields": metric_def.get("required_fields", []),
            "confidence": 0.95
        }

    except Exception as e:
        return {
            "metric_id": metric_id,
            "name": metric_def.get("name", ""),
            "value": None,
            "status": "uncountable",
            "reason": f"计算异常: {str(e)}",
            "required_fields": metric_def.get("required_fields", []),
            "confidence": 0
        }


def run_metrics(metric_defs: list, canonical_dataset: dict) -> list:
    """批量运行所有可算指标"""
    results = []
    for mdef in metric_defs:
        result = compute_metric(mdef, canonical_dataset)
        results.append(result)
    return results
