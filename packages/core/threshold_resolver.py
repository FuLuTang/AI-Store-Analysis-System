"""
threshold_resolver.py — 场景化阈值判断
同一个指标，不同场景使用不同健康判断
"""
from typing import List, Optional


# 默认阈值配置
DEFAULT_THRESHOLDS = {
    "channel_concentration": {
        "default": {"attention": 60, "warning": 80},
        "o2o_driven": {"attention": 85, "warning": 95},
        "offline_driven": {"attention": 50, "warning": 70},
    },
    "revenue_change": {
        "default": {"attention": 20, "warning": 50},
    },
    "volatility": {
        "default": {"attention": 0.25, "warning": 0.4},
    },
    "delivery_timeout_rate": {
        "default": {"attention": 10, "warning": 25},
    },
    "turnover_rate": {
        "default": {"attention": 15, "warning": 30},
    },
}


def _resolve_threshold(metric_id: str, scene: dict) -> dict:
    """根据场景解析阈值"""
    profiles = DEFAULT_THRESHOLDS.get(metric_id, {})
    business_model = scene.get("business_model", "unknown")

    # 优先行业特定阈值 → 业态阈值 → 默认
    for model_key in [business_model, "default"]:
        if model_key in profiles:
            return profiles[model_key]

    return {"attention": 30, "warning": 60}


def _judge_ratio(value: dict, threshold: dict) -> tuple:
    """
    判断比率类指标 (如渠道集中度、占比)
    value 是百分比数值
    """
    pct = value if isinstance(value, (int, float)) else (
        value.get("concentration") if isinstance(value, dict) else None
    )

    if pct is None or not isinstance(pct, (int, float)):
        return ("pass", None)

    if pct >= threshold.get("warning", 80):
        status = "warning"
    elif pct >= threshold.get("attention", 60):
        status = "attention"
    else:
        status = "pass"

    return status, pct


def _judge_change(value: dict, threshold: dict) -> tuple:
    """
    判断变化类指标 (如营收变化)
    value 中取 change_pct / slope 等
    """
    if isinstance(value, (int, float)):
        change = value
    elif isinstance(value, dict):
        change = abs(value.get("slope", 0) or 0)
    else:
        return ("pass", 0)

    abs_change = abs(change) if isinstance(change, (int, float)) else 0

    if abs_change >= threshold.get("warning", 50):
        status = "warning"
    elif abs_change >= threshold.get("attention", 20):
        status = "attention"
    else:
        status = "pass"

    return status, change


def _judge_volatility(value: dict, threshold: dict) -> tuple:
    """判断波动率"""
    cv = value.get("cv", 0) if isinstance(value, dict) else value

    if cv >= threshold.get("warning", 0.4):
        status = "warning"
    elif cv >= threshold.get("attention", 0.25):
        status = "attention"
    else:
        status = "pass"

    return status, cv


def resolve_status(metric_result: dict, scene: dict) -> dict:
    """
    根据场景给指标计算健康状态

    输入: MetricResult + SceneContext
    输出: 补充 status 和 reason 的 MetricResult
    """
    metric_id = metric_result.get("metric_id", "")
    value = metric_result.get("value")

    if metric_result.get("status") == "uncountable":
        return metric_result

    threshold = _resolve_threshold(metric_id, scene)

    # 根据指标类型选择判断方式
    if metric_id in ("channel_concentration", "channel_share", "member_penetration", "top_product_contribution"):
        status, detail = _judge_ratio(value, threshold)
    elif metric_id in ("volatility",):
        status, detail = _judge_volatility(value, threshold)
    else:
        status, detail = _judge_change(value, threshold)

    reason_map = {
        "warning": f"{metric_result.get('name')}处于报警区间",
        "attention": f"{metric_result.get('name')}值得关注",
        "pass": f"{metric_result.get('name')}正常",
    }

    result = dict(metric_result)
    result["status"] = status
    result["reason"] = reason_map.get(status, "")
    result["threshold_used"] = threshold

    return result


def resolve_all_statuses(metric_results: list, scene: dict) -> list:
    """批量判断健康状态"""
    return [resolve_status(r, scene) for r in metric_results]
