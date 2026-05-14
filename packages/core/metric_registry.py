"""
metric_registry.py — 指标注册表
根据标准语义字段判断能计算哪些指标，从 domain_packs 加载
"""
import json
import yaml
from pathlib import Path
from typing import List, Optional


# 内置指标定义（兜底）
_BUILTIN_METRICS = [
    {
        "metric_id": "revenue_change",
        "name": "营收趋势",
        "required_fields": ["date", "revenue"],
        "domains": ["pharmacy", "restaurant", "retail", "generic"],
        "calculator": "period_change",
        "health_profiles": ["default"]
    },
    {
        "metric_id": "avg_order_value",
        "name": "客单价",
        "required_fields": ["revenue", "order_count"],
        "domains": ["pharmacy", "restaurant", "retail", "generic"],
        "calculator": "ratio",
        "params": {"numerator": "revenue", "denominator": "order_count"},
        "health_profiles": ["default"]
    },
    {
        "metric_id": "gross_margin_rate",
        "name": "毛利率",
        "required_fields": ["revenue", "gross_profit"],
        "domains": ["pharmacy", "restaurant", "retail"],
        "calculator": "ratio",
        "params": {"numerator": "gross_profit", "denominator": "revenue", "unit": "%"},
        "health_profiles": ["default"]
    },
    {
        "metric_id": "channel_share",
        "name": "渠道占比",
        "required_fields": ["revenue", "channel"],
        "domains": ["pharmacy", "restaurant", "retail"],
        "calculator": "share_by_dimension",
        "health_profiles": ["default"]
    },
    {
        "metric_id": "channel_concentration",
        "name": "渠道集中度",
        "required_fields": ["revenue", "channel"],
        "domains": ["pharmacy", "restaurant", "retail"],
        "calculator": "concentration",
        "health_profiles": ["default", "o2o_driven"]
    },
    {
        "metric_id": "top_product_contribution",
        "name": "TOP商品贡献",
        "required_fields": ["revenue", "product_name"],
        "domains": ["pharmacy", "restaurant", "retail"],
        "calculator": "top_contribution",
        "params": {"top_n": 3},
        "health_profiles": ["default"]
    },
    {
        "metric_id": "volatility",
        "name": "波动率",
        "required_fields": ["date", "revenue"],
        "domains": ["pharmacy", "restaurant", "retail", "generic"],
        "calculator": "volatility",
        "health_profiles": ["default"]
    },
    {
        "metric_id": "data_completeness",
        "name": "数据完整度",
        "required_fields": [],
        "domains": ["pharmacy", "restaurant", "retail", "hr", "generic"],
        "calculator": "data_quality",
        "health_profiles": ["default"]
    },
    # 药店专属
    {
        "metric_id": "member_penetration",
        "name": "会员渗透率",
        "required_fields": ["member_revenue", "revenue"],
        "domains": ["pharmacy"],
        "calculator": "ratio",
        "params": {"numerator": "member_revenue", "denominator": "revenue", "unit": "%"},
        "health_profiles": ["default"]
    },
    {
        "metric_id": "revenue_consecutive",
        "name": "营收连续涨跌",
        "required_fields": ["date", "revenue"],
        "domains": ["pharmacy", "restaurant", "retail", "hr", "generic"],
        "calculator": "consecutive_change",
        "params": {"field": "revenue"},
        "health_profiles": ["default"]
    },
    {
        "metric_id": "customer_change",
        "name": "客流趋势",
        "required_fields": ["date", "customer_count"],
        "domains": ["pharmacy", "restaurant", "retail", "generic"],
        "calculator": "period_change",
        "params": {"field": "customer_count"},
        "health_profiles": ["default"]
    },
    {
        "metric_id": "gross_margin_trend",
        "name": "毛利率趋势",
        "required_fields": ["date", "revenue", "gross_profit"],
        "domains": ["pharmacy", "restaurant", "retail", "generic"],
        "calculator": "gross_margin_trend",
        "health_profiles": ["default"]
    },
    {
        "metric_id": "customer_consecutive",
        "name": "客流连续涨跌",
        "required_fields": ["date", "customer_count"],
        "domains": ["pharmacy", "restaurant", "retail", "generic"],
        "calculator": "consecutive_change",
        "params": {"field": "customer_count"},
        "health_profiles": ["default"]
    },
    # 餐饮专属
    {
        "metric_id": "delivery_timeout_rate",
        "name": "配送超时率",
        "required_fields": ["delivery_duration"],
        "domains": ["restaurant"],
        "calculator": "threshold_rate",
        "params": {"field": "delivery_duration", "op": ">", "threshold": 30},
        "health_profiles": ["delivery_heavy"]
    },
    # HR专属
    {
        "metric_id": "turnover_rate",
        "name": "离职率",
        "required_fields": ["employee_id", "employee_status"],
        "domains": ["hr"],
        "calculator": "hr_turnover_rate",
        "health_profiles": ["default"]
    },
]


def _domain_pack_dir() -> Path:
    root = Path(__file__).parent.parent
    return root / "domain_packs"


def load_domain_metrics(industry: str) -> List[dict]:
    """从行业包加载指标定义"""
    pack_dir = _domain_pack_dir()
    pack_file = pack_dir / f"{industry}.yaml"
    metrics = []

    if pack_file.exists():
        try:
            data = yaml.safe_load(pack_file.read_text(encoding="utf-8"))
            if isinstance(data, dict) and "metrics" in data:
                metrics = data["metrics"]
        except Exception:
            pass

    # 兜底：使用内置指标
    if not metrics:
        metrics = [m for m in _BUILTIN_METRICS if industry in m.get("domains", [])]
        # 也加上通用指标
        metrics += [m for m in _BUILTIN_METRICS if "generic" in m.get("domains", [])]

    return metrics


def match_metrics(canonical_dataset: dict) -> List[dict]:
    """
    根据标准语义数据匹配可计算指标

    返回: MetricDefinition[] (含可算/不可算标记)
    """
    scene = canonical_dataset.get("scene", {})
    industry = scene.get("industry", "generic")

    # 收集所有可用字段
    available_fields = set()
    all_rows = {}  # table_name → rows
    for table_name, rows in canonical_dataset.get("tables", {}).items():
        all_rows[table_name] = rows
        if rows:
            for row in rows:
                for field in row.keys():
                    available_fields.add(field)

    # 加载指标定义
    metrics_defs = load_domain_metrics(industry)

    results = []
    seen_ids = set()
    for mdef in metrics_defs:
        required = mdef.get("required_fields", [])

        # 全局缺失检查
        global_missing = [f for f in required if f not in available_fields]

        # 同行共现检查：至少有一行同时包含所有 required 字段
        has_joint_row = False
        if not global_missing and required:
            for tn, rows in all_rows.items():
                for row in rows:
                    if all(f in row for f in required):
                        has_joint_row = True
                        break
                if has_joint_row:
                    break

        mid = mdef["metric_id"]
        if mid in seen_ids:
            continue
        seen_ids.add(mid)

        available = len(global_missing) == 0 and (has_joint_row or not required)

        result = {
            "metric_id": mdef["metric_id"],
            "name": mdef.get("name", ""),
            "required_fields": required,
            "calculator": mdef.get("calculator", ""),
            "params": mdef.get("params", {}),
            "health_profiles": mdef.get("health_profiles", ["default"]),
            "available": available,
            "missing_fields": global_missing if global_missing else (
                required if not has_joint_row and required else []
            ),
        }
        results.append(result)

    return results
