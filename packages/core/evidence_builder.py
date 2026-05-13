"""
evidence_builder.py — 证据包构建
把计算结果整理成报告证据，确保报告只引用证据包数据
"""
from typing import List


def build_evidence_item(metric_result: dict, source_rows: List[dict] = None) -> dict:
    """
    构建单条证据

    输出: EvidenceItem = {
        "metric_id": str,
        "title": str,
        "value": any,
        "status": str,
        "source_fields": [str],
        "evidence_table": [...],
        "confidence": float
    }
    """
    metric_id = metric_result.get("metric_id", "")
    name = metric_result.get("name", "")
    value = metric_result.get("value")
    status = metric_result.get("status", "uncountable")
    reason = metric_result.get("reason", "")

    # 构建证据表（精简样本数据）
    evidence_table = []
    if source_rows:
        evidence_table = source_rows[:5]  # 最多保留5行样本

    return {
        "metric_id": metric_id,
        "title": f"{name}{'(' + reason + ')' if reason else ''}",
        "value": value,
        "status": status,
        "source_fields": metric_result.get("required_fields", []),
        "evidence_table": evidence_table,
        "confidence": metric_result.get("confidence", 0)
    }


def build_evidence_bundle(metric_results: list, canonical_dataset: dict = None) -> dict:
    """
    构建完整证据包

    输出: {
        "items": EvidenceItem[],
        "summary": {...},
        "data_quality": {...},
        "mapping_records": [...]  # 字段映射记录
    }
    """
    items = []
    tally = {"pass": 0, "attention": 0, "warning": 0, "uncountable": 0}

    # 取主要数据表行数作为源数据
    source_rows = []
    if canonical_dataset:
        tables = canonical_dataset.get("tables", {})
        if "sales" in tables:
            source_rows = tables["sales"]
        elif tables:
            source_rows = next(iter(tables.values()), [])

    for r in metric_results:
        status = r.get("status", "uncountable")
        tally[status] = tally.get(status, 0) + 1

        item = build_evidence_item(r, source_rows)
        items.append(item)

    # 数据质量（从 data_quality 指标中提取）
    data_quality = {}
    for r in metric_results:
        if r.get("metric_id") == "data_completeness":
            data_quality = r.get("value") or {}
            break

    # 映射记录
    mapping_records = []
    if canonical_dataset:
        mapping_records = [
            {"raw": m.get("raw_field", ""), "semantic": m.get("semantic_field", ""),
             "table": m.get("table", ""), "confidence": m.get("confidence", 0)}
            for m in canonical_dataset.get("mapping", [])
        ]

    return {
        "items": items,
        "summary": {
            "total_metrics": len(items),
            "tally": tally,
            "scene": canonical_dataset.get("scene", {}) if canonical_dataset else {}
        },
        "data_quality": data_quality,
        "mapping_records": mapping_records,
    }
