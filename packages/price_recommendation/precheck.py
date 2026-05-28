"""Fast deterministic checks for price recommendation inputs."""

from __future__ import annotations

import csv
import io
import json
import os
import re
from statistics import mean, median
from typing import Any


PRODUCT_FIELD_KEYWORDS = (
    "商品", "品名", "名称", "药品", "sku", "product", "item", "条码", "货号"
)
PRICE_FIELD_KEYWORDS = (
    "售价", "价格", "单价", "零售价", "price", "sale_price", "retail"
)
SALES_FIELD_KEYWORDS = (
    "销量", "销售数量", "销售量", "数量", "件数", "订单量", "sales", "qty", "quantity", "销售额", "revenue"
)
TIME_FIELD_KEYWORDS = (
    "日期", "时间", "月份", "date", "time", "day", "month"
)


def run_precheck(decoded_files: list[dict], product_name: str) -> dict:
    """Return a user-facing validation result for uploaded files and product name."""
    product_name = (product_name or "").strip()
    issues: list[dict] = []
    warnings: list[dict] = []

    if not product_name:
        issues.append({"code": "missing_product_name", "message": "缺少商品名称"})

    inspection = inspect_uploaded_files(decoded_files)
    tables = inspection["tables"]
    if inspection["errors"]:
        for err in inspection["errors"]:
            warnings.append({"code": "file_partial_parse_failed", "message": err})

    total_rows = sum(len(t["rows"]) for t in tables)
    fields = _collect_fields(tables)
    product_fields = _match_fields(fields, PRODUCT_FIELD_KEYWORDS)
    price_fields = _match_fields(fields, PRICE_FIELD_KEYWORDS)
    sales_fields = _match_fields(fields, SALES_FIELD_KEYWORDS)
    time_fields = _match_fields(fields, TIME_FIELD_KEYWORDS)
    matched_rows = _find_product_rows(tables, product_name, product_fields)
    matched_row_count = len(matched_rows)

    if not tables or total_rows == 0:
        issues.append({"code": "no_tabular_data", "message": "上传文件中没有可用于分析的表格数据"})
    if product_name and len(_normalize_text(product_name)) < 2:
        issues.append({"code": "product_name_too_short", "message": "商品名称过短，请提供更具体的完整品名"})
    elif product_name and len(_normalize_text(product_name)) < 4:
        warnings.append({"code": "product_name_may_be_broad", "message": "商品名称可能偏宽，建议补充规格或完整品名"})
    if tables and not product_fields:
        issues.append({"code": "product_field_missing", "message": "未识别到商品名称、品名或 SKU 字段"})
    if tables and not price_fields:
        issues.append({"code": "price_field_missing", "message": "未识别到价格、售价或单价字段"})
    if tables and not sales_fields:
        issues.append({"code": "sales_field_missing", "message": "未识别到销量、销售额或订单量字段"})
    if product_name and product_fields and matched_row_count == 0:
        issues.append({"code": "product_not_found", "message": "上传文件中没有找到该商品的销售记录"})
    if tables and not time_fields:
        warnings.append({"code": "time_field_missing", "message": "未识别到日期或时间字段，后续趋势判断会受限"})

    valid = not issues
    match_confidence = _confidence(matched_row_count, total_rows, bool(price_fields), bool(sales_fields))

    return {
        "status": "ok" if valid else "failed",
        "valid": valid,
        "productName": product_name,
        "productNameQuality": {
            "status": "ok" if product_name and len(_normalize_text(product_name)) >= 4 else ("warning" if product_name else "failed"),
            "message": "商品名称足够具体" if product_name and len(_normalize_text(product_name)) >= 4 else "商品名称需要更具体",
        },
        "fileQuality": {
            "status": "ok" if tables and total_rows > 0 else "failed",
            "message": f"文件可解析，识别到 {len(tables)} 张表、{total_rows} 行数据" if tables else "文件未解析出表格数据",
        },
        "productDataMatch": {
            "status": "ok" if matched_row_count > 0 else "failed",
            "matchedRows": matched_row_count,
            "confidence": match_confidence,
        },
        "detectedFields": {
            "productFields": product_fields,
            "priceFields": price_fields,
            "salesFields": sales_fields,
            "timeFields": time_fields,
        },
        "issues": issues,
        "warnings": warnings,
    }


def inspect_uploaded_files(decoded_files: list[dict], max_rows_per_table: int = 1000) -> dict:
    """Parse JSON/CSV/XLSX uploads into lightweight table dictionaries."""
    tables: list[dict] = []
    errors: list[str] = []
    for item in decoded_files:
        name = item.get("name") or "unnamed"
        data = item.get("bytes") or b""
        lower = name.lower()
        try:
            if lower.endswith(".json"):
                raw = json.loads(data.decode("utf-8-sig"))
                tables.extend(_json_to_tables(raw, os.path.splitext(name)[0], max_rows_per_table))
            elif lower.endswith(".csv"):
                tables.append(_csv_to_table(data, name, max_rows_per_table))
            elif lower.endswith((".xlsx", ".xls")):
                tables.extend(_xlsx_to_tables(data, name, max_rows_per_table))
        except Exception as exc:
            errors.append(f"{name} 解析失败: {exc}")
    return {"tables": tables, "errors": errors}


def build_basic_recommendation(inspection: dict, product_name: str, candidate_count: int = 2) -> dict:
    """Build a deterministic baseline recommendation for the first implementation."""
    tables = inspection.get("tables", [])
    fields = _collect_fields(tables)
    product_fields = _match_fields(fields, PRODUCT_FIELD_KEYWORDS)
    price_fields = _match_fields(fields, PRICE_FIELD_KEYWORDS)
    sales_fields = _match_fields(fields, SALES_FIELD_KEYWORDS)
    time_fields = _match_fields(fields, TIME_FIELD_KEYWORDS)
    matched_rows = _find_product_rows(tables, product_name, product_fields)
    price_field = price_fields[0] if price_fields else ""

    price_values: list[float] = []
    for row in matched_rows:
        value = _to_number(row.get(price_field)) if price_field else None
        if value is not None and value > 0:
            price_values.append(value)

    if not price_values:
        raise ValueError("目标商品没有可用的历史价格数据")

    median_price = round(median(price_values), 2)
    mean_price = round(mean(price_values), 2)
    candidates = [median_price]
    if abs(mean_price - median_price) >= 0.01:
        candidates.append(mean_price)
    else:
        candidates.append(round(median_price * 1.03, 2))

    recommendations = []
    for idx, price in enumerate(_dedupe_prices(candidates)[:max(1, candidate_count)], start=1):
        recommendations.append({
            "rank": idx,
            "price": price,
            "unit": "元",
            "reason": "基于目标商品历史价格分布生成的基准推荐，后续可替换为价格弹性或曲线拟合模型",
            "confidence": _confidence(len(matched_rows), sum(len(t["rows"]) for t in tables), True, bool(sales_fields)),
        })

    return {
        "taskType": "price_recommendation",
        "productName": product_name,
        "recommendations": recommendations,
        "validPriceRange": {
            "min": round(min(price_values), 2),
            "max": round(max(price_values), 2),
            "unit": "元",
        },
        "evidence": {
            "matchedRows": len(matched_rows),
            "observedPriceCount": len(price_values),
            "priceField": price_field,
            "salesField": sales_fields[0] if sales_fields else "",
            "timeField": time_fields[0] if time_fields else "",
            "sourceTables": [t["name"] for t in tables],
            "notes": ["当前为确定性基准推荐，尚未接入价格弹性模型"],
        },
        "warnings": [],
    }


def _json_to_tables(raw: Any, name: str, max_rows: int) -> list[dict]:
    tables: list[dict] = []
    if isinstance(raw, list):
        return [{"name": name, "rows": _dict_rows(raw[:max_rows])}]
    if isinstance(raw, dict):
        for key, value in raw.items():
            if isinstance(value, list) and value:
                tables.append({"name": f"{name}/{key}", "rows": _dict_rows(value[:max_rows])})
            elif isinstance(value, dict):
                for sub_key, sub_value in value.items():
                    if isinstance(sub_value, list) and sub_value:
                        tables.append({"name": f"{name}/{key}/{sub_key}", "rows": _dict_rows(sub_value[:max_rows])})
        if not tables:
            tables.append({"name": name, "rows": [raw]})
    return tables


def _csv_to_table(data: bytes, name: str, max_rows: int) -> dict:
    content = data.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(content))
    return {"name": name, "rows": [dict(row) for _, row in zip(range(max_rows), reader)]}


def _xlsx_to_tables(data: bytes, name: str, max_rows: int) -> list[dict]:
    import openpyxl

    workbook = openpyxl.load_workbook(io.BytesIO(data), read_only=True, data_only=True)
    tables: list[dict] = []
    try:
        for sheet_name in workbook.sheetnames:
            sheet = workbook[sheet_name]
            rows_iter = sheet.iter_rows(values_only=True)
            header = next(rows_iter, None)
            if not header:
                continue
            columns = [str(value).strip() if value is not None else f"col_{idx}" for idx, value in enumerate(header)]
            rows = []
            for idx, values in enumerate(rows_iter):
                if idx >= max_rows:
                    break
                if not any(value is not None for value in values):
                    continue
                rows.append(dict(zip(columns, values)))
            tables.append({"name": f"{name}/{sheet_name}", "rows": rows})
    finally:
        workbook.close()
    return tables


def _dict_rows(values: list[Any]) -> list[dict]:
    rows: list[dict] = []
    for value in values:
        if isinstance(value, dict):
            rows.append(value)
        else:
            rows.append({"value": value})
    return rows


def _collect_fields(tables: list[dict]) -> list[str]:
    fields: set[str] = set()
    for table in tables:
        for row in table.get("rows", []):
            if isinstance(row, dict):
                fields.update(str(key) for key in row.keys())
    return sorted(fields)


def _match_fields(fields: list[str], keywords: tuple[str, ...]) -> list[str]:
    matched = []
    for field in fields:
        lowered = field.lower()
        if any(keyword.lower() in lowered for keyword in keywords):
            matched.append(field)
    return matched


def _find_product_rows(tables: list[dict], product_name: str, product_fields: list[str]) -> list[dict]:
    target = _normalize_text(product_name)
    if not target:
        return []
    rows = []
    search_fields = product_fields or _collect_fields(tables)
    for table in tables:
        for row in table.get("rows", []):
            if not isinstance(row, dict):
                continue
            for field in search_fields:
                value = row.get(field)
                text = _normalize_text(value)
                if text and (target in text or text in target):
                    rows.append(row)
                    break
    return rows


def _normalize_text(value: Any) -> str:
    return re.sub(r"\s+", "", str(value or "")).lower()


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


def _confidence(matched_rows: int, total_rows: int, has_price: bool, has_sales: bool) -> float:
    score = 0.4
    if matched_rows:
        score += min(0.3, matched_rows / max(total_rows, 1))
    if has_price:
        score += 0.15
    if has_sales:
        score += 0.1
    return round(min(score, 0.95), 2)


def _dedupe_prices(values: list[float]) -> list[float]:
    result: list[float] = []
    seen: set[float] = set()
    for value in values:
        rounded = round(value, 2)
        if rounded not in seen and rounded > 0:
            result.append(rounded)
            seen.add(rounded)
    return result
