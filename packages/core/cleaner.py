"""
cleaner.py — 数据清洗模块 (从 cleaner.js 完整迁移)

职责：将原始 JSON 数据清洗为紧凑格式，供 AI 分析使用
"""

import os
import json
import math
import re
import shutil
from datetime import datetime


def round_val(v):
    """四舍五入为整数"""
    return round(v) if isinstance(v, (int, float)) and math.isfinite(v) else v


def calc_mom(cur, prev):
    """计算环比（百分比整数）"""
    if cur is None or prev is None or prev == 0:
        return None
    return round_val((cur - prev) / prev * 100)


def clean_data(raw_data: dict) -> dict | None:
    """
    清洗 JSON 数据
    根据 module 类型分发到不同的清洗函数
    """
    module = raw_data.get("page", {}).get("module")

    handlers = {
        "business_overview": clean_business_overview,
        "operation_hot_products": clean_store_hot_products,
        "hot_sale_top500": clean_hot_top500,
        "o2o_business_summary": clean_o2o_business_summary,
    }

    if module == "o2o_product_category":
        print("跳过商品资料类 JSON")
        return None

    handler = handlers.get(module)
    if handler:
        return handler(raw_data)

    print(f"未知模块类型: {module}")
    return None


def clean_business_overview(data: dict) -> dict:
    page = data.get("page", {})
    period = page.get("selectedDate") or str(page.get("selectedYear", "")) or ""
    granularity = page.get("viewType", "")

    # summary_rows
    summary_rows = []
    for m in data.get("summary", {}).get("metrics", []):
        mom_data = m.get("mom") or m.get("compare") or {}
        label = m.get("label", "")
        extra = m.get("extra")
        if extra:
            label = f"{label}({extra.get('label', '')}{extra.get('value', '')}{extra.get('unit', '')})"
        rate = m.get("rate")
        rate_unit = m.get("rateUnit", "")
        if rate:
            label += f"(率{rate}{rate_unit})"
        summary_rows.append([
            label,
            round_val(m.get("value", 0)),
            round_val(mom_data.get("value", 0)),
        ])

    # 等效推算
    last_updated = page.get("lastUpdated")
    if last_updated:
        eq_prefix = ""
        if granularity == "day":
            eq_prefix = "等效全天"
        elif granularity == "month":
            eq_prefix = "等效全月"

        if eq_prefix:
            metrics_list = data.get("summary", {}).get("metrics", [])
            metric_map = {m["key"]: m for m in metrics_list if "key" in m}
            for key, label_suffix in [("revenue", "营收"), ("gross_profit", "毛利"), ("visitor_count", "客数")]:
                metric = metric_map.get(key)
                if metric:
                    eq_val = calculate_equivalent(metric["value"], granularity, last_updated)
                    if eq_val is not None:
                        val = round(eq_val) if key == "visitor_count" else eq_val
                        summary_rows.append([f"{eq_prefix}{label_suffix}", val, "-"])

    # ranking_rows
    ranking_rows = [
        [item.get("label", ""), item.get("date") or item.get("period", ""), round_val(item.get("value", 0))]
        for item in data.get("ranking", {}).get("items", [])
    ]

    # table_rows
    table_source = data.get("dailyBusinessTable") or data.get("businessTable") or {}
    raw_rows = table_source.get("rows", [])

    is_descending = True
    if len(raw_rows) > 1:
        d1 = raw_rows[0].get("date") or raw_rows[0].get("period", "")
        d2 = raw_rows[1].get("date") or raw_rows[1].get("period", "")
        if d1 < d2:
            is_descending = False

    table_rows = []
    for i, row in enumerate(raw_rows):
        prev_row = raw_rows[i + 1] if is_descending and i + 1 < len(raw_rows) else \
                   raw_rows[i - 1] if not is_descending and i > 0 else None
        ecom = row.get("ecommerce_amount") or row.get("online_amount") or 0
        prev_ecom = (prev_row.get("ecommerce_amount") or prev_row.get("online_amount") or 0) if prev_row else 0
        table_rows.append([
            row.get("date") or row.get("period", ""),
            round_val(row.get("retail_amount", 0)), calc_mom(row.get("retail_amount", 0), prev_row.get("retail_amount") if prev_row else None),
            round_val(row.get("gross_profit", 0)), calc_mom(row.get("gross_profit", 0), prev_row.get("gross_profit") if prev_row else None),
            round_val(row.get("visitor_count", 0)), calc_mom(row.get("visitor_count", 0), prev_row.get("visitor_count") if prev_row else None),
            round_val(row.get("member_amount", 0)), calc_mom(row.get("member_amount", 0), (prev_row.get("member_amount") or 0) if prev_row else None),
            round_val(ecom), calc_mom(ecom, prev_ecom),
            round_val(row.get("ecommerce_gross_profit", 0)),
            calc_mom(row.get("ecommerce_gross_profit", 0), (prev_row.get("ecommerce_gross_profit") or 0) if prev_row else None),
        ])

    # 业绩来源占比
    source_distribution = [
        [item.get("label", ""), round_val(item.get("value", 0))]
        for item in data.get("sourceDistribution", {}).get("items", [])
    ]

    return {
        "type": "business_overview",
        "period": period,
        "granularity": granularity,
        "summary_schema": ["指标", "值", "环比%"],
        "summary_rows": summary_rows,
        "ranking_schema": ["项目", "周期", "值"],
        "ranking_rows": ranking_rows,
        "table_schema": ["周期", "零售额", "环比%", "毛利", "环比%", "客数", "环比%", "会员额", "环比%", "电商额", "环比%", "电商毛利", "环比%"],
        "table_rows": table_rows,
        "distribution_schema": ["来源", "金额"],
        "distribution_rows": source_distribution,
    }


def calculate_equivalent(value, view_type, last_updated_str):
    """等效全天/全月推算"""
    if not last_updated_str or not isinstance(value, (int, float)) or value == 0:
        return None

    match = re.match(r"(\d{4})-(\d{2})-(\d{2})(?:\s+(\d{2}):(\d{2}))?", last_updated_str)
    if not match:
        return None

    year = int(match.group(1))
    month = int(match.group(2))
    day = int(match.group(3))
    hour = int(match.group(4)) if match.group(4) else 24
    minute = int(match.group(5)) if match.group(5) else 0

    elapsed_hours = hour + minute / 60

    if view_type == "day":
        progress = max(0.01, elapsed_hours / 24)
        return round(value / progress)
    elif view_type == "month":
        import calendar
        days_in_month = calendar.monthrange(year, month)[1]
        progress = max(0.01, (day - 1 + elapsed_hours / 24) / days_in_month)
        return round(value / progress)
    return None


def clean_o2o_business_summary(data: dict) -> dict:
    page = data.get("page", {})
    period = page.get("selectedDate") or page.get("selectedMonth") or str(page.get("selectedYear", "")) or ""
    granularity = page.get("viewType", "")

    raw_rows = data.get("businessTable", {}).get("rows", [])

    is_descending = True
    if len(raw_rows) > 1:
        d1 = raw_rows[0].get("date") or raw_rows[0].get("period", "")
        d2 = raw_rows[1].get("date") or raw_rows[1].get("period", "")
        if d1 < d2:
            is_descending = False

    table_rows = []
    for i, row in enumerate(raw_rows):
        prev_row = raw_rows[i + 1] if is_descending and i + 1 < len(raw_rows) else \
                   raw_rows[i - 1] if not is_descending and i > 0 else None
        table_rows.append([
            row.get("period", ""),
            round_val(row.get("total_order_count", 0)), calc_mom(row.get("total_order_count", 0), (prev_row.get("total_order_count") or 0) if prev_row else None),
            round_val(row.get("total_revenue", 0)), calc_mom(row.get("total_revenue", 0), (prev_row.get("total_revenue") or 0) if prev_row else None),
            round_val(row.get("gross_profit", 0)), calc_mom(row.get("gross_profit", 0), (prev_row.get("gross_profit") or 0) if prev_row else None),
            round_val(row.get("meituan_order_count", 0)), calc_mom(row.get("meituan_order_count", 0), (prev_row.get("meituan_order_count") or 0) if prev_row else None),
            round_val(row.get("eleme_order_count", 0)), calc_mom(row.get("eleme_order_count", 0), (prev_row.get("eleme_order_count") or 0) if prev_row else None),
            round_val(row.get("meituan_revenue", 0)), calc_mom(row.get("meituan_revenue", 0), (prev_row.get("meituan_revenue") or 0) if prev_row else None),
            round_val(row.get("eleme_revenue", 0)), calc_mom(row.get("eleme_revenue", 0), (prev_row.get("eleme_revenue") or 0) if prev_row else None),
        ])

    return {
        "type": "o2o_business_summary",
        "period": period,
        "granularity": granularity,
        "table_schema": ["周期", "总单数", "环比%", "总营业额", "环比%", "毛利", "环比%", "美团单", "环比%", "饿了单", "环比%", "美团营业", "环比%", "饿了营业", "环比%"],
        "table_rows": table_rows,
    }


def clean_store_hot_products(data: dict) -> dict:
    period = data.get("page", {}).get("viewType", "")
    rows = [
        [item.get("rank"), item.get("product_name"), item.get("sales_receipt_count"), item.get("sales_quantity")]
        for item in data.get("ranking", [])
    ]
    return {"type": "store_hot_products", "period": period, "schema": ["排名", "商品名", "笔数", "数量"], "rows": rows}


def clean_hot_top500(data: dict) -> dict:
    products = data.get("products", [])
    city = products[0].get("city", "未知") if products else "未知"
    status = data.get("page", {}).get("viewType", "top500")
    rows = [[status, p.get("rank"), p.get("product_name")] for p in products]
    return {"type": "hot_top500_stock_status", "city": city, "schema": ["状态", "排名", "商品名"], "rows": rows}


def merge_hot_top500(json_list: list) -> dict | None:
    """合并多个热销500文件"""
    top500_files = [j for j in json_list if j.get("page", {}).get("module") == "hot_sale_top500"]
    if not top500_files:
        return None

    city = (top500_files[0].get("products") or [{}])[0].get("city", "未知") if top500_files[0].get("products") else "未知"
    product_map = {}
    status_priority = {"missing_category": 4, "out_of_stock": 3, "in_stock": 2, "top500": 1}

    for f in top500_files:
        file_status = f.get("page", {}).get("viewType", "top500")
        for p in f.get("products", []):
            name = p.get("product_name")
            rank = p.get("sales_rank") or p.get("rank", 999)
            if name not in product_map:
                product_map[name] = {"rank": rank, "name": name, "status": file_status}
            else:
                existing = product_map[name]
                if status_priority.get(file_status, 0) > status_priority.get(existing["status"], 0):
                    existing["status"] = file_status
                if rank < existing["rank"]:
                    existing["rank"] = rank

    sorted_products = sorted(product_map.values(), key=lambda p: p["rank"])
    groups = {"top500": [], "in_stock": [], "out_of_stock": [], "missing_category": []}
    for p in sorted_products:
        if p["status"] in groups:
            groups[p["status"]].append([p["rank"], p["name"]])

    return {
        "type": "hot_top500_stock_status", "city": city,
        "schema": ["排名", "商品名"],
        **groups,
    }


def merge_hot_products(json_list: list) -> dict | None:
    """合并多个热销商品文件"""
    hot_files = [j for j in json_list if j.get("page", {}).get("module") == "operation_hot_products"]
    if not hot_files:
        return None

    period_map = {"today": "今", "yesterday": "昨", "7days": "周", "30days": "月"}
    groups = {}
    for f in hot_files:
        period = f.get("page", {}).get("viewType", "unknown")
        key = period_map.get(period, period)
        groups[key] = [
            [item.get("rank"), item.get("product_name"), item.get("sales_receipt_count"), item.get("sales_quantity")]
            for item in f.get("ranking", [])
        ]

    return {"type": "store_hot_products", "schema": ["排名", "商品名", "笔数", "数量"], **groups}


def clear_cache(cache_dir: str):
    """清理缓存目录"""
    if os.path.exists(cache_dir):
        for f in os.listdir(cache_dir):
            os.remove(os.path.join(cache_dir, f))
    else:
        os.makedirs(cache_dir, exist_ok=True)


def stringify_compact(obj) -> str:
    """紧凑型 JSON 序列化"""
    return json.dumps(obj, ensure_ascii=False, indent=2)
