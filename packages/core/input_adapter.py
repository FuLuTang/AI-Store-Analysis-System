"""
input_adapter.py — 统一输入适配器
支持 JSON（含药店ERP）/ Excel / CSV 多文件解析，输出 DatasetBundle
"""
import json
import io
import os
from pathlib import Path
from typing import List, Optional


def _is_pharmacy_json(raw: dict) -> bool:
    """检测是否为旧版药店ERP JSON（含 page.module 和 businessTable.rows）"""
    if not isinstance(raw, dict):
        return False
    page = raw.get("page", {})
    if not isinstance(page, dict) or "module" not in page:
        return False
    return True


def _extract_pharmacy_tables(raw: dict, name: str) -> list:
    """药店 ERP JSON → 提取 inner table(s) 为 RawTable"""
    tables = []
    module = raw.get("page", {}).get("module", "unknown")
    view_type = raw.get("page", {}).get("viewType", "")

    # business_overview / o2o_business_summary → 提取 businessTable.rows
    biz_table = raw.get("businessTable") or raw.get("dailyBusinessTable")
    if biz_table and isinstance(biz_table, dict) and "rows" in biz_table:
        rows = biz_table["rows"]
        if rows:
            tables.append({"name": f"{name}/business", "rows": rows})

    # 提取 sourceDistribution
    src_dist = raw.get("sourceDistribution")
    if src_dist and isinstance(src_dist, dict) and "items" in src_dist:
        items = src_dist["items"]
        if items:
            tables.append({"name": f"{name}/channels", "rows": items})

    # operation_hot_products → ranking
    ranking = raw.get("ranking")
    if ranking and isinstance(ranking, list) and ranking:
        tables.append({"name": f"{name}/ranking", "rows": ranking})

    # hot_sale_top500 → products
    products = raw.get("products")
    if products and isinstance(products, list) and products:
        tables.append({"name": f"{name}/top500", "rows": products})

    # summary.metrics
    summary = raw.get("summary")
    if summary and isinstance(summary, dict) and "metrics" in summary:
        tables.append({"name": f"{name}/summary", "rows": summary["metrics"]})

    if not tables:
        # 兜底：整条 JSON 当一行
        return [{"name": name, "rows": [{"_meta": f"module={module} view={view_type}"}]}]

    return tables


def _json_to_table(raw, name: str = "data") -> dict:
    """JSON → RawTable：数组转表，对象保留层级路径"""
    if isinstance(raw, list):
        return {"name": name, "rows": raw}
    if isinstance(raw, dict):
        rows = []
        for key, val in raw.items():
            if isinstance(val, list):
                rows.extend(val)
        if rows:
            return {"name": name, "rows": rows}
        return {"name": name, "rows": [raw]}
    return {"name": name, "rows": []}


def _excel_to_tables(file_bytes: bytes, filename: str) -> list:
    """Excel → RawTable[]：每 sheet 一张表"""
    try:
        import openpyxl
    except ImportError:
        raise ImportError("需要 openpyxl 解析 Excel 文件，请执行: pip install openpyxl")

    wb = openpyxl.load_workbook(io.BytesIO(file_bytes), read_only=True, data_only=True)
    tables = []
    for sheet_name in wb.sheetnames:
        ws = wb[sheet_name]
        all_rows = list(ws.iter_rows(values_only=True))
        if not all_rows:
            continue
        header = [str(h) if h is not None else f"col_{i}" for i, h in enumerate(all_rows[0])]
        rows = [dict(zip(header, r)) for r in all_rows[1:] if any(v is not None for v in r)]
        tables.append({"name": f"{filename}/{sheet_name}", "rows": rows})
    wb.close()
    return tables


def _csv_to_table(file_bytes: bytes, filename: str) -> dict:
    """CSV → RawTable"""
    import csv
    content = file_bytes.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(content))
    rows = [dict(row) for row in reader]
    return {"name": filename, "rows": rows}


def parse_file(file_bytes: bytes, filename: str) -> dict:
    """根据后缀分发解析"""
    lower = filename.lower()
    if lower.endswith(".json"):
        raw = json.loads(file_bytes.decode("utf-8-sig"))
        if _is_pharmacy_json(raw):
            return _extract_pharmacy_tables(raw, os.path.splitext(filename)[0])
        return _json_to_table(raw, os.path.splitext(filename)[0])
    elif lower.endswith(".xlsx") or lower.endswith(".xls"):
        return _excel_to_tables(file_bytes, filename)
    elif lower.endswith(".csv"):
        return _csv_to_table(file_bytes, filename)
    else:
        raise ValueError(f"不支持的文件类型: {filename}")


def parse_uploaded_files(decoded_files: List[dict]) -> dict:
    """
    批量解析上传文件，输出 DatasetBundle

    输入: [{"name": "概览-日.json", "bytes": b"..."}, ...]
    输出: DatasetBundle = {
        "source_type": "json|excel|csv",
        "tables": [RawTable],
        "received_at": "2026-05-12T10:00:00"
    }
    """
    from datetime import datetime

    all_tables = []
    source_type = "json"  # 默认

    for item in decoded_files:
        name = item.get("name", "unnamed")
        data_bytes = item.get("bytes", b"")
        if not data_bytes:
            continue

        lower = name.lower()
        if lower.endswith(".xlsx") or lower.endswith(".xls"):
            source_type = "excel"
        elif lower.endswith(".csv"):
            source_type = "csv"

        result = parse_file(data_bytes, name)
        if isinstance(result, list):
            all_tables.extend(result)
        else:
            all_tables.append(result)

    return {
        "source_type": source_type,
        "tables": all_tables,
        "received_at": datetime.now().isoformat()
    }


def infer_source_type(filename: str) -> str:
    """从文件名推断来源类型"""
    lower = filename.lower()
    if lower.endswith(".json"):
        return "json"
    elif lower.endswith(".xlsx") or lower.endswith(".xls"):
        return "excel"
    elif lower.endswith(".csv"):
        return "csv"
    return "unknown"
