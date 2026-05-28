"""
profiler.py — 数据画像
输入 RawTable[]，输出每个字段的 ColumnProfile
"""
from typing import List


def _guess_dtype(values: list) -> str:
    """推断字段类型"""
    non_none = [v for v in values if v is not None]
    if not non_none:
        return "unknown"

    numbers = 0
    strings = 0
    for v in non_none:
        if isinstance(v, (int, float)):
            numbers += 1
        elif isinstance(v, str):
            strings += 1

    total = numbers + strings
    if total == 0:
        return "unknown"
    if numbers / total > 0.8:
        return "number"
    if strings / total > 0.8:
        return "string"
    return "unknown"


def profile_table(table: dict) -> List[dict]:
    """
    输入: RawTable = {"name": "orders", "rows": [{...}]}
    输出: ColumnProfile[]
    """
    rows = table.get("rows", [])
    table_name = table.get("name", "unknown")

    if not rows:
        return []

    # 收集所有列名
    columns = {}
    for row in rows:
        if isinstance(row, dict):
            for key in row:
                if key not in columns:
                    columns[key] = []
                columns[key].append(row[key])

    profiles = []
    for col, values in columns.items():
        non_null = [v for v in values if v is not None]
        null_count = len(values) - len(non_null)
        null_rate = round(null_count / len(values), 3) if values else 0

        dtype = _guess_dtype(non_null)

        profile = {
            "table": table_name,
            "column": col,
            "dtype": dtype,
            "samples": non_null[:5],
            "null_rate": null_rate,
            "unique_count": len(set(str(v) for v in non_null)),
        }

        # 数值字段补充 min/max
        num_vals = [v for v in non_null if isinstance(v, (int, float))]
        if num_vals:
            profile["min"] = min(num_vals)
            profile["max"] = max(num_vals)

        profiles.append(profile)

    return profiles


def profile_dataset(dataset_bundle: dict) -> List[dict]:
    """
    输入: DatasetBundle
    输出: ColumnProfile[] (所有表的所有字段)
    """
    all_profiles = []
    for table in dataset_bundle.get("tables", []):
        all_profiles.extend(profile_table(table))
    return all_profiles
