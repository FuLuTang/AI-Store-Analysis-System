"""
canonical.py — 标准语义数据层
根据字段映射结果，将原始表转为标准语义表
支持按 date 合并来自不同源文件的同行数据
"""
from typing import List


def build_canonical_dataset(dataset_bundle: dict, mappings: list, scene: dict) -> dict:
    """
    将原始数据转换为标准语义数据集

    输入:
      - dataset_bundle: DatasetBundle (含原始 tables)
      - mappings: SemanticMapping[] (含 raw_field → semantic_field 映射)
      - scene: SceneContext

    输出: CanonicalDataset = {
        "tables": { "sales": [{}], "products": [{}], ... },
        "mapping": [SemanticMapping],
        "scene": SceneContext
    }

    规则: 如果同一 target 表中有多行的 date 字段相同，则合并为一行（后者补充前者缺失的字段）
    """
    # 构建 raw_field → semantic_field 的查找表
    field_map = {}
    for m in mappings:
        sf = m.get("semantic_field", "unknown")
        if sf not in ("unknown", "ignore"):
            table_name = m.get("table", "")
            raw_field = m["raw_field"]
            field_map[f"{table_name}::{raw_field}"] = sf
            field_map[raw_field] = sf

    # 第一遍：收集所有待归类的行
    pending = {}  # target → list of dicts
    for table in dataset_bundle.get("tables", []):
        table_name = table.get("name", "unknown")
        rows = table.get("rows", [])

        for orig_row in rows:
            if not isinstance(orig_row, dict):
                continue

            new_row = {}
            for raw_field, value in orig_row.items():
                key = f"{table_name}::{raw_field}"
                sf = field_map.get(key) or field_map.get(raw_field)
                if sf and sf not in new_row:  # 同行不互相覆盖，保留第一个值
                    new_row[sf] = value

            if not new_row:
                continue

            if "revenue" in new_row or "order_count" in new_row or "gross_profit" in new_row:
                target = "sales"
            elif "product_name" in new_row or "product_id" in new_row:
                target = "products"
            elif "inventory_qty" in new_row or "inventory_amount" in new_row:
                target = "inventory"
            elif "employee_id" in new_row or "department" in new_row:
                target = "hr"
            else:
                target = "other"

            if target not in pending:
                pending[target] = []
            pending[target].append(new_row)

    # 第二遍：在每个 target 内，按 date 合并同行
    canonical = {}
    for target, rows in pending.items():
        date_index = {}  # date_value → merged_row

        for row in rows:
            date_val = row.get("date")
            if date_val is not None:
                if date_val in date_index:
                    # 合并：后者补充前者缺失的字段
                    existing = date_index[date_val]
                    for k, v in row.items():
                        if k not in existing or existing[k] is None:
                            existing[k] = v
                else:
                    date_index[date_val] = dict(row)
            else:
                # 无 date 的行不合并，追加到末尾
                merged_rows = []
                if target not in canonical:
                    canonical[target] = []
                # 先放有 date 的合并行，再放无 date 的行
                # 这里暂时直接 append

        # 有 date 的行按 key 排序后加入
        if date_index:
            if target not in canonical:
                canonical[target] = []
            for dk in sorted(date_index.keys(), key=str):
                canonical[target].append(date_index[dk])

        # 无 date 的行追加
        for row in rows:
            if row.get("date") is None:
                if target not in canonical:
                    canonical[target] = []
                canonical[target].append(row)

    return {
        "tables": canonical,
        "mapping": mappings,
        "scene": scene
    }
