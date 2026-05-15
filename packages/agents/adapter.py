"""Agent 管线适配器：桥接 input_adapter（core）产物和 Agent workspace 体系。

将 core/input_adapter.parse_uploaded_files() 返回的 dict 型 DatasetBundle
转为 Pydantic DatasetBundle，并落地为 workspace parquet 文件。
"""

from __future__ import annotations

from packages.agents.models import DatasetBundle, RawTable


def adapt_to_dataset_bundle(raw: dict) -> DatasetBundle:
    """将 input_adapter dict 输出转为 Pydantic DatasetBundle。"""
    tables = [
        RawTable(name=t.get("name", "unnamed"), rows=t.get("rows", []))
        for t in raw.get("tables", [])
    ]
    return DatasetBundle(
        source_type=raw.get("source_type", "json"),
        tables=tables,
        received_at=raw.get("received_at"),
        tenant_id=raw.get("tenant_id"),
    )
