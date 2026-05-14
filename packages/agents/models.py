"""
models.py — 共享 Pydantic 数据模型
"""
from datetime import datetime
from typing import Any, List
from pydantic import BaseModel, Field


class RawTable(BaseModel):
    name: str
    columns: List[str] = Field(default_factory=list)
    rows: List[dict] = Field(default_factory=list)


class DatasetBundle(BaseModel):
    source_type: str = "json"
    tables: List[RawTable] = Field(default_factory=list)
    received_at: str = Field(default_factory=lambda: datetime.now().isoformat())


class MetricResult(BaseModel):
    metric_id: str = ""
    name: str = ""
    value: Any = None
    status: str = "uncountable"
    reason: str = ""
    evidence: str = ""


class AgentResult(BaseModel):
    scene: dict = Field(default_factory=dict)
    mappings: List[dict] = Field(default_factory=list)
    metrics: List[MetricResult] = Field(default_factory=list)
    raw_output: str = ""
