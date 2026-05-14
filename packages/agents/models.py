"""共享 Pydantic 模型：两套 pipeline 共用同一套数据结构。"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, Field


# ============================================================
# 输入
# ============================================================

class RawTable(BaseModel):
    name: str
    rows: list[dict[str, Any]]


class DatasetBundle(BaseModel):
    source_type: Literal["json", "excel", "csv", "api", "database"] = "json"
    tables: list[RawTable]
    received_at: datetime = Field(default_factory=datetime.now)
    tenant_id: str | None = None


# ============================================================
# 工作区 metadata
# ============================================================

class ColumnMeta(BaseModel):
    name: str
    dtype: str
    null_count: int = 0
    sample_values: list[Any] = Field(default_factory=list)


class TableMeta(BaseModel):
    name: str
    path: str
    columns: list[ColumnMeta] = Field(default_factory=list)
    row_count: int = 0
    sample_rows: list[dict[str, Any]] = Field(default_factory=list)


class Manifest(BaseModel):
    report_id: str
    workspace_dir: str
    tables: list[TableMeta] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=datetime.now)


# ============================================================
# Pydantic 管线结构化输出
# ============================================================

class FlattenColumnPlan(BaseModel):
    source_field: str
    target_column: str
    extract_strategy: Literal["direct", "unnest", "json_extract"]


class FlattenTablePlan(BaseModel):
    source_table: str
    strategy: Literal["pass", "explode_array", "unfold_object", "pivot"]
    target_name: str
    columns: list[str] = Field(default_factory=list)
    notes: str = ""


class FlattenPlan(BaseModel):
    tables: list[FlattenTablePlan] = Field(default_factory=list)


class SemanticMapping(BaseModel):
    raw_field: str
    table: str
    semantic_field: str
    confidence: float = Field(ge=0.0, le=1.0)
    reason: str = ""
    need_confirm: bool = False


class MetricSql(BaseModel):
    metric_id: str
    name: str
    sql: str
    required_fields: list[str] = Field(default_factory=list)
    depends_on: list[str] = Field(default_factory=list)


class SqlPlan(BaseModel):
    metrics: list[MetricSql] = Field(default_factory=list)


# ============================================================
# 输出
# ============================================================

class MetricStatus(str, Enum):
    PASS = "pass"
    ATTENTION = "attention"
    WARNING = "warning"
    UNCOUNTABLE = "uncountable"


class MetricResult(BaseModel):
    metric_id: str
    name: str
    value: Any = None
    unit: str | None = None
    status: MetricStatus = MetricStatus.PASS
    reason: str = ""
    evidence: list[Any] = Field(default_factory=list)
    required_fields: list[str] = Field(default_factory=list)
    missing_fields: list[str] = Field(default_factory=list)
    confidence: float = 1.0


class SceneContext(BaseModel):
    industry: Literal["pharmacy", "restaurant", "retail", "hr", "generic"]
    business_model: Literal[
        "offline_driven", "o2o_driven", "delivery_heavy", "internal_department", "unknown"
    ]
    data_scope: list[str] = Field(default_factory=list)
    analysis_goal: str = ""
    confidence: float = 1.0


class ReportCard(BaseModel):
    title: str
    explanation: str = ""
    suggestion: str = ""
    evidence: str = ""
    color: Literal["green", "yellow", "pink", "red"] = "green"


class AgentResult(BaseModel):
    report_id: str
    tables: list[TableMeta] = Field(default_factory=list)
    mapping: list[SemanticMapping] = Field(default_factory=list)
    metrics: list[MetricResult] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    scene: SceneContext | None = None
    cards: list[ReportCard] = Field(default_factory=list)
    full_report: str = ""
    pipeline: str = ""
    elapsed_ms: float = 0.0
