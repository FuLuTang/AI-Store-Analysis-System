"""端到端测试：mock LLM，验证 staged pipeline 全链路结构。"""
import asyncio
import sys
import json
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from pydantic_ai.models.function import FunctionModel, AgentInfo
from pydantic_ai.messages import ModelMessage, ModelResponse, TextPart

from packages.agents.models import (
    DatasetBundle, RawTable,
    FlattenPlan, FlattenTablePlan,
    SemanticMapping, SqlPlan, MetricSql,
    AgentResult, MetricResult, MetricStatus, PhaseResult,
)
from packages.agents.pydantic_pipeline import PydanticPipeline

# ── Mock 数据 ──
test_rows = [
    {"日期": "2026-05-01", "零售金额": 2467.5, "来客数": 101, "毛利": 850.25},
    {"日期": "2026-05-02", "零售金额": 3214.8, "来客数": 98, "毛利": 1100.4},
]
bundle = DatasetBundle(
    source_type="json",
    tables=[RawTable(name="销售日报", rows=test_rows)],
)

# ── Mock LLM ──
def mock_flatten_fn(messages, info):
    plan = FlattenPlan(tables=[
        FlattenTablePlan(source_table="销售日报", strategy="pass", target_name="销售日报_flat",
                         columns=["日期","零售金额","来客数","毛利"])
    ])
    return ModelResponse(parts=[TextPart(plan.model_dump_json())])

def mock_mapping_fn(messages, info):
    mappings = [
        SemanticMapping(raw_field="零售金额", table="销售日报", semantic_field="revenue", confidence=0.95, reason="金额字段"),
        SemanticMapping(raw_field="来客数", table="销售日报", semantic_field="customer_count", confidence=0.92, reason="客流"),
        SemanticMapping(raw_field="毛利", table="销售日报", semantic_field="gross_profit", confidence=0.90, reason="利润"),
        SemanticMapping(raw_field="日期", table="销售日报", semantic_field="date", confidence=0.98, reason="日期"),
    ]
    return ModelResponse(parts=[TextPart(json.dumps([m.model_dump() for m in mappings]))])

def mock_sql_fn(messages, info):
    plan = SqlPlan(metrics=[
        MetricSql(metric_id="avg_revenue", name="日均零售金额",
                  sql='SELECT AVG("零售金额") FROM "销售日报"', required_fields=["revenue"]),
        MetricSql(metric_id="gross_margin_pct", name="毛利率",
                  sql='SELECT SUM("毛利")*100.0/NULLIF(SUM("零售金额"),0) FROM "销售日报"',
                  required_fields=["revenue","gross_profit"]),
    ])
    return ModelResponse(parts=[TextPart(plan.model_dump_json())])

# ── 测试子类，注入 mock model ──
class MockPydanticPipeline(PydanticPipeline):
    def _build_phase_agent(self, ws, output_type, phase):
        from pydantic_ai import Agent
        from pydantic_ai.settings import ModelSettings

        mock_map = {
            "flatten": FunctionModel(mock_flatten_fn),
            "mapping": FunctionModel(mock_mapping_fn),
            "sql":     FunctionModel(mock_sql_fn),
        }
        model = mock_map[phase]

        agent = Agent(
            model,
            deps_type=type(ws),
            output_type=output_type,
            model_settings=ModelSettings(temperature=0.0),
            tool_retries=3,
            output_retries=3,
        )
        return agent

    async def _run_mapping_phase(self, ws, flat_metas):
        # mock: 直接返回预置映射，跳过 LLM（list[T] output_type 在 FunctionModel 不支持 tool call）
        mappings = [
            SemanticMapping(raw_field="零售金额", table="销售日报", semantic_field="revenue", confidence=0.95, reason="金额字段"),
            SemanticMapping(raw_field="来客数", table="销售日报", semantic_field="customer_count", confidence=0.92, reason="客流"),
            SemanticMapping(raw_field="毛利", table="销售日报", semantic_field="gross_profit", confidence=0.90, reason="利润"),
            SemanticMapping(raw_field="日期", table="销售日报", semantic_field="date", confidence=0.98, reason="日期字段"),
        ]
        return PhaseResult(phase="mapping", status="success", attempts=1, output=mappings)

# ── Run ──
async def main():
    print("=" * 60)
    print("  PydanticPipeline E2E (Mock LLM)")
    print("=" * 60)

    pipeline = MockPydanticPipeline()
    result = await pipeline.run(bundle)

    print(f"\n── AgentResult ──")
    print(f"  report_id: {result.report_id}")
    print(f"  pipeline:  {result.pipeline}")
    print(f"  elapsed:   {result.elapsed_ms:.0f}ms")
    print(f"  tables:    {len(result.tables)}")
    print(f"  mapping:   {len(result.mapping)}")
    for mp in result.mapping:
        print(f"    → {mp.raw_field} → {mp.semantic_field} (conf={mp.confidence})")
    print(f"  metrics:   {len(result.metrics)}")
    for mt in result.metrics:
        print(f"    📊 {mt.metric_id}: {mt.name} = {mt.value} [{mt.status.value}]")
    print(f"  warnings:  {len(result.warnings)}")
    for w in result.warnings:
        print(f"    ⚠ {w}")

    errors = []
    if not result.tables: errors.append("tables 为空")
    if len(result.mapping) < 3: errors.append(f"mapping 过少 ({len(result.mapping)})")
    if not result.metrics: errors.append("metrics 为空")
    has_pass = any(m.status == MetricStatus.PASS for m in result.metrics)
    if not has_pass: errors.append("无 PASS 指标")

    print()
    if errors:
        print(f"  ❌ {len(errors)} 问题:")
        for e in errors: print(f"     - {e}")
    else:
        print(f"  ✅ 全链路通过")

    return len(errors)

exit(asyncio.run(main()))
