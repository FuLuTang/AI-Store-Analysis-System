"""校验工具：校验 manifest / mapping / metrics 的结构完整性。"""

from packages.agents.models import AgentResult, Manifest, SemanticMapping


async def validate_manifest(manifest: Manifest) -> list[str]:
    """校验 manifest 中所有表的结构。返回警告列表（空 = 无问题）。"""
    ...


async def validate_mapping(mappings: list[SemanticMapping]) -> list[str]:
    """校验 SemanticMapping 的必填字段和置信度。"""
    ...


async def validate_result(result: AgentResult) -> list[str]:
    """校验最终 AgentResult 的完整性。"""
    ...
