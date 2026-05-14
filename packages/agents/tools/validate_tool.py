"""校验工具：校验 manifest / mapping / metrics 的结构完整性。"""

from ..models import AgentResult, Manifest, SemanticMapping


def validate_result(raw: dict) -> dict:
    """用 Pydantic 校验输出，兜底缺省值"""
    try:
        result = AgentResult.model_validate(raw)
        return {"valid": True, "result": result.model_dump()}
    except Exception as e:
        return {"valid": False, "errors": str(e)[:500]}


async def validate_manifest(manifest: Manifest) -> list[str]:
    """校验 manifest 中所有表的结构。返回警告列表（空 = 无问题）。"""
    ...


async def validate_mapping(mappings: list[SemanticMapping]) -> list[str]:
    """校验 SemanticMapping 的必填字段和置信度。"""
    ...
