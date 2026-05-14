"""
validate_tool.py — 校验 Agent 输出（两边共用）
"""
from ..models import AgentResult


def validate_result(raw: dict) -> dict:
    """用 Pydantic 校验输出，兜底缺省值"""
    try:
        result = AgentResult.model_validate(raw)
        return {"valid": True, "result": result.model_dump()}
    except Exception as e:
        return {"valid": False, "errors": str(e)[:500]}
