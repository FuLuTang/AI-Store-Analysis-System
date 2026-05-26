"""底层纯函数：校验 Agent 输出"""
from ...models import AgentResult


def validate_result_impl(raw: dict) -> dict:
    try:
        result = AgentResult.model_validate(raw)
        return {"valid": True, "result": result.model_dump()}
    except Exception as e:
        return {"valid": False, "errors": str(e)[:500]}
