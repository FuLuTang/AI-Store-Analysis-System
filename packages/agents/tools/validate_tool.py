"""AgentResult 校验（委托到 impl）"""
from .impl.validate_impl import validate_result_impl


def validate_result(raw: dict) -> dict:
    return validate_result_impl(raw)
