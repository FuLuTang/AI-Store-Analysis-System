"""Pipeline 注册表 — 根据名称统一创建管线实例。"""

from typing import Callable, Optional

from .base import AgentPipeline
from .traditional_pipeline import TraditionalPipeline
from .pydantic_pipeline import PydanticPipeline
from .smol_pipeline import SmolPipeline

PIPELINE_MAP = {
    "traditional": TraditionalPipeline,
    "pydantic": PydanticPipeline,
    "smol": SmolPipeline,
}


def create_pipeline(name: str, get_llm_preset: Optional[Callable[[], dict]] = None, check_aborted: Optional[Callable[[], None]] = None) -> AgentPipeline:
    kwargs = {"get_llm_preset": get_llm_preset}
    if name == "traditional":
        kwargs["check_aborted"] = check_aborted
    cls = PIPELINE_MAP.get(name, TraditionalPipeline)
    return cls(**kwargs)
