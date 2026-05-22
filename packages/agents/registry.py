"""Pipeline 注册表 — 根据名称统一创建管线实例。"""

from pathlib import Path
from typing import Callable, Optional

from .base import AgentPipeline
from .traditional_pipeline import TraditionalPipeline
from .pydantic_pipeline import PydanticPipeline
from .smol_pipeline import SmolPipeline
from .custom_pipeline import CustomPipeline

PIPELINE_MAP = {
    "traditional": TraditionalPipeline,
    "pydantic": PydanticPipeline,
    "smol": SmolPipeline,
    "custom": CustomPipeline,
}


def create_pipeline(name: str, llm_preset: Optional[dict] = None, check_aborted: Optional[Callable[[], None]] = None, workspace_dir: Optional[Path] = None, analysis_params: str = "") -> AgentPipeline:
    kwargs = {"llm_preset": llm_preset or {}, "check_aborted": check_aborted, "workspace_dir": workspace_dir, "analysis_params": analysis_params}
    cls = PIPELINE_MAP.get(name, TraditionalPipeline)
    return cls(**kwargs)
