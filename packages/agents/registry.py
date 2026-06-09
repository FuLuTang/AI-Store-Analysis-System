"""Pipeline 注册表 — 注册 custom 和 price_recommendation 管线。"""

from pathlib import Path
from typing import Callable, Optional

from .core.base import AgentPipeline
from .diagnosis.pipeline import CustomPipeline
from .price_recommendation.pipeline import PricePipeline

def create_pipeline(
    name: str,
    llm_preset: Optional[dict] = None,
    check_aborted: Optional[Callable[[], None]] = None,
    workspace_dir: Optional[Path] = None,
    analysis_params: str = "",
    workspace_options: Optional[dict] = None,
) -> AgentPipeline:
    kwargs = {
        "llm_preset": llm_preset or {},
        "check_aborted": check_aborted,
        "workspace_dir": workspace_dir,
        "analysis_params": analysis_params,
        "workspace_options": workspace_options,
    }
    return CustomPipeline(**kwargs)
