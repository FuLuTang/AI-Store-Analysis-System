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
    product_name: str = "",
    candidate_count: int = 2,
) -> AgentPipeline:
    kwargs = {
        "llm_preset": llm_preset or {},
        "check_aborted": check_aborted,
        "workspace_dir": workspace_dir,
    }
    if name == "price_recommendation":
        kwargs.update({
            "product_name": product_name,
            "candidate_count": candidate_count,
        })
        return PricePipeline(**kwargs)
    
    # 默认返回 CustomPipeline (diagnosis)
    kwargs["analysis_params"] = analysis_params
    return CustomPipeline(**kwargs)
