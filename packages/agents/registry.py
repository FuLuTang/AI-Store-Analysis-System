"""Pipeline 注册表 — 当前仅保留 custom 管线。"""

from pathlib import Path
from typing import Callable, Optional

from .core.base import AgentPipeline
from .diagnosis.pipeline import CustomPipeline

def create_pipeline(name: str, llm_preset: Optional[dict] = None, check_aborted: Optional[Callable[[], None]] = None, workspace_dir: Optional[Path] = None, analysis_params: str = "") -> AgentPipeline:
    kwargs = {"llm_preset": llm_preset or {}, "check_aborted": check_aborted, "workspace_dir": workspace_dir, "analysis_params": analysis_params}
    return CustomPipeline(**kwargs)
