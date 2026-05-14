"""Agent 路由：POST /api/analyze?pipeline=agent → pydantic / smol 切换。

单一路由，通过 pipeline 参数选择 PydanticPipeline 或 SmolPipeline。
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from packages.agents.models import AgentResult, DatasetBundle
from packages.agents.pydantic_pipeline import PydanticPipeline
from packages.agents.smol_pipeline import SmolPipeline

agent_router = APIRouter(prefix="/api", tags=["agent"])


@agent_router.post("/analyze")
async def agent_analyze(
    bundle: DatasetBundle,
    engine: str = Query(default="pydantic", pattern="^(pydantic|smol)$"),
) -> AgentResult:
    """Agent 管线统一入口。engine=pydantic 或 engine=smol。"""
    if engine == "pydantic":
        pipeline = PydanticPipeline()
    elif engine == "smol":
        pipeline = SmolPipeline()
    else:
        raise HTTPException(status_code=400, detail=f"unknown engine: {engine}")

    return await pipeline.run(bundle)
