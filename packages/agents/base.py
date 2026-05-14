"""AgentPipeline 抽象基类：两边管线都实现同一个接口。"""

from __future__ import annotations

from abc import ABC, abstractmethod

from packages.agents.models import AgentResult, DatasetBundle


class AgentPipeline(ABC):
    """两边都实现同一个接口 run(bundle) -> AgentResult。"""

    name: str = "base"

    @abstractmethod
    async def run(self, bundle: DatasetBundle) -> AgentResult:
        ...
