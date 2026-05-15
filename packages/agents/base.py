"""AgentPipeline 抽象基类：两边管线都实现同一个接口。"""

from abc import ABC, abstractmethod

from .models import DatasetBundle, AgentResult


class AgentPipeline(ABC):
    name: str = "base"

    @abstractmethod
    async def run(self, bundle: DatasetBundle) -> AgentResult:
        ...
    @abstractmethod
    async def run(self, bundle: DatasetBundle) -> AgentResult:
        ...
