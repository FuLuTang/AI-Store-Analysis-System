"""
base.py — AgentPipeline 抽象接口
"""
from abc import ABC, abstractmethod
from .models import DatasetBundle, AgentResult


class AgentPipeline(ABC):
    @abstractmethod
    async def run(self, bundle: DatasetBundle) -> AgentResult:
        ...
