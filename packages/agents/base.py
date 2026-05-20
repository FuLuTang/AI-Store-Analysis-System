"""AgentPipeline 抽象基类：两边管线都实现同一个接口。"""

from abc import ABC, abstractmethod
from typing import Callable, Optional

from .models import DatasetBundle, AgentResult


StatusCallback = Callable[[str, str], None]
"""(node_id, status)"""

LogCallback = Callable[[str, str], None]
"""(node_id, message)"""


class AgentPipeline(ABC):
    name: str = "base"

    def __init__(self):
        self._on_status: Optional[StatusCallback] = None
        self._on_log: Optional[LogCallback] = None

    def set_event_callbacks(self, on_status: Optional[StatusCallback] = None, on_log: Optional[LogCallback] = None):
        self._on_status = on_status
        self._on_log = on_log

    def _emit_status(self, node_id: str, status: str):
        if self._on_status:
            self._on_status(node_id, status)

    def _emit_log(self, node_id: str, message: str):
        if self._on_log:
            self._on_log(node_id, message)

    @abstractmethod
    async def run(self, bundle: DatasetBundle) -> AgentResult:
        ...
