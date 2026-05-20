"""AgentPipeline 抽象基类：三条管线共用一个接口。"""

from abc import ABC, abstractmethod
from typing import Callable, Optional

from .models import DatasetBundle, AgentResult


StatusCallback = Callable[[str, str], None]
LogCallback = Callable[[str, str], None]
ProgressCallback = Callable[[str, int, int], None]
TallyCallback = Callable[[str, dict], None]


class AgentPipeline(ABC):
    name: str = "base"

    def __init__(self):
        self._on_status: Optional[StatusCallback] = None
        self._on_log: Optional[LogCallback] = None
        self._on_progress: Optional[ProgressCallback] = None
        self._on_tally: Optional[TallyCallback] = None

    def set_event_callbacks(self,
                            on_status: Optional[StatusCallback] = None,
                            on_log: Optional[LogCallback] = None,
                            on_progress: Optional[ProgressCallback] = None,
                            on_tally: Optional[TallyCallback] = None):
        self._on_status = on_status
        self._on_log = on_log
        self._on_progress = on_progress
        self._on_tally = on_tally

    def _emit_status(self, node_id: str, status: str):
        if self._on_status:
            self._on_status(node_id, status)

    def _emit_log(self, node_id: str, message: str):
        if self._on_log:
            self._on_log(node_id, message)

    def _emit_progress(self, node_id: str, current: int, total: int):
        if self._on_progress:
            self._on_progress(node_id, current, total)

    def _emit_tally(self, node_id: str, tally: dict):
        if self._on_tally:
            self._on_tally(node_id, tally)

    @abstractmethod
    async def run(self, bundle: DatasetBundle) -> AgentResult:
        ...
