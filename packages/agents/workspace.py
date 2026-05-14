"""
workspace.py — 每次任务独立的沙箱工作区

- 创建临时目录
- 写入输入文件
- 读取输出文件
- 清理资源
"""
import json
import shutil
import tempfile
from pathlib import Path
from typing import Optional


class AgentWorkspace:
    def __init__(self, label: str = ""):
        prefix = f"agent_{label}_" if label else "agent_"
        self._dir = Path(tempfile.mkdtemp(prefix=prefix))
        self._input_dir = self._dir / "input"
        self._output_dir = self._dir / "output"
        self._input_dir.mkdir()
        self._output_dir.mkdir()
        self._context_dir = self._dir / "context"
        self._context_dir.mkdir()

    @property
    def dir(self) -> Path:
        return self._dir

    @property
    def input_dir(self) -> Path:
        return self._input_dir

    @property
    def output_dir(self) -> Path:
        return self._output_dir

    @property
    def context_dir(self) -> Path:
        return self._context_dir

    def write_input(self, name: str, data: bytes) -> Path:
        p = self._input_dir / name
        p.write_bytes(data)
        return p

    def write_input_json(self, name: str, obj: dict) -> Path:
        p = self._input_dir / name
        p.write_text(json.dumps(obj, ensure_ascii=False), encoding="utf-8")
        return p

    def write_context(self, name: str, text: str) -> Path:
        p = self._context_dir / name
        p.write_text(text, encoding="utf-8")
        return p

    def resolve(self, rel: str) -> Path:
        """解析相对路径到 workspace 内（限制不能跳出）"""
        resolved = (self._dir / rel).resolve()
        if not str(resolved).startswith(str(self._dir.resolve())):
            raise ValueError(f"路径越界: {rel}")
        return resolved

    def list_inputs(self) -> list[str]:
        return [p.name for p in self._input_dir.iterdir() if p.is_file()]

    def list_outputs(self) -> list[str]:
        return [p.name for p in self._output_dir.iterdir() if p.is_file()]

    def read_output(self, name: str) -> Optional[bytes]:
        p = self._output_dir / name
        return p.read_bytes() if p.exists() else None

    def read_output_json(self, name: str) -> Optional[dict]:
        raw = self.read_output(name)
        return json.loads(raw.decode()) if raw else None

    def cleanup(self):
        shutil.rmtree(self._dir, ignore_errors=True)
