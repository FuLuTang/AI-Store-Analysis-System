"""文件工具：工作区中的文件读写/列表。"""

from pathlib import Path
from packages.agents.workspace import Workspace


async def read_workspace_file(ws: Workspace, filename: str) -> str:
    """读取 workspace 中的文件内容。"""
    ...


async def write_workspace_file(ws: Workspace, filename: str, content: str) -> Path:
    """将内容写入 workspace 文件。"""
    ...


async def list_workspace_files(ws: Workspace) -> list[str]:
    """列出 workspace 中所有文件。"""
    ...
