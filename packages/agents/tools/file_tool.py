"""
file_tool.py — workspace 文件读写工具（两边共用）
"""
from ..workspace import AgentWorkspace


def get_workspace() -> AgentWorkspace:
    """从上下文获取当前 workspace（由编排器注入）"""
    raise NotImplementedError("workspace 由编排器注入到工具上下文")


def read_workspace_file(path: str) -> str:
    """读 workspace 内文件内容"""
    ws = get_workspace()
    return ws.resolve(path).read_text(encoding="utf-8")


def write_workspace_file(path: str, content: str) -> str:
    """写文件到 workspace（限制在 output/ 目录）"""
    ws = get_workspace()
    p = ws.resolve(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(content, encoding="utf-8")
    return f"写入成功: {path}"


def list_workspace_files(subdir: str = "") -> list[str]:
    """列出 workspace 内文件"""
    ws = get_workspace()
    target = ws.resolve(subdir) if subdir else ws.dir
    return sorted([str(p.relative_to(ws.dir)) for p in target.rglob("*") if p.is_file()])
