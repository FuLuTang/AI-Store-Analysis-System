"""底层纯函数：workspace 文件读写"""
from ...workspace import Workspace


def read_file_impl(ws: Workspace, path: str) -> str:
    return ws.resolve(path).read_text(encoding="utf-8")


def write_file_impl(ws: Workspace, path: str, content: str) -> str:
    p = ws.resolve(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(content, encoding="utf-8")
    return f"写入成功: {path}"


def list_files_impl(ws: Workspace, subdir: str = "") -> list[str]:
    target = ws.resolve(subdir) if subdir else ws.dir
    return sorted([str(p.relative_to(ws.dir)) for p in target.rglob("*") if p.is_file()])
