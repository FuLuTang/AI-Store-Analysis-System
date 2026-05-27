"""底层纯函数：workspace 文件读写"""
import json

from ...workspace import Workspace


DEFAULT_READ_LINES = 2000
MAX_READ_LINES = 2000
MAX_READ_BYTES = 500 * 1024


def _format_size(size: int) -> str:
    units = ["B", "KB", "MB", "GB"]
    value = float(size)
    for unit in units:
        if value < 1024 or unit == units[-1]:
            return f"{value:.1f}{unit}" if unit != "B" else f"{size}B"
        value /= 1024


def _is_binary_sample(sample: bytes) -> bool:
    if b"\0" in sample:
        return True
    try:
        sample.decode("utf-8")
        return False
    except UnicodeDecodeError:
        return True


def read_file_impl(ws: Workspace, path: str, offset: int = 0, limit: int = DEFAULT_READ_LINES) -> str:
    p = ws.resolve(path)
    if not p.exists():
        return json.dumps({"error": f"文件不存在: {path}"}, ensure_ascii=False)
    if not p.is_file():
        return json.dumps({"error": f"不是文件: {path}"}, ensure_ascii=False)

    offset = max(0, int(offset or 0))
    limit = max(1, min(int(limit or DEFAULT_READ_LINES), MAX_READ_LINES))
    size = p.stat().st_size

    with p.open("rb") as fh:
        sample = fh.read(4096)
    if _is_binary_sample(sample):
        return json.dumps({
            "error": "疑似二进制文件，read_file 不返回原始内容。请改用 read_document 或 run_python 处理。",
            "path": path,
            "size": size,
            "size_human": _format_size(size),
        }, ensure_ascii=False)

    selected: list[str] = []
    total_lines = 0
    bytes_used = 0
    hit_byte_cap = False
    end_line = offset

    with p.open("r", encoding="utf-8", errors="replace") as fh:
        for line_no, line in enumerate(fh):
            total_lines = line_no + 1
            if line_no < offset:
                continue
            if len(selected) >= limit:
                continue
            encoded_len = len(line.encode("utf-8", errors="replace"))
            if bytes_used + encoded_len > MAX_READ_BYTES:
                remaining = max(0, MAX_READ_BYTES - bytes_used)
                if remaining > 0:
                    selected.append(
                        line.encode("utf-8", errors="replace")[:remaining].decode("utf-8", errors="ignore")
                    )
                    end_line = line_no + 1
                hit_byte_cap = True
                continue
            selected.append(line)
            bytes_used += encoded_len
            end_line = line_no + 1

    has_more = end_line < total_lines
    payload = {
        "path": path,
        "size": size,
        "size_human": _format_size(size),
        "offset": offset,
        "limit": limit,
        "line_start": offset,
        "line_end": end_line,
        "total_lines": total_lines,
        "has_more": has_more,
        "next_offset": end_line if has_more else None,
        "truncated": has_more or hit_byte_cap,
        "content": "".join(selected),
    }
    if hit_byte_cap:
        payload["note"] = "本次读取达到输出上限，请用更小 limit 或 next_offset 继续分页读取。"
    elif has_more:
        payload["note"] = "文件未读完，请用 next_offset 继续分页读取。"
    return json.dumps(payload, ensure_ascii=False)


def write_file_impl(ws: Workspace, path: str, content: str, mode: str = "overwrite") -> str:
    p = ws.resolve(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    mode = (mode or "overwrite").strip().lower()
    if mode not in {"overwrite", "append"}:
        return json.dumps({"error": "mode 只能是 overwrite 或 append", "path": path}, ensure_ascii=False)

    if mode == "append":
        with p.open("a", encoding="utf-8") as fh:
            fh.write(content)
    else:
        tmp = p.with_name(f".{p.name}.tmp")
        tmp.write_text(content, encoding="utf-8")
        tmp.replace(p)

    size = p.stat().st_size
    return json.dumps({
        "ok": True,
        "path": path,
        "mode": mode,
        "bytes_written": len(content.encode("utf-8")),
        "size": size,
        "size_human": _format_size(size),
    }, ensure_ascii=False)


def list_files_impl(ws: Workspace, subdir: str = "") -> list[dict]:
    target = ws.resolve(subdir) if subdir else ws.dir
    files = []
    for p in sorted(target.rglob("*")):
        if not p.is_file():
            continue
        size = p.stat().st_size
        files.append({
            "path": str(p.relative_to(ws.dir)),
            "name": p.name,
            "size": size,
            "size_human": _format_size(size),
        })
    return files
