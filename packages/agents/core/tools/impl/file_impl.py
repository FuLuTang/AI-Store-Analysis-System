"""底层纯函数：workspace 文件读写与列表侦察"""
import json
from pathlib import Path
from ...workspace import Workspace


DEFAULT_READ_LINES = 2000
MAX_READ_LINES = 2000
MAX_READ_BYTES = 500 * 1024  # 500KB


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


def read_file_impl(ws: Workspace, path: str, offset: int = 0, limit: int = DEFAULT_READ_LINES, head: int = None, tail: int = None) -> str:
    """读取文件内容，支持分页(offset/limit)以及首尾快捷读取(head/tail)。"""
    p = ws.resolve(path)
    if not p.exists():
        return json.dumps({"error": f"文件不存在: {path}"}, ensure_ascii=False)
    if not p.is_file():
        return json.dumps({"error": f"不是文件: {path}"}, ensure_ascii=False)

    size = p.stat().st_size

    # 检测二进制文件
    with p.open("rb") as fh:
        sample = fh.read(4096)
    if _is_binary_sample(sample):
        return json.dumps({
            "error": "疑似二进制文件，read_file 不返回原始内容。请改用 read_document_structure 或 run_python 处理。",
            "path": path,
            "size": size,
            "size_human": _format_size(size),
        }, ensure_ascii=False)

    # 读取所有文本行
    try:
        with p.open("r", encoding="utf-8", errors="replace") as fh:
            lines = fh.readlines()
    except Exception as e:
        return json.dumps({"error": f"文件读取失败: {e}"}, ensure_ascii=False)

    total_lines = len(lines)

    # 确定读取范围与行号区间
    line_start = 0
    line_end = total_lines
    selected_ranges = []  # 保存要输出的行 (1-based line number, line_text)
    is_head_tail_mode = False
    skipped_count = 0

    if head is not None or tail is not None:
        is_head_tail_mode = True
        h_val = max(0, int(head or 0)) if head is not None else 0
        t_val = max(0, int(tail or 0)) if tail is not None else 0

        if head is not None and tail is not None:
            if h_val + t_val >= total_lines:
                # 范围覆盖了全部行，直接全量读取
                for idx, line in enumerate(lines):
                    selected_ranges.append((idx + 1, line))
            else:
                # 截断读取首尾
                for idx in range(h_val):
                    selected_ranges.append((idx + 1, lines[idx]))
                skipped_count = total_lines - h_val - t_val
                for idx in range(total_lines - t_val, total_lines):
                    selected_ranges.append((idx + 1, lines[idx]))
        elif head is not None:
            limit_val = min(h_val, total_lines)
            for idx in range(limit_val):
                selected_ranges.append((idx + 1, lines[idx]))
            skipped_count = total_lines - limit_val
        else:  # tail is not None
            start_idx = max(0, total_lines - t_val)
            for idx in range(start_idx, total_lines):
                selected_ranges.append((idx + 1, lines[idx]))
            skipped_count = start_idx
    else:
        # 传统的分页偏移读取模式
        offset = max(0, int(offset or 0))
        limit = max(1, min(int(limit or DEFAULT_READ_LINES), MAX_READ_LINES))
        line_start = offset
        line_end = min(offset + limit, total_lines)
        for idx in range(line_start, line_end):
            selected_ranges.append((idx + 1, lines[idx]))

    # 按字节限制拼接输出内容
    content_parts = []
    bytes_used = 0
    hit_byte_cap = False
    actual_line_end = line_start

    for idx, (line_no, line_text) in enumerate(selected_ranges):
        # 检查是否为合并跳过的间隔点
        if is_head_tail_mode and head is not None and tail is not None and idx == head and skipped_count > 0:
            sep = f"\n... [已省略中间 {skipped_count} 行] ...\n"
            content_parts.append(sep)
            bytes_used += len(sep.encode("utf-8"))

        encoded_len = len(line_text.encode("utf-8"))
        if bytes_used + encoded_len > MAX_READ_BYTES:
            remaining = max(0, MAX_READ_BYTES - bytes_used)
            if remaining > 0:
                content_parts.append(
                    line_text.encode("utf-8")[:remaining].decode("utf-8", errors="ignore")
                )
            hit_byte_cap = True
            break
        
        content_parts.append(line_text)
        bytes_used += encoded_len
        actual_line_end = line_no

    has_more = False
    if not is_head_tail_mode:
        has_more = line_end < total_lines

    payload = {
        "path": path,
        "size": size,
        "size_human": _format_size(size),
        "offset": offset if not is_head_tail_mode else None,
        "limit": limit if not is_head_tail_mode else None,
        "line_start": line_start + 1 if not is_head_tail_mode else (selected_ranges[0][0] if selected_ranges else 1),
        "line_end": actual_line_end,
        "total_lines": total_lines,
        "has_more": has_more,
        "next_offset": line_end if has_more else None,
        "truncated": has_more or hit_byte_cap,
        "content": "".join(content_parts),
    }

    if hit_byte_cap:
        payload["note"] = "本次读取达到 500KB 字节数上限，请用更小 limit 或是用 search_files 检索。"
    elif has_more:
        payload["note"] = "文件未读完，请用 next_offset 继续分页读取。"
    elif is_head_tail_mode and skipped_count > 0 and (head is None or tail is None):
        payload["note"] = f"已通过 head/tail 截断，共省略了 {skipped_count} 行。"
        payload["truncated"] = True

    return json.dumps(payload, ensure_ascii=False)


def write_file_impl(ws: Workspace, path: str, content: str, mode: str = "overwrite") -> str:
    p = ws.resolve(path)
    old_scripts_dir = ws.scripts_dir / "old_session_scripts"
    try:
        if old_scripts_dir.resolve() in p.resolve().parents or p.resolve() == old_scripts_dir.resolve():
            return json.dumps({"error": f"不允许修改或写入 old_session_scripts 目录中的文件: {path}"}, ensure_ascii=False)
    except Exception:
        pass
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
    target = ws.resolve(subdir) if subdir else ws.dir.resolve()
    ws_dir_abs = ws.dir.resolve()
    files = []
    for p in sorted(target.rglob("*")):
        if not p.is_file():
            continue
        rel_parts = p.relative_to(ws_dir_abs).parts
        if any(part.startswith(".") for part in rel_parts):
            continue
        
        size = p.stat().st_size
        ext = p.suffix.lower()
        
        # 判定 kind
        if ext in {".xlsx", ".xls"}:
            kind = "excel"
        elif ext == ".csv":
            kind = "csv"
        elif ext == ".pdf":
            kind = "pdf"
        elif ext in {".docx", ".doc"}:
            kind = "word"
        elif ext in {".txt", ".log"}:
            kind = "text"
        elif ext == ".md":
            kind = "markdown"
        elif ext == ".json":
            kind = "json"
        elif ext in {".sqlite", ".db"}:
            kind = "sqlite"
        elif ext in {".zip", ".tar", ".gz", ".rar"}:
            kind = "archive"
        elif ext == ".py":
            kind = "python"
        else:
            kind = "other"
            
        # 判定 recommended_tool
        if kind in {"excel", "csv", "pdf", "word", "json", "markdown", "sqlite", "archive"}:
            rec_tool = "read_document_structure"
        elif kind == "text":
            if size < 100 * 1024:  # 小于 100KB
                rec_tool = "read_file"
            else:
                rec_tool = "search_files"
        elif kind == "python":
            rec_tool = "read_file"
        else:
            rec_tool = "run_python"

        files.append({
            "path": str(p.relative_to(ws_dir_abs)),
            "name": p.name,
            "size": size,
            "size_human": _format_size(size),
            "ext": ext,
            "kind": kind,
            "recommended_tool": rec_tool
        })
    return files
