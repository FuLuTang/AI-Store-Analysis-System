"""底层纯函数：workspace 文件读写与列表侦察"""
import json
import shutil
from typing import Callable, Optional
from pathlib import Path
from ...workspace import Workspace
from ...file_domains import DEFAULT_FILE_DOMAINS, join_domain_path, split_domain_path


DEFAULT_READ_LINES = 800
MAX_READ_LINES = 2000
MAX_READ_BYTES = 64 * 1024  # 64KB
FORBIDDEN_READ_BASENAMES = {"plan.json"}


def _normalize_workspace_write_path(path: str) -> str:
    rel = str(path or "").strip().replace("\\", "/")
    while rel.startswith("./"):
        rel = rel[2:]
    changed = True
    while changed:
        changed = False
        for prefix in ("chatbot/workspace/",):
            if rel.startswith(prefix):
                rel = rel[len(prefix):]
                changed = True
    return rel


def _format_size(size: int) -> str:
    units = ["B", "KB", "MB", "GB"]
    value = float(size)
    for unit in units:
        if value < 1024 or unit == units[-1]:
            return f"{value:.1f}{unit}" if unit != "B" else f"{size}B"
        value /= 1024


def _classify_file(path: Path) -> tuple[str, str]:
    ext = path.suffix.lower()
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
    elif ext == ".jsonl":
        kind = "jsonl"
    elif ext in {".sqlite", ".db"}:
        kind = "sqlite"
    elif ext in {".zip", ".tar", ".gz", ".rar"}:
        kind = "archive"
    elif ext == ".py":
        kind = "python"
    else:
        kind = "other"

    if kind in {"excel", "csv", "pdf", "word", "json", "jsonl", "markdown", "sqlite", "archive"}:
        rec_tool = "read_document_structure"
    elif kind == "text":
        rec_tool = "read_file" if path.stat().st_size < 100 * 1024 else "search"
    elif kind == "python":
        rec_tool = "read_file"
    else:
        rec_tool = "run_python"
    return kind, rec_tool


def _is_hidden_relative(path: Path, root: Path) -> bool:
    try:
        rel_parts = path.relative_to(root).parts
    except ValueError:
        return True
    return any(part.startswith(".") for part in rel_parts)


def _domain_entry_path(domain: str, domain_root: Path, path: Path) -> str:
    rel = str(path.relative_to(domain_root)).replace("\\", "/")
    return join_domain_path(domain, rel)


def _list_domain_entries(domain: str, domain_root: Path, target: Path) -> dict:
    entries = []
    totals = {
        "files": 0,
        "directories": 0,
        "empty_directories": 0,
        "bytes": 0,
    }

    candidates = [target]
    if target.is_dir():
        candidates.extend(sorted(target.rglob("*")))

    for p in candidates:
        if p == target and target.is_dir():
            continue
        if _is_hidden_relative(p, domain_root):
            continue

        path_label = _domain_entry_path(domain, domain_root, p)
        if p.is_dir():
            visible_children = [
                child for child in p.iterdir()
                if not child.name.startswith(".")
            ]
            is_empty = len(visible_children) == 0
            totals["directories"] += 1
            if is_empty:
                totals["empty_directories"] += 1
            entries.append({
                "path": path_label,
                "absolute_path": str(p.resolve()),
                "name": p.name,
                "type": "directory",
                "children_count": len(visible_children),
                "is_empty": is_empty,
            })
            continue

        if not p.is_file():
            continue

        size = p.stat().st_size
        kind, rec_tool = _classify_file(p)
        totals["files"] += 1
        totals["bytes"] += size
        entries.append({
            "path": path_label,
            "absolute_path": str(p.resolve()),
            "name": p.name,
            "type": "file",
            "size": size,
            "size_human": _format_size(size),
            "ext": p.suffix.lower(),
            "kind": kind,
            "recommended_tool": rec_tool,
        })

    totals["bytes_human"] = _format_size(totals["bytes"])
    return {
        "domain": domain,
        "root_path": _domain_entry_path(domain, domain_root, target),
        "root_absolute_path": str(target.resolve()),
        "entries": entries,
        "totals": totals,
    }


def _is_binary_sample(sample: bytes) -> bool:
    if b"\0" in sample:
        return True
    try:
        sample.decode("utf-8")
        return False
    except UnicodeDecodeError:
        return True


def read_file_impl(
    ws: Workspace,
    path: str,
    offset: int = 0,
    limit: int = DEFAULT_READ_LINES,
    head: int = None,
    tail: int = None,
    emit_log: Optional[Callable[[str, str | dict], None]] = None,
) -> str:
    """读取文件内容，支持分页(offset/limit)以及首尾快捷读取(head/tail)。"""
    try:
        p = ws.resolve_read(path)
    except ValueError as e:
        return json.dumps({"error": str(e), "path": path}, ensure_ascii=False)
    if p.name in FORBIDDEN_READ_BASENAMES:
        return json.dumps({
            "error": f"不允许使用 read_file 读取受限文件: {path}",
            "path": path,
        }, ensure_ascii=False)
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

    result = json.dumps(payload, ensure_ascii=False)
    if emit_log:
        emit_log("custom_agent", {"level": "info", "message": f"✅ read_file 调用成功: {path}"})
    return result


def write_file_impl(
    ws: Workspace,
    path: str,
    content: str,
    mode: str = "overwrite",
    emit_log: Optional[Callable[[str, str | dict], None]] = None,
) -> str:
    original_path = str(path or "")
    path = _normalize_workspace_write_path(path)
    p = ws.resolve_write(path)
    if p.name in FORBIDDEN_READ_BASENAMES:
        return json.dumps({"error": f"不允许使用 write_file 修改受限文件: {path}"}, ensure_ascii=False)
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
    result = json.dumps({
        "ok": True,
        "path": path,
        "original_path": original_path if original_path != path else "",
        "mode": mode,
        "bytes_written": len(content.encode("utf-8")),
        "size": size,
        "size_human": _format_size(size),
    }, ensure_ascii=False)
    if emit_log:
        emit_log("custom_agent", {"level": "info", "message": f"✅ write_file 调用成功: {path}"})
    return result


def replace_text_impl(
    ws: Workspace,
    path: str,
    old_text: str,
    new_text: str,
    emit_log: Optional[Callable[[str, str | dict], None]] = None,
) -> str:
    p = ws.resolve_write(path)
    if p.name in FORBIDDEN_READ_BASENAMES:
        return json.dumps({"error": f"不允许使用 replace_text 修改受限文件: {path}"}, ensure_ascii=False)
    old_scripts_dir = ws.scripts_dir / "old_session_scripts"
    try:
        if old_scripts_dir.resolve() in p.resolve().parents or p.resolve() == old_scripts_dir.resolve():
            return json.dumps({"error": f"不允许修改或写入 old_session_scripts 目录中的文件: {path}"}, ensure_ascii=False)
    except Exception:
        pass
    if not p.exists():
        return json.dumps({"error": f"文件不存在: {path}"}, ensure_ascii=False)
    if not p.is_file():
        return json.dumps({"error": f"不是文件: {path}"}, ensure_ascii=False)
    if old_text == "":
        return json.dumps({"error": "old_text 不能为空", "path": path}, ensure_ascii=False)

    try:
        content = p.read_text(encoding="utf-8")
    except Exception as e:
        return json.dumps({"error": f"文件读取失败: {e}", "path": path}, ensure_ascii=False)

    match_count = content.count(old_text)
    if match_count == 0:
        return json.dumps({
            "error": "未找到要替换的文本，要求唯一匹配但当前匹配数为 0",
            "path": path,
            "match_count": 0,
        }, ensure_ascii=False)
    if match_count >= 2:
        return json.dumps({
            "error": f"找到多处匹配，要求唯一匹配但当前匹配数为 {match_count}",
            "path": path,
            "match_count": match_count,
        }, ensure_ascii=False)

    updated = content.replace(old_text, new_text, 1)
    tmp = p.with_name(f".{p.name}.tmp")
    tmp.write_text(updated, encoding="utf-8")
    tmp.replace(p)

    result = json.dumps({
        "ok": True,
        "path": path,
        "match_count": 1,
        "bytes_written": len(updated.encode("utf-8")),
        "size": p.stat().st_size,
        "size_human": _format_size(p.stat().st_size),
    }, ensure_ascii=False)
    if emit_log:
        emit_log("custom_agent", {"level": "info", "message": f"✅ replace_text 调用成功: {path}"})
    return result


def copy_file_impl(
    ws: Workspace,
    source_path: str,
    destination_path: str,
    emit_log: Optional[Callable[[str, str | dict], None]] = None,
) -> str:
    if Path(destination_path).name in FORBIDDEN_READ_BASENAMES:
        return json.dumps({"error": f"不允许将文件复制到受限文件名: {destination_path}"}, ensure_ascii=False)
    try:
        src = ws.resolve_read(source_path)
    except ValueError as e:
        return json.dumps({"error": str(e), "source_path": source_path}, ensure_ascii=False)
    dst = ws.resolve_write(destination_path)
    old_scripts_dir = ws.scripts_dir / "old_session_scripts"
    try:
        if old_scripts_dir.resolve() in dst.resolve().parents or dst.resolve() == old_scripts_dir.resolve():
            return json.dumps({"error": f"不允许修改或写入 old_session_scripts 目录中的文件: {destination_path}"}, ensure_ascii=False)
    except Exception:
        pass

    if not src.exists():
        return json.dumps({"error": f"源文件不存在: {source_path}"}, ensure_ascii=False)
    if not src.is_file():
        return json.dumps({"error": f"源路径不是文件: {source_path}"}, ensure_ascii=False)

    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(src, dst)

    size = dst.stat().st_size
    result = json.dumps({
        "ok": True,
        "source_path": source_path,
        "destination_path": destination_path,
        "size": size,
        "size_human": _format_size(size),
    }, ensure_ascii=False)
    if emit_log:
        emit_log("custom_agent", {
            "level": "info",
            "message": f"✅ copy_file 调用成功: {source_path} -> {destination_path}"
        })
    return result


def list_files_impl(
    ws: Workspace,
    subdir: str = "",
    emit_log: Optional[Callable[[str, str | dict], None]] = None,
) -> dict:
    try:
        read_roots = ws.read_roots
        if subdir:
            if ws.has_multi_read_roots:
                domain, _ = split_domain_path(subdir, allowed_domains=read_roots.keys())
            else:
                domain = DEFAULT_FILE_DOMAINS[0]
            target = ws.resolve_read(subdir)
            domains = [(domain, read_roots[domain].resolve(), target)]
        else:
            domains = [
                (domain, root.resolve(), root.resolve())
                for domain, root in read_roots.items()
            ]
        payload = {
            "domains": [
                _list_domain_entries(domain, domain_root, target)
                for domain, domain_root, target in domains
            ]
        }
    except ValueError as e:
        return {"error": str(e), "path": subdir or ""}
    if emit_log:
        emit_log("custom_agent", {"level": "info", "message": f"✅ list_files 调用成功: {subdir or '.'}"})
    return payload
