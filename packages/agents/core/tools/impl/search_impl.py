"""底层实现：在 workspace 内搜索文本、正则模式或聊天历史记录"""
import json
import re
from pathlib import Path
from ...workspace import Workspace
from ...file_domains import join_domain_path
CHAT_HISTORY_FILENAME = "chat.jsonl"


def _is_binary(path: Path) -> bool:
    try:
        with path.open("rb") as fh:
            sample = fh.read(1024)
        if b"\0" in sample:
            return True
        sample.decode("utf-8")
        return False
    except Exception:
        return True


def _history_time(message: dict) -> str:
    return str(message.get("datetime") or message.get("time") or "").strip()


def _public_history_records_with_reasoning(messages: list[dict]) -> list[dict]:
    visible: list[dict] = []
    for message in messages:
        role = str(message.get("role", "")).strip().lower()
        if role == "system":
            name = str(message.get("name", "")).strip().lower()
            if name == "notice":
                notice = {
                    "role": "notice",
                    "content": str(message.get("content") or ""),
                }
                history_time = _history_time(message)
                if history_time:
                    notice["datetime"] = history_time
                visible.append(notice)
                continue
            if str(message.get("title") or "").strip():
                card = {
                    "role": "card",
                    "name": str(message.get("name") or ""),
                    "title": str(message.get("title") or ""),
                    "detail": str(message.get("detail") or ""),
                }
                options = message.get("options")
                if isinstance(options, list):
                    card["options"] = [str(item) for item in options]
                if "choice" in message:
                    card["choice"] = str(message.get("choice") or "")
                history_time = _history_time(message)
                if history_time:
                    card["datetime"] = history_time
                visible.append(card)
            continue
        if role in {"user", "assistant"}:
            content_val = str(message.get("content") or "")
            reasoning_val = str(message.get("reasoning_content") or "")
            tool_calls = message.get("tool_calls")
            if role == "assistant" and not content_val.strip() and not reasoning_val.strip() and not tool_calls:
                continue
            public_msg = {
                "role": role,
                "content": content_val,
            }
            if reasoning_val.strip():
                public_msg["reasoning_content"] = reasoning_val
            history_time = _history_time(message)
            if history_time:
                public_msg["datetime"] = history_time
            attachments = message.get("attachments")
            if isinstance(attachments, list):
                public_msg["attachments"] = attachments
            visible.append(public_msg)
    return visible


def _read_chat_history_lines(ws: Workspace) -> tuple[str, list[dict]]:
    candidates = []
    if ws.has_multi_read_roots:
        candidates.append("chatbot/" + CHAT_HISTORY_FILENAME)
        candidates.append(CHAT_HISTORY_FILENAME)
    else:
        candidates.append(CHAT_HISTORY_FILENAME)

    history_path = None
    for candidate in candidates:
        try:
            resolved = ws.resolve_read(candidate)
        except ValueError:
            continue
        if resolved.exists() and resolved.is_file():
            history_path = resolved
            break

    if history_path is None:
        raise FileNotFoundError("未找到 chat_history 记录文件")

    messages: list[dict] = []
    with history_path.open("r", encoding="utf-8") as fh:
        for line in fh:
            raw = line.strip()
            if not raw:
                continue
            try:
                record = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if isinstance(record, dict):
                messages.append(record)

    visible = _public_history_records_with_reasoning(messages)
    return str(history_path), visible


def _search_serialized_lines(lines: list[dict], pattern: str, regex: bool, max_matches: int, path_label: str) -> dict:
    rx = None
    pattern_lower = None
    if regex:
        rx = re.compile(pattern, re.IGNORECASE)
    else:
        pattern_lower = pattern.lower()

    results = []
    total_matches = 0
    hit_limit = False

    for line_idx, record in enumerate(lines, start=1):
        serialized = json.dumps(record, ensure_ascii=False)
        matched = False
        if regex and rx:
            matched = bool(rx.search(serialized))
        elif pattern_lower is not None:
            matched = pattern_lower in serialized.lower()
        if not matched:
            continue

        preview = serialized
        if len(preview) > 500:
            preview = preview[:500] + "..."
        results.append({
            "line": line_idx,
            "text": preview,
        })
        total_matches += 1
        if total_matches >= max_matches:
            hit_limit = True
            break

    output = {
        "pattern": pattern,
        "regex": regex,
        "domain": "chat_history",
        "path": path_label,
        "total_lines": len(lines),
        "total_matches_found": total_matches,
        "hit_limit": hit_limit,
        "results": [
            {
                "path": path_label,
                "total_matches": len(results),
                "matches": results,
            }
        ] if results else [],
    }
    if hit_limit:
        output["note"] = f"匹配数量已达到最大上限 {max_matches}，部分匹配未展示。"
    return output


def _collect_files_for_search(target: Path, rel_prefix: str) -> list[tuple[Path, str]]:
    files: list[tuple[Path, str]] = []
    if target.is_file():
        files.append((target, rel_prefix))
        return files

    if target.is_dir():
        for p in sorted(target.rglob("*")):
            if not p.is_file():
                continue
            rel_parts = p.relative_to(target).parts
            if any(part.startswith(".") for part in rel_parts):
                continue
            rel_path = str(p.relative_to(target)).replace("\\", "/")
            label = rel_prefix.rstrip("/")
            if label:
                label = f"{label}/{rel_path}"
            else:
                label = rel_path
            files.append((p, label))
    return files


def search_files_impl(
    ws: Workspace,
    pattern: str,
    domain: str = "all",
    path: str = None,
    regex: bool = False,
    max_matches: int = 50,
) -> str:
    """在工作区内检索匹配的文本，返回匹配行和行号的 JSON。"""
    max_matches = max(1, min(int(max_matches or 50), 200))
    domain = str(domain or "all").strip()

    if domain == "chat_history":
        try:
            history_path, visible_records = _read_chat_history_lines(ws)
        except FileNotFoundError as e:
            return json.dumps({"error": str(e), "domain": domain}, ensure_ascii=False)
        except Exception as e:
            return json.dumps({"error": f"读取聊天历史失败: {e}", "domain": domain}, ensure_ascii=False)

        try:
            output = _search_serialized_lines(visible_records, pattern, regex, max_matches, history_path)
        except re.error as e:
            return json.dumps({"error": f"无效的正则表达式: {e}"}, ensure_ascii=False)
        return json.dumps(output, ensure_ascii=False)

    if domain not in {"", "all"} and path is None:
        path = domain

    if path:
        try:
            target_path = ws.resolve_read(path)
        except ValueError as e:
            return json.dumps({"error": str(e), "path": path}, ensure_ascii=False)
        if not target_path.exists():
            return json.dumps({"error": f"路径不存在: {path}"}, ensure_ascii=False)
        files = _collect_files_for_search(target_path, path)
    else:
        files = []
        if ws.has_multi_read_roots:
            for domain, root in ws.read_roots.items():
                for p in sorted(root.rglob("*")):
                    if not p.is_file():
                        continue
                    rel_parts = p.relative_to(root).parts
                    if any(part.startswith(".") for part in rel_parts):
                        continue
                    files.append((domain, p, root))
        else:
            for p in sorted(ws.read_root.rglob("*")):
                if not p.is_file():
                    continue
                rel_parts = p.relative_to(ws.read_root).parts
                # 排除隐藏文件/目录（如 .git, .gemini 等）
                if any(part.startswith(".") for part in rel_parts):
                    continue
                files.append(p)

    results = []
    global_matches_count = 0
    hit_limit = False

    # 编译正则或转换子串模式
    rx = None
    pattern_lower = None
    if regex:
        try:
            rx = re.compile(pattern, re.IGNORECASE)
        except re.error as e:
            return json.dumps({"error": f"无效的正则表达式: {e}"}, ensure_ascii=False)
    else:
        pattern_lower = pattern.lower()

    for item in files:
        if hit_limit:
            break
        
        if isinstance(item, tuple):
            if len(item) == 3:
                domain, p, root = item
                rel_path = join_domain_path(domain, str(p.relative_to(root)).replace("\\", "/"))
            else:
                p, rel_path = item
        else:
            p = item
            rel_path = str(p.relative_to(ws.read_root))

        # 排除二进制文件
        if _is_binary(p):
            continue

        file_matches = []

        try:
            with p.open("r", encoding="utf-8", errors="replace") as fh:
                for line_idx, line in enumerate(fh):
                    if global_matches_count >= max_matches:
                        hit_limit = True
                        break
                    
                    matched = False
                    if regex and rx:
                        if rx.search(line):
                            matched = True
                    else:
                        if pattern_lower and pattern_lower in line.lower():
                            matched = True
                            
                    if matched:
                        # 截断单行长度，防止极长的单行文本挤爆上下文
                        text = line.rstrip("\r\n")
                        if len(text) > 500:
                            text = text[:500] + "..."
                        
                        file_matches.append({
                            "line": line_idx + 1,
                            "text": text
                        })
                        global_matches_count += 1
        except Exception:
            # 容忍单个文件读取错误，继续搜索其他文件
            continue

        if file_matches:
            results.append({
                "path": rel_path,
                "total_matches": len(file_matches),
                "matches": file_matches
            })

    output = {
        "pattern": pattern,
        "regex": regex,
        "domain": domain,
        "total_files_searched": len(files),
        "total_matches_found": global_matches_count,
        "hit_limit": hit_limit,
        "results": results
    }
    
    if hit_limit:
        output["note"] = f"匹配数量已达到最大上限 {max_matches}，部分匹配未展示。"
        
    return json.dumps(output, ensure_ascii=False)
