"""底层实现：在 workspace 内搜索文本或正则模式"""
import json
import re
from pathlib import Path
from ...workspace import Workspace


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


def search_files_impl(ws: Workspace, pattern: str, path: str = None, regex: bool = False, max_matches: int = 50) -> str:
    """在工作区内检索匹配的文本，返回匹配行和行号的 JSON。"""
    max_matches = max(1, min(int(max_matches or 50), 200))
    
    if path:
        target_path = ws.resolve(path)
        if not target_path.exists():
            return json.dumps({"error": f"路径不存在: {path}"}, ensure_ascii=False)
        if not target_path.is_file():
            return json.dumps({"error": f"不是文件: {path}"}, ensure_ascii=False)
        files = [target_path]
    else:
        files = []
        for p in sorted(ws.dir.rglob("*")):
            if not p.is_file():
                continue
            rel_parts = p.relative_to(ws.dir).parts
            # 排除隐藏文件/目录（如 .git, .gemini 等）
            if any(part.startswith(".") for part in rel_parts):
                continue
            files.append(p)

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

    results = []
    global_matches_count = 0
    hit_limit = False

    for p in files:
        if hit_limit:
            break
        
        # 排除二进制文件
        if _is_binary(p):
            continue

        rel_path = str(p.relative_to(ws.dir))
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
        "total_files_searched": len(files),
        "total_matches_found": global_matches_count,
        "hit_limit": hit_limit,
        "results": results
    }
    
    if hit_limit:
        output["note"] = f"匹配数量已达到最大上限 {max_matches}，部分匹配未展示。"
        
    return json.dumps(output, ensure_ascii=False)
