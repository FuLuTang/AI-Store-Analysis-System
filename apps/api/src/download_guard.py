import os
import yaml
import fnmatch
from pathlib import Path
from threading import Lock

_cache_lock = Lock()
_patterns_cache = {}

def get_yaml_path(root_dir: Path) -> Path:
    return root_dir / ".no_download.yaml"

def load_restricted_patterns(root_dir: Path) -> list[str]:
    yaml_path = get_yaml_path(root_dir)
    if not yaml_path.exists():
        return []
    
    try:
        mtime = yaml_path.stat().st_mtime
    except Exception:
        mtime = 0.0
        
    with _cache_lock:
        cached = _patterns_cache.get(yaml_path)
        if cached and cached.get("mtime") == mtime:
            return cached["patterns"]
            
        patterns = []
        try:
            content = yaml_path.read_text(encoding="utf-8")
            data = yaml.safe_load(content)
            if isinstance(data, dict) and isinstance(data.get("restricted"), list):
                patterns = [str(p).strip() for p in data["restricted"] if p]
        except Exception:
            pass
            
        _patterns_cache[yaml_path] = {
            "mtime": mtime,
            "patterns": patterns
        }
        return patterns

def _write_restricted_patterns(root_dir: Path, patterns: list[str]) -> None:
    yaml_path = get_yaml_path(root_dir)
    unique_patterns = sorted(list(set(patterns)))
    data = {"restricted": unique_patterns}
    
    yaml_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        content = yaml.safe_dump(data, default_flow_style=False, allow_unicode=True)
        yaml_path.write_text(content, encoding="utf-8")
        
        mtime = yaml_path.stat().st_mtime
        with _cache_lock:
            _patterns_cache[yaml_path] = {
                "mtime": mtime,
                "patterns": unique_patterns
            }
    except Exception as e:
        raise e

def match_parts(path_parts: list[str], pattern_parts: list[str]) -> bool:
    if not pattern_parts:
        return not path_parts
        
    head = pattern_parts[0]
    if head == '**':
        for i in range(len(path_parts) + 1):
            if match_parts(path_parts[i:], pattern_parts[1:]):
                return True
        return False
    else:
        if not path_parts:
            return False
        if fnmatch.fnmatchcase(path_parts[0], head):
            return match_parts(path_parts[1:], pattern_parts[1:])
        return False

def match_glob(path_str: str, pattern_str: str) -> bool:
    path_str = path_str.replace('\\', '/').strip('/')
    pattern_str = pattern_str.replace('\\', '/').strip('/')
    
    if path_str == pattern_str:
        return True
        
    pattern_parts = pattern_str.split('/')
    path_parts = path_str.split('/')
    
    return match_parts(path_parts, pattern_parts)

def is_downloadable(relative_path: str, root_dir: Path) -> bool:
    patterns = load_restricted_patterns(root_dir)
    for pattern in patterns:
        if match_glob(relative_path, pattern):
            return False
    return True

def add_restricted_pattern(relative_path: str, root_dir: Path) -> None:
    patterns = load_restricted_patterns(root_dir)
    normalized_path = relative_path.replace('\\', '/').strip('/')
    if normalized_path not in patterns:
        patterns.append(normalized_path)
        _write_restricted_patterns(root_dir, patterns)

def remove_restricted_pattern(relative_path: str, root_dir: Path) -> None:
    patterns = load_restricted_patterns(root_dir)
    normalized_path = relative_path.replace('\\', '/').strip('/')
    
    new_patterns = [p for p in patterns if p != normalized_path]
    if len(new_patterns) != len(patterns):
        _write_restricted_patterns(root_dir, new_patterns)

def get_restricted_status(relative_path: str, root_dir: Path) -> bool:
    patterns = load_restricted_patterns(root_dir)
    normalized_path = relative_path.replace('\\', '/').strip('/')
    return normalized_path in patterns
