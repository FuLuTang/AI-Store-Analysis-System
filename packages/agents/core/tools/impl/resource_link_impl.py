import json
from typing import Callable, Optional
from ...workspace import Workspace
from ...file_domains import split_domain_path
from apps.api.src.download_guard import is_downloadable

def get_resource_link_impl(
    ws: Workspace,
    path: str,
    emit_log: Optional[Callable[[str, str | dict], None]] = None,
) -> str:
    """
    获取资源链接工具实现。
    根据路径域前缀解析出实际的 root 目录和相对路径，校验是否存在、是否限制下载。
    返回 JSON 结果，包含 status 和 markdown 格式或 URL 链接。
    """
    try:
        domain, inner_path = split_domain_path(path)
        resolved_path = ws.resolve_read(path)
    except Exception as e:
        return json.dumps({"status": "failed", "message": f"路径解析失败: {str(e)}"}, ensure_ascii=False)
        
    if not resolved_path.exists():
        return json.dumps({"status": "failed", "message": f"文件不存在: {path}"}, ensure_ascii=False)
        
    if not resolved_path.is_file():
        return json.dumps({"status": "failed", "message": f"不是一个有效文件: {path}"}, ensure_ascii=False)
        
    if domain in ws.read_roots:
        root_dir = ws.read_roots[domain]
    else:
        root_dir = resolved_path.parent
        
    if not is_downloadable(inner_path, root_dir):
        return json.dumps({"status": "no_permission", "message": f"无权限下载此文件: {path}"}, ensure_ascii=False)
        
    url = f"/api/chatbot/resource/{domain}/{inner_path}?token={{{{AUTH_TOKEN}}}}"
    
    ext = resolved_path.suffix.lower()
    is_image = ext in {".png", ".jpg", ".jpeg", ".gif", ".svg"}
    
    if is_image:
        markdown = f"![{resolved_path.name}]({url})"
    else:
        markdown = f"[{resolved_path.name}]({url})"
        
    result_data = {
        "status": "success",
        "url": url,
        "markdown": markdown,
        "is_image": is_image,
        "message": "获取成功"
    }
    
    if emit_log:
        emit_log("custom_agent", {"level": "info", "message": f"✅ get_resource_link 调用成功: {path}"})
        
    return json.dumps(result_data, ensure_ascii=False)
