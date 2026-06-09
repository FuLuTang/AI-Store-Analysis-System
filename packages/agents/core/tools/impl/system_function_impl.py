"""底层实现：系统服务功能的列出、说明查看与执行"""

import os
import sys
import ast
import json
import importlib.util
from pathlib import Path
from typing import Union

def list_system_functions_impl() -> str:
    """列出 system_service_functions 目录下的所有系统功能，树形结构展示且不显示 .py 后缀"""
    # 查找 system_service_functions 目录，其位于 packages/agents 下
    sys_functions_dir = Path(__file__).resolve().parents[3] / "system_service_functions"
    if not sys_functions_dir.exists() or not sys_functions_dir.is_dir():
        return "系统暂未注册任何服务功能。"

    # 构建树形结构字典
    tree = {}
    for p in sys_functions_dir.rglob("*.py"):
        if p.name == "__init__.py":
            continue
        try:
            rel_parts = p.relative_to(sys_functions_dir).parts
            if rel_parts:
                parts_list = list(rel_parts)
                # 剔除后缀
                parts_list[-1] = p.stem
                
                curr = tree
                for part in parts_list:
                    if part not in curr:
                        curr[part] = {}
                    curr = curr[part]
        except Exception:
            pass

    # 递归渲染为树形文本
    lines = []
    def _format_tree(node, indent=""):
        keys = sorted(node.keys())
        for idx, key in enumerate(keys):
            is_last = (idx == len(keys) - 1)
            prefix = "└── " if is_last else "├── "
            lines.append(f"{indent}{prefix}{key}")
            
            child_indent = indent + ("    " if is_last else "│   ")
            _format_tree(node[key], child_indent)

    _format_tree(tree)
    if not lines:
        return "系统暂未注册任何服务功能。"
        
    return "系统可用的服务功能列表树如下：\n" + "\n".join(lines)


def view_system_function_doc_impl(path_str: str) -> str:
    """查看指定功能路径的头部说明文档"""
    if not path_str:
        return "错误：未指定功能路径。"
        
    sys_functions_dir = Path(__file__).resolve().parents[3] / "system_service_functions"
    
    # 安全检查，防止路径穿越
    target_path = (sys_functions_dir / f"{path_str}.py").resolve()
    try:
        target_path.relative_to(sys_functions_dir.resolve())
    except ValueError:
        return f"错误：非法的服务功能路径 '{path_str}'。"

    if not target_path.exists() or not target_path.is_file():
        return f"错误：找不到对应的服务功能 '{path_str}'。"

    # 读取并解析文件头部 docstring
    try:
        content = target_path.read_text(encoding="utf-8")
        tree = ast.parse(content)
        doc = ast.get_docstring(tree)
        if doc:
            return f"系统服务功能 [{path_str}] 的说明说明如下：\n\n{doc}"
    except Exception:
        pass

    # 兜底正则匹配
    import re
    try:
        content = target_path.read_text(encoding="utf-8")
        match = re.search(r'"""(.*?)"""', content, re.DOTALL)
        if match:
            return f"系统服务功能 [{path_str}] 的说明说明如下：\n\n{match.group(1).strip()}"
    except Exception:
        pass

    return f"服务功能 [{path_str}] 当前暂无详细使用文档。"


def execute_system_function_impl(ws, path_str: str, params: Union[dict, str], llm_preset: dict) -> str:
    """执行指定的系统服务功能"""
    if not path_str:
        return "错误：未指定功能路径。"
        
    sys_functions_dir = Path(__file__).resolve().parents[3] / "system_service_functions"
    
    # 安全检查，防止路径穿越
    target_path = (sys_functions_dir / f"{path_str}.py").resolve()
    try:
        target_path.relative_to(sys_functions_dir.resolve())
    except ValueError:
        return f"错误：非法的服务功能路径 '{path_str}'。"

    if not target_path.exists() or not target_path.is_file():
        return f"错误：找不到对应的服务功能 '{path_str}'。"

    # 解析参数为 dict
    parsed_params = params
    if isinstance(params, str):
        try:
            parsed_params = json.loads(params)
        except Exception as e:
            return f"错误：'参数'字段必须是合法的 JSON 格式。解析失败: {str(e)}"

    if not isinstance(parsed_params, dict):
        return "错误：'参数'字段解析后必须是键值对/字典对象。"

    # 动态导入功能脚本
    try:
        module_name = f"packages.agents.system_service_functions.{path_str.replace('/', '.')}"
        spec = importlib.util.spec_from_file_location(module_name, str(target_path))
        if spec is None or spec.loader is None:
            return f"错误：无法加载功能模块 '{path_str}'。"
            
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        
        if not hasattr(module, "run"):
            return f"错误：功能服务脚本中没有定义 run(ws, params, llm_preset) 入口函数。"
            
        # 运行 run 函数
        print(f"动态调用系统功能: {path_str} ...")
        result = module.run(ws, parsed_params, llm_preset or {})
        return str(result)
        
    except Exception as e:
        import traceback
        err_trace = traceback.format_exc()
        return f"错误：运行系统功能失败，原因: {str(e)}\n\n详细错误堆栈:\n{err_trace}"
