"""Python 工具：在 workspace 沙箱中执行 Python 脚本。"""

from packages.agents.workspace import Workspace


async def run_python_script(
    ws: Workspace,
    script: str,
    timeout: int = 30,
    memory_mb: int = 512,
) -> str:
    """在 workspace 沙箱中执行 Python 脚本，限制路径/超时/内存。
    
    返回 stdout（或错误信息）。
    """
    ...
