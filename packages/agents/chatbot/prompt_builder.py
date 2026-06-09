"""chatbot prompt builder."""

from pathlib import Path


ASSISTANT_PROMPT_PATH = Path(__file__).with_name("assistant.md")


def build_system_content() -> str:
    try:
        return ASSISTANT_PROMPT_PATH.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        return (
            "你是一个账号级 AI 客服 Agent。你可以读取 chatbot/ 与 service_docs/ 两个文件域，"
            "读类工具路径必须带域名前缀。不要编造事实；如果信息不足，直接说明不足。"
        )
