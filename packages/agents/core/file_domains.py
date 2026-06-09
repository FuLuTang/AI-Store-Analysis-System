"""固定文件域与域路径解析。"""

from pathlib import PurePosixPath
from typing import Iterable


CHATBOT_DOMAIN = "chatbot"
SERVICE_DOCS_DOMAIN = "service_docs"
DEFAULT_FILE_DOMAINS = (CHATBOT_DOMAIN, SERVICE_DOCS_DOMAIN)


def normalize_domain_path(value: str | None) -> str:
    text = str(value or "").replace("\\", "/").strip()
    while text.startswith("/"):
        text = text[1:]
    while text.endswith("/") and text != "/":
        text = text[:-1]
    if text in {".", ""}:
        return ""
    return text


def split_domain_path(
    value: str | None,
    *,
    allowed_domains: Iterable[str] = DEFAULT_FILE_DOMAINS,
) -> tuple[str, str]:
    text = normalize_domain_path(value)
    if not text:
        raise ValueError("路径缺少域名")

    parts = PurePosixPath(text).parts
    if not parts:
        raise ValueError("路径缺少域名")

    domain = parts[0]
    allowed = tuple(allowed_domains)
    if domain not in allowed:
        raise ValueError(f"不支持的文件域: {domain}，仅支持: {', '.join(allowed)}")

    rel = "/".join(parts[1:])
    return domain, rel


def join_domain_path(domain: str, rel: str | None = "") -> str:
    rel_norm = normalize_domain_path(rel)
    return f"{domain}/{rel_norm}" if rel_norm else f"{domain}/"
