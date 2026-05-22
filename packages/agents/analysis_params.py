"""analysis_params — 用户自定义分析参数：wash / validate"""

import json

def wash_analysis_params(raw) -> str:
    """将 UI-rich JSON 数组或向后兼容的字符串洗为干净 KV 文本供 LLM 使用。"""
    if isinstance(raw, str):
        s = raw.strip()
        # 向后兼容：旧格式是纯 JSON 对象 {"粒度":"月"} → 转为 KV
        if s.startswith('{') and s.endswith('}'):
            try:
                obj = json.loads(s)
                if isinstance(obj, dict):
                    return "\n".join(
                        f"{k}: {'true' if v is True else 'false' if v is False else v}"
                        for k, v in obj.items()
                    )
            except json.JSONDecodeError:
                pass
        # 纯文本 → 原样返回
        return raw

    if isinstance(raw, list):
        lines = []
        for item in raw:
            if isinstance(item, dict) and "key" in item:
                k = item["key"]
                v = item.get("value", "")
                if isinstance(v, bool):
                    v = "true" if v else "false"
                lines.append(f"{k}: {v}")
            else:
                lines.append(str(item))
        return "\n".join(lines)

    # 兜底
    return str(raw)


def validate_analysis_params(raw):
    """校验前端传入的原始 JSON（可能已解析为 list/dict），失败时 fallback 保存。
    返回原始对象（list/dict）而非 JSON 字符串，避免被下游 json.dumps 双重编码。
    """
    if isinstance(raw, (list, dict)):
        return raw
    if not raw or not raw.strip():
        return ""
    s = raw.strip()
    try:
        data = json.loads(s)
    except json.JSONDecodeError:
        return s

    if isinstance(data, list):
        cleaned = []
        for item in data:
            if isinstance(item, dict):
                entry = {"key": item.get("key", ""), "value": item.get("value", "")}
                if "options" in item:
                    entry["options"] = item["options"]
                if "type" in item:
                    entry["type"] = item["type"]
                if "min" in item:
                    entry["min"] = item["min"]
                if "max" in item:
                    entry["max"] = item["max"]
                cleaned.append(entry)
            else:
                cleaned.append(item)
        return json.dumps(cleaned, ensure_ascii=False)

    # 对象/字符串 → 原样存
    return s
