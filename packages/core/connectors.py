import json
import time
from typing import Optional, Union

def format_xiaotang_push_payload(
    full_report: str, 
    summary_short: Union[dict, str], 
    company_code: str = "4366454557",
    plat: str = "11"
) -> dict:
    """
    将分析结果打包成 XiaoTangPush 协议格式。
    
    :param full_report: 深度报告 (Markdown 文本字符串)
    :param summary_short: 精简报告内容。可以是 dict，也可以是 JSON 字符串。
                         包含 health_status, overview_text, cards 等。
    :param company_code: 客户/企业代码，默认 4366454557
    :param plat: 平台标识，默认 11
    :return: 符合 XiaoTangPush 规范的字典对象
    """
    
    # 如果 summary_short 是字符串，尝试解析为 dict
    short_data = summary_short
    if isinstance(summary_short, str):
        try:
            short_data = json.loads(summary_short)
        except Exception:
            # 如果解析失败，保留原样（可能是纯文本）
            short_data = {"overview_text": summary_short}

    payload = {
        "method": "XiaoTangPush",
        "time": str(int(time.time())),
        "plat": plat,
        "company_code": company_code,
        "data": {
            "summary_short": short_data,
            "summary_long": full_report
        }
    }
    
    return payload

def save_xiaotang_push_to_file(payload: dict, file_path: str):
    """
    将打包好的推送数据保存到 JSON 文件
    """
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
