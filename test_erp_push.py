import os
import sys
import json
import time
import re
import ssl
import urllib.request
import urllib.parse
from pathlib import Path

# 尝试导入 requests
try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

# 终端彩色输出
class Color:
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    BOLD = '\033[1m'
    END = '\033[0m'

def log_info(msg):
    print(f"{Color.BLUE}[INFO]{Color.END} {msg}")

def log_success(msg):
    print(f"{Color.GREEN}[SUCCESS]{Color.END} {msg}")

def log_warn(msg):
    print(f"{Color.YELLOW}[WARN]{Color.END} {msg}")

def log_err(msg):
    print(f"{Color.RED}[ERROR]{Color.END} {msg}")

# 定义测试用的基本 Payload
summary_short_mock = {
    "health_status": "正常",
    "overview_text": "这是一次来自本地一键测试脚本的手工 Push 测试",
    "cards": [
        {
            "title": "门店销售表现",
            "explanation": "销售额同比上升 5%",
            "suggestion": "继续保持当前促销节奏",
            "evidence": "销售数据",
            "color": "green"
        }
    ]
}
full_report_mock = "# 店铺经营 AI 手动测试报告\n\n测试连接是否通畅。\n\n- 状态: OK\n- 触发源: 本地一键测试脚本"

base_payload = {
    "method": "XiaoTangPush",
    "time": str(int(time.time())),
    "plat": "11",
    "company_code": "4366454557",
    "data": {
        "summary_short": summary_short_mock,
        "summary_long": full_report_mock
    }
}

# 1. 抓取并解析 Apipost 文档
def fetch_and_parse_apipost():
    print(f"\n{Color.BOLD}=== 1. 尝试抓取并解析 Apipost 文档 ==={Color.END}")
    url = "https://console-docs.apipost.cn/preview/6704c0602dc41c35/3b9f8d25b1ae985f?target_id=0b119a23-012a-42f7-a289-61cdbc630ba8"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    
    html = None
    
    # 尝试使用 requests
    if HAS_REQUESTS:
        try:
            log_info("正在使用 requests 库抓取 Apipost 文档...")
            r = requests.get(url, headers=headers, verify=False, timeout=10)
            if r.status_code == 200:
                html = r.text
        except Exception as e:
            log_warn(f"requests 抓取失败: {e}")
            
    # 如果没有 requests 或 requests 失败，尝试 urllib
    if not html:
        try:
            log_info("正在使用 urllib 抓取 Apipost 文档...")
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, context=ctx, timeout=10) as response:
                html = response.read().decode('utf-8')
        except Exception as e:
            log_err(f"urllib 抓取失败: {e}")
            
    if not html:
        log_err("⚠️ 无法在本地抓取文档。您可以尝试在 Chrome 浏览器中直接打开以下链接查看文档：")
        print(f"👉 {url}\n")
        return
        
    log_success(f"成功获取 HTML 文档，长度: {len(html)}")
    
    # 保存 HTML 备份以便于排查
    with open("apipost_doc_local.html", "w", encoding="utf-8") as f:
        f.write(html)
    log_info("已将文档 HTML 备份保存至本地文件 `apipost_doc_local.html`")

    # 尝试在 HTML 中寻找接口的关键 JSON 数据
    log_info("正在从 HTML 中提取接口配置结构...")
    
    # 查找可能的 window.__PRELOADED_STATE__ 或类似数据
    # Apipost 分享页通常把项目的所有数据都放在 window.defaultValue 或者是 preloaded state 里
    json_blocks = re.findall(r'window\.(?:defaultValue|defaultValueObject|preloadedState|shareData)\s*=\s*(\{.*?\});', html, re.DOTALL)
    if not json_blocks:
        # 尝试匹配大括号 JSON 格式的 script 内容
        json_blocks = re.findall(r'<script[^>]*>\s*window\.[a-zA-Z0-9_]+\s*=\s*(\{.*?\})\s*</script>', html, re.DOTALL)

    found_info = False
    for idx, block in enumerate(json_blocks):
        try:
            # 清理和压缩，确保是合规的 json
            data = json.loads(block)
            # 在 JSON 中搜索 "XiaoTangPush" 看看是否有关联项
            data_str = json.dumps(data, ensure_ascii=False)
            if "XiaoTangPush" in data_str:
                found_info = True
                log_success(f"在 window 数据块 [{idx}] 中找到了 XiaoTangPush 相关接口定义！")
                
                # 保存为本地 JSON 文件，方便用户和我们查看
                out_path = "apipost_api_detail.json"
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                log_success(f"已将接口明细 JSON 保存至本地文件 `{out_path}`")
                
                # 简单解析出一些接口基本信息
                # 这里做个简单的关键字正则搜索
                print(f"{Color.CYAN}--- 文档中提取的疑似定义 ---{Color.END}")
                # 查找 parameters, headers, request 等键
                for key in ["request", "response", "parameter", "query", "body", "headers"]:
                    if key in data_str:
                        print(f"- 发现含有关键字 '{key}' 的定义，详细结构已保存在 `{out_path}`")
                break
        except Exception:
            continue

    if not found_info:
        log_warn("未能在 HTML 脚本块中自动提取出 XiaoTangPush 相关的 JSON 数据结构。")
        log_info("请直接在浏览器中打开文档链接，并将网页上的参数说明/格式贴给助手。")


# 2. 模拟不同变体发送测试
def send_post(name, payload_data=None, json_data=None, params_data=None):
    url = "https://api.shangboshop.com/Other/OtherErpApi/index"
    log_info(f"正在测试: {name}")
    
    # 构造请求
    if HAS_REQUESTS:
        try:
            r = requests.post(url, data=payload_data, json=json_data, params=params_data, verify=False, timeout=10)
            print(f"  {Color.BOLD}HTTP 状态码:{Color.END} {r.status_code}")
            try:
                res_json = r.json()
                color = Color.GREEN if res_json.get("retstatus") == 1 else Color.RED
                print(f"  {Color.BOLD}返回 JSON:{Color.END} {color}{json.dumps(res_json, ensure_ascii=False)}{Color.END}")
            except Exception:
                print(f"  {Color.BOLD}返回 Text:{Color.END} {r.text[:300]}")
        except Exception as e:
            log_err(f"请求发送失败: {e}")
    else:
        # 使用 urllib 模拟 POST
        try:
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            
            headers = {}
            data_bytes = None
            
            # 处理 URL params
            target_url = url
            if params_data:
                target_url += "?" + urllib.parse.urlencode(params_data)
                
            if json_data:
                headers['Content-Type'] = 'application/json; charset=utf-8'
                data_bytes = json.dumps(json_data, ensure_ascii=False).encode('utf-8')
            elif payload_data:
                headers['Content-Type'] = 'application/x-www-form-urlencoded; charset=utf-8'
                data_bytes = urllib.parse.urlencode(payload_data).encode('utf-8')
                
            req = urllib.request.Request(target_url, data=data_bytes, headers=headers)
            with urllib.request.urlopen(req, context=ctx, timeout=10) as response:
                body = response.read().decode('utf-8')
                print(f"  {Color.BOLD}HTTP 状态码:{Color.END} {response.status}")
                try:
                    res_json = json.loads(body)
                    color = Color.GREEN if res_json.get("retstatus") == 1 else Color.RED
                    print(f"  {Color.BOLD}返回 JSON:{Color.END} {color}{json.dumps(res_json, ensure_ascii=False)}{Color.END}")
                except Exception:
                    print(f"  {Color.BOLD}返回 Text:{Color.END} {body[:300]}")
        except Exception as e:
            log_err(f"urllib 请求发送失败: {e}")

def run_push_tests():
    print(f"\n{Color.BOLD}=== 2. 模拟不同参数格式的 POST 测试 ==={Color.END}")
    
    # 变体 1: 直接以 JSON Body 发送完整 Payload (开发文档中的原写法)
    send_post("1. 纯 JSON Body (直接发送 payload)", json_data=base_payload)
    
    # 变体 2: 表单提交，整体作为一个名为 `data` 的字段
    send_post("2. Form Data (整个 payload 作为 data 字段)", payload_data={
        "data": json.dumps(base_payload, ensure_ascii=False)
    })
    
    # 变体 3: 表单提交，整体作为一个名为 `payload` 的字段
    send_post("3. Form Data (整个 payload 作为 payload 字段)", payload_data={
        "payload": json.dumps(base_payload, ensure_ascii=False)
    })

    # 变体 4: 表单提交，将 data 里面的内容作为 json string，其他扁平化
    form_4 = {
        "method": base_payload["method"],
        "time": base_payload["time"],
        "plat": base_payload["plat"],
        "company_code": base_payload["company_code"],
        "data": json.dumps(base_payload["data"], ensure_ascii=False)
    }
    send_post("4. Form Data (把 data 子项序列化为 JSON 字符串，其余在顶层)", payload_data=form_4)

    # 变体 5: 表单提交，完全扁平化（无 nested data，把 summary_short 也序列化）
    form_5 = {
        "method": base_payload["method"],
        "time": base_payload["time"],
        "plat": base_payload["plat"],
        "company_code": base_payload["company_code"],
        "summary_short": json.dumps(summary_short_mock, ensure_ascii=False),
        "summary_long": full_report_mock
    }
    send_post("5. Form Data (无 data 包装，所有字段直接扁平化，summary_short 为 JSON 字符串)", payload_data=form_5)
    
    # 变体 6: Query 参数形式 (把所有字段放入 URL query params 传)
    query_6 = {
        "method": base_payload["method"],
        "time": base_payload["time"],
        "plat": base_payload["plat"],
        "company_code": base_payload["company_code"],
        "data": json.dumps(base_payload["data"], ensure_ascii=False)
    }
    send_post("6. Query Params (把字段放在 URL 中，以 GET/POST params 传)", params_data=query_6)
    
    # 变体 7: 混合（Query 中放 method 识别，JSON 放具体数据）
    send_post("7. 混合形式 (Query 传 method 等基本项，Body 传数据 JSON)", 
              params_data={
                  "method": base_payload["method"],
                  "time": base_payload["time"],
                  "plat": base_payload["plat"],
                  "company_code": base_payload["company_code"]
              },
              json_data={"data": base_payload["data"]})

    # 变体 8: 混合（Query 中放 method 识别，Form 表单放 data JSON 串）
    send_post("8. 混合形式 (Query 传基本项，Form 传 data JSON 字符串)", 
              params_data={
                  "method": base_payload["method"],
                  "time": base_payload["time"],
                  "plat": base_payload["plat"],
                  "company_code": base_payload["company_code"]
              },
              payload_data={"data": json.dumps(base_payload["data"], ensure_ascii=False)})

    # 变体 9: 商搏特殊的 wrapper (例如有些 ERP 的通用网关格式)
    # 整个 json 包在 info 字段内
    send_post("9. Form Data (整个 payload 作为 info 字段)", payload_data={
        "info": json.dumps(base_payload, ensure_ascii=False)
    })
    
    # 变体 10: 整个 JSON 包在 param 字段内
    send_post("10. Form Data (整个 payload 作为 param 字段)", payload_data={
        "param": json.dumps(base_payload, ensure_ascii=False)
    })

if __name__ == "__main__":
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    print(f"{Color.BOLD}=================================================={Color.END}")
    print(f"{Color.BOLD}    🚀 商搏 ERP Push 连通性与参数一键检测脚本 🚀{Color.END}")
    print(f"{Color.BOLD}=================================================={Color.END}")
    
    fetch_and_parse_apipost()
    run_push_tests()
    
    print(f"\n{Color.BOLD}=================================================={Color.END}")
    print(f"🎉 检测完毕。请查看上述各测试的返回输出。")
    print(f"如果抓取到了 `apipost_api_detail.json`，请查看其结构或将其发给助手。")
    print(f"如果全部返回 '获取参数失败'，说明可能有特殊鉴权、签名或者字段，请对照浏览器里打开的 apipost 页面抓包确认。")
    print(f"{Color.BOLD}=================================================={Color.END}")
