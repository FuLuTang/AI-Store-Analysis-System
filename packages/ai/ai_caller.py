"""
ai_caller.py — AI 调用模块 (v2: 多行业支持 + 格式化输入)
"""
import json
import asyncio
from datetime import datetime

# ── 行业角色模板 ──

INDUSTRY_ROLES = {
    "pharmacy": {
        "role": "连锁药店经营分析顾问",
        "domain": "连锁药店",
        "scopes": ["营收趋势", "毛利率", "会员渗透率", "O2O渠道占比", "热销商品波动", "缺货风险"],
    },
    "restaurant": {
        "role": "餐饮经营分析顾问",
        "domain": "餐饮门店",
        "scopes": ["营收趋势", "客单价", "堂食/外卖占比", "菜品TOP贡献", "出餐超时", "翻台率"],
    },
    "hr": {
        "role": "HR组织效率分析顾问",
        "domain": "企业组织",
        "scopes": ["离职率", "招聘漏斗", "考勤异常", "绩效分布", "部门人数变化"],
    },
    "generic": {
        "role": "数据分析顾问",
        "domain": "经营实体",
        "scopes": ["营收趋势", "指标变化", "异常波动", "数据完整度"],
    },
}

def _role_config(scene: dict) -> dict:
    industry = (scene or {}).get("industry", "generic")
    return INDUSTRY_ROLES.get(industry, INDUSTRY_ROLES["generic"])


def _build_system_prompt(scene: dict) -> str:
    cfg = _role_config(scene)
    scopes_text = "、".join(cfg["scopes"])
    return f"""你是一位资深{cfg["role"]}。你将收到一家{cfg["domain"]}的多维经营数据。

数据含：
- 算法引擎预先计算的指标结果（附状态: pass/attention/warning/uncountable）
- 由证据包（evidence）支撑的量化结果
- 字段语义映射记录和数据质量报告

你的任务：
生成一份【经营诊断报告】。

# 第一部分：现状诊断报告
1. 核心经营判断（涨跌稳定波动，可能原因，带emojis如📈📉）
2. 核心指标逐项解读（优先关注 attention/warning 项）
3. 关联分析（多指标交叉解读，如营收涨但毛利跌说明什么）
4. 风险预警

# 第二部分：优化行动方案
1. 紧急事项（高风险指标对应的动作）
2. 中期改善（结构性问题的优化方向）
3. 基于行业经验的其他建议

分析要点：
- {scopes_text}
- 优先使用证据包（evidence）中的数据，避免编造数值
- 标记为 uncountable 的指标说明数据不足，报告中应注明"数据缺失"
- 环比注意折算（可能未到期末）
- 没有同比数据时，环比结论需留有余地

输出要求：
- Markdown 格式，大小标题，列点，必要时用表格或Mermaid
- 用 # 现状诊断报告 ... \\n --- \\n # 优化行动方案 ... 分隔
- 全文约1000字，bullet 不超45字，最多4个核心问题
- 禁止"不是，而是"句式，禁止结尾客套话"""


def _build_detail_prompt(scene: dict) -> str:
    cfg = _role_config(scene)
    return f"""你是一位资深数据分析师与{cfg["role"]}。你将收到一份融合报告：

1. 初级AI诊断报告
2. 错误评审意见
3. 证据包（量化支撑）

任务：深度重写为结构严谨的最终诊断报告。

规则：
- 纠正初级报告中被评审指出的错误
- 结合证据包挖掘深层根因，不允许引用证据包外的数据
- 提供具体可落地的行动方案
- 保持专业商业语调，格式化输出
- 禁止"不是，而是"句式，禁止结尾客套话"""


def _build_simplified_prompt(scene: dict = None) -> str:
    return """你是一位经营分析顾问。请为管理层提供"一眼定真问题"的精简诊断。

严格按照 JSON 输出，不含其他字符（不含 ```json）：

{
  "health_status": "1-2词整体状态",
  "overview_text": "一句大白话总结当前经营状况",
  "cards": [{
    "title": "问题标题",
    "explanation": "大白话说怎么回事",
    "suggestion": "咋办",
    "evidence": "数据证据（优先 Markdown 迷你表格）",
    "color": "red/yellow/green/blue/pink"
  }]
}

规则：
- cards 最多 7 个
- 大白话
- color: red=报警 yellow=关注 green=正常 blue=信息 pink=数据口径不一致
- 禁止"不是，而是"句式"""


# ── 格式化工具 ──

def _format_metrics_text(metric_results: list) -> str:
    if not metric_results:
        return "暂无可计算指标。"

    header = "| 指标 | 结果 | 状态 | 说明 |\n|------|------|------|------|\n"
    rows = []
    for r in metric_results:
        name = r.get("name", "?")
        status = r.get("status", "uncountable")
        icon = {"warning": "🔴", "attention": "🟡", "pass": "🟢", "uncountable": "⚪"}.get(status, "⚪")
        val = r.get("value")
        if isinstance(val, (dict, list)):
            val_str = _value_brief(val)
        elif val is not None:
            val_str = f"{val:.2f}" if isinstance(val, float) else str(val)
        else:
            val_str = "-"
        reason = r.get("reason", "") or ""
        rows.append(f"| {icon} {name} | {val_str} | {status} | {reason} |")

    return "【指标计算结果】\n" + header + "\n".join(rows)


def _value_brief(val) -> str:
    if isinstance(val, dict):
        parts = []
        for k, v in val.items():
            if k in ("evidence_table", "issues"):
                continue
            parts.append(f"{k}={v}")
        return ", ".join(parts[:3])
    if isinstance(val, list):
        return f"[{len(val)}条]"
    return str(val)


def _format_evidence_summary(evidence_bundle: dict) -> str:
    items = evidence_bundle.get("items", [])
    if not items:
        return "暂无证据数据。"
    lines = ["【证据包摘要】"]
    lines.append(f"共 {len(items)} 条证据，tally: {evidence_bundle.get('summary', {}).get('tally', {})}")
    lines.append("")

    # 按状态分组展示
    for status in ("warning", "attention"):
        filtered = [i for i in items if i.get("status") == status]
        if not filtered:
            continue
        icon = "🔴" if status == "warning" else "🟡"
        lines.append(f"{icon} {status.upper()} ({len(filtered)}条):")
        for item in filtered:
            val = item.get("value")
            val_str = _value_brief(val) if isinstance(val, (dict, list)) else str(val)[:60]
            lines.append(f"  - {item.get('title', '?')}: {val_str}")
        lines.append("")
    return "\n".join(lines)


def _format_mapping_summary(mappings: list) -> str:
    mapped = [m for m in mappings if m.get("semantic_field") != "unknown"]
    return f"【字段映射】{len(mapped)}/{len(mappings)} 个字段已识别为标准化语义字段。"


# ── API 调用基础 ──

async def shared_stream_fetch(base_url: str, api_key: str, payload: dict, abort_event=None) -> dict:
    import httpx
    url = base_url.rstrip("/") + "/chat/completions"
    payload["stream"] = True
    full_text = ""
    usage = None
    async with httpx.AsyncClient(timeout=httpx.Timeout(180.0)) as client:
        async with client.stream(
            "POST", url,
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"},
            json=payload,
        ) as response:
            if response.status_code != 200:
                err = await response.aread()
                raise Exception(f"AI API 调用失败 ({response.status_code}): {err.decode()[:300]}")
            buffer = ""
            async for chunk in response.aiter_text():
                buffer += chunk
                lines = buffer.split("\n")
                buffer = lines.pop()
                for line in lines:
                    line = line.strip()
                    if line.startswith("data:"):
                        data_str = line[5:].strip()
                        if data_str == "[DONE]":
                            continue
                        try:
                            data_obj = json.loads(data_str)
                            delta = data_obj.get("choices", [{}])[0].get("delta", {})
                            if delta.get("content"):
                                full_text += delta["content"]
                            if data_obj.get("usage"):
                                usage = data_obj["usage"]
                        except json.JSONDecodeError:
                            pass
    return {"choices": [{"message": {"content": full_text}}], "usage": usage}


def _build_context_header(scene: dict = None) -> str:
    now = datetime.now()
    cfg = _role_config(scene)
    city = "福州"
    header = (
        f"【分析环境】\n"
        f"- 城市：{city}\n"
        f"- 行业：{cfg['domain']}\n"
        f"- 角色：{cfg['role']}\n"
        f"- 日期：{now.strftime('%Y年%m月%d日')} {now.strftime('%H:%M')}\n"
        f"- 重要提醒：排名/排序类数值不具有指标含义，不可用于经营分析\n"
    )
    return header


def _resolve_reasoning_effort(settings: dict) -> str:
    if not isinstance(settings, dict):
        return "medium"
    raw = settings.get("reasoningEffort") or settings.get("reasoning_effort") or ""
    value = str(raw).strip().lower()
    return value if value in {"low", "medium", "high"} else "medium"


# ── AI-1: 初级报告 ──

def _build_data_context_text(profiles: list, mappings: list = None, scene: dict = None) -> str:
    """从画像+映射构建数据上下文（不含指标结果，供早期AI调用）"""
    industry = (scene or {}).get("industry", "generic")
    lines = [f"【分析场景】行业={industry}", ""]

    # 表结构概览
    tables = {}
    for p in profiles:
        t = p.get("table", "?")
        if t not in tables:
            tables[t] = []
        tables[t].append(p)

    lines.append(f"共 {len(tables)} 张表, {len(profiles)} 个字段:")
    for tname, cols in tables.items():
        lines.append(f"  {tname} ({len(cols)} 列)")
    lines.append("")

    # 字段映射结果（如果有）
    if mappings:
        mapped = [m for m in mappings if m.get("semantic_field") not in ("unknown", "ignore")]
        unknown = [m for m in mappings if m.get("semantic_field") == "unknown"]
        lines.append(f"【字段映射】{len(mapped)}/{len(mappings)} 已识别")
        for m in mapped:
            lines.append(f"  {m.get('table', '?')}.{m['raw_field']} → {m['semantic_field']}")
        if unknown:
            lines.append(f"  未识别: {len(unknown)} 个")
            for m in unknown[:5]:
                lines.append(f"    {m.get('table', '?')}.{m['raw_field']}")

    return "\n".join(lines)


async def call_ai(settings: dict, cleaned_data_texts: list, algo_data: dict = None) -> dict:
    """AI-1: 初级报告 (兼容旧管线，有指标传指标，无指标传raw data)"""
    context = _build_context_header()
    reasoning_effort = _resolve_reasoning_effort(settings)

    algo_text = "【算法引擎预诊结果】\n暂无算法诊断结果。"
    if algo_data and algo_data.get("anomalies"):
        a = algo_data["anomalies"]
        algo_text = f"【算法引擎预诊结果】\n> {a.get('summary', '')}\n\n"
        for alert in a.get("alerts", []):
            icon = "🔴" if alert.get("severity") == "warning" else "🟡"
            algo_text += f"{icon} **{alert['metric']}** ({alert['severity']}): {alert['detail']}\n"

    user_content = context + "\n" + algo_text + "\n\n【底层数据】\n" + "\n\n---\n\n".join(cleaned_data_texts)

    return await shared_stream_fetch(settings["baseUrl"], settings["apiKey"], {
        "model": settings["model"],
        "messages": [
            {"role": "system", "content": _build_system_prompt(None)},
            {"role": "user", "content": user_content},
        ],
        "reasoning_effort": reasoning_effort, "temperature": 0.3, "max_tokens": 16384,
    })


async def call_ai_early(settings: dict, data_context_text: str, scene: dict) -> dict:
    """AI-1 早期调用：在指标算出前先用数据上下文生成初诊"""
    reasoning_effort = _resolve_reasoning_effort(settings)
    context = _build_context_header(scene)

    user_content = context + "\n\n【数据概览】\n" + data_context_text + \
        "\n\n【注意】\n当前展示的是字段结构和映射关系，精确指标尚在计算中。" \
        "请基于已有信息做初步诊断，如数据结构特征、映射覆盖率、明显的数据范围等。"

    cfg = _role_config(scene)
    scopes_text = "、".join(cfg["scopes"])
    system_prompt = f"""你是一位资深{cfg["role"]}。你将收到一家{cfg["domain"]}的数据概览。

数据含：
- 解析后的表格结构和字段名
- 标准语义字段映射（原始字段名 → 标准字段名）
- 未识别的字段列表

你的任务：
基于数据结构做初步经营诊断，发现明显的业务特征或异常信号。

分析方向：
- {scopes_text}
- 字段映射的质量和覆盖率（哪些字段没识别出来？可能缺失了哪些关键维度？）
- 数据的时间范围和颗粒度（有日、月、年数据吗？）
- 基于结构判断能做哪些分析、缺什么数据

输出要求：
- Markdown 格式，简约
- 指出关键发现和数据缺口
- 不要编造没有数据的数值"""

    return await shared_stream_fetch(settings["baseUrl"], settings["apiKey"], {
        "model": settings["model"],
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_content},
        ],
        "reasoning_effort": reasoning_effort, "temperature": 0.3, "max_tokens": 8192,
    })


async def call_ai_new(settings: dict, scene: dict, metric_results: list, evidence: dict, mappings: list) -> dict:
    """AI-1: 初级报告 (新管线 — 格式化输入)"""
    reasoning_effort = _resolve_reasoning_effort(settings)

    parts = [
        _build_context_header(scene),
        _format_metrics_text(metric_results),
        "",
        _format_evidence_summary(evidence),
        "",
        _format_mapping_summary(mappings),
    ]
    user_content = "\n".join(parts)

    return await shared_stream_fetch(settings["baseUrl"], settings["apiKey"], {
        "model": settings["model"],
        "messages": [
            {"role": "system", "content": _build_system_prompt(scene)},
            {"role": "user", "content": user_content},
        ],
        "reasoning_effort": reasoning_effort, "temperature": 0.3, "max_tokens": 16384,
    })


# ── AI-3: 深度报告 ──

async def call_detailed_ai(settings: dict, fused_report_text: str) -> dict:
    """AI-3: 详细报告 (兼容旧管线)"""
    user_content = _build_context_header() + "\n" + fused_report_text
    reasoning_effort = _resolve_reasoning_effort(settings)
    return await shared_stream_fetch(settings["baseUrl"], settings["apiKey"], {
        "model": settings["model"],
        "messages": [
            {"role": "system", "content": _build_detail_prompt(None)},
            {"role": "user", "content": user_content},
        ],
        "reasoning_effort": reasoning_effort, "temperature": 0.4, "max_tokens": 16384,
    })


async def call_detailed_ai_new(settings: dict, scene: dict, fused_report_text: str) -> dict:
    """AI-3: 深度报告 (新管线)"""
    user_content = _build_context_header(scene) + "\n" + fused_report_text
    reasoning_effort = _resolve_reasoning_effort(settings)
    return await shared_stream_fetch(settings["baseUrl"], settings["apiKey"], {
        "model": settings["model"],
        "messages": [
            {"role": "system", "content": _build_detail_prompt(scene)},
            {"role": "user", "content": user_content},
        ],
        "reasoning_effort": reasoning_effort, "temperature": 0.4, "max_tokens": 16384,
    })


# ── AI-4: 精简报告 ──

async def call_simplified_ai(settings: dict, detailed_report_text: str) -> dict:
    """AI-4: 老板视图"""
    user_content = _build_context_header() + "\n\n【详细报告内容】\n" + detailed_report_text
    reasoning_effort = _resolve_reasoning_effort(settings)
    return await shared_stream_fetch(settings["baseUrl"], settings["apiKey"], {
        "model": settings["model"],
        "messages": [
            {"role": "system", "content": _build_simplified_prompt()},
            {"role": "user", "content": user_content},
        ],
        "response_format": {"type": "json_object"},
        "reasoning_effort": reasoning_effort, "temperature": 0.3, "max_tokens": 8192,
    })


# ── AI-5: 行业分类 ──

_INDUSTRY_CLASSIFY_SYSTEM_PROMPT = """你是一位数据分类专家。根据上传文件的字段名特征，判断这份数据所属的行业。

可选行业（只能选一个）：
- pharmacy: 药店/药品零售（关键词: 药品, 零售金额, 会员金额, barcode, 批准文号, OTC, O2O）
- restaurant: 餐饮（关键词: 菜品, 外卖, 堂食, dish, 配送, 翻台）
- hr: 人力资源（关键词: 员工, 离职, 入职, 绩效, 考勤, 招聘）
- retail: 零售（关键词: 商品, 库存, SKU, 零售, 门店）
- generic: 通用（以上都不匹配时）

输出严格 JSON，不含其他字符：
{
  "industry": "pharmacy|restaurant|hr|retail|generic",
  "business_model": "o2o_driven|offline_driven|delivery_heavy|internal_department|unknown",
  "confidence": 0.0-1.0,
  "reason": "判断依据简要说明"
}"""


async def call_industry_classifier(settings: dict, profiles: list) -> dict:
    """AI-5: 行业分类"""
    lines = ["请判断以下字段属于哪个行业：\n"]
    for p in profiles:
        table = p.get("table", "?")
        col = p.get("column", "?")
        dtype = p.get("dtype", "?")
        samples = p.get("samples", [])[:3]
        lines.append(f"  表: {table} | 字段: {col} | 类型: {dtype} | 样本: {samples}")
    user_content = "\n".join(lines)

    reasoning_effort = _resolve_reasoning_effort(settings)
    return await shared_stream_fetch(settings["baseUrl"], settings["apiKey"], {
        "model": settings["model"],
        "messages": [
            {"role": "system", "content": _INDUSTRY_CLASSIFY_SYSTEM_PROMPT},
            {"role": "user", "content": user_content},
        ],
        "response_format": {"type": "json_object"},
        "reasoning_effort": reasoning_effort, "temperature": 0.2, "max_tokens": 1024,
    })


# ── AI-6: 字段映射 ──

_FIELD_MAP_SYSTEM_PROMPT = """你是一位数据工程师。你需要将原始字段名映射为标准语义字段。

规则：
1. 识别每个字段的业务含义，映射到最匹配的标准字段
2. 如果某个字段没有业务价值（如内部编号、元数据、展示标签等），标记为 "ignore"
3. 充分使用字段所在的表名、路径上下文来判断含义
4. 样本值可以帮助判断字段的实际含义

输出严格 JSON 数组，不含其他字符：
[
  {
    "raw_field": "原始字段名",
    "table": "来源表名",
    "semantic_field": "标准字段名 或 ignore",
    "confidence": 0.0-1.0,
    "reason": "映射理由"
  }
]"""


def _build_field_map_standard_fields() -> str:
    """构建可用标准字段列表说明"""
    items = [
        ("revenue", "营业收入/营业额/销售额"),
        ("gross_profit", "毛利"),
        ("order_count", "订单数/单量"),
        ("customer_count", "来客数/客流"),
        ("cost", "成本/进价"),
        ("discount_amount", "折扣/满减/补贴"),
        ("channel", "渠道/平台"),
        ("product_name", "商品名称/产品名"),
        ("product_id", "商品编号/SKU/条码"),
        ("category", "分类/品类"),
        ("store_id", "门店编号"),
        ("department", "部门"),
        ("employee_id", "员工编号"),
        ("date", "日期"),
        ("time_slot", "时段/小时"),
        ("inventory_qty", "库存量"),
        ("inventory_amount", "库存金额"),
        ("member_revenue", "会员营收"),
        ("member_count", "会员数"),
        ("delivery_duration", "出餐/配送时长"),
        ("rating", "评分/评价"),
        ("hire_date", "入职日期"),
        ("leave_date", "离职日期"),
        ("employee_status", "员工状态(在职/离职)"),
        ("attendance_hours", "出勤工时"),
        ("performance_score", "绩效分"),
        ("candidate_count", "简历数/候选人"),
        ("interview_count", "面试数"),
        ("offer_count", "Offer数"),
        ("onboard_count", "入职数"),
        ("sales_quantity", "销售数量"),
        ("unit_price", "单价"),
        ("retail_price", "零售价"),
        ("member_price", "会员价"),
        ("avg_order_value", "客单价"),
        ("sales_rank", "销售排名"),
        ("manufacturer", "生产厂家"),
        ("specification", "规格"),
        ("approval_no", "批准文号"),
    ]
    lines = ["可用标准字段列表："]
    for field, desc in items:
        lines.append(f"  - {field}: {desc}")
    lines.append("")
    lines.append("特殊标记说明：")
    lines.append("  - ignore: 该字段无业务分析价值，如内部ID、展示标签、元数据等")
    return "\n".join(lines)


async def call_field_mapper(settings: dict, profiles: list, industry: str) -> dict:
    """AI-6: 字段映射"""
    lines = ["请将以下原始字段映射到标准语义字段：\n"]
    lines.append(f"已识别行业: {industry}\n")
    for p in profiles:
        table = p.get("table", "?")
        col = p.get("column", "?")
        dtype = p.get("dtype", "?")
        samples = p.get("samples", [])[:3]
        lines.append(f"  表: {table} | 字段: {col} | 类型: {dtype} | 样本: {samples}")
    lines.append("")
    lines.append(_build_field_map_standard_fields())
    user_content = "\n".join(lines)

    reasoning_effort = _resolve_reasoning_effort(settings)
    return await shared_stream_fetch(settings["baseUrl"], settings["apiKey"], {
        "model": settings["model"],
        "messages": [
            {"role": "system", "content": _FIELD_MAP_SYSTEM_PROMPT},
            {"role": "user", "content": user_content},
        ],
        "response_format": {"type": "json_object"},
        "reasoning_effort": reasoning_effort, "temperature": 0.2, "max_tokens": 4096,
    })
