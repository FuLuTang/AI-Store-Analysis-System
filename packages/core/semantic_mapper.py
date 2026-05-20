"""
semantic_mapper.py — 字段语义映射
将原始字段名映射为标准语义字段名
支持规则匹配 + LLM 辅助
"""
import json
import os
from pathlib import Path
from typing import List, Optional
# 关键字 → 标准字段 映射表
KEYWORD_MAP = {
    "revenue": ["营收", "营业额", "销售额", "金额", "revenue", "turnover", "GMV", "gmv", "收入", "零售金额",
                "retail_amount", "total_revenue", "meituan_revenue", "eleme_revenue", "ecommerce_amount",
                "online_amount", "零售额"],
    "gross_profit": ["毛利", "利润额", "gross_profit", "利润", "grossProfit",
                     "member_gross_profit", "ecommerce_gross_profit", "online_gross_profit",
                     "gross_margin", "电商毛利"],
    "order_count": ["订单数", "单量", "小票数", "order_count", "订单", "orders", "单数",
                    "total_order_count", "sales_receipt_count", "receipt_count",
                    "meituan_order_count", "eleme_order_count"],
    "customer_count": ["来客数", "客流", "人数", "访客数", "visitor_count", "customer_count", "客数"],
    "cost": ["成本", "进价", "采购成本", "cost", "purchase_price", "进价"],
    "discount_amount": ["折扣", "满减", "补贴", "优惠", "discount"],
    "channel": ["平台", "渠道", "来源", "channel", "platform", "美团", "饿了么"],
    "product_name": ["商品名称", "商品名", "菜品名称", "菜品名", "product_name", "dish_name",
                     "名称", "common_name", "通用名称"],
    "product_id": ["商品编号", "SKU", "菜品ID", "product_id", "sku", "条码", "barcode", "product_code"],
    "category": ["分类", "品类", "类目", "category", "部门类目"],
    "store_id": ["门店编号", "门店ID", "shop_id", "store_id", "门店"],
    "department": ["部门", "组织", "department", "team", "dept"],
    "employee_id": ["员工编号", "工号", "employee_id", "员工ID"],
    "date": ["日期", "period", "date", "日期", "时间", "created_at"],
    "time_slot": ["时段", "小时", "班次", "meal_period", "班", "time_slot"],
    "inventory_qty": ["库存", "库存量", "inventory", "库存数量", "stock_quantity", "stock"],
    "inventory_amount": ["库存金额", "inventory_amount"],
    "member_revenue": ["会员金额", "会员营业额", "member_revenue", "member_amount", "会员"],
    "member_count": ["会员人数", "会员数", "member_count"],
    "delivery_duration": ["出餐时长", "配送时长", "超时分钟", "delivery_duration"],
    "rating": ["评分", "评价分", "星级", "rating"],
    "hire_date": ["入职日期", "入职时间", "hire_date"],
    "leave_date": ["离职日期", "离职时间", "leave_date"],
    "employee_status": ["员工状态", "在职", "离职", "employee_status"],
    "attendance_hours": ["出勤小时", "工时", "出勤时长", "attendance_hours"],
    "performance_score": ["绩效分", "绩效", "评分", "performance_score"],
    "candidate_count": ["简历数", "候选人", "candidate_count"],
    "interview_count": ["面试人数", "面试数", "interview_count"],
    "offer_count": ["offer数量", "offer数", "offer_count"],
    "onboard_count": ["入职人数", "入职数", "onboard_count"],
    # ── 新增字段 ──
    "sales_quantity": ["销售数量", "销售数", "sales_quantity", "销量", "数量"],
    "avg_order_value": ["客单价", "avg_order_value", "average_order_value", "平均客单价",
                        "average_spend"],
    "retail_price": ["零售价", "零售价格", "retail_price", "单价", "price", "售价"],
    "member_price": ["会员价", "会员价格", "member_price"],
    "manufacturer": ["厂家", "生产厂家", "制造商", "manufacturer", "生产企业"],
    "specification": ["规格", "specification", "规格型号", "包装规格"],
    "approval_no": ["批准文号", "approval_no", "国药准字", "注册证号"],
    "sales_rank": ["排名", "销售排名", "rank", "sales_rank", "排序", "序号"],
    "city": ["城市", "city", "地区", "区域"],
    "unit_price": ["单价", "unit_price", "销售单价"],
}

# 场景感知消歧规则：同一关键字匹配到多个标准字段时，根据 scene.industry 消歧
_SCENE_DISAMBIGUATION = {
    "评分": {
        "hr": "performance_score",
        "default": "rating",
    },
}
def _score_field(raw_field: str, keywords: list) -> float:
    """计算字段名与关键字的匹配度"""
    raw_lower = raw_field.lower().strip()
    best = 0.0
    for kw in keywords:
        kw_lower = kw.lower().strip()
        if raw_lower == kw_lower:
            return 1.0
        if kw_lower in raw_lower or raw_lower in kw_lower:
            best = max(best, 0.85)
    return best
def map_field(raw_field: str, samples: Optional[list] = None, scene: Optional[dict] = None) -> dict:
    """
    规则匹配：将一个原始字段名映射为标准语义字段

    返回: SemanticMapping = {
        "raw_field": str,
        "semantic_field": str,
        "confidence": float,
        "need_confirm": bool,
        "reason": str
    }
    """
    best_field = None
    best_score = 0.0

    for semantic_field, keywords in KEYWORD_MAP.items():
        score = _score_field(raw_field, keywords)
        if score > best_score:
            best_score = score
            best_field = semantic_field

    # 场景感知消歧：同一关键字可能匹配多个标准字段，根据 scene 决定
    if scene and raw_field in _SCENE_DISAMBIGUATION:
        industry = scene.get("industry", "")
        disamb = _SCENE_DISAMBIGUATION[raw_field]
        resolved = disamb.get(industry, disamb.get("default"))
        if resolved and resolved != best_field:
            best_field = resolved
            best_score = max(best_score, 0.8)

    if best_score < 0.5:
        return {
            "raw_field": raw_field,
            "semantic_field": "unknown",
            "confidence": 0.0,
            "need_confirm": True,
            "reason": f"未匹配到标准字段"
        }

    need_confirm = best_score < 0.75

    return {
        "raw_field": raw_field,
        "semantic_field": best_field,
        "confidence": round(best_score, 2),
        "need_confirm": need_confirm,
        "reason": f"关键字匹配 (score={round(best_score, 2)})"
    }
def map_profiles(profiles: list, scene: Optional[dict] = None) -> list:
    """
    对所有字段画像做语义映射
    输入: ColumnProfile[]
    输出: SemanticMapping[]
    """
    mappings = []
    for p in profiles:
        m = map_field(p["column"], p.get("samples", []), scene)
        m["table"] = p.get("table", "unknown")
        m["dtype"] = p.get("dtype", "unknown")
        mappings.append(m)
    return mappings
# ── LLM 辅助映射 (留桩，后续接入) ──
def _build_mapping_prompt(profiles: list, scene: Optional[dict] = None) -> str:
    """构建 LLM 映射提示词"""
    lines = ["以下是一份数据的字段信息，请帮我把每个字段映射到标准语义字段。"]
    lines.append("\n字段列表：")
    for p in profiles:
        lines.append(f"  - 表: {p.get('table', '?')}, 字段: {p['column']}, "
                     f"类型: {p.get('dtype', '?')}, 样本: {p.get('samples', [])[:3]}")
    lines.append("\n可用标准字段：")
    standard_fields = sorted(KEYWORD_MAP.keys())
    for sf in standard_fields:
        lines.append(f"  - {sf}: ({', '.join(KEYWORD_MAP[sf][:3])}...)")
    lines.append("\n请为每个字段返回 JSON 格式: [{raw_field, semantic_field, confidence, reason}]")
    return "\n".join(lines)
async def llm_map_profiles(profiles: list, llm_settings: dict, scene: Optional[dict] = None) -> list:
    """
    AI 字段映射：将字段信息发给 LLM，返回 SemanticMapping[]
    如果 LLM 调用失败或没有 API Key，回退到规则版
    """
    if not llm_settings or not llm_settings.get("apiKey"):
        return map_profiles(profiles, scene)

    try:
        from packages.ai.ai_caller import call_field_mapper
        industry = (scene or {}).get("industry", "generic")
        resp = await call_field_mapper(llm_settings, profiles, industry)
        content = resp["choices"][0]["message"]["content"]
        import json as _json
        ai_results = _json.loads(content)

        # 转换成标准 SemanticMapping[] 格式
        mappings = []
        for item in ai_results:
            raw = item.get("raw_field", "")
            table = item.get("table", "")
            sf = item.get("semantic_field", "unknown")
            conf = item.get("confidence", 0.5)

            if sf == "ignore":
                mappings.append({
                    "raw_field": raw,
                    "table": table,
                    "semantic_field": "ignore",
                    "confidence": 1.0,
                    "need_confirm": False,
                    "reason": item.get("reason", "AI: 无业务价值，忽略"),
                    "dtype": "unknown",
                })
            else:
                need_confirm = conf < 0.75
                mappings.append({
                    "raw_field": raw,
                    "table": table,
                    "semantic_field": sf,
                    "confidence": round(conf, 2),
                    "need_confirm": need_confirm,
                    "reason": f"AI映射: {item.get('reason', '')}",
                    "dtype": "unknown",
                })

        # AI 返回的数量应该匹配输入
        if len(mappings) != len(profiles):
            # 如果 AI 漏了某些字段，用规则补全
            rule_mappings = map_profiles(profiles, scene)
            ai_raw_map = {m["raw_field"]: m for m in mappings}
            for rm in rule_mappings:
                if rm["raw_field"] not in ai_raw_map:
                    mappings.append(rm)

        # 补充 dtype
        profile_lookup = {}
        for p in profiles:
            col = p.get("column", "")
            tbl = p.get("table", "")
            key = f"{tbl}::{col}"
            profile_lookup[col] = p
            profile_lookup[key] = p
        for m in mappings:
            for p in profiles:
                if p.get("column") == m["raw_field"] and p.get("table", "") == m.get("table", ""):
                    m["dtype"] = p.get("dtype", "unknown")
                    break

        return mappings
    except Exception as e:
        # 失败时回退规则
        return map_profiles(profiles, scene)
# ── 映射持久化 ──
def _mappings_dir():
    """映射存储目录"""
    from pathlib import Path
    root = Path(__file__).parent.parent.parent
    dir_path = root / "storage" / "mappings"
    dir_path.mkdir(parents=True, exist_ok=True)
    return dir_path
def save_mappings(tenant_id: str, mappings: list):
    """保存映射结果，下次复用"""
    file_path = _mappings_dir() / f"{tenant_id}.json"
    file_path.write_text(json.dumps(mappings, ensure_ascii=False, indent=2), encoding="utf-8")
def load_mappings(tenant_id: str) -> Optional[list]:
    """加载历史映射"""
    file_path = _mappings_dir() / f"{tenant_id}.json"
    if file_path.exists():
        try:
            return json.loads(file_path.read_text(encoding="utf-8"))
        except Exception:
            pass
    return None