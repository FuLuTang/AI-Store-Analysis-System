"""
scene_classifier.py — 行业/业态识别
根据原始字段名和数据特征判断分析场景
"""
from typing import List, Optional


# 行业关键字 → 行业包名
INDUSTRY_KEYWORDS = {
    "pharmacy": ["药", "药品", "零售金额", "会员金额", "电商金额", "barcode", "条码", "批准文号", "处方", "OTC"],
    "restaurant": ["菜品", "外卖", "堂食", "dish", "出餐", "配送", "桌台", "翻台", "用餐"],
    "hr": ["员工", "离职", "入职", "绩效", "考勤", "招聘", "面试", "简历", "人力", "薪酬", "人事"],
    "retail": ["商品", "库存", "SKU", "零售", "门店", "品类"],
}

# 业态关键字
MODEL_KEYWORDS = {
    "o2o_driven": ["O2O", "美团", "饿了么", "京东到家", "电商", "线上", "平台", "o2o"],
    "offline_driven": ["线下", "实体", "普通", "零售", "收银"],
    "delivery_heavy": ["外卖", "配送", "出餐", "骑手", "配送超时"],
    "internal_department": ["部门", "员工", "绩效", "考勤", "入职", "离职"],
}


def _count_keywords(text_list: list, keyword_set: set) -> int:
    """统计文本列表中出现的关键字数"""
    count = 0
    text_lower = " ".join(str(t).lower() for t in text_list)
    for kw in keyword_set:
        if kw.lower() in text_lower:
            count += 1
    return count


def classify_industry(field_names: list) -> dict:
    """识别行业"""
    results = {}
    for industry, keywords in INDUSTRY_KEYWORDS.items():
        score = _count_keywords(field_names, set(keywords))
        if score > 0:
            results[industry] = score

    if not results:
        return {"industry": "generic", "confidence": 0.3}

    best = max(results, key=results.get)
    confidence = min(0.95, results[best] / max(3, len(field_names) * 0.3))

    return {
        "industry": best,
        "confidence": round(confidence, 2),
        "scores": results
    }


def classify_business_model(field_names: list, industry: str) -> dict:
    """识别业态/经营模式"""
    results = {}
    for model, keywords in MODEL_KEYWORDS.items():
        score = _count_keywords(field_names, set(keywords))
        if score > 0:
            results[model] = score

    if not results:
        # 根据行业推断默认业态
        if industry == "hr":
            return {"business_model": "internal_department", "confidence": 0.7}
        return {"business_model": "unknown", "confidence": 0.3}

    best = max(results, key=results.get)
    confidence = min(0.95, results[best] / max(2, len(field_names) * 0.1))

    return {
        "business_model": best,
        "confidence": round(confidence, 2),
        "scores": results
    }


def classify_data_scope(mappings: list) -> list:
    """识别数据范围"""
    semantic_fields = [m["semantic_field"] for m in mappings if m.get("semantic_field") != "unknown"]
    scope = set()

    scope_map = {
        "sales": ["revenue", "order_count", "customer_count", "gross_profit"],
        "channel": ["channel"],
        "inventory": ["inventory_qty", "inventory_amount"],
        "member": ["member_revenue", "member_count"],
        "product": ["product_name", "product_id", "category"],
        "delivery": ["delivery_duration"],
        "hr": ["employee_id", "department", "hire_date", "leave_date", "performance_score", "attendance_hours"],
        "recruitment": ["candidate_count", "interview_count", "offer_count", "onboard_count"],
    }

    for name, fields in scope_map.items():
        if any(f in semantic_fields for f in fields):
            scope.add(name)

    return sorted(scope)


def classify_scene(profiles: list) -> dict:
    """
    综合识别分析场景（仅依赖原始字段名，不依赖语义映射结果）

    输出: SceneContext = {
        "industry": str,
        "business_model": str,
        "data_scope": [str],
        "analysis_goal": str,
        "confidence": float
    }
    """
    field_names = [p["column"] for p in profiles]

    industry_result = classify_industry(field_names)
    model_result = classify_business_model(field_names, industry_result["industry"])

    # data_scope 后续由 classify_data_scope(mappings) 在语义映射之后补充
    data_scope = []

    industry = industry_result["industry"]
    if industry == "pharmacy":
        goal = "经营诊断"
    elif industry == "restaurant":
        goal = "经营诊断"
    elif industry == "hr":
        goal = "HR效率"
    else:
        goal = "通用表格分析"

    avg_conf = (industry_result["confidence"] + model_result["confidence"]) / 2

    return {
        "industry": industry,
        "business_model": model_result["business_model"],
        "data_scope": data_scope,
        "analysis_goal": goal,
        "confidence": round(avg_conf, 2)
    }


# ── LLM 辅助分类 (留桩，后续接入) ──

async def llm_classify_scene(profiles: list, llm_settings: dict) -> dict:
    """
    LLM 辅助行业/业态识别（异步，留桩）
    当规则识别 confidence < 0.6 时调用
    """
    # TODO: 接入 ai_caller 进行 LLM 识别
    return classify_scene(profiles)
