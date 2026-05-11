"""
metrics.py — 算法处理模块 (从 metrics.js 完整迁移)
职责：从原始 JSON 数据中提取并计算经营指标
"""
import math

def is_valid(v):
    return isinstance(v, (int, float)) and math.isfinite(v)

def round_to(v, d=1):
    if not is_valid(v): return 0
    return round(v, d)

def to_tally(results):
    tally = {"warning": 0, "attention": 0, "pass": 0, "uncountable": 0}
    for r in (results or {}).values():
        k = r.get("status") if isinstance(r, dict) else None
        if k and k in tally: tally[k] += 1
    return tally

# ── Normalize 层 ──
def normalize_overview_rows(raw):
    rows = raw.get("businessTable", {}).get("rows", [])
    return [{"period": r.get("period"), "revenue": r.get("retail_amount", 0),
             "grossProfit": r.get("gross_profit", 0), "visitorCount": r.get("visitor_count", 0),
             "memberAmount": r.get("member_amount", 0), "memberGrossProfit": r.get("member_gross_profit", 0),
             "ecommerceAmount": r.get("ecommerce_amount") or r.get("online_amount", 0),
             "ecommerceGrossProfit": r.get("ecommerce_gross_profit") or r.get("online_gross_profit", 0)} for r in rows]

def normalize_hot_products(raw):
    return [{"rank": p.get("rank"), "name": p.get("product_name"), "barcode": p.get("barcode"),
             "receiptCount": p.get("sales_receipt_count"), "quantity": p.get("sales_quantity")}
            for p in raw.get("ranking", []) if p.get("sales_receipt_count") is not None]

def get_metric_by_key(summary, key):
    for m in summary.get("metrics", []):
        if m.get("key") == key: return m
    return None

# ── A类指标 ──
def calc_channel_mix(src_dist):
    if not src_dist or not src_dist.get("items"): return {"total": 0, "channels": [], "dominant": None, "dominantPct": 0, "status": "uncountable"}
    items = [i for i in src_dist["items"] if is_valid(i.get("value"))]
    if not items: return {"total": 0, "channels": [], "dominant": None, "dominantPct": 0, "status": "uncountable"}
    total = sum(i["value"] for i in items)
    if total <= 0: return {"total": 0, "channels": [], "dominant": None, "dominantPct": 0, "status": "uncountable"}
    channels = [{"key": i.get("key"), "label": i.get("label"), "value": i["value"], "pct": round(i["value"] / total * 100, 1)} for i in items]
    dom = max(channels, key=lambda c: c["pct"])
    st = "warning" if dom["pct"] > 85 else ("attention" if dom["pct"] > 70 else "pass")
    return {"total": round(total, 2), "channels": channels, "dominant": dom["label"], "dominantPct": dom["pct"], "status": st}

def calc_revenue_change(rows):
    if not rows or len(rows) < 2: return {"current": 0, "previous": 0, "changePct": 0, "direction": "flat", "status": "uncountable"}
    cur = rows[0].get("revenue", 0); prev = rows[1].get("revenue", 0)
    if prev == 0: return {"current": cur, "previous": prev, "changePct": 0, "direction": "up" if cur > 0 else "flat", "status": "uncountable"}
    pct = round((cur - prev) / prev * 100, 1)
    d = "up" if pct > 0 else ("down" if pct < 0 else "flat")
    st = "warning" if abs(pct) > 65 else ("attention" if abs(pct) > 30 else "pass")
    return {"current": cur, "previous": prev, "changePct": pct, "direction": d, "status": st}

def calc_o2o_vs_total(o2o_rev, overview_rev):
    if not is_valid(overview_rev) or overview_rev <= 0 or not is_valid(o2o_rev): return {"ratioPct": 0, "status": "uncountable"}
    pct = round(o2o_rev / overview_rev * 100, 1)
    return {"ratioPct": pct, "status": "warning" if pct > 88 else ("attention" if pct > 70 else "pass")}

def calc_consecutive_change(rows, field):
    if not rows or len(rows) < 2 or rows[0].get(field) is None or rows[1].get(field) is None:
        return {"direction": "flat", "consecutiveDays": 0, "totalChangePct": 0, "status": "uncountable"}
    d = "up" if rows[0][field] > rows[1][field] else ("down" if rows[0][field] < rows[1][field] else "flat")
    if d == "flat": return {"direction": "flat", "consecutiveDays": 1, "status": "pass"}
    count = 1
    for i in range(1, len(rows) - 1):
        dd = "up" if rows[i][field] > rows[i+1][field] else ("down" if rows[i][field] < rows[i+1][field] else "flat")
        if dd == d: count += 1
        else: break
    sv, ev = rows[count][field], rows[0][field]
    tcp = round((ev - sv) / sv * 100, 1) if sv != 0 else 0
    st = "warning" if d == "down" and count >= 6 else ("attention" if d == "down" and count >= 4 else "pass")
    return {"direction": d, "consecutiveDays": count, "startPeriod": rows[count].get("period"),
            "endPeriod": rows[0].get("period"), "startValue": sv, "endValue": ev, "totalChangePct": tcp, "status": st}

def calc_gross_margin_trend(rows):
    if not rows or len(rows) < 2: return {"margins": [], "avgMargin": 0, "latestMargin": 0, "trend": "stable", "slope": 0, "status": "uncountable"}
    margins = [{"period": r.get("period"), "marginPct": round(r["grossProfit"] / r["revenue"] * 100, 1) if r.get("revenue", 0) > 0 else 0} for r in rows]
    vals = [m["marginPct"] for m in margins]
    avg = sum(vals) / len(vals)
    rev = list(reversed(vals)); n = len(rev)
    sx = sum(range(n)); sy = sum(rev); sxy = sum(i * rev[i] for i in range(n)); sx2 = sum(i*i for i in range(n))
    slope = (n * sxy - sx * sy) / (n * sx2 - sx * sx) if n > 1 and (n * sx2 - sx * sx) != 0 else 0
    trend = "improving" if slope > 0.5 else ("declining" if slope < -0.5 else "stable")
    st = "warning" if trend == "declining" and margins[0]["marginPct"] < (avg - 3) else ("attention" if trend == "declining" else "pass")
    return {"margins": margins, "avgMargin": round(avg, 1), "latestMargin": margins[0]["marginPct"], "trend": trend, "slope": round(slope, 2), "status": st}

def calc_product_stability(hot_data):
    if not hot_data or not hot_data.get("today") or not hot_data.get("month") or len(hot_data["today"]) == 0:
        return {"stable": [], "rising": [], "fading": [], "todayOnly": [], "summary": {"stableCount": 0, "risingCount": 0, "fadingCount": 0, "todayOnlyCount": 0}, "status": "uncountable"}
    sets = {p: set(pr.get("barcode") for pr in prods if pr.get("barcode")) for p, prods in hot_data.items()}
    name_map = {}
    for prods in hot_data.values():
        for p in prods:
            if p.get("barcode"): name_map[p["barcode"]] = p.get("name", p.get("barcode"))
    stable = [bc for bc in sets.get("today", set()) if bc in sets.get("week", set()) and bc in sets.get("month", set())]
    rising = [bc for bc in sets.get("today", set()) if bc not in sets.get("month", set())]
    fading = [bc for bc in sets.get("month", set()) if bc not in sets.get("today", set()) and bc not in sets.get("yesterday", set())]
    today_only = [bc for bc in sets.get("today", set()) if bc not in sets.get("yesterday", set()) and bc not in sets.get("week", set()) and bc not in sets.get("month", set())]
    to_names = lambda bcs: [name_map.get(bc, bc) for bc in bcs]
    st = "warning" if len(fading) > 4 else ("attention" if len(fading) > 2 else "pass")
    return {"stable": to_names(stable), "rising": to_names(rising), "fading": to_names(fading), "todayOnly": to_names(today_only),
            "summary": {"stableCount": len(stable), "risingCount": len(rising), "fadingCount": len(fading), "todayOnlyCount": len(today_only)}, "status": st}

def calc_high_rank_stockout_alert(products, threshold=50):
    if not products: return {"alerts": [], "totalStockout": 0, "highCount": 0, "mediumCount": 0, "status": "uncountable"}
    alerts = sorted([{"name": p.get("product_name"), "salesRank": p.get("sales_rank")} for p in products if (p.get("sales_rank") or 999) <= threshold], key=lambda a: a["salesRank"])
    hh = any(a["salesRank"] <= 15 for a in alerts); hm = any(a["salesRank"] <= 35 for a in alerts)
    return {"alerts": alerts, "totalStockout": len(products), "highCount": sum(1 for a in alerts if a["salesRank"] <= 20),
            "mediumCount": sum(1 for a in alerts if 20 < a["salesRank"] <= 35), "status": "warning" if hh else ("attention" if hm else "pass")}

def calc_gross_margin(gp, rev):
    if not is_valid(gp) or not is_valid(rev) or rev <= 0: return {"marginPct": 0, "status": "uncountable"}
    pct = round_to(gp / rev * 100); return {"marginPct": pct, "status": "warning" if pct < 12 else ("attention" if pct < 18 else "pass")}

def calc_avg_order_value(rev, vc, baseline=None):
    if not is_valid(rev) or not is_valid(vc) or vc <= 0: return {"avgOrderValue": 0, "baselineAvgOrder": 0, "deviationPct": 0, "status": "uncountable"}
    avg = round_to(rev / vc, 2)
    if not is_valid(baseline) or baseline <= 0: return {"avgOrderValue": avg, "baselineAvgOrder": 0, "deviationPct": 0, "status": "uncountable"}
    dev = round_to((avg - baseline) / baseline * 100)
    return {"avgOrderValue": avg, "baselineAvgOrder": round_to(baseline, 2), "deviationPct": dev, "status": "warning" if abs(dev) > 50 else ("attention" if abs(dev) > 25 else "pass")}

def calc_member_penetration(mr, tr):
    if not is_valid(mr) or not is_valid(tr) or tr <= 0: return {"penetrationPct": 0, "status": "uncountable"}
    pct = round_to(mr / tr * 100); return {"penetrationPct": pct, "status": "warning" if pct < 3 else ("attention" if pct < 10 else "pass")}

def calc_member_vs_overall(m_avg, o_avg):
    if not is_valid(m_avg) or not is_valid(o_avg) or o_avg <= 0: return {"memberAvg": 0, "overallAvg": 0, "diffPct": 0, "status": "uncountable"}
    diff = round_to((m_avg - o_avg) / o_avg * 100)
    return {"memberAvg": round_to(m_avg, 2), "overallAvg": round_to(o_avg, 2), "diffPct": diff, "status": "warning" if diff < -60 else ("attention" if diff < -20 else "pass")}

def calc_platform_concentration(row):
    if not row or not is_valid(row.get("total_revenue")) or row["total_revenue"] <= 0: return {"dominantPlatform": None, "concentrationPct": 0, "status": "uncountable"}
    mt = row.get("meituan_revenue", 0) or 0; el = row.get("eleme_revenue", 0) or 0
    dp = "meituan" if mt >= el else "eleme"; pct = round_to(max(mt, el) / row["total_revenue"] * 100)
    return {"dominantPlatform": dp, "concentrationPct": pct, "status": "warning" if pct > 88 else ("attention" if pct > 78 else "pass")}

def calc_o2o_gross_margin(row):
    if not row or not is_valid(row.get("total_revenue")) or row["total_revenue"] <= 0 or not is_valid(row.get("gross_profit")): return {"marginPct": 0, "status": "uncountable"}
    pct = round_to(row["gross_profit"] / row["total_revenue"] * 100)
    return {"marginPct": pct, "status": "warning" if pct < 8 else ("attention" if pct < 13 else "pass")}

def calc_o2o_trend(rows, field):
    if not rows or len(rows) < 2: return {"changes": [], "overallTrend": "stable", "status": "uncountable"}
    changes = []
    for i in range(1, len(rows)):
        prev = rows[i-1].get(field); cur = rows[i].get(field)
        if not is_valid(prev) or not is_valid(cur) or prev == 0: continue
        changes.append({"period": rows[i].get("period"), "changePct": round_to((cur - prev) / prev * 100)})
    if not changes: return {"changes": [], "overallTrend": "stable", "status": "uncountable"}
    avg = sum(c["changePct"] for c in changes) / len(changes)
    trend = "rising" if avg > 3 else ("falling" if avg < -3 else "stable")
    tail = changes[-3:]; cf = len(tail) == 3 and all(c["changePct"] < 0 for c in tail)
    st = "warning" if trend == "falling" and cf else ("attention" if trend == "falling" else "pass")
    return {"changes": changes, "overallTrend": trend, "status": st}

def calc_hot_product_concentration(ranking, top_n=3):
    if not ranking: return {"topNReceiptCount": 0, "totalReceiptCount": 0, "concentrationPct": 0, "status": "uncountable"}
    vals = [p.get("receiptCount") or p.get("sales_receipt_count", 0) for p in ranking if is_valid(p.get("receiptCount") or p.get("sales_receipt_count"))]
    if not vals: return {"topNReceiptCount": 0, "totalReceiptCount": 0, "concentrationPct": 0, "status": "uncountable"}
    total = sum(vals)
    if total <= 0: return {"topNReceiptCount": 0, "totalReceiptCount": 0, "concentrationPct": 0, "status": "uncountable"}
    top = sum(vals[:top_n]); pct = round_to(top / total * 100)
    return {"topNReceiptCount": top, "totalReceiptCount": total, "concentrationPct": pct, "status": "warning" if pct > 78 else ("attention" if pct > 58 else "pass")}

def calc_stockout_rate(total_prods, out_prods):
    hs = 50; head = [p for p in (out_prods or []) if isinstance(p.get("sales_rank"), (int, float)) and p["sales_rank"] <= hs]
    pct = round_to(len(head) / hs * 100)
    return {"stockoutCount": len(out_prods or []), "headStockoutCount": len(head), "stockoutPct": pct, "status": "warning" if pct > 15 else ("attention" if pct > 5 else "pass")}

def calc_missing_category_rate(total_prods, missing_prods):
    hs = 50; head = [p for p in (missing_prods or []) if isinstance(p.get("sales_rank"), (int, float)) and p["sales_rank"] <= hs]
    pct = round_to(len(head) / hs * 100)
    return {"missingCount": len(missing_prods or []), "headMissingCount": len(head), "missingPct": pct, "status": "warning" if pct > 10 else ("attention" if pct > 3 else "pass")}

def calc_active_sku_count(ranking):
    if not ranking: return {"activeSKUs": 0, "status": "uncountable"}
    cnt = sum(1 for p in ranking if p.get("barcode") or p.get("name") or p.get("product_name"))
    return {"activeSKUs": cnt, "status": "warning" if cnt < 4 else ("attention" if cnt < 8 else "pass")}

# ── 检测指标 ──
def detect_consecutive_decline(rows, field, alert_days=3):
    base = calc_consecutive_change(rows, field)
    if base["status"] == "uncountable": return {"declineDays": 0, "totalDeclinePct": 0, "status": "uncountable"}
    dd = base["consecutiveDays"] if base["direction"] == "down" else 0
    tp = base["totalChangePct"] if base["direction"] == "down" else 0
    return {"declineDays": dd, "totalDeclinePct": tp, "status": "warning" if dd >= alert_days + 3 else ("attention" if dd >= alert_days else "pass")}

def detect_low_member_alert(mr, tr, threshold=5):
    p = calc_member_penetration(mr, tr)
    if p["status"] == "uncountable": return p
    return {"penetrationPct": p["penetrationPct"], "status": "warning" if p["penetrationPct"] < max(1, threshold - 2) else ("attention" if p["penetrationPct"] < threshold else "pass")}

def detect_channel_imbalance(cm):
    if not cm or cm.get("status") == "uncountable": return {"dominantChannel": None, "dominantPct": 0, "status": "uncountable"}
    return {"dominantChannel": cm.get("dominant"), "dominantPct": cm.get("dominantPct", 0),
            "status": "warning" if cm.get("dominantPct", 0) > 86 else ("attention" if cm.get("dominantPct", 0) > 72 else "pass")}

def prepare_store_status_label(params):
    needed = ["revenueChange", "grossMarginChange", "visitorChange", "avgOrderChange", "memberPenetration", "ecommerceRatio", "consecutiveDecline", "volatility"]
    if not params or any(not is_valid(params.get(k)) for k in needed):
        return {"suggestedLabels": [], "rawData": params, "status": "uncountable"}
    labels = []
    if params["revenueChange"] > 0 and params["grossMarginChange"] > 0: labels.append("稳步增长")
    if params["revenueChange"] > 0 and params["grossMarginChange"] < 0: labels.append("假增长")
    if params["visitorChange"] < 0 and params["avgOrderChange"] > 0: labels.append("流量下滑但客单补偿")
    if params["consecutiveDecline"] >= 5: labels.append("衰退")
    if params["volatility"] > 0.45: labels.append("波动式增长")
    if params["ecommerceRatio"] > 85: labels.append("促销依赖")
    if not labels: labels.append("瓶颈")
    st = "warning" if "衰退" in labels or "假增长" in labels else ("attention" if len(labels) > 1 else "pass")
    return {"suggestedLabels": labels, "rawData": params, "status": st}


# ── B类: 算法+AI 辅助指标 ──

def _translate_factor(key):
    return {"fromVisitor": "来客变化", "fromAvgOrder": "客单变化", "fromMember": "会员变化", "fromEcommerce": "电商变化"}.get(key, key)

def prepare_growth_decomposition(rows):
    if not rows or len(rows) < 2:
        return {"revenueChange": 0, "decomposition": {"fromVisitor": 0, "fromAvgOrder": 0, "fromMember": 0, "fromEcommerce": 0}, "aiPromptData": None, "status": "uncountable"}
    curr, prev = rows[0], rows[1]
    rc = curr["revenue"] - prev["revenue"]
    ca = curr["revenue"] / curr["visitorCount"] if curr.get("visitorCount", 0) > 0 else 0
    pa = prev["revenue"] / prev["visitorCount"] if prev.get("visitorCount", 0) > 0 else 0
    fv = round((curr.get("visitorCount", 0) - prev.get("visitorCount", 0)) * pa, 1)
    fa = round((ca - pa) * curr.get("visitorCount", 0), 1)
    fe = round((curr.get("ecommerceAmount", 0) or 0) - (prev.get("ecommerceAmount", 0) or 0), 1)
    fm = round((curr.get("memberAmount", 0) or 0) - (prev.get("memberAmount", 0) or 0), 1)
    decomp = {"fromVisitor": fv, "fromAvgOrder": fa, "fromMember": fm, "fromEcommerce": fe}
    am = max(decomp.items(), key=lambda x: abs(x[1]))
    rcp = round(rc / prev["revenue"] * 100, 1) if prev["revenue"] > 0 else 0
    return {"revenueChange": round(rc, 1), "decomposition": decomp, "aiPromptData": {
        "period": f"{prev.get('period')} → {curr.get('period')}", "revenueChange": round(rc, 1), "revenueChangePct": rcp,
        "decomposition": decomp, "primaryDriver": am[0], "primaryDriverValue": am[1],
        "summary": f"营收{'增长' if rc >= 0 else '下降'}{abs(round(rc, 1))}元({rcp}%)，主因: {_translate_factor(am[0])} {'+' if am[1] >= 0 else ''}{am[1]}元"},
        "status": "warning" if rcp < -8 else ("attention" if rcp < -3 else "pass")}

def prepare_sales_quality_check(params):
    p = params or {}
    rcp, gpcp, gmc, gmp = p.get("revenueChangePct"), p.get("grossProfitChangePct"), p.get("grossMarginCurrent"), p.get("grossMarginPrevious")
    if any(v is None for v in [rcp, gpcp, gmc, gmp]):
        return {"revenueGrowsFasterThanProfit": False, "marginDelta": 0, "qualityScore": 0, "aiPromptData": None, "status": "uncountable"}
    rgf = rcp > gpcp; md = round(gmc - gmp, 1)
    qs = max(0, min(100, round(100 - abs(rcp - gpcp))))
    st = "warning" if rcp > 0 and gpcp < -5 else ("attention" if rcp - gpcp > 25 else "pass")
    return {"revenueGrowsFasterThanProfit": rgf, "marginDelta": md, "qualityScore": qs,
            "aiPromptData": {"revenueChangePct": rcp, "grossProfitChangePct": gpcp, "marginDelta": md, "revenueGrowsFasterThanProfit": rgf,
                "summary": f"营收增速({rcp}%) {'大于' if rgf else '小于'} 毛利增速({gpcp}%)，毛利率变化{md}%，质量分{qs}"},
            "status": st}

def prepare_member_health_check(params):
    R = {"penetrationCritical": 3, "penetrationLow": 8, "penetrationPenaltyCritical": 45, "penetrationPenaltyLow": 25,
         "revenueChangeDropCritical": -10, "revenueChangeDropLow": -3, "revenuePenaltyCritical": 18, "revenuePenaltyLow": 10,
         "orderChangeDropCritical": -10, "orderChangeDropLow": -3, "orderPenaltyCritical": 15, "orderPenaltyLow": 8,
         "avgOrderGapCritical": -20, "avgOrderGapLow": -8, "avgOrderPenaltyCritical": 12, "avgOrderPenaltyLow": 6,
         "marginGapCritical": -4, "marginGapLow": -2, "marginPenaltyCritical": 10, "marginPenaltyLow": 5,
         "warningScore": 55, "attentionScore": 75}
    p = params or {}
    mr = p.get("memberRevenue"); tr = p.get("totalRevenue")
    if not is_valid(mr) or not is_valid(tr) or tr <= 0:
        return {"penetrationPct": 0, "memberAvgVsOverall": None, "memberGrossMarginVsOverall": None, "healthScore": 0, "signals": [], "aiPromptData": None, "status": "uncountable"}
    pp = round_to(mr / tr * 100)
    mavo = round_to((p.get("memberAvgOrder", 0) - p.get("overallAvgOrder", 1)) / p.get("overallAvgOrder", 1) * 100) if is_valid(p.get("memberAvgOrder")) and is_valid(p.get("overallAvgOrder")) and p.get("overallAvgOrder", 0) > 0 else None
    mgvo = round_to((p.get("memberGrossMarginPct") or 0) - (p.get("overallGrossMarginPct") or 0)) if is_valid(p.get("memberGrossMarginPct")) and is_valid(p.get("overallGrossMarginPct")) else None
    sigs = []; hs = 100
    if pp < R["penetrationCritical"]: sigs.append("会员渗透率过低"); hs -= R["penetrationPenaltyCritical"]
    elif pp < R["penetrationLow"]: sigs.append("会员渗透率偏低"); hs -= R["penetrationPenaltyLow"]
    mc = p.get("memberChangePct")
    if is_valid(mc) and mc < R["revenueChangeDropCritical"]: sigs.append("会员营收明显下滑"); hs -= R["revenuePenaltyCritical"]
    elif is_valid(mc) and mc < R["revenueChangeDropLow"]: sigs.append("会员营收轻度下滑"); hs -= R["revenuePenaltyLow"]
    moc = p.get("memberOrderChangePct")
    if is_valid(moc) and moc < R["orderChangeDropCritical"]: sigs.append("会员订单明显下滑"); hs -= R["orderPenaltyCritical"]
    elif is_valid(moc) and moc < R["orderChangeDropLow"]: sigs.append("会员订单轻度下滑"); hs -= R["orderPenaltyLow"]
    if is_valid(mavo) and mavo < R["avgOrderGapCritical"]: sigs.append("会员客单明显低于整体"); hs -= R["avgOrderPenaltyCritical"]
    elif is_valid(mavo) and mavo < R["avgOrderGapLow"]: sigs.append("会员客单偏低"); hs -= R["avgOrderPenaltyLow"]
    if is_valid(mgvo) and mgvo < R["marginGapCritical"]: sigs.append("会员毛利率明显偏低"); hs -= R["marginPenaltyCritical"]
    elif is_valid(mgvo) and mgvo < R["marginGapLow"]: sigs.append("会员毛利率偏低"); hs -= R["marginPenaltyLow"]
    hs = max(0, min(100, round(hs)))
    if not sigs: sigs.append("会员结构整体稳定")
    st = "warning" if hs < R["warningScore"] or pp < R["penetrationCritical"] else ("attention" if hs < R["attentionScore"] or len(sigs) >= 2 else "pass")
    return {"penetrationPct": pp, "memberAvgVsOverall": mavo, "memberGrossMarginVsOverall": mgvo, "healthScore": hs, "signals": sigs,
            "aiPromptData": {"penetrationPct": pp, "healthScore": hs, "signals": sigs, "summary": f"会员渗透率{pp}%，健康分{hs}，核心信号：{'、'.join(sigs[:3])}"}, "status": st}

def prepare_stockout_loss_estimate(products, avg_order_value=35):
    if not products: return {"estimatedDailyLoss": 0, "highImpactItems": [], "aiPromptData": None, "status": "uncountable"}
    aov = avg_order_value or 35
    def est(rank):
        if rank <= 10: return 8
        if rank <= 20: return 5
        if rank <= 50: return 3
        if rank <= 100: return 1.5
        return 0.5
    items = sorted([{"name": p.get("product_name"), "salesRank": p["sales_rank"], "estimatedDailyOrders": est(p["sales_rank"]),
                     "estimatedLoss": round(est(p["sales_rank"]) * aov)} for p in products if p.get("sales_rank") is not None], key=lambda i: i["salesRank"])
    edl = sum(i["estimatedLoss"] for i in items)
    hi = [i for i in items if i["salesRank"] <= 35]
    st = "warning" if edl > 900 else ("attention" if len(hi) > 1 else "pass")
    return {"estimatedDailyLoss": edl, "highImpactItems": hi, "aiPromptData": {
        "totalStockoutCount": len(products), "estimatedDailyLoss": edl, "avgOrderValueUsed": aov,
        "highImpactItems": [{"name": i["name"], "rank": i["salesRank"], "loss": f"{i['estimatedLoss']}元/天"} for i in hi],
        "summary": f"{len(products)}个缺货商品，预估日损失{edl}元，其中{len(hi)}个高影响品(rank≤35)"}, "status": st}

def prepare_channel_risk_assessment(params):
    CR = {"dominantHigh": 85, "dominantMedium": 72, "platformHigh": 88, "platformMedium": 78,
          "scoreDominantHigh": 38, "scoreDominantMedium": 22, "scorePlatformHigh": 28, "scorePlatformMedium": 16,
          "scoreTrendContinuousFalling": 28, "scoreTrendFalling": 15, "scoreCoupledDecline": 10, "warningScore": 70, "attentionScore": 35}
    p = params or {}; cm = p.get("channelMix"); o2o = p.get("o2oTrend"); pc = p.get("platformConcentration")
    if not cm or cm.get("status") == "uncountable": return {"riskFactors": [], "riskScore": 0, "aiPromptData": None, "status": "uncountable"}
    rf = []; rs = 0
    dp = cm.get("dominantPct", 0)
    if is_valid(dp) and dp > CR["dominantHigh"]: rf.append(f"渠道高度集中({cm.get('dominant','?')}:{dp}%)"); rs += CR["scoreDominantHigh"]
    elif is_valid(dp) and dp > CR["dominantMedium"]: rf.append(f"渠道偏集中({cm.get('dominant','?')}:{dp}%)"); rs += CR["scoreDominantMedium"]
    if pc and pc.get("status") != "uncountable":
        cp = pc.get("concentrationPct", 0)
        if cp > CR["platformHigh"]: rf.append(f"平台单边依赖({pc.get('dominantPlatform')}:{cp}%)"); rs += CR["scorePlatformHigh"]
        elif cp > CR["platformMedium"]: rf.append(f"平台集中度偏高({pc.get('dominantPlatform')}:{cp}%)"); rs += CR["scorePlatformMedium"]
    if o2o and o2o.get("status") != "uncountable":
        tail = (o2o.get("changes") or [])[-3:]
        af = len(tail) == 3 and all(c["changePct"] < 0 for c in tail)
        if o2o.get("overallTrend") == "falling" and af: rf.append("O2O连续走弱(近3期均下降)"); rs += CR["scoreTrendContinuousFalling"]
        elif o2o.get("overallTrend") == "falling": rf.append("O2O整体走弱"); rs += CR["scoreTrendFalling"]
    if cm.get("dominant") == "电商" and any("O2O" in f for f in rf): rf.append("高依赖渠道同步下滑"); rs += CR["scoreCoupledDecline"]
    rs = max(0, min(100, round(rs)))
    st = "warning" if rs >= CR["warningScore"] else ("attention" if rs >= CR["attentionScore"] else "pass")
    return {"riskFactors": rf, "riskScore": rs, "aiPromptData": {
        "dominantChannel": cm.get("dominant"), "dominantPct": dp, "riskScore": rs, "riskFactors": rf,
        "summary": f"渠道风险分{rs}，{'关键风险：' + '；'.join(rf[:3]) if rf else '结构总体稳定'}"}, "status": st}

# ── B3: 异常汇总 ──

def _format_list(arr, limit=3, map_fn=None):
    if not arr: return ""
    mf = map_fn or (lambda x: str(x))
    items = "、".join(mf(a) for a in arr[:limit])
    return f"[{items}{'等' if len(arr) > limit else ''}]"

def _extract_alert_detail(name, result):
    if "ChannelMix" in name:
        top = sorted(result.get("channels", []), key=lambda c: c.get("pct", 0), reverse=True)[:2]
        return f"主导渠道为{result.get('dominant')}，前二: {'、'.join(f'{c['label']}({c['pct']}%)' for c in top)}"
    if "ConsecutiveChange" in name:
        d = "下降" if result.get("direction") == "down" else "上升"
        return f"连续{d}{result.get('consecutiveDays')}天 ({result.get('startValue')}→{result.get('endValue')})，累计{result.get('totalChangePct')}%"
    if "GrossMarginTrend" in name:
        b = "<" if result.get("latestMargin", 0) < result.get("avgMargin", 0) else ">"
        return f"毛利率{result.get('trend')} (最新{result.get('latestMargin')}% {b} 均值{result.get('avgMargin')}%)"
    if "ProductStability" in name:
        s = result.get("summary", {})
        parts = []
        if s.get("fadingCount", 0) > 0: parts.append(f"退热品:{s['fadingCount']} {_format_list(result.get('fading', []), 2)}")
        if s.get("todayOnlyCount", 0) > 0: parts.append(f"仅今日:{s['todayOnlyCount']} {_format_list(result.get('todayOnly', []), 2)}")
        if s.get("risingCount", 0) > 0: parts.append(f"新晋品:{s['risingCount']} {_format_list(result.get('rising', []), 2)}")
        return "；".join(parts) or "商品结构稳定"
    if "StockoutAlert" in name:
        parts = [f"总缺货:{result.get('totalStockout', 0)}"]
        if result.get("highCount", 0) > 0:
            ha = [a for a in result.get("alerts", []) if a.get("salesRank", 999) <= 20]
            parts.append(f"高危:{result['highCount']} {_format_list(ha, 2, lambda a: f"{a['name']}(排{a['salesRank']})")}")
        return "，".join(parts)
    if "StockoutLoss" in name:
        desc = f"预估日损失{result.get('estimatedDailyLoss', 0)}元"
        hi = result.get("highImpactItems", [])
        if hi: desc += f"，高损品: {_format_list(hi, 2, lambda i: f"{i['name']}(损{i['estimatedLoss']}元)")}"
        return desc
    if "GrowthDecomposition" in name:
        pd = _translate_factor(result.get("aiPromptData", {}).get("primaryDriver", "unknown"))
        pv = result.get("aiPromptData", {}).get("primaryDriverValue", 0)
        return f"营收变动{result.get('revenueChange', 0)}元，主因: {pd}({'+' if pv > 0 else ''}{pv}元)"
    if "RevenueChange" in name:
        return f"营收{'下降' if result.get('direction') == 'down' else '增长'} {abs(result.get('changePct', 0))}%"
    if "GrossMargin" in name:
        return f"毛利率{result.get('marginPct') or result.get('latestMargin', 0)}%"
    if "MemberPenetration" in name: return f"会员渗透率 {result.get('penetrationPct', 0)}%"
    if "MemberVsOverall" in name: return f"会员客单较整体 {result.get('diffPct', 0)}%"
    if "PlatformConcentration" in name: return f"{result.get('dominantPlatform', '平台')}占比 {result.get('concentrationPct', 0)}%"
    if "StockoutRate" in name: return f"Top50缺货率 {result.get('stockoutPct', 0)}% (前50名缺{result.get('headStockoutCount', 0)}个)"
    if "MissingCategoryRate" in name: return f"Top50缺种率 {result.get('missingPct', 0)}% (前50名缺{result.get('headMissingCount', 0)}个)"
    if "O2OvsTotal" in name: return f"O2O营收占比达 {result.get('ratioPct', 0)}%"
    if "SalesQualityCheck" in name: return f"销售质量分{result.get('qualityScore', 0)}"
    if "MemberHealthCheck" in name: return f"会员健康分{result.get('healthScore', 0)}，渗透率{result.get('penetrationPct', 0)}%"
    if "ChannelRiskAssessment" in name: return f"渠道风险分{result.get('riskScore', 0)}，风险项{len(result.get('riskFactors', []))}个"
    return str(result)[:100]


def prepare_anomaly_summary(metric_results):
    if not metric_results: return {"totalAlerts": 0, "sortedAlerts": [], "aiPromptData": None, "status": "uncountable"}
    pmap = {"warning": 3, "attention": 2, "pass": 1, "uncountable": 0}
    alerts = []
    for name, result in metric_results.items():
        if not isinstance(result, dict) or result.get("status") == "uncountable": continue
        alerts.append({"metric": name, "status": result["status"], "priority": pmap.get(result["status"], 0), "detail": _extract_alert_detail(name, result)})
    alerts.sort(key=lambda a: a["priority"], reverse=True)
    tally = to_tally(metric_results)
    st = "warning" if tally["warning"] > 0 else ("attention" if tally["attention"] > 0 else "pass")
    return {"totalAlerts": len(alerts), "sortedAlerts": alerts, "aiPromptData": {
        "totalMetrics": len(metric_results), "tally": tally,
        "alerts": [{"metric": a["metric"], "severity": a["status"], "detail": a["detail"]} for a in alerts],
        "summary": f"共{len(metric_results)}项指标，{tally['warning']}项warning，{tally['attention']}项attention，{tally['pass']}项pass，{tally['uncountable']}项无法计算"}, "status": st}
