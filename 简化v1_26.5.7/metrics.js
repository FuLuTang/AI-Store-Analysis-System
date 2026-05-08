/**
 * metrics.js — 算法处理模块
 * 
 * 职责：从原始 JSON 数据中提取并计算经营指标
 * 架构位置：流程图中的 "算法处理" → alg1数据整理 + alg2算指标 + alg3检查异常
 */

const fs = require('fs');
const path = require('path');

// ============================================================
// alg1: 数据整理 — Normalize 层
// 把各种 JSON 的字段名差异抹平，输出统一结构
// ============================================================

/**
 * 统一概览 businessTable.rows 的字段名
 * 解决：概览-日用 ecommerce_amount，概览-月用 online_amount
 */
function normalizeOverviewRows(raw) {
  return raw.businessTable.rows.map(row => ({
    period:           row.period,
    revenue:          row.retail_amount,
    grossProfit:      row.gross_profit,
    visitorCount:     row.visitor_count,
    memberAmount:     row.member_amount,
    memberGrossProfit: row.member_gross_profit,
    ecommerceAmount:  row.ecommerce_amount ?? row.online_amount,
    ecommerceGrossProfit: row.ecommerce_gross_profit ?? row.online_gross_profit,
  }));
}

/**
 * 从 summary.metrics 数组中按 key 取值
 */
function getMetricByKey(summary, key) {
  const m = summary.metrics.find(m => m.key === key);
  return m || null;
}

/**
 * 统一热销排名数据，过滤掉无效条目
 */
function normalizeHotProducts(raw) {
  return raw.ranking
    .filter(p => p.sales_receipt_count != null)
    .map(p => ({
      rank:         p.rank,
      name:         p.product_name,
      barcode:      p.barcode,
      receiptCount: p.sales_receipt_count,
      quantity:     p.sales_quantity,
    }));
}

function isValidNumber(v) {
  return typeof v === 'number' && Number.isFinite(v);
}

function roundTo(v, digits = 1) {
  if (!isValidNumber(v)) return 0;
  const factor = 10 ** digits;
  return Math.round(v * factor) / factor;
}

function toTally(results) {
  const tally = { warning: 0, attention: 0, pass: 0, uncountable: 0 };
  Object.values(results || {}).forEach(result => {
    const k = result?.status;
    if (k && tally[k] != null) tally[k]++;
  });
  return tally;
}

// ============================================================
// alg2: 算指标 — 5 个核心指标函数
// ============================================================

/**
 * 1. calcChannelMix — 渠道结构占比
 * 
 * 输入: sourceDistribution (概览 JSON 中的 sourceDistribution 对象)
 * 输出: { total, channels: [{key, label, value, pct}], dominant, isDominant }
 */
function calcChannelMix(sourceDistribution) {
  if (!sourceDistribution || !sourceDistribution.items || sourceDistribution.items.length === 0) {
    return { total: 0, channels: [], dominant: null, dominantPct: 0, status: 'uncountable' };
  }

  const items = sourceDistribution.items.filter(item => isValidNumber(item?.value));
  if (items.length === 0) {
    return { total: 0, channels: [], dominant: null, dominantPct: 0, status: 'uncountable' };
  }
  const total = items.reduce((sum, item) => sum + item.value, 0);
  if (total <= 0) {
    return { total: 0, channels: [], dominant: null, dominantPct: 0, status: 'uncountable' };
  }

  const channels = items.map(item => ({
    key:   item.key,
    label: item.label,
    value: item.value,
    pct:   total > 0 ? Math.round(item.value / total * 1000) / 10 : 0,  // 保留1位小数
  }));

  // 找出占比最大的渠道
  const dominant = channels.reduce((a, b) => a.pct > b.pct ? a : b);

  return {
    total:       Math.round(total * 100) / 100,
    channels,
    dominant:    dominant.label,
    dominantPct: dominant.pct,
    status:      dominant.pct > 85 ? 'warning' : dominant.pct > 70 ? 'attention' : 'pass',
  };
}


/**
 * 1.5. calcRevenueChange — 营收环比变化率
 * 
 * 输入: rows (normalizeOverviewRows 输出，至少2行)
 * 输出: { current, previous, changePct, direction, status }
 */
function calcRevenueChange(rows) {
  if (!rows || rows.length < 2) {
    return { current: 0, previous: 0, changePct: 0, direction: 'flat', status: 'uncountable' };
  }
  const current = rows[0].revenue || rows[0].retail_amount || 0;
  const previous = rows[1].revenue || rows[1].retail_amount || 0;
  
  if (previous === 0) {
     return { current, previous, changePct: 0, direction: current > 0 ? 'up' : 'flat', status: 'uncountable' };
  }
  
  const changePct = Math.round((current - previous) / previous * 1000) / 10;
  const direction = changePct > 0 ? 'up' : changePct < 0 ? 'down' : 'flat';
  
  const absChange = Math.abs(changePct);
  const status = absChange > 65 ? 'warning' : absChange > 30 ? 'attention' : 'pass';
  
  return { current, previous, changePct, direction, status };
}


/**
 * 1.6. calcO2OvsTotal — O2O营收占整体营收比重
 * 
 * 输入: o2oRevenue (O2O总营收), overviewRevenue (整体概览营收)
 * 输出: { ratioPct, status }
 */
function calcO2OvsTotal(o2oRevenue, overviewRevenue) {
  if (!isValidNumber(overviewRevenue) || overviewRevenue <= 0 || !isValidNumber(o2oRevenue)) {
    return { ratioPct: 0, status: 'uncountable' };
  }
  
  const ratioPct = Math.round((o2oRevenue / overviewRevenue) * 1000) / 10;
  const status = ratioPct > 88 ? 'warning' : ratioPct > 70 ? 'attention' : 'pass';
  
  return { ratioPct, status };
}


/**
 * 2. calcConsecutiveChange — 连续涨跌天数
 * 
 * 输入: rows (normalizeOverviewRows 输出，按时间倒序), field (要检测的字段名)
 * 输出: { direction, consecutiveDays, startPeriod, endPeriod, totalChangePct, isAlert }
 */
function calcConsecutiveChange(rows, field) {
  if (!rows || rows.length < 2 || rows[0][field] == null || rows[1][field] == null) {
    return { direction: 'flat', consecutiveDays: 0, totalChangePct: 0, status: 'uncountable' };
  }

  // rows 是按时间倒序的（最新的在前面）
  // 比较 rows[0] vs rows[1] 确定当前方向
  const currentDirection = rows[0][field] > rows[1][field] ? 'up' : 
                           rows[0][field] < rows[1][field] ? 'down' : 'flat';

  if (currentDirection === 'flat') {
    return { direction: 'flat', consecutiveDays: 1, isAlert: false };
  }

  let count = 1;
  for (let i = 1; i < rows.length - 1; i++) {
    const dir = rows[i][field] > rows[i + 1][field] ? 'up' :
                rows[i][field] < rows[i + 1][field] ? 'down' : 'flat';
    if (dir === currentDirection) {
      count++;
    } else {
      break;
    }
  }

  // 计算整段涨跌幅
  const startValue = rows[count][field];  // 变化起点
  const endValue = rows[0][field];        // 最新值
  const totalChangePct = startValue !== 0
    ? Math.round((endValue - startValue) / startValue * 1000) / 10
    : 0;

  return {
    direction: currentDirection,
    consecutiveDays: count,
    startPeriod: rows[count].period,
    endPeriod: rows[0].period,
    startValue,
    endValue,
    totalChangePct,
    status: currentDirection === 'down' && count >= 6 ? 'warning' :
            currentDirection === 'down' && count >= 4 ? 'attention' : 'pass',
  };
}


/**
 * 3. calcGrossMarginTrend — 毛利率变化趋势
 * 
 * 输入: rows (normalizeOverviewRows 输出，按时间倒序)
 * 输出: { margins: [{period, marginPct}], avgMargin, latestMargin, trend, slope, isAlert }
 */
function calcGrossMarginTrend(rows) {
  if (!rows || rows.length < 2) {
    return { margins: [], avgMargin: 0, latestMargin: 0, trend: 'stable', slope: 0, status: 'uncountable' };
  }
  // 计算每期毛利率
  const margins = rows.map(row => ({
    period:    row.period,
    marginPct: row.revenue > 0
      ? Math.round(row.grossProfit / row.revenue * 1000) / 10
      : 0,
  }));

  const values = margins.map(m => m.marginPct);
  const avg = values.reduce((a, b) => a + b, 0) / values.length;

  // 简单线性回归算斜率（x轴用索引，注意rows是倒序所以要反转）
  const reversed = [...values].reverse(); // 变成时间正序
  const n = reversed.length;
  let sumX = 0, sumY = 0, sumXY = 0, sumX2 = 0;
  for (let i = 0; i < n; i++) {
    sumX += i;
    sumY += reversed[i];
    sumXY += i * reversed[i];
    sumX2 += i * i;
  }
  const slope = n > 1 ? (n * sumXY - sumX * sumY) / (n * sumX2 - sumX * sumX) : 0;

  // 判断趋势
  let trend = 'stable';
  if (slope > 0.5)  trend = 'improving';
  if (slope < -0.5) trend = 'declining';

  return {
    margins,
    avgMargin:    Math.round(avg * 10) / 10,
    latestMargin: margins[0].marginPct,
    trend,
    slope:        Math.round(slope * 100) / 100,
    status:       trend === 'declining' && margins[0].marginPct < (avg - 3) ? 'warning' :
                  trend === 'declining' ? 'attention' : 'pass',
  };
}


/**
 * 4. calcProductStability — 爆品稳定性（跨时段对比）
 * 
 * 输入: hotData: { today: HotProduct[], yesterday: HotProduct[], week: HotProduct[], month: HotProduct[] }
 *       （每个都是 normalizeHotProducts 的输出）
 * 输出: { stable, rising, fading, todayOnly, summary }
 */
function calcProductStability(hotData) {
  if (!hotData || !hotData.today || !hotData.month || hotData.today.length === 0) {
    return { stable: [], rising: [], fading: [], todayOnly: [], summary: { stableCount: 0, risingCount: 0, fadingCount: 0, todayOnlyCount: 0 }, status: 'uncountable' };
  }

  // 用 barcode 建立各时段的集合
  const sets = {};
  for (const [period, products] of Object.entries(hotData)) {
    sets[period] = new Set(products.map(p => p.barcode).filter(Boolean));
  }

  // 建立 barcode → 商品名 的映射（取各时段中最新的名字）
  const nameMap = {};
  for (const products of Object.values(hotData)) {
    for (const p of products) {
      if (p.barcode) nameMap[p.barcode] = p.name;
    }
  }

  // 稳定品：所有时段都在
  const allBarcodes = new Set([...sets.today, ...sets.yesterday, ...sets.week, ...sets.month]);
  const stable = [...allBarcodes].filter(bc =>
    sets.today.has(bc) && sets.week.has(bc) && sets.month.has(bc)
  );

  // 新晋品：今天有，但月榜没有
  const rising = [...sets.today].filter(bc => !sets.month.has(bc));

  // 退热品：月榜有，但今天和昨天都没有
  const fading = [...sets.month].filter(bc => !sets.today.has(bc) && !sets.yesterday.has(bc));

  // 仅今日出现
  const todayOnly = [...sets.today].filter(bc =>
    !sets.yesterday.has(bc) && !sets.week.has(bc) && !sets.month.has(bc)
  );

  const toNames = (barcodes) => barcodes.map(bc => nameMap[bc] || bc);

  return {
    stable:    toNames(stable),
    rising:    toNames(rising),
    fading:    toNames(fading),
    todayOnly: toNames(todayOnly),
    summary: {
      stableCount:    stable.length,
      risingCount:    rising.length,
      fadingCount:    fading.length,
      todayOnlyCount: todayOnly.length,
    },
    status: fading.length > 4 ? 'warning' : fading.length > 2 ? 'attention' : 'pass',
  };
}


/**
 * 5. calcHighRankStockoutAlert — 高排名缺货预警
 * 
 * 输入: outOfStock: Top500Product[], threshold: number (默认50)
 * 输出: { alerts: [{name, salesRank, severity}], totalStockout, highCount, mediumCount }
 */
function calcHighRankStockoutAlert(outOfStockProducts, threshold = 50) {
  if (!outOfStockProducts || outOfStockProducts.length === 0) {
    return { alerts: [], totalStockout: 0, highCount: 0, mediumCount: 0, status: 'uncountable' };
  }

  const alerts = outOfStockProducts
    .filter(p => p.sales_rank <= threshold)
    .map(p => ({
      name:      p.product_name,
      salesRank: p.sales_rank,
    }))
    .sort((a, b) => a.salesRank - b.salesRank);

  const hasHighRank = alerts.some(a => a.salesRank <= 15);
  const hasMedRank  = alerts.some(a => a.salesRank <= 35);

  return {
    alerts,
    totalStockout: outOfStockProducts.length,
    highCount:     alerts.filter(a => a.salesRank <= 20).length,
    mediumCount:   alerts.filter(a => a.salesRank > 20 && a.salesRank <= 35).length,
    status:        hasHighRank ? 'warning' : hasMedRank ? 'attention' : 'pass',
  };
}

function calcGrossMargin(grossProfit, revenue) {
  if (!isValidNumber(grossProfit) || !isValidNumber(revenue) || revenue <= 0) {
    return { marginPct: 0, status: 'uncountable' };
  }
  const marginPct = roundTo((grossProfit / revenue) * 100, 1);
  const status = marginPct < 12 ? 'warning' : marginPct < 18 ? 'attention' : 'pass';
  return { marginPct, status };
}

function calcAvgOrderValue(revenue, visitorCount, baselineAvgOrder = null) {
  if (!isValidNumber(revenue) || !isValidNumber(visitorCount) || visitorCount <= 0) {
    return { avgOrderValue: 0, baselineAvgOrder: 0, deviationPct: 0, status: 'uncountable' };
  }
  const avgOrderValue = roundTo(revenue / visitorCount, 2);
  if (!isValidNumber(baselineAvgOrder) || baselineAvgOrder <= 0) {
    return { avgOrderValue, baselineAvgOrder: 0, deviationPct: 0, status: 'uncountable' };
  }
  const deviationPct = roundTo(((avgOrderValue - baselineAvgOrder) / baselineAvgOrder) * 100, 1);
  const absDev = Math.abs(deviationPct);
  const status = absDev > 50 ? 'warning' : absDev > 25 ? 'attention' : 'pass';
  return { avgOrderValue, baselineAvgOrder: roundTo(baselineAvgOrder, 2), deviationPct, status };
}

function calcMemberPenetration(memberRevenue, totalRevenue) {
  if (!isValidNumber(memberRevenue) || !isValidNumber(totalRevenue) || totalRevenue <= 0) {
    return { penetrationPct: 0, status: 'uncountable' };
  }
  const penetrationPct = roundTo((memberRevenue / totalRevenue) * 100, 1);
  const status = penetrationPct < 3 ? 'warning' : penetrationPct < 10 ? 'attention' : 'pass';
  return { penetrationPct, status };
}

function calcMemberVsOverall(memberAvgOrder, overallAvgOrder) {
  if (!isValidNumber(memberAvgOrder) || !isValidNumber(overallAvgOrder) || overallAvgOrder <= 0) {
    return { memberAvg: 0, overallAvg: 0, diffPct: 0, status: 'uncountable' };
  }
  const diffPct = roundTo(((memberAvgOrder - overallAvgOrder) / overallAvgOrder) * 100, 1);
  const status = diffPct < -60 ? 'warning' : diffPct < -20 ? 'attention' : 'pass';
  return { memberAvg: roundTo(memberAvgOrder, 2), overallAvg: roundTo(overallAvgOrder, 2), diffPct, status };
}

function calcPlatformConcentration(row) {
  if (!row || !isValidNumber(row.total_revenue) || row.total_revenue <= 0) {
    return { dominantPlatform: null, concentrationPct: 0, status: 'uncountable' };
  }
  const mt = isValidNumber(row.meituan_revenue) ? row.meituan_revenue : 0;
  const el = isValidNumber(row.eleme_revenue) ? row.eleme_revenue : 0;
  const dominantPlatform = mt >= el ? 'meituan' : 'eleme';
  const concentrationPct = roundTo((Math.max(mt, el) / row.total_revenue) * 100, 1);
  const status = concentrationPct > 88 ? 'warning' : concentrationPct > 78 ? 'attention' : 'pass';
  return { dominantPlatform, concentrationPct, status };
}

function calcO2OGrossMargin(row) {
  if (!row || !isValidNumber(row.total_revenue) || row.total_revenue <= 0 || !isValidNumber(row.gross_profit)) {
    return { marginPct: 0, status: 'uncountable' };
  }
  const marginPct = roundTo((row.gross_profit / row.total_revenue) * 100, 1);
  const status = marginPct < 8 ? 'warning' : marginPct < 13 ? 'attention' : 'pass';
  return { marginPct, status };
}

function calcO2OTrend(rows, field) {
  if (!rows || rows.length < 2) {
    return { changes: [], overallTrend: 'stable', status: 'uncountable' };
  }
  const changes = [];
  for (let i = 1; i < rows.length; i++) {
    const prev = rows[i - 1]?.[field];
    const curr = rows[i]?.[field];
    if (!isValidNumber(prev) || !isValidNumber(curr) || prev === 0) continue;
    changes.push({
      period: rows[i].period,
      changePct: roundTo(((curr - prev) / prev) * 100, 1),
    });
  }
  if (changes.length === 0) {
    return { changes: [], overallTrend: 'stable', status: 'uncountable' };
  }
  const avg = changes.reduce((sum, c) => sum + c.changePct, 0) / changes.length;
  const overallTrend = avg > 3 ? 'rising' : avg < -3 ? 'falling' : 'stable';
  const tail = changes.slice(-3);
  const consecutiveFalling = tail.length === 3 && tail.every(c => c.changePct < 0);
  const status = overallTrend === 'falling' && consecutiveFalling ? 'warning' :
                 overallTrend === 'falling' ? 'attention' : 'pass';
  return { changes, overallTrend, status };
}

function calcHotProductConcentration(ranking, topN = 3) {
  if (!ranking || ranking.length === 0) {
    return { topNReceiptCount: 0, totalReceiptCount: 0, concentrationPct: 0, status: 'uncountable' };
  }
  const valid = ranking.filter(p => isValidNumber(p.receiptCount) || isValidNumber(p.sales_receipt_count));
  if (valid.length === 0) {
    return { topNReceiptCount: 0, totalReceiptCount: 0, concentrationPct: 0, status: 'uncountable' };
  }
  const values = valid.map(p => p.receiptCount ?? p.sales_receipt_count);
  const totalReceiptCount = values.reduce((sum, v) => sum + v, 0);
  if (totalReceiptCount <= 0) {
    return { topNReceiptCount: 0, totalReceiptCount: 0, concentrationPct: 0, status: 'uncountable' };
  }
  const topNReceiptCount = values.slice(0, topN).reduce((sum, v) => sum + v, 0);
  const concentrationPct = roundTo((topNReceiptCount / totalReceiptCount) * 100, 1);
  const status = concentrationPct > 78 ? 'warning' : concentrationPct > 58 ? 'attention' : 'pass';
  return { topNReceiptCount, totalReceiptCount, concentrationPct, status };
}

function calcStockoutRate(top500Total, outOfStock) {
  const totalCount = top500Total?.length || 0;
  const stockoutCount = outOfStock?.length || 0;
  if (totalCount <= 0) {
    return { stockoutCount, stockoutPct: 0, status: 'uncountable' };
  }
  const stockoutPct = roundTo((stockoutCount / totalCount) * 100, 1);
  const status = stockoutPct > 18 ? 'warning' : stockoutPct > 8 ? 'attention' : 'pass';
  return { stockoutCount, stockoutPct, status };
}

function calcMissingCategoryRate(top500Total, missingCategory) {
  const totalCount = top500Total?.length || 0;
  const missingCount = missingCategory?.length || 0;
  if (totalCount <= 0) {
    return { missingCount, missingPct: 0, status: 'uncountable' };
  }
  const missingPct = roundTo((missingCount / totalCount) * 100, 1);
  const status = missingPct > 12 ? 'warning' : missingPct > 5 ? 'attention' : 'pass';
  return { missingCount, missingPct, status };
}

function calcActiveSKUCount(ranking) {
  if (!ranking || ranking.length === 0) {
    return { activeSKUs: 0, status: 'uncountable' };
  }
  const activeSKUs = ranking.filter(p => p?.barcode || p?.name || p?.product_name).length;
  const status = activeSKUs < 4 ? 'warning' : activeSKUs < 8 ? 'attention' : 'pass';
  return { activeSKUs, status };
}

function detectConsecutiveDecline(rows, field, alertDays = 3) {
  const base = calcConsecutiveChange(rows, field);
  if (base.status === 'uncountable') {
    return { declineDays: 0, totalDeclinePct: 0, status: 'uncountable' };
  }
  const declineDays = base.direction === 'down' ? base.consecutiveDays : 0;
  const totalDeclinePct = base.direction === 'down' ? base.totalChangePct : 0;
  const status = declineDays >= (alertDays + 3) ? 'warning' :
                 declineDays >= alertDays ? 'attention' : 'pass';
  return { declineDays, totalDeclinePct, status };
}

function detectLowMemberAlert(memberRevenue, totalRevenue, threshold = 5) {
  const penetration = calcMemberPenetration(memberRevenue, totalRevenue);
  if (penetration.status === 'uncountable') return penetration;
  const status = penetration.penetrationPct < Math.max(1, threshold - 2) ? 'warning' :
                 penetration.penetrationPct < threshold ? 'attention' : 'pass';
  return { penetrationPct: penetration.penetrationPct, status };
}

function detectChannelImbalance(channelMix) {
  if (!channelMix || channelMix.status === 'uncountable') {
    return { dominantChannel: null, dominantPct: 0, status: 'uncountable' };
  }
  return {
    dominantChannel: channelMix.dominant,
    dominantPct: channelMix.dominantPct,
    status: channelMix.dominantPct > 86 ? 'warning' : channelMix.dominantPct > 72 ? 'attention' : 'pass',
  };
}

function prepareStoreStatusLabel(params) {
  const needed = ['revenueChange', 'grossMarginChange', 'visitorChange', 'avgOrderChange', 'memberPenetration', 'ecommerceRatio', 'consecutiveDecline', 'volatility'];
  if (!params || needed.some(k => !isValidNumber(params[k]))) {
    return { suggestedLabels: [], rawData: params || null, status: 'uncountable' };
  }
  const labels = [];
  if (params.revenueChange > 0 && params.grossMarginChange > 0) labels.push('稳步增长');
  if (params.revenueChange > 0 && params.grossMarginChange < 0) labels.push('假增长');
  if (params.visitorChange < 0 && params.avgOrderChange > 0) labels.push('流量下滑但客单补偿');
  if (params.consecutiveDecline >= 5) labels.push('衰退');
  if (params.volatility > 0.45) labels.push('波动式增长');
  if (params.ecommerceRatio > 85) labels.push('促销依赖');
  if (labels.length === 0) labels.push('瓶颈');
  const status = labels.includes('衰退') || labels.includes('假增长') ? 'warning' :
                 labels.length > 1 ? 'attention' : 'pass';
  return { suggestedLabels: labels, rawData: params, status };
}


// ============================================================
// B类: 算法+AI 辅助指标（算法算数据，AI做解读）
// ============================================================

/**
 * B1. prepareGrowthDecomposition — 增长来源拆解
 * 
 * 把营收变化拆分为：来客变化贡献、客单变化贡献、会员贡献、电商贡献
 * 让 AI 知道"增长（或下滑）到底从哪来的"
 * 
 * 输入: rows (normalizeOverviewRows 输出，按时间倒序，至少2行)
 * 输出: { revenueChange, decomposition, aiPromptData, status }
 */
function prepareGrowthDecomposition(rows) {
  if (!rows || rows.length < 2) {
    return {
      revenueChange: 0,
      decomposition: { fromVisitor: 0, fromAvgOrder: 0, fromMember: 0, fromEcommerce: 0 },
      aiPromptData: null,
      status: 'uncountable',
    };
  }

  const curr = rows[0];  // 最新期
  const prev = rows[1];  // 上一期

  const revenueChange = curr.revenue - prev.revenue;

  // 客单价 = 营收 / 来客
  const currAvgOrder = curr.visitorCount > 0 ? curr.revenue / curr.visitorCount : 0;
  const prevAvgOrder = prev.visitorCount > 0 ? prev.revenue / prev.visitorCount : 0;

  // 来客贡献 = (本期来客 - 上期来客) × 上期客单
  const fromVisitor = (curr.visitorCount - prev.visitorCount) * prevAvgOrder;
  // 客单贡献 = (本期客单 - 上期客单) × 本期来客
  const fromAvgOrder = (currAvgOrder - prevAvgOrder) * curr.visitorCount;
  // 电商贡献 = 本期电商 - 上期电商
  const fromEcommerce = (curr.ecommerceAmount || 0) - (prev.ecommerceAmount || 0);
  // 会员贡献 = 本期会员 - 上期会员
  const fromMember = (curr.memberAmount || 0) - (prev.memberAmount || 0);

  const round1 = v => Math.round(v * 10) / 10;

  const decomposition = {
    fromVisitor:   round1(fromVisitor),
    fromAvgOrder:  round1(fromAvgOrder),
    fromMember:    round1(fromMember),
    fromEcommerce: round1(fromEcommerce),
  };

  // 找最大影响因素
  const factors = Object.entries(decomposition);
  const absMax = factors.reduce((a, b) => Math.abs(a[1]) > Math.abs(b[1]) ? a : b);

  // 营收变化百分比
  const revenueChangePct = prev.revenue > 0
    ? Math.round((revenueChange / prev.revenue) * 1000) / 10
    : 0;

  const aiPromptData = {
    period: `${prev.period} → ${curr.period}`,
    revenueChange: round1(revenueChange),
    revenueChangePct,
    decomposition,
    primaryDriver: absMax[0],
    primaryDriverValue: absMax[1],
    summary: `营收${revenueChange >= 0 ? '增长' : '下降'}${Math.abs(round1(revenueChange))}元(${revenueChangePct}%)，` +
             `主因: ${translateFactor(absMax[0])} ${absMax[1] >= 0 ? '+' : ''}${absMax[1]}元`,
  };

  // status: 下降且超5%→warning, 下降→attention, else pass
  const status = revenueChangePct < -8 ? 'warning' :
                 revenueChangePct < -3 ? 'attention' : 'pass';

  return { revenueChange: round1(revenueChange), decomposition, aiPromptData, status };
}


/**
 * B1.5. prepareSalesQualityCheck — 销售质量判断
 * 
 * 比较营收与毛利的增速，判断增长是否健康
 * 
 * 输入: { revenueChangePct, grossProfitChangePct, grossMarginCurrent, grossMarginPrevious }
 * 输出: { revenueGrowsFasterThanProfit, marginDelta, qualityScore, aiPromptData, status }
 */
function prepareSalesQualityCheck(params) {
  const { revenueChangePct, grossProfitChangePct, grossMarginCurrent, grossMarginPrevious } = params || {};
  if (revenueChangePct == null || grossProfitChangePct == null || grossMarginCurrent == null || grossMarginPrevious == null) {
    return { revenueGrowsFasterThanProfit: false, marginDelta: 0, qualityScore: 0, aiPromptData: null, status: 'uncountable' };
  }
  
  const revenueGrowsFasterThanProfit = revenueChangePct > grossProfitChangePct;
  const marginDelta = Math.round((grossMarginCurrent - grossMarginPrevious) * 10) / 10;
  
  let diff = Math.abs(revenueChangePct - grossProfitChangePct);
  let qualityScore = Math.round(100 - diff);
  if (qualityScore < 0) qualityScore = 0;
  if (qualityScore > 100) qualityScore = 100;
  
  const aiPromptData = {
    revenueChangePct,
    grossProfitChangePct,
    marginDelta,
    revenueGrowsFasterThanProfit,
    summary: `营收增速(${revenueChangePct}%) ${revenueGrowsFasterThanProfit ? '大于' : '小于'} 毛利增速(${grossProfitChangePct}%)，毛利率变化${marginDelta}%，质量分${qualityScore}`,
  };
  
  let status = 'pass';
  if (revenueChangePct > 0 && grossProfitChangePct < -5) {
    status = 'warning'; // 增收不增利
  } else if (revenueChangePct - grossProfitChangePct > 25) {
    status = 'attention'; // 增速背离
  }
  
  return { revenueGrowsFasterThanProfit, marginDelta, qualityScore, aiPromptData, status };
}


/**
 * B1.8. prepareMemberHealthCheck — 会员健康度评估
 *
 * 输入: {
 *   memberRevenue, memberChangePct, memberOrderCount, memberOrderChangePct,
 *   memberAvgOrder, memberGrossMarginPct, totalRevenue, overallAvgOrder, overallGrossMarginPct
 * }
 * 输出: { penetrationPct, memberAvgVsOverall, memberGrossMarginVsOverall, healthScore, signals, aiPromptData, status }
 */
function prepareMemberHealthCheck(params) {
  const {
    memberRevenue,
    memberChangePct = null,
    memberOrderCount = null,
    memberOrderChangePct = null,
    memberAvgOrder = null,
    memberGrossMarginPct = null,
    totalRevenue,
    overallAvgOrder = null,
    overallGrossMarginPct = null,
  } = params || {};

  if (!isValidNumber(memberRevenue) || !isValidNumber(totalRevenue) || totalRevenue <= 0) {
    return {
      penetrationPct: 0,
      memberAvgVsOverall: null,
      memberGrossMarginVsOverall: null,
      healthScore: 0,
      signals: [],
      aiPromptData: null,
      status: 'uncountable',
    };
  }

  const penetrationPct = roundTo((memberRevenue / totalRevenue) * 100, 1);
  const memberAvgVsOverall = isValidNumber(memberAvgOrder) && isValidNumber(overallAvgOrder) && overallAvgOrder > 0
    ? roundTo(((memberAvgOrder - overallAvgOrder) / overallAvgOrder) * 100, 1)
    : null;
  const memberGrossMarginVsOverall = isValidNumber(memberGrossMarginPct) && isValidNumber(overallGrossMarginPct)
    ? roundTo(memberGrossMarginPct - overallGrossMarginPct, 1)
    : null;

  const signals = [];
  let healthScore = 100;

  if (penetrationPct < 3) { signals.push('会员渗透率过低'); healthScore -= 45; }
  else if (penetrationPct < 8) { signals.push('会员渗透率偏低'); healthScore -= 25; }

  if (isValidNumber(memberChangePct) && memberChangePct < -10) { signals.push('会员营收明显下滑'); healthScore -= 18; }
  else if (isValidNumber(memberChangePct) && memberChangePct < -3) { signals.push('会员营收轻度下滑'); healthScore -= 10; }

  if (isValidNumber(memberOrderChangePct) && memberOrderChangePct < -10) { signals.push('会员订单明显下滑'); healthScore -= 15; }
  else if (isValidNumber(memberOrderChangePct) && memberOrderChangePct < -3) { signals.push('会员订单轻度下滑'); healthScore -= 8; }

  if (isValidNumber(memberAvgVsOverall) && memberAvgVsOverall < -20) { signals.push('会员客单明显低于整体'); healthScore -= 12; }
  else if (isValidNumber(memberAvgVsOverall) && memberAvgVsOverall < -8) { signals.push('会员客单偏低'); healthScore -= 6; }

  if (isValidNumber(memberGrossMarginVsOverall) && memberGrossMarginVsOverall < -4) { signals.push('会员毛利率明显偏低'); healthScore -= 10; }
  else if (isValidNumber(memberGrossMarginVsOverall) && memberGrossMarginVsOverall < -2) { signals.push('会员毛利率偏低'); healthScore -= 5; }

  healthScore = Math.max(0, Math.min(100, Math.round(healthScore)));
  if (signals.length === 0) signals.push('会员结构整体稳定');

  const aiPromptData = {
    penetrationPct,
    memberRevenue,
    totalRevenue,
    memberChangePct,
    memberOrderCount,
    memberOrderChangePct,
    memberAvgOrder,
    memberAvgVsOverall,
    memberGrossMarginPct,
    overallGrossMarginPct,
    memberGrossMarginVsOverall,
    healthScore,
    signals,
    summary: `会员渗透率${penetrationPct}%，健康分${healthScore}，核心信号：${signals.slice(0, 3).join('、')}`,
  };

  const status = healthScore < 55 || penetrationPct < 3 ? 'warning' :
                 healthScore < 75 || signals.length >= 2 ? 'attention' : 'pass';

  return { penetrationPct, memberAvgVsOverall, memberGrossMarginVsOverall, healthScore, signals, aiPromptData, status };
}

/**
 * B2.5. prepareChannelRiskAssessment — 渠道依赖风险评估
 *
 * 输入: { channelMix, o2oTrend, platformConcentration }
 * 输出: { riskFactors, riskScore, aiPromptData, status }
 */
function prepareChannelRiskAssessment(params) {
  const { channelMix, o2oTrend, platformConcentration } = params || {};
  if (!channelMix || channelMix.status === 'uncountable') {
    return { riskFactors: [], riskScore: 0, aiPromptData: null, status: 'uncountable' };
  }

  const riskFactors = [];
  let riskScore = 0;

  if (isValidNumber(channelMix.dominantPct) && channelMix.dominantPct > 85) {
    riskFactors.push(`渠道高度集中(${channelMix.dominant || '未知'}:${channelMix.dominantPct}%)`);
    riskScore += 38;
  } else if (isValidNumber(channelMix.dominantPct) && channelMix.dominantPct > 72) {
    riskFactors.push(`渠道偏集中(${channelMix.dominant || '未知'}:${channelMix.dominantPct}%)`);
    riskScore += 22;
  }

  if (platformConcentration && platformConcentration.status !== 'uncountable') {
    if (platformConcentration.concentrationPct > 88) {
      riskFactors.push(`平台单边依赖(${platformConcentration.dominantPlatform}:${platformConcentration.concentrationPct}%)`);
      riskScore += 28;
    } else if (platformConcentration.concentrationPct > 78) {
      riskFactors.push(`平台集中度偏高(${platformConcentration.dominantPlatform}:${platformConcentration.concentrationPct}%)`);
      riskScore += 16;
    }
  }

  if (o2oTrend && o2oTrend.status !== 'uncountable') {
    const tail = (o2oTrend.changes || []).slice(-3);
    const allFalling = tail.length === 3 && tail.every(c => c.changePct < 0);
    if (o2oTrend.overallTrend === 'falling' && allFalling) {
      riskFactors.push('O2O连续走弱(近3期均下降)');
      riskScore += 28;
    } else if (o2oTrend.overallTrend === 'falling') {
      riskFactors.push('O2O整体走弱');
      riskScore += 15;
    }
  }

  if (channelMix.dominant === '电商' && riskFactors.some(f => f.includes('O2O'))) {
    riskFactors.push('高依赖渠道同步下滑');
    riskScore += 10;
  }

  riskScore = Math.max(0, Math.min(100, Math.round(riskScore)));
  const status = riskScore >= 70 ? 'warning' :
                 riskScore >= 35 ? 'attention' : 'pass';

  const aiPromptData = {
    dominantChannel: channelMix.dominant || null,
    dominantPct: channelMix.dominantPct || 0,
    o2oTrend: o2oTrend?.overallTrend || 'unknown',
    platform: platformConcentration?.dominantPlatform || null,
    platformConcentrationPct: platformConcentration?.concentrationPct || null,
    riskScore,
    riskFactors,
    summary: riskFactors.length > 0
      ? `渠道风险分${riskScore}，关键风险：${riskFactors.slice(0, 3).join('；')}`
      : `渠道风险分${riskScore}，结构总体稳定`,
  };

  return { riskFactors, riskScore, aiPromptData, status };
}


/**
 * B2. prepareStockoutLossEstimate — 缺货损失估算
 * 
 * 根据缺货商品的销售排名反推日均单量，估算日均损失
 * 排名越靠前 → 预估单量越高 → 损失越大
 * 
 * 输入: outOfStockProducts (热销500缺货 products[]), avgOrderValue (O2O均单或全店客单)
 * 输出: { estimatedDailyLoss, highImpactItems, aiPromptData, status }
 */
function prepareStockoutLossEstimate(outOfStockProducts, avgOrderValue) {
  if (!outOfStockProducts || outOfStockProducts.length === 0) {
    return {
      estimatedDailyLoss: 0,
      highImpactItems: [],
      aiPromptData: null,
      status: 'uncountable',
    };
  }

  // 用均单来估算，如果没传就用行业默认(药店约35元)
  const avgOV = avgOrderValue || 35;

  // 排名→预估日均单量的映射（经验公式，排名越靠前单量越高）
  // rank 1-10: ~8单/天, 11-20: ~5单/天, 21-50: ~3单/天, 51-100: ~1.5单/天
  function estimateDailyOrders(rank) {
    if (rank <= 10)  return 8;
    if (rank <= 20)  return 5;
    if (rank <= 50)  return 3;
    if (rank <= 100) return 1.5;
    return 0.5;
  }

  const items = outOfStockProducts
    .filter(p => p.sales_rank != null)
    .map(p => {
      const dailyOrders = estimateDailyOrders(p.sales_rank);
      const estimatedLoss = Math.round(dailyOrders * avgOV);
      return {
        name: p.product_name,
        salesRank: p.sales_rank,
        estimatedDailyOrders: dailyOrders,
        estimatedLoss,
      };
    })
    .sort((a, b) => a.salesRank - b.salesRank);

  const estimatedDailyLoss = items.reduce((sum, i) => sum + i.estimatedLoss, 0);
  const highImpactItems = items.filter(i => i.salesRank <= 35);

  const aiPromptData = {
    totalStockoutCount: outOfStockProducts.length,
    estimatedDailyLoss,
    avgOrderValueUsed: avgOV,
    highImpactItems: highImpactItems.map(i => ({
      name: i.name,
      rank: i.salesRank,
      loss: `${i.estimatedLoss}元/天`,
    })),
    summary: `${outOfStockProducts.length}个缺货商品，预估日损失${estimatedDailyLoss}元，` +
             `其中${highImpactItems.length}个高影响品(rank≤35)`,
  };

  // status: 日损失>500→warning, 有高影响品→attention, else pass
  const status = estimatedDailyLoss > 900 ? 'warning' :
                 highImpactItems.length > 1 ? 'attention' : 'pass';

  return { estimatedDailyLoss, highImpactItems, aiPromptData, status };
}


/**
 * B3. prepareAnomalySummary — 汇总所有异常，供AI生成解读
 * 
 * 把之前所有指标的结果汇总起来，按严重程度排序
 * 生成一份结构化 "异常清单" 传给 AI
 * 
 * 输入: metricResults: { [metricName]: { status, ...data } }
 * 输出: { totalAlerts, sortedAlerts, aiPromptData, status }
 */
function prepareAnomalySummary(metricResults) {
  if (!metricResults || Object.keys(metricResults).length === 0) {
    return {
      totalAlerts: 0,
      sortedAlerts: [],
      aiPromptData: null,
      status: 'uncountable',
    };
  }

  const priorityMap = { warning: 3, attention: 2, pass: 1, uncountable: 0 };

  // 提取所有非 pass 且非 uncountable 的条目
  const alerts = [];
  for (const [name, result] of Object.entries(metricResults)) {
    if (result.status === 'pass' || result.status === 'uncountable') continue;
    alerts.push({
      metric: name,
      status: result.status,
      priority: priorityMap[result.status],
      // 抽取关键数据（根据不同指标取不同摘要）
      detail: extractAlertDetail(name, result),
    });
  }

  // 按优先级降序排列
  alerts.sort((a, b) => b.priority - a.priority);

  // 统计
  const tally = toTally(metricResults);

  const aiPromptData = {
    totalMetrics: Object.keys(metricResults).length,
    tally,
    alerts: alerts.map(a => ({
      metric: a.metric,
      severity: a.status,
      detail: a.detail,
    })),
    summary: `共${Object.keys(metricResults).length}项指标，` +
             `${tally.warning}项warning，${tally.attention}项attention，` +
             `${tally.pass}项pass，${tally.uncountable}项无法计算`,
  };

  // status: 有任何 warning → warning，有 attention → attention，else pass
  const status = tally.warning > 0 ? 'warning' :
                 tally.attention > 0 ? 'attention' : 'pass';

  return {
    totalAlerts: alerts.length,
    sortedAlerts: alerts,
    aiPromptData,
    status,
  };
}

/**
 * 辅助：从各指标结果中抽取简短的异常描述
 */
function extractAlertDetail(metricName, result) {
  // 辅助函数：格式化列表，限制数量，超出的用“等”
  const formatList = (arr, limit = 3, mapFn = (x => x)) => {
    if (!arr || arr.length === 0) return '';
    const items = arr.slice(0, limit).map(mapFn).join('、');
    return `[${items}${arr.length > limit ? '等' : ''}]`;
  };

  if (metricName.includes('ChannelMix')) {
    // 找出占比前两名的渠道
    const topChannels = [...result.channels].sort((a, b) => b.pct - a.pct).slice(0, 2);
    const details = topChannels.map(c => `${c.label}(${c.pct}%)`).join('、');
    return `主导渠道为${result.dominant}，前二: ${details}`;
  }
  if (metricName.includes('ConsecutiveChange')) {
    const dir = result.direction === 'down' ? '下降' : '上升';
    return `连续${dir}${result.consecutiveDays}天 (${result.startValue}→${result.endValue})，累计${result.totalChangePct}%`;
  }
  if (metricName.includes('GrossMarginTrend')) {
    const isBelowAvg = result.latestMargin < result.avgMargin;
    return `毛利率${result.trend} (最新${result.latestMargin}% ${isBelowAvg ? '<' : '>'} 均值${result.avgMargin}%)`;
  }
  if (metricName.includes('ProductStability')) {
    let parts = [];
    if (result.summary.fadingCount > 0) {
      parts.push(`退热品:${result.summary.fadingCount} ${formatList(result.fading, 2)}`);
    }
    if (result.summary.todayOnlyCount > 0) {
      parts.push(`仅今日:${result.summary.todayOnlyCount} ${formatList(result.todayOnly, 2)}`);
    }
    if (result.summary.risingCount > 0) {
      parts.push(`新晋品:${result.summary.risingCount} ${formatList(result.rising, 2)}`);
    }
    return parts.join('；') || '商品结构稳定';
  }
  if (metricName.includes('StockoutAlert')) {
    let parts = [`总缺货:${result.totalStockout}`];
    if (result.highCount > 0) {
      const highAlerts = result.alerts.filter(a => a.salesRank <= 20);
      parts.push(`高危:${result.highCount} ${formatList(highAlerts, 2, a => `${a.name}(排${a.salesRank})`)}`);
    }
    return parts.join('，');
  }
  if (metricName.includes('StockoutLoss')) {
    let desc = `预估日损失${result.estimatedDailyLoss}元`;
    if (result.highImpactItems && result.highImpactItems.length > 0) {
      desc += `，高损品: ${formatList(result.highImpactItems, 2, i => `${i.name}(损${i.estimatedLoss}元)`)}`;
    }
    return desc;
  }
  if (metricName.includes('GrowthDecomposition')) {
    const primaryStr = translateFactor(result.aiPromptData?.primaryDriver || 'unknown');
    const primaryVal = result.aiPromptData?.primaryDriverValue || 0;
    return `营收变动${result.revenueChange}元，主因: ${primaryStr}(${primaryVal > 0 ? '+' : ''}${primaryVal}元)`;
  }
  if (metricName.includes('RevenueChange')) {
    return `营收${result.direction === 'down' ? '下降' : '增长'} ${Math.abs(result.changePct)}%`;
  }
  if (metricName.includes('GrossMargin')) {
    return `毛利率${result.marginPct ?? result.latestMargin ?? 0}%`;
  }
  if (metricName.includes('MemberPenetration')) {
    return `会员渗透率 ${result.penetrationPct}%`;
  }
  if (metricName.includes('MemberVsOverall')) {
    return `会员客单较整体 ${result.diffPct}%`;
  }
  if (metricName.includes('PlatformConcentration')) {
    return `${result.dominantPlatform || '平台'}占比 ${result.concentrationPct}%`;
  }
  if (metricName.includes('StockoutRate')) {
    return `缺货率 ${result.stockoutPct}% (${result.stockoutCount}个)`;
  }
  if (metricName.includes('MissingCategoryRate')) {
    return `缺种率 ${result.missingPct}% (${result.missingCount}个)`;
  }
  if (metricName.includes('O2OvsTotal')) {
    return `O2O营收占比达 ${result.ratioPct}%`;
  }
  if (metricName.includes('SalesQualityCheck')) {
    return `销售质量分${result.qualityScore}，营收增速(${result.aiPromptData?.revenueChangePct}%) vs 毛利增速(${result.aiPromptData?.grossProfitChangePct}%)`;
  }
  if (metricName.includes('MemberHealthCheck')) {
    return `会员健康分${result.healthScore}，渗透率${result.penetrationPct}%`;
  }
  if (metricName.includes('ChannelRiskAssessment')) {
    return `渠道风险分${result.riskScore}，风险项${result.riskFactors?.length || 0}个`;
  }
  // 通用 fallback
  return JSON.stringify(result).substring(0, 100);
}

/**
 * 辅助：翻译因素名称
 */
function translateFactor(key) {
  const map = {
    fromVisitor:   '来客变化',
    fromAvgOrder:  '客单变化',
    fromMember:    '会员变化',
    fromEcommerce: '电商变化',
  };
  return map[key] || key;
}


// ============================================================
// 演示：加载真实 JSON 并运行全部指标
// ============================================================

function runDemo() {
  const jsonDir = path.join(__dirname, '..', 'json案例');

  // 加载数据
  const load = (filename) => JSON.parse(fs.readFileSync(path.join(jsonDir, filename), 'utf-8'));

  const overviewDay   = load('概览-日.json');
  const overviewMonth = load('概览-月.json');
  const o2oDay        = load('o2o营业-日.json');
  const hotToday      = load('店热销-今.json');
  const hotYesterday  = load('店热销-昨.json');
  const hotWeek       = load('店热销-周.json');
  const hotMonth      = load('店热销.月.json');
  const top500Out     = load('热销500-缺货.json');

  console.log('========================================');
  console.log('  算法处理模块 — 核心指标演示');
  console.log('========================================\n');

  // --- A类指标 ---

  // --- 指标1：渠道结构 ---
  const channelMix = calcChannelMix(overviewDay.sourceDistribution);
  console.log('【A1. 渠道结构 calcChannelMix】');
  console.log(`   总营收: ${channelMix.total}元`);
  channelMix.channels.forEach(ch => {
    console.log(`   ${ch.label}: ${ch.value}元 (${ch.pct}%)`);
  });
  console.log(`   主导渠道: ${channelMix.dominant} (${channelMix.dominantPct}%)`);
  console.log(`   状态: ${channelMix.status}`);
  console.log();

  const dayRows = normalizeOverviewRows(overviewDay);

  // --- 指标1.5：营收环比 ---
  const revenueChangeRes = calcRevenueChange(dayRows);
  console.log('【A1.5. 营收环比 calcRevenueChange】');
  console.log(`   当期: ${revenueChangeRes.current}元，上期: ${revenueChangeRes.previous}元`);
  console.log(`   变化率: ${revenueChangeRes.changePct}%`);
  console.log(`   状态: ${revenueChangeRes.status}`);
  console.log();

  // --- 指标1.6：O2O营收占比 ---
  const o2oRevenue = o2oDay.businessTable?.rows?.[0]?.total_revenue || 0;
  const overviewRevenue = dayRows[0]?.revenue || 0;
  const o2oRatioRes = calcO2OvsTotal(o2oRevenue, overviewRevenue);
  console.log('【A1.6. O2O占比 calcO2OvsTotal】');
  console.log(`   O2O营收: ${o2oRevenue}元，整体营收: ${overviewRevenue}元`);
  console.log(`   占比: ${o2oRatioRes.ratioPct}%`);
  console.log(`   状态: ${o2oRatioRes.status}`);
  console.log();

  // --- 指标2：连续涨跌 ---
  const revenueConsec = calcConsecutiveChange(dayRows, 'revenue');
  console.log('【A2. 连续涨跌 calcConsecutiveChange (日营收)】');
  console.log(`   方向: ${revenueConsec.direction === 'down' ? '📉 下降' : '📈 上升'}`);
  console.log(`   连续天数: ${revenueConsec.consecutiveDays}天`);
  console.log(`   区间: ${revenueConsec.startPeriod} → ${revenueConsec.endPeriod}`);
  console.log(`   累计变化: ${revenueConsec.totalChangePct}%`);
  console.log(`   状态: ${revenueConsec.status}`);
  console.log();

  // --- 指标3：毛利率趋势 ---
  const monthRows = normalizeOverviewRows(overviewMonth);
  const marginTrend = calcGrossMarginTrend(monthRows);
  console.log('【A3. 毛利率趋势 calcGrossMarginTrend (月)】');
  marginTrend.margins.forEach(m => {
    console.log(`   ${m.period}: ${m.marginPct}%`);
  });
  console.log(`   平均毛利率: ${marginTrend.avgMargin}%`);
  console.log(`   最新毛利率: ${marginTrend.latestMargin}%`);
  console.log(`   趋势: ${marginTrend.trend} (斜率: ${marginTrend.slope})`);
  console.log(`   状态: ${marginTrend.status}`);
  console.log();

  // --- 指标4：爆品稳定性 ---
  const stability = calcProductStability({
    today:     normalizeHotProducts(hotToday),
    yesterday: normalizeHotProducts(hotYesterday),
    week:      normalizeHotProducts(hotWeek),
    month:     normalizeHotProducts(hotMonth),
  });
  console.log('【A4. 爆品稳定性 calcProductStability】');
  console.log(`   稳定品(${stability.summary.stableCount}): ${stability.stable.join(', ') || '无'}`);
  console.log(`   新晋品(${stability.summary.risingCount}): ${stability.rising.join(', ') || '无'}`);
  console.log(`   退热品(${stability.summary.fadingCount}): ${stability.fading.join(', ') || '无'}`);
  console.log(`   仅今日(${stability.summary.todayOnlyCount}): ${stability.todayOnly.join(', ') || '无'}`);
  console.log(`   状态: ${stability.status}`);
  console.log();

  // --- 指标5：缺货预警 ---
  const stockoutAlert = calcHighRankStockoutAlert(top500Out.products);
  console.log('【A5. 缺货预警 calcHighRankStockoutAlert】');
  console.log(`   总缺货品: ${stockoutAlert.totalStockout}个`);
  console.log(`   高危(rank≤20): ${stockoutAlert.highCount}个`);
  console.log(`   中危(rank≤35): ${stockoutAlert.mediumCount}个`);
  console.log(`   状态: ${stockoutAlert.status}`);
  stockoutAlert.alerts.forEach(a => {
    const icon = a.salesRank <= 20 ? '🔴' : a.salesRank <= 35 ? '🟡' : '🟢';
    console.log(`   ${icon} [排名${a.salesRank}] ${a.name}`);
  });
  console.log();

  // --- B类指标 ---

  console.log('========================================');
  console.log('  B类：算法+AI 辅助指标');
  console.log('========================================\n');

  // --- B1：增长来源拆解 ---
  const growth = prepareGrowthDecomposition(dayRows);
  console.log('【B1. 增长来源拆解 prepareGrowthDecomposition】');
  console.log(`   营收变化: ${growth.revenueChange}元`);
  console.log(`   来客贡献: ${growth.decomposition.fromVisitor}元`);
  console.log(`   客单贡献: ${growth.decomposition.fromAvgOrder}元`);
  console.log(`   会员贡献: ${growth.decomposition.fromMember}元`);
  console.log(`   电商贡献: ${growth.decomposition.fromEcommerce}元`);
  if (growth.aiPromptData) {
    console.log(`   📋 AI摘要: ${growth.aiPromptData.summary}`);
  }
  console.log(`   状态: ${growth.status}`);
  console.log();

  // --- B1.5：销售质量判断 ---
  const currRow = dayRows[0];
  const prevRow = dayRows[1] || currRow;
  const revChangePct = prevRow.revenue > 0 ? (currRow.revenue - prevRow.revenue) / prevRow.revenue * 100 : 0;
  const gpChangePct = prevRow.grossProfit > 0 ? (currRow.grossProfit - prevRow.grossProfit) / prevRow.grossProfit * 100 : 0;
  const marginCur = currRow.revenue > 0 ? currRow.grossProfit / currRow.revenue * 100 : 0;
  const marginPrev = prevRow.revenue > 0 ? prevRow.grossProfit / prevRow.revenue * 100 : 0;
  
  const salesQuality = prepareSalesQualityCheck({
    revenueChangePct: Math.round(revChangePct * 10) / 10,
    grossProfitChangePct: Math.round(gpChangePct * 10) / 10,
    grossMarginCurrent: Math.round(marginCur * 10) / 10,
    grossMarginPrevious: Math.round(marginPrev * 10) / 10
  });
  console.log('【B1.5. 销售质量判断 prepareSalesQualityCheck】');
  console.log(`   质量分: ${salesQuality.qualityScore}`);
  if (salesQuality.aiPromptData) {
    console.log(`   📋 AI摘要: ${salesQuality.aiPromptData.summary}`);
  }
  console.log(`   状态: ${salesQuality.status}`);
  console.log();

  // --- B1.8：会员健康度评估 ---
  const memberRevenue = dayRows[0]?.memberAmount || 0;
  const totalRevenue = dayRows[0]?.revenue || 0;
  const prevMemberRevenue = dayRows[1]?.memberAmount || 0;
  const memberChangePct = prevMemberRevenue > 0 ? ((memberRevenue - prevMemberRevenue) / prevMemberRevenue) * 100 : null;
  const memberAvgOrder = overviewDay.summary?.metrics?.find(m => m.key === 'member_avg_order_value')?.value ?? null;
  const overallAvgOrder = totalRevenue > 0 && dayRows[0]?.visitorCount > 0 ? totalRevenue / dayRows[0].visitorCount : null;
  const memberGrossMarginPct = memberRevenue > 0 && dayRows[0]?.memberGrossProfit > 0 ? (dayRows[0].memberGrossProfit / memberRevenue) * 100 : null;
  const overallGrossMarginPct = totalRevenue > 0 && dayRows[0]?.grossProfit > 0 ? (dayRows[0].grossProfit / totalRevenue) * 100 : null;
  const memberHealth = prepareMemberHealthCheck({
    memberRevenue,
    memberChangePct: isValidNumber(memberChangePct) ? roundTo(memberChangePct, 1) : null,
    memberAvgOrder,
    memberGrossMarginPct: isValidNumber(memberGrossMarginPct) ? roundTo(memberGrossMarginPct, 1) : null,
    totalRevenue,
    overallAvgOrder: isValidNumber(overallAvgOrder) ? roundTo(overallAvgOrder, 2) : null,
    overallGrossMarginPct: isValidNumber(overallGrossMarginPct) ? roundTo(overallGrossMarginPct, 1) : null,
  });
  console.log('【B1.8. 会员健康度 prepareMemberHealthCheck】');
  console.log(`   健康分: ${memberHealth.healthScore}`);
  if (memberHealth.aiPromptData) {
    console.log(`   📋 AI摘要: ${memberHealth.aiPromptData.summary}`);
  }
  console.log(`   状态: ${memberHealth.status}`);
  console.log();

  // --- B2：缺货损失估算 ---
  const stockoutLoss = prepareStockoutLossEstimate(top500Out.products, 35);
  console.log('【B2. 缺货损失估算 prepareStockoutLossEstimate】');
  console.log(`   预估日损失: ${stockoutLoss.estimatedDailyLoss}元`);
  console.log(`   高影响品: ${stockoutLoss.highImpactItems.length}个`);
  stockoutLoss.highImpactItems.forEach(i => {
    console.log(`   🔴 [排名${i.salesRank}] ${i.name} → ~${i.estimatedLoss}元/天`);
  });
  if (stockoutLoss.aiPromptData) {
    console.log(`   📋 AI摘要: ${stockoutLoss.aiPromptData.summary}`);
  }
  console.log(`   状态: ${stockoutLoss.status}`);
  console.log();

  // --- B2.5：渠道风险评估 ---
  const platformConcentration = calcPlatformConcentration(o2oDay.businessTable?.rows?.[0] || null);
  const o2oTrend = calcO2OTrend(o2oDay.businessTable?.rows || [], 'total_revenue');
  const channelRisk = prepareChannelRiskAssessment({
    channelMix,
    o2oTrend,
    platformConcentration,
  });
  console.log('【B2.5. 渠道风险评估 prepareChannelRiskAssessment】');
  console.log(`   风险分: ${channelRisk.riskScore}`);
  if (channelRisk.aiPromptData) {
    console.log(`   📋 AI摘要: ${channelRisk.aiPromptData.summary}`);
  }
  console.log(`   状态: ${channelRisk.status}`);
  console.log();

  // --- B3：异常汇总 ---
  const allResults = {
    'calcChannelMix (渠道结构)': channelMix,
    'calcRevenueChange (营收环比)': revenueChangeRes,
    'calcO2OvsTotal (O2O占比)': o2oRatioRes,
    'calcConsecutiveChange (连续涨跌-营收)': revenueConsec,
    'calcGrossMarginTrend (毛利率趋势)': marginTrend,
    'calcProductStability (爆品稳定性)': stability,
    'calcHighRankStockoutAlert (缺货预警)': stockoutAlert,
    'prepareGrowthDecomposition (增长拆解)': growth,
    'prepareSalesQualityCheck (销售质量)': salesQuality,
    'prepareMemberHealthCheck (会员健康)': memberHealth,
    'prepareStockoutLossEstimate (缺货损失)': stockoutLoss,
    'prepareChannelRiskAssessment (渠道风险)': channelRisk,
  };
  const anomalySummary = prepareAnomalySummary(allResults);
  console.log('【B3. 异常汇总 prepareAnomalySummary】');
  console.log(`   总异常数: ${anomalySummary.totalAlerts}`);
  anomalySummary.sortedAlerts.forEach(a => {
    const icon = a.status === 'warning' ? '🔴' : '🟡';
    console.log(`   ${icon} [${a.status}] ${a.metric}: ${a.detail}`);
  });
  if (anomalySummary.aiPromptData) {
    console.log(`   📋 AI摘要: ${anomalySummary.aiPromptData.summary}`);
  }
  console.log(`   状态: ${anomalySummary.status}`);
  console.log();
}

// ============================================================
// 导出
// ============================================================

module.exports = {
  // normalize
  normalizeOverviewRows,
  normalizeHotProducts,
  getMetricByKey,
  // A类：纯算法指标
  calcChannelMix,
  calcRevenueChange,
  calcO2OvsTotal,
  calcConsecutiveChange,
  calcGrossMarginTrend,
  calcProductStability,
  calcHighRankStockoutAlert,
  calcGrossMargin,
  calcAvgOrderValue,
  calcMemberPenetration,
  calcMemberVsOverall,
  calcPlatformConcentration,
  calcO2OGrossMargin,
  calcO2OTrend,
  calcHotProductConcentration,
  calcStockoutRate,
  calcMissingCategoryRate,
  calcActiveSKUCount,
  detectConsecutiveDecline,
  detectLowMemberAlert,
  detectChannelImbalance,
  // B类：算法+AI辅助指标
  prepareStoreStatusLabel,
  prepareGrowthDecomposition,
  prepareSalesQualityCheck,
  prepareMemberHealthCheck,
  prepareStockoutLossEstimate,
  prepareChannelRiskAssessment,
  prepareAnomalySummary,
};

// 如果直接运行此文件则执行演示
if (require.main === module) {
  runDemo();
}
