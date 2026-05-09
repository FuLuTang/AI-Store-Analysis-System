const fs = require('fs');
const path = require('path');

/**
 * 辅助函数：将数字四舍五入为整数，省去小数以优化 tokens
 */
function roundVal(v) {
    return typeof v === 'number' ? Math.round(v) : v;
}

/**
 * 辅助函数：计算环比（百分比整数）
 */
function calcMom(cur, prev) {
    if (cur == null || prev == null || prev === 0) return null;
    return roundVal((cur - prev) / prev * 100);
}

/**
 * 清洗 JSON 数据
 * @param {Object} rawData 原始 JSON 数据
 * @returns {Object|null} 清洗后的数据，如果跳过则返回 null
 */
function cleanData(rawData) {
    const module = rawData.page?.module;

    switch (module) {
        case 'business_overview':
            return cleanBusinessOverview(rawData);
        case 'operation_hot_products':
            return cleanStoreHotProducts(rawData);
        case 'hot_sale_top500':
            return cleanHotTop500(rawData);
        case 'o2o_business_summary':
            return cleanO2oBusinessSummary(rawData);
        case 'o2o_product_category':
            console.log('跳过商品资料类 JSON');
            return null;
        default:
            console.warn(`未知模块类型: ${module}`);
            return null;
    }
}

function cleanBusinessOverview(data) {
    const period = data.page.selectedDate || data.page.selectedYear?.toString() || '';
    const granularity = data.page.viewType || '';

    const summary_rows = (data.summary?.metrics || []).map(m => {
        const momData = m.mom || m.compare;
        // 如果有备注且不为空，合并到指标名称里，省掉一列
        const label = m.extra ? `${m.label}(${m.extra.label}${m.extra.value}${m.extra.unit})` : m.label;
        const remark = m.rate ? `(率${m.rate}${m.rateUnit})` : '';
        
        return [
            label + remark,
            roundVal(m.value),
            roundVal(momData?.value || 0)
        ];
    });

    // 添加等效推算
    if (data.page.lastUpdated) {
        const eqLabelPrefix = granularity === 'day' ? '等效全天' : (granularity === 'month' ? '等效全月' : '');
        if (eqLabelPrefix) {
            const revenueMetric = (data.summary?.metrics || []).find(m => m.key === 'revenue');
            const grossProfitMetric = (data.summary?.metrics || []).find(m => m.key === 'gross_profit');
            const visitorMetric = (data.summary?.metrics || []).find(m => m.key === 'visitor_count');
            
            if (revenueMetric) {
                const eqRev = calculateEquivalent(revenueMetric.value, granularity, data.page.lastUpdated);
                if (eqRev !== null) summary_rows.push([`${eqLabelPrefix}营收`, eqRev, '-']);
            }
            if (grossProfitMetric) {
                const eqGp = calculateEquivalent(grossProfitMetric.value, granularity, data.page.lastUpdated);
                if (eqGp !== null) summary_rows.push([`${eqLabelPrefix}毛利`, eqGp, '-']);
            }
            if (visitorMetric) {
                const eqVis = calculateEquivalent(visitorMetric.value, granularity, data.page.lastUpdated);
                if (eqVis !== null) summary_rows.push([`${eqLabelPrefix}客数`, Math.round(eqVis), '-']);
            }
        }
    }

    const ranking_rows = (data.ranking?.items || []).map(item => [
        item.label,
        item.date || item.period || '',
        roundVal(item.value)
    ]);

    const tableSource = data.dailyBusinessTable || data.businessTable;
    const raw_table_rows = tableSource?.rows || [];
    
    let isDescending = true;
    if (raw_table_rows.length > 1) {
        const d1 = new Date(raw_table_rows[0].date || raw_table_rows[0].period || '');
        const d2 = new Date(raw_table_rows[1].date || raw_table_rows[1].period || '');
        if (d1 < d2) isDescending = false;
    }

    const table_rows = raw_table_rows.map((row, i) => {
        const prevRow = isDescending ? raw_table_rows[i + 1] : raw_table_rows[i - 1];
        return [
            row.date || row.period || '',
            roundVal(row.retail_amount), calcMom(row.retail_amount, prevRow?.retail_amount),
            roundVal(row.gross_profit), calcMom(row.gross_profit, prevRow?.gross_profit),
            roundVal(row.visitor_count), calcMom(row.visitor_count, prevRow?.visitor_count),
            roundVal(row.member_amount || 0), calcMom(row.member_amount || 0, prevRow?.member_amount || 0),
            roundVal(row.ecommerce_amount || row.online_amount || 0), calcMom(row.ecommerce_amount || row.online_amount || 0, prevRow?.ecommerce_amount || prevRow?.online_amount || 0),
            roundVal(row.ecommerce_gross_profit || 0), calcMom(row.ecommerce_gross_profit || 0, prevRow?.ecommerce_gross_profit || 0)
        ];
    });

    // 业绩来源占比（环形图数据），对 AI 宏观判断很有用
    const source_distribution = (data.sourceDistribution?.items || []).map(item => [
        item.label,
        roundVal(item.value)
    ]);

    return {
        type: "business_overview",
        period,
        granularity,
        summary_schema: ["指标", "值", "环比%"],
        summary_rows,
        ranking_schema: ["项目", "周期", "值"],
        ranking_rows,
        table_schema: ["周期", "零售额", "环比%", "毛利", "环比%", "客数", "环比%", "会员额", "环比%", "电商额", "环比%", "电商毛利", "环比%"],
        table_rows,
        distribution_schema: ["来源", "金额"],
        distribution_rows: source_distribution
    };
}

function calculateEquivalent(value, viewType, lastUpdatedStr) {
    if (!lastUpdatedStr || typeof value !== 'number' || value === 0) return null;
    
    const match = lastUpdatedStr.match(/(\d{4})-(\d{2})-(\d{2})(?:\s+(\d{2}):(\d{2}))?/);
    if (!match) return null;
    
    const year = parseInt(match[1]);
    const month = parseInt(match[2]);
    const day = parseInt(match[3]);
    const hour = match[4] ? parseInt(match[4]) : 24;
    const minute = match[5] ? parseInt(match[5]) : 0;
    
    const elapsedHours = hour + minute / 60;
    
    if (viewType === 'day') {
        const progress = Math.max(0.01, elapsedHours / 24);
        return Math.round(value / progress);
    } else if (viewType === 'month') {
        const daysInMonth = new Date(year, month, 0).getDate();
        const progress = Math.max(0.01, (day - 1 + elapsedHours / 24) / daysInMonth);
        return Math.round(value / progress);
    }
    return null;
}

function cleanO2oBusinessSummary(data) {
    const period = data.page.selectedDate || data.page.selectedMonth || data.page.selectedYear?.toString() || '';
    const granularity = data.page.viewType || '';
    
    const tableSource = data.businessTable;
    const raw_table_rows = tableSource?.rows || [];

    let isDescending = true;
    if (raw_table_rows.length > 1) {
        const d1 = new Date(raw_table_rows[0].date || raw_table_rows[0].period || '');
        const d2 = new Date(raw_table_rows[1].date || raw_table_rows[1].period || '');
        if (d1 < d2) isDescending = false;
    }

    const table_rows = raw_table_rows.map((row, i) => {
        const prevRow = isDescending ? raw_table_rows[i + 1] : raw_table_rows[i - 1];
        return [
            row.period || '',
            roundVal(row.total_order_count || 0), calcMom(row.total_order_count || 0, prevRow?.total_order_count || 0),
            roundVal(row.total_revenue || 0), calcMom(row.total_revenue || 0, prevRow?.total_revenue || 0),
            roundVal(row.gross_profit || 0), calcMom(row.gross_profit || 0, prevRow?.gross_profit || 0),
            roundVal(row.meituan_order_count || 0), calcMom(row.meituan_order_count || 0, prevRow?.meituan_order_count || 0),
            roundVal(row.eleme_order_count || 0), calcMom(row.eleme_order_count || 0, prevRow?.eleme_order_count || 0),
            roundVal(row.meituan_revenue || 0), calcMom(row.meituan_revenue || 0, prevRow?.meituan_revenue || 0),
            roundVal(row.eleme_revenue || 0), calcMom(row.eleme_revenue || 0, prevRow?.eleme_revenue || 0)
        ];
    });

    return {
        type: "o2o_business_summary",
        period,
        granularity,
        table_schema: ["周期", "总单数", "环比%", "总营业额", "环比%", "毛利", "环比%", "美团单", "环比%", "饿了单", "环比%", "美团营业", "环比%", "饿了营业", "环比%"],
        table_rows
    };
}

function cleanStoreHotProducts(data) {
    const period = data.page.viewType || '';
    const rows = (data.ranking || []).map(item => [
        item.rank,
        item.product_name,
        item.sales_receipt_count,
        item.sales_quantity
    ]);

    return {
        type: "store_hot_products",
        period,
        schema: ["排名", "商品名", "笔数", "数量"],
        rows
    };
}

function cleanHotTop500(data) {
    const city = data.products?.[0]?.city || '未知';
    const status = data.page.viewType || 'top500';
    const rows = (data.products || []).map(item => [
        status,
        item.rank,
        item.product_name
    ]);

    return {
        type: "hot_top500_stock_status",
        city,
        schema: ["状态", "排名", "商品名"],
        rows
    };
}

function mergeHotTop500(jsonList) {
    const top500Files = jsonList.filter(j => j.page?.module === 'hot_sale_top500');
    if (top500Files.length === 0) return null;

    const city = top500Files[0].products?.[0]?.city || '未知';
    const productMap = new Map();

    // 状态优先级：缺种 > 缺货 > 有货 > top500
    const statusPriority = {
        'missing_category': 4,
        'out_of_stock': 3,
        'in_stock': 2,
        'top500': 1
    };

    top500Files.forEach(file => {
        const fileStatus = file.page.viewType || 'top500';
        (file.products || []).forEach(p => {
            const name = p.product_name;
            const rank = p.sales_rank || p.rank; // 优先使用全城销售排名
            if (!productMap.has(name)) {
                productMap.set(name, {
                    rank: rank,
                    name: name,
                    status: fileStatus
                });
            } else {
                const existing = productMap.get(name);
                // 更新为优先级更高的状态
                if (statusPriority[fileStatus] > statusPriority[existing.status]) {
                    existing.status = fileStatus;
                }
                // 排名取最小（最高）
                if (rank < existing.rank) existing.rank = rank;
            }
        });
    });

    const sorted = Array.from(productMap.values()).sort((a, b) => a.rank - b.rank);

    // 按状态分组
    const groups = { top500: [], in_stock: [], out_of_stock: [], missing_category: [] };
    sorted.forEach(p => {
        const row = [p.rank, p.name];
        if (groups[p.status]) {
            groups[p.status].push(row);
        }
    });

    return {
        type: "hot_top500_stock_status",
        city,
        schema: ["排名", "商品名"],
        top500: groups.top500,
        in_stock: groups.in_stock,
        out_of_stock: groups.out_of_stock,
        missing_category: groups.missing_category
    };
}

function mergeHotProducts(jsonList) {
    const hotFiles = jsonList.filter(j => j.page?.module === 'operation_hot_products');
    if (hotFiles.length === 0) return null;

    const periodMap = { today: '今', yesterday: '昨', '7days': '周', '30days': '月' };
    const groups = {};

    hotFiles.forEach(file => {
        const period = file.page.viewType || 'unknown';
        const key = periodMap[period] || period;
        groups[key] = (file.ranking || []).map(item => [
            item.rank,
            item.product_name,
            item.sales_receipt_count,
            item.sales_quantity
        ]);
    });

    return {
        type: "store_hot_products",
        schema: ["排名", "商品名", "笔数", "数量"],
        ...groups
    };
}

/**
 * 清理缓存目录
 * @param {string} cacheDir 
 */
function clearCache(cacheDir) {
    if (fs.existsSync(cacheDir)) {
        const files = fs.readdirSync(cacheDir);
        for (const file of files) {
            fs.unlinkSync(path.join(cacheDir, file));
        }
    } else {
        fs.mkdirSync(cacheDir, { recursive: true });
    }
}

/**
 * 紧凑型 JSON 序列化
 * 使 schema 和 rows 的数组保持在同一行，减少换行
 */
function stringifyCompact(obj) {
    return JSON.stringify(obj, (key, value) => {
        if (Array.isArray(value) && !value.some(item => typeof item === 'object' && item !== null)) {
            return JSON.stringify(value);
        }
        return value;
    }, 2)
    .replace(/"\[/g, '[')
    .replace(/\]"/g, ']')
    .replace(/\\"/g, '"');
}

module.exports = {
    cleanData,
    mergeHotProducts,
    mergeHotTop500,
    clearCache,
    stringifyCompact
};
