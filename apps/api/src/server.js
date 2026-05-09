const express = require('express');
const cors = require('cors');
const fs = require('fs');
const path = require('path');
const cleaner = require('../../../packages/core/cleaner');
const aiCaller = require('../../../packages/ai/ai-caller');
const metrics = require('../../../packages/core/metrics');
const errorReviewer = require('../../../packages/ai/error-reviewer');

const app = express();
app.use(cors());
app.use(express.json({ limit: '50mb' }));
app.use(express.static(path.join(__dirname, '../../web/public')));

const CONFIG_FILE = path.join(__dirname, 'config.json');

app.get('/api/config', (req, res) => {
    try {
        if (fs.existsSync(CONFIG_FILE)) {
            const data = fs.readFileSync(CONFIG_FILE, 'utf8');
            res.json(JSON.parse(data));
        } else {
            res.json({});
        }
    } catch (err) {
        res.status(500).json({ error: '读取配置失败' });
    }
});

app.post('/api/config', (req, res) => {
    try {
        fs.writeFileSync(CONFIG_FILE, JSON.stringify(req.body, null, 2));
        res.json({ success: true });
    } catch (err) {
        res.status(500).json({ error: '保存配置失败' });
    }
});

const CACHE_DIR = path.join(__dirname, '../../../storage/cache');
if (!fs.existsSync(CACHE_DIR)) fs.mkdirSync(CACHE_DIR);

let sseClients = [];

const globalJobState = {
    status: 'idle', // idle, running, completed, error
    result: null,
    fullResult: null,
    errorMessage: '',
    logs: [],
    forceStop: false,
    abortController: null
};

// SSE 端点
app.get('/api/stream', (req, res) => {
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    sseClients.push(res);
    
    // 发送 reset 以清空客户端状态，然后倒出历史日志
    res.write(`data: ${JSON.stringify({ type: 'reset', time: new Date().toLocaleTimeString() })}\n\n`);
    globalJobState.logs.forEach(payload => {
        res.write(`data: ${JSON.stringify(payload)}\n\n`);
    });

    req.on('close', () => {
        sseClients = sseClients.filter(c => c !== res);
    });
});

const sendEvent = (type, data) => {
    const payload = {
        type: type,
        time: new Date().toLocaleTimeString(),
        ...data
    };
    if (type === 'reset') {
        globalJobState.logs = [];
    } else {
        globalJobState.logs.push(payload);
    }
    sseClients.forEach(client => client.write(`data: ${JSON.stringify(payload)}\n\n`));
};

// 状态发送辅助函数
const sendStatus = (nodeId, status) => sendEvent('status', { nodeId, status });
const sendLog = (nodeId, msg) => sendEvent('log', { nodeId, message: msg });
const sendProgress = (nodeId, current, total) => sendEvent('progress', { nodeId, current, total });
const sendTally = (nodeId, tally) => sendEvent('tally', { nodeId, tally });
const resetNodes = () => sendEvent('reset', {});

async function withTimerLog(nodeId, nodeName, promiseFn) {
    const startTime = Date.now();
    const intervalId = setInterval(() => {
        const elapsed = Math.floor((Date.now() - startTime) / 1000);
        sendLog(nodeId, `⏳ ${nodeName} 正在思考中... 已耗时 ${elapsed} 秒`);
    }, 5000);

    try {
        return await promiseFn();
    } finally {
        clearInterval(intervalId);
    }
}

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

app.get('/api/status', (req, res) => {
    res.json({
        status: globalJobState.status,
        result: globalJobState.result,
        fullResult: globalJobState.fullResult,
        errorMessage: globalJobState.errorMessage
    });
});

app.post('/api/stop', (req, res) => {
    if (globalJobState.status === 'running') {
        globalJobState.forceStop = true;
        if (globalJobState.abortController) {
            globalJobState.abortController.abort();
        }
        globalJobState.status = 'aborted';
        globalJobState.errorMessage = '任务被用户强行终止。';
        sendLog('fusion', '⚠️ 用户强制终止了任务！');
    }
    res.json({ success: true });
});

// 分析接口（旧接口别名）
app.post('/analyze', handleRun);
app.post('/api/run', handleRun);

// 获取案例 JSON 文件内容
app.get('/api/examples', (req, res) => {
    const examplesDir = path.join(__dirname, '../../../data/samples');
    if (!fs.existsSync(examplesDir)) {
        return res.status(404).json({ error: '未找到案例目录' });
    }
    
    try {
        const files = fs.readdirSync(examplesDir).filter(f => f.endsWith('.json'));
        const contents = files.map(f => {
            const content = fs.readFileSync(path.join(examplesDir, f), 'utf8');
            return JSON.parse(content);
        });
        res.json({ files: contents, count: contents.length });
    } catch (err) {
        res.status(500).json({ error: '读取案例文件失败: ' + err.message });
    }
});

function handleRun(req, res) {
    if (globalJobState.status === 'running') {
        return res.status(400).json({ error: '系统当前有任务正在运行，请等待或停止它。' });
    }

    const jsonList = Array.isArray(req.body) ? req.body : req.body.files;
    const settings = req.body.settings || null;

    globalJobState.status = 'running';
    globalJobState.result = null;
    globalJobState.fullResult = null;
    globalJobState.errorMessage = '';
    globalJobState.forceStop = false;
    globalJobState.abortController = new AbortController();

    if (settings) {
        settings.signal = globalJobState.abortController.signal;
    }

    // 启动异步任务
    runWorker(jsonList, settings).catch(err => {
        console.error("Worker error:", err);
        if (!globalJobState.forceStop) {
            globalJobState.status = 'error';
            globalJobState.errorMessage = err.message;
        }
    });

    res.json({ status: 'started' });
}

async function runWorker(jsonList, settings) {
    try {
        resetNodes();

        // 1. JSON 传入
        sendStatus('input', 'active');
        sendLog('input', `收到 ${jsonList.length} 个 JSON 文件`);
        
        // 每次重新生成之前，先清理缓存
        sendLog('input', '清理旧缓存目录...');
        cleaner.clearCache(CACHE_DIR);
        jsonList.forEach((j, i) => {
            sendLog('input', `  [${i}] ${j.page?.module || '?'} - ${j.page?.title || ''}`);
        });
        await sleep(500);
        sendStatus('input', 'success');

        let aiReport = null;
        let reviewResult = null;
        let anomalySummaryData = null;

        const aiFlow = async () => {
            // 2. 清洗
            if (globalJobState.forceStop) throw new Error('Aborted');
            sendStatus('clean', 'active');
            let cleanCount = 0;

            jsonList.forEach((rawJson, index) => {
                const mod = rawJson.page?.module;
                if (mod === 'hot_sale_top500' || mod === 'operation_hot_products') return;
                if (mod === 'o2o_product_category') {
                    sendLog('clean', `  跳过: 商品资料类`);
                    return;
                }
                const cleaned = cleaner.cleanData(rawJson);
                if (cleaned) {
                    fs.writeFileSync(path.join(CACHE_DIR, `cleaned_${index}_${cleaned.type}.json`), cleaner.stringifyCompact(cleaned));
                    sendLog('clean', `  ✓ ${cleaned.type} (${cleaned.granularity || ''})`);
                    cleanCount++;
                }
            });

            const mergedTop500 = cleaner.mergeHotTop500(jsonList);
            if (mergedTop500) {
                fs.writeFileSync(path.join(CACHE_DIR, 'cleaned_merged_hot_top500.json'), cleaner.stringifyCompact(mergedTop500));
                sendLog('clean', `  ✓ 合并Top500: 缺货${mergedTop500.out_of_stock.length}/缺种${mergedTop500.missing_category.length}`);
                cleanCount++;
            }

            const mergedHot = cleaner.mergeHotProducts(jsonList);
            if (mergedHot) {
                fs.writeFileSync(path.join(CACHE_DIR, 'cleaned_merged_hot_products.json'), cleaner.stringifyCompact(mergedHot));
                const periods = Object.keys(mergedHot).filter(k => !['type', 'schema'].includes(k));
                sendLog('clean', `  ✓ 合并热销: ${periods.join('/')} ${periods.length}个时段`);
                cleanCount++;
            }

            sendLog('clean', `完成: 共 ${cleanCount} 个清洗文件`);
            sendStatus('clean', 'success');

            // 3. API 调用
            if (globalJobState.forceStop) throw new Error('Aborted');
            sendStatus('api', 'active');
            let cleanedTexts = [];
            if (settings && settings.apiKey) {
                const cacheFiles = fs.readdirSync(CACHE_DIR).filter(f => f.endsWith('.json'));
                cleanedTexts = cacheFiles.map(f => fs.readFileSync(path.join(CACHE_DIR, f), 'utf8'));
                const totalChars = cleanedTexts.reduce((s, t) => s + t.length, 0);

                sendLog('api', `模型: ${settings.model}`);
                sendLog('api', `数据源: ${cleanedTexts.length} 个, ~${totalChars} 字符`);
                sendLog('api', `请求 ${settings.baseUrl} ...`);

                const t0 = Date.now();
                const aiFullResponse = await withTimerLog('api', '初级诊断分析 AI', () => aiCaller.callAI(settings, cleanedTexts));
                const elapsed = ((Date.now() - t0) / 1000).toFixed(1);

                // 打印原始 JSON 到监控日志
                sendLog('api', '--- 原始 AI 响应 JSON ---');
                sendLog('api', JSON.stringify(aiFullResponse, null, 2));
                
                const aiResponseContent = aiFullResponse.choices[0].message.content;
                const usage = aiFullResponse.usage;
                if (usage) {
                    sendLog('api', `Token 消耗: 输入 ${usage.prompt_tokens}, 输出 ${usage.completion_tokens}, 总计 ${usage.total_tokens}`);
                }
                
                sendLog('api', `响应完成 (${elapsed}s)`);
                aiReport = aiResponseContent;
                sendStatus('api', 'success');
            } else {
                sendLog('api', '未配置 API Key，模拟模式');
                await sleep(2000);
                sendStatus('api', 'simulated');
            }

            // 4. 输出
            if (globalJobState.forceStop) throw new Error('Aborted');
            sendStatus('output', 'active');
            if (aiReport) {
                sendLog('output', `报告内容: ${aiReport.length} 字符`);
            } else {
                sendLog('output', '使用模拟报告');
            }
            await sleep(300);
            sendStatus('output', aiReport ? 'success' : 'simulated');

            // 5. 报告明显错误评审 (静默流式，防止超时)
            if (globalJobState.forceStop) throw new Error('Aborted');
            sendStatus('review', 'active');
            const reviewFullRes = await withTimerLog('review', '错误评审 AI', () => errorReviewer.reviewError(
                settings, 
                aiReport, 
                cleanedTexts, 
                null // 不传回调，不实时打印
            ));
            
            // 结束后统一处理
            if (typeof reviewFullRes === 'object') {
                reviewResult = reviewFullRes.choices[0].message.content;
                sendLog('review', '--- 原始错误评审 AI 响应 JSON ---');
                sendLog('review', JSON.stringify(reviewFullRes, null, 2));
            } else {
                reviewResult = reviewFullRes;
            }

            sendLog('review', `完成错误审核: ${reviewResult}`);
            await sleep(300);
            sendStatus('review', 'success');
        };

        const algoFlow = async () => {
            const pickModule = (module, viewType = null) => {
                const list = (jsonList || []).filter(j => j?.page?.module === module);
                if (list.length === 0) return null;
                if (!viewType) return list[0];
                return list.find(j => j?.page?.viewType === viewType) || null;
            };

            // ── alg1: 数据整理 ──
            if (globalJobState.forceStop) throw new Error('Aborted');
            sendStatus('alg1', 'active');
            sendLog('alg1', '从用户导入文件中加载 JSON 数据源...');

            const overviewDay   = pickModule('business_overview', 'day');
            const overviewMonth = pickModule('business_overview', 'month');
            const overviewFallback = pickModule('business_overview');
            const o2oDay        = pickModule('o2o_business_summary', 'day');
            const hotToday      = pickModule('operation_hot_products', 'today');
            const hotYesterday  = pickModule('operation_hot_products', 'yesterday');
            const hotWeek       = pickModule('operation_hot_products', '7days');
            const hotMonth      = pickModule('operation_hot_products', '30days');
            const top500Total   = pickModule('hot_sale_top500', 'top500');
            const top500Out     = pickModule('hot_sale_top500', 'out_of_stock');
            const top500Missing = pickModule('hot_sale_top500', 'missing_category');

            const sources = { overviewDay, overviewMonth, o2oDay, hotToday, hotYesterday, hotWeek, hotMonth, top500Total, top500Out, top500Missing };
            const loadedCount = Object.values(sources).filter(Boolean).length;
            sendLog('alg1', `  成功加载 ${loadedCount}/${Object.keys(sources).length} 个数据源`);

            const realtimeOverviewSource = overviewDay || overviewMonth || overviewFallback;
            const trendOverviewSource = overviewMonth || overviewDay || overviewFallback;
            const getOverviewViewType = overview => overview?.page?.viewType || 'unknown';
            if (!overviewDay && !overviewMonth && overviewFallback) {
                sendLog('alg1', '  ⚠ 未找到概览-日/月，回退使用默认概览数据');
            }

            // Normalize
            let realtimeRows = null, trendRows = null;
            if (realtimeOverviewSource) {
                realtimeRows = metrics.normalizeOverviewRows(realtimeOverviewSource);
                sendLog('alg1', `  ✓ 即时指标使用概览-${getOverviewViewType(realtimeOverviewSource)} 数据`);
            }
            if (trendOverviewSource) {
                trendRows = metrics.normalizeOverviewRows(trendOverviewSource);
                sendLog('alg1', `  ✓ 趋势指标使用概览-${getOverviewViewType(trendOverviewSource)} 数据`);
            }

            let hotNorm = {};
            if (hotToday)     { hotNorm.today = metrics.normalizeHotProducts(hotToday);         sendLog('alg1', '  ✓ 店热销-今 已规范化'); }
            if (hotYesterday) { hotNorm.yesterday = metrics.normalizeHotProducts(hotYesterday);  sendLog('alg1', '  ✓ 店热销-昨 已规范化'); }
            if (hotWeek)      { hotNorm.week = metrics.normalizeHotProducts(hotWeek);           sendLog('alg1', '  ✓ 店热销-周 已规范化'); }
            if (hotMonth)     { hotNorm.month = metrics.normalizeHotProducts(hotMonth);         sendLog('alg1', '  ✓ 店热销-月 已规范化'); }

            const o2oRows = o2oDay?.businessTable?.rows || [];
            const latestO2ORow = o2oRows.length ? o2oRows[o2oRows.length - 1] : null;
            const latestDayRow = realtimeRows?.[0] || null;
            const previousDayRow = realtimeRows?.[1] || null;
            const baselineRows = realtimeRows && realtimeRows.length > 1
                ? realtimeRows.slice(1).filter(r => r?.visitorCount > 0)
                : [];
            const baselineAvgOrder = baselineRows.length > 0
                ? baselineRows.reduce((sum, r) => sum + (r.revenue / r.visitorCount), 0) / baselineRows.length
                : null;
            const overallAvgOrder = latestDayRow && latestDayRow.visitorCount > 0 ? latestDayRow.revenue / latestDayRow.visitorCount : null;
            const summaryMetrics = realtimeOverviewSource?.summary?.metrics || [];
            const findMetric = key => summaryMetrics.find(m => m.key === key) || null;
            const metricValue = key => findMetric(key)?.value ?? null;
            const getMetricComparisonValue = key => {
                const metric = findMetric(key);
                return metric?.mom?.value ?? metric?.compare?.value ?? null;
            };
            const memberAvgOrder = metricValue('member_avg_order_value');
            const memberRevenue = metricValue('member_revenue') ?? latestDayRow?.memberAmount ?? null;
            const totalRevenue = latestDayRow?.revenue ?? metricValue('revenue') ?? null;
            const previousMemberRevenue = previousDayRow?.memberAmount ?? null;
            const memberChangePct = previousMemberRevenue > 0
                ? ((memberRevenue - previousMemberRevenue) / previousMemberRevenue) * 100
                : getMetricComparisonValue('member_revenue');
            const memberOrderCount = metricValue('member_order_count');
            const memberOrderChangePct = getMetricComparisonValue('member_order_count');
            const memberGrossMarginPct = memberRevenue > 0 && latestDayRow?.memberGrossProfit != null
                ? (latestDayRow.memberGrossProfit / memberRevenue) * 100
                : metricValue('member_gross_margin');
            const overallGrossMarginPct = latestDayRow?.revenue > 0
                ? (latestDayRow.grossProfit / latestDayRow.revenue) * 100
                : null;
            const revenueChangePctForQuality = previousDayRow?.revenue > 0
                ? ((latestDayRow.revenue - previousDayRow.revenue) / previousDayRow.revenue) * 100
                : null;
            const grossProfitChangePctForQuality = previousDayRow?.grossProfit > 0
                ? ((latestDayRow.grossProfit - previousDayRow.grossProfit) / previousDayRow.grossProfit) * 100
                : null;
            const o2oRevenue = latestO2ORow?.total_revenue ?? null;
            const top500TotalProducts = top500Total?.products || [];
            const top500OutProducts = top500Out?.products || [];
            const top500MissingProducts = top500Missing?.products || [];
            const revenueChangeRes = metrics.calcRevenueChange(realtimeRows);
            const revenueConsecutiveRes = metrics.calcConsecutiveChange(realtimeRows, 'revenue');
            const channelMixRes = metrics.calcChannelMix(realtimeOverviewSource?.sourceDistribution);
            const platformConcentrationRes = metrics.calcPlatformConcentration(latestO2ORow);
            const o2oTrendRevenueRes = metrics.calcO2OTrend(o2oRows, 'total_revenue');
            const o2oChangeValues = o2oTrendRevenueRes?.changes?.map(c => c.changePct) || [];
            const o2oVolatility = o2oChangeValues.length > 1
                ? Math.sqrt(o2oChangeValues.reduce((sum, v) => sum + (v ** 2), 0) / o2oChangeValues.length) / 100
                : null;

            await sleep(200);
            sendStatus('alg1', 'success');

            // ── alg2: 算指标 ──
            if (globalJobState.forceStop) throw new Error('Aborted');
            sendStatus('alg2', 'active');
            sendLog('alg2', '开始逐项计算指标...');

            // 准备指标任务列表 — 全部注册，缺数据时函数自行返回 uncountable
            const metricTasks = [];
            const metricResults = {};
            const metricResultsAll = {};
            const metricsSkippedToNext = [];

            metricTasks.push({ name: 'calcChannelMix (渠道结构)', fn: () => channelMixRes });
            metricTasks.push({ name: 'calcGrossMargin (毛利率)', fn: () => metrics.calcGrossMargin(latestDayRow?.grossProfit, latestDayRow?.revenue) });
            metricTasks.push({ name: 'calcAvgOrderValue (客单价)', fn: () => metrics.calcAvgOrderValue(latestDayRow?.revenue, latestDayRow?.visitorCount, baselineAvgOrder) });
            metricTasks.push({ name: 'calcMemberPenetration (会员渗透率)', fn: () => metrics.calcMemberPenetration(memberRevenue, totalRevenue) });
            metricTasks.push({ name: 'calcMemberVsOverall (会员客单价对比)', fn: () => metrics.calcMemberVsOverall(memberAvgOrder, overallAvgOrder) });
            metricTasks.push({ name: 'calcConsecutiveChange (连续涨跌-营收)', fn: () => metrics.calcConsecutiveChange(realtimeRows, 'revenue') });
            metricTasks.push({ name: 'calcConsecutiveChange (连续涨跌-来客)', fn: () => metrics.calcConsecutiveChange(realtimeRows, 'visitorCount') });
            metricTasks.push({ name: 'calcGrossMarginTrend (毛利率趋势)', fn: () => metrics.calcGrossMarginTrend(trendRows) });
            metricTasks.push({ name: 'calcPlatformConcentration (平台集中度)', fn: () => platformConcentrationRes });
            metricTasks.push({ name: 'calcO2OGrossMargin (O2O毛利率)', fn: () => metrics.calcO2OGrossMargin(latestO2ORow) });
            metricTasks.push({ name: 'calcO2OTrend (O2O营收趋势)', fn: () => o2oTrendRevenueRes });
            metricTasks.push({ name: 'calcO2OvsTotal (O2O占整体营收比)', fn: () => metrics.calcO2OvsTotal(o2oRevenue, totalRevenue) });
            metricTasks.push({ name: 'calcHotProductConcentration (热销集中度)', fn: () => metrics.calcHotProductConcentration(hotNorm?.today) });
            metricTasks.push({ name: 'calcProductStability (爆品稳定性)', fn: () => metrics.calcProductStability(hotNorm) });
            metricTasks.push({ name: 'calcHighRankStockoutAlert (缺货预警)', fn: () => metrics.calcHighRankStockoutAlert(top500Out?.products) });
            metricTasks.push({ name: 'calcStockoutRate (缺货率)', fn: () => metrics.calcStockoutRate(top500TotalProducts, top500OutProducts) });
            metricTasks.push({ name: 'calcMissingCategoryRate (缺种率)', fn: () => metrics.calcMissingCategoryRate(top500TotalProducts, top500MissingProducts) });
            metricTasks.push({ name: 'calcActiveSKUCount (动销SKU数)', fn: () => metrics.calcActiveSKUCount(hotNorm?.today) });
            metricTasks.push({ name: 'detectConsecutiveDecline (连续下滑预警)', fn: () => metrics.detectConsecutiveDecline(realtimeRows, 'revenue') });
            metricTasks.push({ name: 'detectLowMemberAlert (会员异常低预警)', fn: () => metrics.detectLowMemberAlert(memberRevenue, totalRevenue) });
            metricTasks.push({ name: 'detectChannelImbalance (渠道失衡预警)', fn: () => metrics.detectChannelImbalance(channelMixRes) });
            metricTasks.push({ name: 'prepareStoreStatusLabel (门店状态标签预判)', fn: () => metrics.prepareStoreStatusLabel({
                revenueChange: revenueChangeRes.status === 'uncountable' ? null : revenueChangeRes.changePct,
                grossMarginChange: (latestDayRow?.revenue > 0 && previousDayRow?.revenue > 0)
                    ? (((latestDayRow.grossProfit / latestDayRow.revenue) - (previousDayRow.grossProfit / previousDayRow.revenue)) * 100)
                    : null,
                visitorChange: previousDayRow?.visitorCount > 0 ? ((latestDayRow.visitorCount - previousDayRow.visitorCount) / previousDayRow.visitorCount) * 100 : null,
                avgOrderChange: previousDayRow?.visitorCount > 0 && latestDayRow?.visitorCount > 0
                    ? (((latestDayRow.revenue / latestDayRow.visitorCount) - (previousDayRow.revenue / previousDayRow.visitorCount)) / (previousDayRow.revenue / previousDayRow.visitorCount)) * 100
                    : null,
                memberPenetration: totalRevenue > 0 ? (memberRevenue / totalRevenue) * 100 : null,
                ecommerceRatio: totalRevenue > 0 ? ((latestDayRow?.ecommerceAmount || 0) / totalRevenue) * 100 : null,
                consecutiveDecline: revenueConsecutiveRes.status === 'uncountable'
                    ? null
                    : (revenueConsecutiveRes.direction === 'down' ? revenueConsecutiveRes.consecutiveDays : 0),
                volatility: o2oTrendRevenueRes.status === 'uncountable' ? null : o2oVolatility
            }) });
            // B类
            metricTasks.push({ name: 'calcRevenueChange (营收环比)', fn: () => revenueChangeRes });
            metricTasks.push({ name: 'prepareGrowthDecomposition (增长拆解)', fn: () => metrics.prepareGrowthDecomposition(realtimeRows) });
            metricTasks.push({ name: 'prepareSalesQualityCheck (销售质量)', fn: () => metrics.prepareSalesQualityCheck({
                revenueChangePct: revenueChangePctForQuality,
                grossProfitChangePct: grossProfitChangePctForQuality,
                grossMarginCurrent: overallGrossMarginPct,
                grossMarginPrevious: previousDayRow?.revenue > 0 ? (previousDayRow.grossProfit / previousDayRow.revenue) * 100 : null
            }) });
            metricTasks.push({ name: 'prepareMemberHealthCheck (会员健康)', fn: () => metrics.prepareMemberHealthCheck({
                memberRevenue,
                memberChangePct,
                memberOrderCount,
                memberOrderChangePct,
                memberAvgOrder,
                memberGrossMarginPct,
                totalRevenue,
                overallAvgOrder,
                overallGrossMarginPct
            }) });
            metricTasks.push({ name: 'prepareStockoutLossEstimate (缺货损失)', fn: () => metrics.prepareStockoutLossEstimate(top500Out?.products, 35) });
            metricTasks.push({ name: 'prepareChannelRiskAssessment (渠道风险)', fn: () => metrics.prepareChannelRiskAssessment({
                channelMix: channelMixRes,
                o2oTrend: o2oTrendRevenueRes,
                platformConcentration: platformConcentrationRes
            }) });

            const total = metricTasks.length;
            for (let i = 0; i < total; i++) {
                if (globalJobState.forceStop) throw new Error('Aborted');
                const task = metricTasks[i];
                const result = task.fn();
                const statusIcon = result.status === 'warning' ? '🔴' :
                                   result.status === 'attention' ? '🟡' :
                                   result.status === 'uncountable' ? '⚪' : '🟢';
                metricResultsAll[task.name] = result;
                if (result.status !== 'uncountable') {
                    metricResults[task.name] = result;
                } else {
                    metricsSkippedToNext.push(task.name);
                }
                sendLog('alg2', `  [${i + 1}/${total}] ${statusIcon} ${task.name} → ${result.status}`);
                // 发送进度事件
                sendProgress('alg2', i + 1, total);
                await sleep(150);  // 略微延迟让前端看到逐条更新
            }

            if (metricsSkippedToNext.length > 0) {
                sendLog('alg2', `  ⚪ uncountable 不下传: ${metricsSkippedToNext.length} 项`);
            }
            sendLog('alg2', `完成: ${total} 个指标已计算`);
            sendStatus('alg2', 'success');

            // ── alg3: 检查异常（含 B3 异常汇总） ──
            if (globalJobState.forceStop) throw new Error('Aborted');
            sendStatus('alg3', 'active');
            sendLog('alg3', '汇总异常检测结果...');

            // B3: prepareAnomalySummary — 整合所有结果
            const anomalySummary = metrics.prepareAnomalySummary(metricResults);
            anomalySummaryData = anomalySummary;
            const tally = {
                ...(anomalySummary.aiPromptData?.tally || { pass: 0, attention: 0, warning: 0, uncountable: 0 }),
                // uncountable 不下传到下一层，因此这里按全量指标重新统计用于监控展示
                uncountable: Object.values(metricResultsAll).filter(r => r.status === 'uncountable').length
            };

            sendLog('alg3', `  🟢 pass: ${tally.pass}`);
            sendLog('alg3', `  🟡 attention: ${tally.attention}`);
            sendLog('alg3', `  🔴 warning: ${tally.warning}`);
            if (tally.uncountable > 0) {
                sendLog('alg3', `  ⚪ uncountable: ${tally.uncountable}`);
            }

            // 输出排序后的异常清单
            anomalySummary.sortedAlerts.forEach(a => {
                let icon = '🟡';
                if (a.status === 'warning') icon = '🔴';
                if (a.status === 'pass') icon = '✅';
                sendLog('alg3', `  ${icon} [${a.status}] ${a.metric}: ${a.detail}`);
            });

            // 发送异常汇总事件给前端
            sendTally('alg3', tally);

            if (tally.warning > 0) {
                sendLog('alg3', `⚠️ 发现 ${tally.warning} 项 warning 级异常`);
            } else if (tally.attention > 0) {
                sendLog('alg3', `💡 发现 ${tally.attention} 项 attention 级提醒`);
            } else {
                sendLog('alg3', '✅ 所有指标正常');
            }

            await sleep(200);
            sendStatus('alg3', 'success');


        };

        await Promise.all([aiFlow(), algoFlow()]);

        if (globalJobState.forceStop) throw new Error('Aborted');
        sendStatus('fusion', 'active');
        sendLog('fusion', '数据融合：合并初级报告、错误评审、异常日志...');
        
        const aiReportData = aiReport || `## 现状分析报告 (基于 ${jsonList.length} 个数据源)\n\n- **数据清洗**：已完成自动识别与结构化处理。\n\n> ⚠️ 模拟报告。请在设置中配置 API。\n\n## 优化建议\n\n> ⚠️ 模拟建议。配置 API 后将获得个性化建议。`;
        const finalReviewStr = reviewResult || errorReviewer.reviewError(aiReport);
        
        let anomalyLogsStr = "### 算法检查异常日志 (全量)\n\n";
        if (anomalySummaryData && anomalySummaryData.sortedAlerts && anomalySummaryData.sortedAlerts.length > 0) {
            anomalySummaryData.sortedAlerts.forEach(a => {
                let icon = '🟡';
                if (a.status === 'warning') icon = '🔴';
                if (a.status === 'pass') icon = '✅';
                anomalyLogsStr += `${icon} **${a.metric}** (${a.status}): ${a.detail}\n\n`;
            });
        } else {
            anomalyLogsStr += "✅ 所有算法指标均未见明显异常。\n\n";
        }

        const fusedReportText = aiReportData + "\n\n### 错误评审意见\n> " + finalReviewStr + "\n\n" + anomalyLogsStr;
        await sleep(500);
        sendStatus('fusion', 'success');

        sendStatus('rep1', 'active');
        sendLog('rep1', '调用AI输出完整报告...');
        if (globalJobState.forceStop) throw new Error('Aborted');
        
        let detailedReport = null;
        if (settings && settings.apiKey) {
            try {
                const t0 = Date.now();
                const detailedRes = await withTimerLog('rep1', '完整报告 AI', () => aiCaller.callDetailedAI(settings, fusedReportText));
                const elapsed = ((Date.now() - t0) / 1000).toFixed(1);

                // 打印原始 JSON 到监控日志
                sendLog('rep1', '--- 原始完整报告 AI 响应 JSON ---');
                sendLog('rep1', JSON.stringify(detailedRes, null, 2));

                detailedReport = detailedRes.choices[0].message.content;
                sendLog('rep1', `完整报告生成完成 (${elapsed}s)`);
                sendStatus('rep1', 'success');
            } catch (err) {
                sendLog('rep1', `AI调用失败: ${err.message}`);
                detailedReport = fusedReportText + "\n\n> ⚠️ 完整报告生成失败: " + err.message;
                sendStatus('rep1', 'error');
            }
        } else {
            sendLog('rep1', '未配置 API Key，模拟完整报告输出');
            await sleep(500);
            detailedReport = fusedReportText + "\n\n> ⚠️ 模拟完整报告。请在设置中配置 API。\n";
            sendStatus('rep1', 'simulated');
        }

        sendStatus('rep2', 'active');
        sendLog('rep2', '调用AI输出精简报告...');
        if (globalJobState.forceStop) throw new Error('Aborted');
        
        let simplifiedReport = null;
        if (settings && settings.apiKey) {
            try {
                const t0 = Date.now();
                const simpleRes = await withTimerLog('rep2', '精简报告 AI', () => aiCaller.callSimplifiedAI(settings, detailedReport));
                const elapsed = ((Date.now() - t0) / 1000).toFixed(1);
                
                // 打印原始 JSON 到监控日志
                sendLog('rep2', '--- 原始精简报告 AI 响应 JSON ---');
                sendLog('rep2', JSON.stringify(simpleRes, null, 2));

                simplifiedReport = simpleRes.choices[0].message.content; // It should be a string of JSON
                sendLog('rep2', `精简报告生成完成 (${elapsed}s)`);
                sendStatus('rep2', 'success');
            } catch (err) {
                sendLog('rep2', `AI调用失败: ${err.message}`);
                simplifiedReport = JSON.stringify({ 
                    health_status: "获取失败", 
                    overview_text: "精简报告生成失败: " + err.message,
                    cards: []
                });
                sendStatus('rep2', 'error');
            }
        } else {
            sendLog('rep2', '未配置 API Key，模拟精简报告输出');
            await sleep(500);
            simplifiedReport = JSON.stringify({
                health_status: "模拟模式",
                overview_text: "当前处于模拟模式，未连接大模型。请配置 API Key 获得真实的经营诊断。",
                cards: [
                    {
                        title: "配置缺失",
                        explanation: "系统未能请求到真实大模型。",
                        suggestion: "点击右上角设置图标，填写你的 API Key 和接口地址。",
                        evidence: "系统检测到 settings 中 apiKey 为空。",
                        color: "yellow"
                    }
                ]
            });
            sendStatus('rep2', 'simulated');
        }

        const report = simplifiedReport; // 这是精简报告 (JSON string)
        const fullReport = detailedReport; // 这是完整报告 (Markdown)

        if (!globalJobState.forceStop) {
            globalJobState.status = 'completed';
            globalJobState.result = report;
            globalJobState.fullResult = fullReport;
        }

    } catch (error) {
        if (error.message === 'Aborted') {
            console.log("Worker execution was aborted.");
        } else {
            console.error(error);
            throw error; // Let the caller catch and set error status
        }
    }
}

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    console.log(`服务器运行在 http://localhost:${PORT}`);
});
