const express = require('express');
const cors = require('cors');
const fs = require('fs');
const path = require('path');
const cleaner = require('./cleaner');
const aiCaller = require('./ai-caller');
const metrics = require('./metrics');
const errorReviewer = require('./error-reviewer');

const app = express();
app.use(cors());
app.use(express.json({ limit: '50mb' }));
app.use(express.static('public'));

const CACHE_DIR = path.join(__dirname, 'data_cache');
if (!fs.existsSync(CACHE_DIR)) fs.mkdirSync(CACHE_DIR);

let sseClients = [];
let aiReportCache = null;

// SSE 端点
app.get('/api/stream', (req, res) => {
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    sseClients.push(res);
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
    // Send as unnamed event so eventSource.onmessage catches it
    sseClients.forEach(client => client.write(`data: ${JSON.stringify(payload)}\n\n`));
};

// 状态发送辅助函数
const sendStatus = (nodeId, status) => sendEvent('status', { nodeId, status });
const sendLog = (nodeId, msg) => sendEvent('log', { nodeId, message: msg });
const sendProgress = (nodeId, current, total) => sendEvent('progress', { nodeId, current, total });
const sendTally = (nodeId, tally) => sendEvent('tally', { nodeId, tally });
const resetNodes = () => sendEvent('reset', {});

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

// 分析接口（旧接口别名）
app.post('/analyze', (req, res) => {
    // 转发给 /api/run 逻辑或直接复用
    return handleRun(req, res);
});

app.post('/api/run', handleRun);

async function handleRun(req, res) {
    const jsonList = Array.isArray(req.body) ? req.body : req.body.files;
    const settings = req.body.settings || null;

    try {
        resetNodes();

        // 1. JSON 传入
        sendStatus('input', 'active');
        sendLog('input', `收到 ${jsonList.length} 个 JSON 文件`);
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
            sendStatus('clean', 'active');
            sendLog('clean', '清理缓存目录...');
            cleaner.clearCache(CACHE_DIR);
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
            sendStatus('api', 'active');
            if (settings && settings.apiKey) {
                const cacheFiles = fs.readdirSync(CACHE_DIR).filter(f => f.endsWith('.json'));
                const cleanedTexts = cacheFiles.map(f => fs.readFileSync(path.join(CACHE_DIR, f), 'utf8'));
                const totalChars = cleanedTexts.reduce((s, t) => s + t.length, 0);

                sendLog('api', `模型: ${settings.model}`);
                sendLog('api', `数据源: ${cleanedTexts.length} 个, ~${totalChars} 字符`);
                sendLog('api', `请求 ${settings.baseUrl} ...`);

                const t0 = Date.now();
                const aiFullResponse = await aiCaller.callAI(settings, cleanedTexts);
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
            sendStatus('output', 'active');
            if (aiReport) {
                sendLog('output', `报告内容: ${aiReport.length} 字符`);
            } else {
                sendLog('output', '使用模拟报告');
            }
            await sleep(300);
            sendStatus('output', aiReport ? 'success' : 'simulated');

            // 5. 报告明显错误评审
            sendStatus('review', 'active');
            reviewResult = errorReviewer.reviewError(aiReport);
            sendLog('review', `完成错误审核: ${reviewResult}`);
            await sleep(300);
            sendStatus('review', 'success');
        };

        const algoFlow = async () => {
            const jsonDir = path.join(__dirname, '..', 'json案例');
            const loadJson = (filename) => {
                try { return JSON.parse(fs.readFileSync(path.join(jsonDir, filename), 'utf-8')); }
                catch { return null; }
            };

            // ── alg1: 数据整理 ──
            sendStatus('alg1', 'active');
            sendLog('alg1', '加载 JSON 数据源...');

            const overviewDay   = loadJson('概览-日.json');
            const overviewMonth = loadJson('概览-月.json');
            const hotToday      = loadJson('店热销-今.json');
            const hotYesterday  = loadJson('店热销-昨.json');
            const hotWeek       = loadJson('店热销-周.json');
            const hotMonth      = loadJson('店热销.月.json');
            const top500Out     = loadJson('热销500-缺货.json');

            const sources = { overviewDay, overviewMonth, hotToday, hotYesterday, hotWeek, hotMonth, top500Out };
            const loadedCount = Object.values(sources).filter(Boolean).length;
            sendLog('alg1', `  成功加载 ${loadedCount}/${Object.keys(sources).length} 个数据源`);

            // Normalize
            let dayRows = null, monthRows = null;
            if (overviewDay)   { dayRows = metrics.normalizeOverviewRows(overviewDay);   sendLog('alg1', '  ✓ 概览-日 已规范化'); }
            if (overviewMonth) { monthRows = metrics.normalizeOverviewRows(overviewMonth); sendLog('alg1', '  ✓ 概览-月 已规范化'); }

            let hotNorm = {};
            if (hotToday)     { hotNorm.today = metrics.normalizeHotProducts(hotToday);         sendLog('alg1', '  ✓ 店热销-今 已规范化'); }
            if (hotYesterday) { hotNorm.yesterday = metrics.normalizeHotProducts(hotYesterday);  sendLog('alg1', '  ✓ 店热销-昨 已规范化'); }
            if (hotWeek)      { hotNorm.week = metrics.normalizeHotProducts(hotWeek);           sendLog('alg1', '  ✓ 店热销-周 已规范化'); }
            if (hotMonth)     { hotNorm.month = metrics.normalizeHotProducts(hotMonth);         sendLog('alg1', '  ✓ 店热销-月 已规范化'); }

            await sleep(200);
            sendStatus('alg1', 'success');

            // ── alg2: 算指标 ──
            sendStatus('alg2', 'active');
            sendLog('alg2', '开始逐项计算指标...');

            // 准备指标任务列表 — 全部注册，缺数据时函数自行返回 uncountable
            const metricTasks = [];
            const metricResults = {};

            metricTasks.push({ name: 'calcChannelMix (渠道结构)', fn: () => metrics.calcChannelMix(overviewDay?.sourceDistribution) });
            metricTasks.push({ name: 'calcConsecutiveChange (连续涨跌-营收)', fn: () => metrics.calcConsecutiveChange(dayRows, 'revenue') });
            metricTasks.push({ name: 'calcConsecutiveChange (连续涨跌-来客)', fn: () => metrics.calcConsecutiveChange(dayRows, 'visitorCount') });
            metricTasks.push({ name: 'calcGrossMarginTrend (毛利率趋势)', fn: () => metrics.calcGrossMarginTrend(monthRows) });
            metricTasks.push({ name: 'calcProductStability (爆品稳定性)', fn: () => metrics.calcProductStability(hotNorm) });
            metricTasks.push({ name: 'calcHighRankStockoutAlert (缺货预警)', fn: () => metrics.calcHighRankStockoutAlert(top500Out?.products) });
            // B类
            metricTasks.push({ name: 'prepareGrowthDecomposition (增长拆解)', fn: () => metrics.prepareGrowthDecomposition(dayRows) });
            metricTasks.push({ name: 'prepareStockoutLossEstimate (缺货损失)', fn: () => metrics.prepareStockoutLossEstimate(top500Out?.products, 35) });

            const total = metricTasks.length;
            for (let i = 0; i < total; i++) {
                const task = metricTasks[i];
                const result = task.fn();
                const statusIcon = result.status === 'warning' ? '🔴' :
                                   result.status === 'attention' ? '🟡' :
                                   result.status === 'uncountable' ? '⚪' : '🟢';
                metricResults[task.name] = result;
                sendLog('alg2', `  [${i + 1}/${total}] ${statusIcon} ${task.name} → ${result.status}`);
                // 发送进度事件
                sendProgress('alg2', i + 1, total);
                await sleep(150);  // 略微延迟让前端看到逐条更新
            }

            sendLog('alg2', `完成: ${total} 个指标已计算`);
            sendStatus('alg2', 'success');

            // ── alg3: 检查异常（含 B3 异常汇总） ──
            sendStatus('alg3', 'active');
            sendLog('alg3', '汇总异常检测结果...');

            // B3: prepareAnomalySummary — 整合所有结果
            const anomalySummary = metrics.prepareAnomalySummary(metricResults);
            anomalySummaryData = anomalySummary;
            const tally = anomalySummary.aiPromptData?.tally || { pass: 0, attention: 0, warning: 0, uncountable: 0 };

            sendLog('alg3', `  🟢 pass: ${tally.pass}`);
            sendLog('alg3', `  🟡 attention: ${tally.attention}`);
            sendLog('alg3', `  🔴 warning: ${tally.warning}`);
            if (tally.uncountable > 0) {
                sendLog('alg3', `  ⚪ uncountable: ${tally.uncountable}`);
            }

            // 输出排序后的异常清单
            anomalySummary.sortedAlerts.forEach(a => {
                const icon = a.status === 'warning' ? '🔴' : '🟡';
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

        sendStatus('fusion', 'active');
        sendLog('fusion', '数据融合：合并初级报告、错误评审、异常日志...');
        
        const aiReportData = aiReport || `## 现状分析报告 (基于 ${jsonList.length} 个数据源)\n\n- **数据清洗**：已完成自动识别与结构化处理。\n\n> ⚠️ 模拟报告。请在设置中配置 API。\n\n## 优化建议\n\n> ⚠️ 模拟建议。配置 API 后将获得个性化建议。`;
        const finalReviewStr = reviewResult || errorReviewer.reviewError(aiReport);
        
        let anomalyLogsStr = "### 算法检查异常日志\n\n";
        if (anomalySummaryData && anomalySummaryData.sortedAlerts && anomalySummaryData.sortedAlerts.length > 0) {
            anomalySummaryData.sortedAlerts.forEach(a => {
                const icon = a.status === 'warning' ? '🔴' : '🟡';
                anomalyLogsStr += `${icon} **${a.metric}** (${a.status}): ${a.detail}\n\n`;
            });
        } else {
            anomalyLogsStr += "✅ 所有算法指标均未见明显异常。\n\n";
        }

        const fusedReportText = aiReportData + "\n\n### 错误评审意见\n> " + finalReviewStr + "\n\n" + anomalyLogsStr;
        await sleep(500);
        sendStatus('fusion', 'success');

        sendStatus('rep1', 'active');
        sendLog('rep1', '调用AI输出详细完整报告...');
        
        let detailedReport = null;
        if (settings && settings.apiKey) {
            try {
                const t0 = Date.now();
                const detailedRes = await aiCaller.callDetailedAI(settings, fusedReportText);
                const elapsed = ((Date.now() - t0) / 1000).toFixed(1);
                detailedReport = detailedRes.choices[0].message.content;
                sendLog('rep1', `详细报告生成完成 (${elapsed}s)`);
                sendStatus('rep1', 'success');
            } catch (err) {
                sendLog('rep1', `AI调用失败: ${err.message}`);
                detailedReport = fusedReportText + "\n\n> ⚠️ 详细报告生成失败: " + err.message;
                sendStatus('rep1', 'error');
            }
        } else {
            sendLog('rep1', '未配置 API Key，模拟详细报告输出');
            await sleep(500);
            detailedReport = fusedReportText + "\n\n> ⚠️ 模拟详细报告。请在设置中配置 API。\n";
            sendStatus('rep1', 'simulated');
        }

        sendStatus('rep2', 'active');
        sendLog('rep2', '输出简化报告（未实现）');
        await sleep(300);
        sendStatus('rep2', 'simulated');

        const report = ""; // 简化报告
        const fullReport = detailedReport;

        res.json({ status: 'success', report, fullReport });

    } catch (error) {
        console.error(error);
        res.status(500).json({ error: error.message });
    }
}

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    console.log(`服务器运行在 http://localhost:${PORT}`);
});
