const SYSTEM_PROMPT = `
你是一位资深连锁药店经营分析顾问。你将收到一家门店的多维经营数据，数据已清洗为紧凑格式。

数据类型：
- business_overview：营业概览，含营收、来客数、客单价、毛利、会员、电商、业绩来源占比及环比趋势
- store_hot_products：门店热销商品，按今/昨/周/月分组
- hot_top500_stock_status：全城热销TOP500库存对照
- **Algorithm Analysis Results**: 算法引擎预先计算的诊断指标和异常清单，请优先参考其中的结论。

你的任务：
基于数据生成一份【经营诊断报告】，聚焦颗粒度为年、月、周。请将现状诊断报告与优化行动方案（如果有）写在一起。

# 第一部分：现状诊断报告
1. 核心经营判断 （现在是涨是跌还是稳定还是波动，可能原因是什么，数据是否正常，带emojis如📈📉）
2. 热销商品变化趋势分析
3. 缺货/缺种损失评估
4. 风险预警（可选）

# 第二部分：优化行动方案（可选部分，不一定都有）（可根据不同策略提供多元化建议）
1. 紧急补货
2. 品种引进
3. 毛利/客单提升
4. 促活
5. 根据你的经验，能想到的其他可能有效的方案

ideas:
1. 考虑到店的位置，气温，时间天气，社会环境因素，近期事件。开拓思维
2. 可结合当前季节、气温（春/夏季交替）及社会因素进行发散性诊断
3. 因为没有同比数据，所以环比数据要注意折算！关于今日/月/年，考虑到当前时间，要对当前月年数据计算出等效“全月/年”数据才有参考性

输出要求：
- Markdown 格式，大小标题（比如 # ## ### ...），列点，格式化，必要时用表格或Mermaid美化输出
- 两大段：用 # 现状诊断报告 ... \n --- \n # 优化行动方案 ... 来分隔
- 全文控制在约1000字
- 不要使用过长段 bullet
- 每个 bullet 不超过 45 字
- 总结只保留 4 个核心问题
- 商品最多提 6 个
- 不要把“建议：”写成单独一行
`;

/**
 * 调用 OpenAI-compatible API
 */
async function callAI(settings, cleanedDataTexts, algoData) {
    const { baseUrl, apiKey, model } = settings;

    // 构建基础上下文信息
    const now = new Date();
    const dateStr = now.toLocaleDateString('zh-CN', { year: 'numeric', month: 'long', day: 'numeric', weekday: 'long' });
    const timeStr = now.toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit' });

    const contextHeader = `【当前分析环境】
- 城市：福州
- 日期：${dateStr}
- 时间：${timeStr}
`;

    // 将算法结果转换为 Markdown
    let algoText = '【算法引擎预诊结果】\n暂无算法诊断结果。';
    if (algoData && algoData.anomalies) {
        const { summary, alerts } = algoData.anomalies;
        algoText = `【算法引擎预诊结果】\n> ${summary}\n\n`;
        alerts.forEach(a => {
            const icon = a.severity === 'warning' ? '🔴' : '🟡';
            algoText += `${icon} **${a.metric}** (${a.severity}): ${a.detail}\n`;
        });
    }

    const userContent = contextHeader + '\n' + algoText + '\n\n【底层原始数据】\n' +
        cleanedDataTexts.join('\n\n---\n\n');

    const url = baseUrl.replace(/\/+$/, '') + '/chat/completions';

    const response = await fetch(url, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${apiKey}`
        },
            body: JSON.stringify({
                model,
                messages: [
                    { role: 'system', content: SYSTEM_PROMPT },
                    { role: 'user', content: userContent }
                ],
                reasoning_effort: "medium",
                temperature: 0.3,
                max_tokens: 16384
            })
    });

    if (!response.ok) {
        const errText = await response.text();
        throw new Error(`AI API 调用失败 (${response.status}): ${errText}`);
    }

    const data = await response.json();
    return data; // 返回完整对象
}

const DETAILED_SYSTEM_PROMPT = `
你是一位资深数据分析师与零售专家。你将收到一份经过初步处理的【门店分析融合报告】，包含：
1. 初级AI诊断报告
2. 明显错误评审意见
3. 算法引擎检测出的异常日志

你的任务：
基于这些融合信息，深度重写并输出一份【更详细、结构更严谨的最终经营诊断报告】。

注意：
- 初级报告中可能存在谬误，请务必结合评审意见进行核对
- “指标”是用来补充初级报告中未发现的问题的，主要是看给出的数据和二次计算出的信息。
- “指标”中的结果很可能和初级报告有冲突，请自行判断并融合

你需要：
- 纠正初级报告中被“错误评审”指出的逻辑或计算谬误。
- 结合“异常检测日志”，挖掘更深层次的业务根因。
- 提供更加具体、可落地的优化行动方案。
- 保持专业的商业分析语调。
- 格式化输出，采用合适的标题、列表和加粗，让重点一目了然。
- 结尾不需要“如果你愿意，我可以帮你...”等字样
`;

async function callDetailedAI(settings, fusedReportText) {
    const { baseUrl, apiKey, model } = settings;

    // 构建基础上下文信息
    const now = new Date();
    const dateStr = now.toLocaleDateString('zh-CN', { year: 'numeric', month: 'long', day: 'numeric', weekday: 'long' });
    const timeStr = now.toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit' });

    const contextHeader = `【当前分析环境】
- 城市：福州
- 日期：${dateStr}
- 时间：${timeStr}
`;

    const userContent = contextHeader + '\n' + fusedReportText;

    const url = baseUrl.replace(/\/+$/, '') + '/chat/completions';

    const response = await fetch(url, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${apiKey}`
        },
        body: JSON.stringify({
            model,
            messages: [
                { role: 'system', content: DETAILED_SYSTEM_PROMPT },
                { role: 'user', content: userContent }
            ],
            reasoning_effort: "medium",
            temperature: 0.4,
            max_tokens: 16384
        })
    });

    if (!response.ok) {
        const errText = await response.text();
        throw new Error(`详细报告 AI API 调用失败 (${response.status}): ${errText}`);
    }

    const data = await response.json();
    return data;
}

const SIMPLIFIED_SYSTEM_PROMPT = `
你是一位资深连锁药店经营分析顾问，你的任务是为管理层（老板）提供一份“一眼定真问题”的【精简诊断报告】。
你将收到一份经过深度分析的详细报告。请提取最核心的信息，并严格按照以下 JSON 格式输出，不要包含任何其他字符或 Markdown 格式（不要包含 \`\`\`json）：

{
  "health_status": "这里填1-2个词的整体状态，如：健康 / 波动下 / 季节性影响 / 需紧急干预",
  "overview_text": "用一句大白话总结门店当前的整体经营状况，突出最关键的结论。",
  "cards": [
    {
      "title": "问题标题（如：客流严重下滑问题；需要紧急补货；注意毛利下降；爆品的连带效应）",
      "explanation": "大白话大概说说怎么回事，发生了什么，为什么。",
      "suggestion": "咋办（具体的行动建议）。",
      "evidence": "相关数据（解释来由，怎么分析出来的，给出证据，可以是带重点数据的一句话或列表）。",
      "color": "体现问题严重性，可选值：red(严重警告), yellow(需要注意), green(表现良好), blue(中性信息)"
    }
  ]
}

要求：
1. cards 最多只能有 7 个，提取最严重或最值得关注的点。没问题的不需要强行凑数展示。
2. 语言必须是大白话，让老板能看懂。
3. color 的选择：跌得很惨/缺货用 red，轻微下滑/潜在风险用 yellow，涨得好用 green，正常情况介绍用 blue。
`;

async function callSimplifiedAI(settings, detailedReportText) {
    const { baseUrl, apiKey, model } = settings;

    // 构建基础上下文信息
    const now = new Date();
    const dateStr = now.toLocaleDateString('zh-CN', { year: 'numeric', month: 'long', day: 'numeric', weekday: 'long' });
    const timeStr = now.toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit' });

    const contextHeader = `【当前分析环境】
- 城市：福州
- 日期：${dateStr}
- 时间：${timeStr}
`;

    const userContent = contextHeader + '\n\n【详细报告内容】\n' + detailedReportText;

    const url = baseUrl.replace(/\/+$/, '') + '/chat/completions';

    const response = await fetch(url, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${apiKey}`
        },
        body: JSON.stringify({
            model,
            messages: [
                { role: 'system', content: SIMPLIFIED_SYSTEM_PROMPT },
                { role: 'user', content: userContent }
            ],
            response_format: { type: "json_object" },
            temperature: 0.3,
            max_tokens: 4000
        })
    });

    if (!response.ok) {
        const errText = await response.text();
        throw new Error(`精简报告 AI API 调用失败 (${response.status}): ${errText}`);
    }

    const data = await response.json();
    return data;
}

module.exports = { callAI, callDetailedAI, callSimplifiedAI };
