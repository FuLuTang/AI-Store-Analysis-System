const REVIEW_SYSTEM_PROMPT = `
你是一位极其严谨的零售数据审计专家。你的任务是“检查初级报告中是否有明显数据异常”，进行二次复核。
你需要重点关注以下几类常见分析错误，并根据提供给你的【底层原始数据】进行核对：
1. **周期换算错误**：比如年/月下滑 30% 是否仅仅是因为当前还没到年/月底？（例如今天是 5 月 8 日，5 月的当前累积数据如果直接和 4 月全月比，必然是下滑的）。如果是这种情况，请按天数比例换算过来（等效完整年/月收入），看看等效后的数据是否有参考性，具体数值大概是多少。
2. **无意义商品的干扰**：检查初级报告里提到的“热销商品”或“缺货商品”，是否有矿泉水、普通塑料袋等对整体经营诊断无重大指导意义的低毛利/高频凑单商品影响了判断？如果有，请指出来。
3. **数据自相矛盾**：初级报告中的结论是否与提供的数据明显冲突？（比如数据明明是上涨的，报告却说是下跌；或者提到某个商品缺货，但底层数据里明确显示有货）。

**输出要求：**
- 你的输出应当是纯文本形式的【评审意见】。
- 如果初级报告没问题，只需回答：“错误审核通过，暂未发现明显逻辑或计算谬误。”
- 如果发现了问题，请逐条列出：“发现异常：1. ... 2. ...”，明确指出初级报告哪里说错了，并在后续的详细重写阶段建议 AI 纠正。
- 请直接输出结论，无需包含过多的客套话。
`;

async function reviewError(settings, report, cleanedDataTexts) {
    if (!settings || !settings.apiKey) {
        return "初级分析报告 - 错误审核：未配置 API Key，已跳过真实审核，采用模拟通过。";
    }

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

    const userContent = contextHeader + 
        '\n\n【初级分析报告】\n' + report + 
        '\n\n【底层原始数据】\n' + (cleanedDataTexts ? cleanedDataTexts.join('\n\n---\n\n') : '暂无底层数据');

    const url = baseUrl.replace(/\/+$/, '') + '/chat/completions';

    try {
        const response = await fetch(url, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${apiKey}`
            },
            body: JSON.stringify({
                model,
                messages: [
                    { role: 'system', content: REVIEW_SYSTEM_PROMPT },
                    { role: 'user', content: userContent }
                ],
                temperature: 0.2, // 较低温度以保证客观严谨
                max_tokens: 2048
            })
        });

        if (!response.ok) {
            const errText = await response.text();
            throw new Error(`(${response.status}): ${errText}`);
        }

        const data = await response.json();
        return data.choices[0].message.content;
    } catch (err) {
        console.error("错误评审 AI 调用失败:", err);
        return `错误审核失败，跳过审核流程。错误原因：${err.message}`;
    }
}

module.exports = { reviewError };
