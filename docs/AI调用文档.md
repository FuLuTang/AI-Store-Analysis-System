# AI 调用文档

> 相关文档：[架构设计](./架构设计.md) | [开发文档](./开发文档.md) | [指标计算](./指标计算文档.md)

> 本文档记录系统中每一次 LLM 调用的触发时机、输入/输出格式、提示词策略和内部参数。供后期优化 prompt 和参数调参时参考。

---

## 概览

```
evidence 构建完成
    │
    ▼
┌─────────────────┐
│  AI-1: 初级报告  │  temperature=0.3  max_tokens=16384  reasoning_effort=可配
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  AI-2: 错误审计  │  temperature=0.2  max_tokens=8192   reasoning_effort=可配
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  AI-3: 深度报告  │  temperature=0.4  max_tokens=16384  reasoning_effort=可配
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  AI-4: 精简卡片  │  temperature=0.3  max_tokens=8192   response_format=json
└─────────────────┘
```

**基础配置**（从 LLM Preset 读取）:
- `model`: 模型名（如 `deepseek-v4-flash`）
- `baseUrl`: API 基础地址
- `apiKey`: API 密钥
- `reasoning_effort`: low / medium / high

**请求方式**: 全部使用 HTTP POST 流式 (`/chat/completions`)，使用 `httpx.AsyncClient` 的 SSE 流式读取，绕过超时限制。

---

## AI-1: 初级诊断报告

**函数**: `call_ai(settings, cleaned_data_texts, algo_data)`  
**文件**: `packages/ai/ai_caller.py:158`

### 触发时机

证据包 `build_evidence_bundle()` 完成后，将所有数据打包为 JSON context 传入。

### 输入结构

**System Prompt** (`SYSTEM_PROMPT`, 行 9-48):

```
你是一位资深连锁药店经营分析顾问。你将收到一家门店的多维经营数据。
数据类型：
- business_overview：营业概览
- store_hot_products：门店热销商品
- hot_top500_stock_status：热销TOP500库存
- Algorithm Analysis Results：算法引擎预诊指标和异常清单

你的任务：生成【经营诊断报告】，分两大段：
1. 现状诊断报告：核心经营判断、热销趋势、缺货评估、风险预警
2. 优化行动方案：紧急补货、品种引进、毛利提升、促活等

输出：Markdown，约1000字，bullet不超45字，商品最多6个，总结4个核心问题。
```

**User Content** 组装逻辑 (`call_ai` 行 158-180):

```
【当前分析环境】
- 城市：福州
- 日期：2026年05月12日
- 时间：14:36

【算法引擎预诊结果】
> 共N项指标，X warning，Y attention ...
🔴 metric_name (warning): detail
🟡 metric_name (attention): detail

【底层原始数据】
{context_text}  ← 新管线: JSON对象含 scene/metrics/evidence/mapping/data_quality
                ← 旧管线: 清洗后的紧凑文本
```

### 内部调用参数

| 参数 | 值 | 说明 |
|------|----|------|
| `temperature` | 0.3 | 低温度，保证诊断一致性 |
| `max_tokens` | 16384 | |
| `reasoning_effort` | 从 LLM Preset 读取 | low/medium/high |
| `stream` | true | SSE 流式 |

### 输出格式

Markdown 自由文本，结构：

```
# 现状诊断报告
📈 核心经营判断 ...
1. ...
2. ...

---
# 优化行动方案
1. 紧急补货 ...
2. ...
```

### 关键优化点

- 旧版 system prompt 写死"药店"，未来多行业时需改为动态注入
- `reasoning_effort` 可从用户配置传递
- 报告字数、bullet 长度、商品数量均有硬编码上限

---

## AI-2: 错误审计

**函数**: `review_error(settings, report, cleaned_data_texts)`  
**文件**: `packages/ai/error_reviewer.py:25`

### 触发时机

AI-1 初诊报告生成后立即执行。

### 输入结构

**System Prompt** (`REVIEW_SYSTEM_PROMPT`, 行 8-22):

```
极其严谨的零售数据审计专家。
重点检查：
1. 周期换算错误（年/月下滑是否因为未到月底？）
2. 无意义商品干扰（矿泉水、塑料袋等）
3. 数据矛盾（结论与数据明显冲突）
4. 缺数据导致的误判
5. 推荐行动的可执行性

输出：纯文本评审意见
- 没问题："错误审核通过，暂未发现明显逻辑或计算谬误。"
- 有问题：逐条列出"发现异常：1. ... 2. ..."
```

**User Content** 组装逻辑 (行 25-48):

```
【当前分析环境】
- 城市：福州
- 日期：...
- 时间：...

【初级分析报告】
{AI-1 的完整输出}

【底层原始数据】
{context_text 或 cleaned_data_texts}
```

### 内部调用参数

| 参数 | 值 | 说明 |
|------|----|------|
| `temperature` | 0.2 | 极低温度，审计需要严格、确定性的判断 |
| `max_tokens` | 8192 | 审计意见通常较短 |
| `reasoning_effort` | 从 LLM Preset 读取 | |

### 输出格式

纯文本，两条路径：
- `"错误审核通过，暂未发现明显逻辑或计算谬误。"`
- `"发现异常：\n1. [问题描述]\n2. [问题描述]"`

### 关键优化点

- Temperature 最低 (0.2)，因为审计需要高度一致性和可复现性
- 目前只检查逻辑错误，不检查"报告质量"（如表述、结构）

---

## AI-3: 深度报告

**函数**: `call_detailed_ai(settings, fused_report_text)`  
**文件**: `packages/ai/ai_caller.py:183`

### 触发时机

融合阶段后：`initial_report + review_text + evidence_json` 拼接为 `fused_context` 后调用。

### 输入结构

**System Prompt** (`DETAILED_SYSTEM_PROMPT`, 行 50-73):

```
资深数据分析师与零售专家。收到融合报告（初级报告 + 错误评审 + 异常日志）。

任务：
- 深度重写，输出更详细、结构更严谨的诊断报告
- 纠正初级报告中被评审指出的错误
- 结合异常日志挖掘深层根因
- 提供可落地的行动方案
- 不用"不是，而是"句式
- 不写"如果你愿意，我可以..."等结尾
```

**User Content** 组装逻辑 (行 183-194):

```
【当前分析环境】...
【初级报告】{AI-1 输出}
【审计意见】{AI-2 输出}
【证据数据】{evidence items 前10条 JSON}
```

### 内部调用参数

| 参数 | 值 | 说明 |
|------|----|------|
| `temperature` | 0.4 | 较高于初诊，允许模型更多发散 |
| `max_tokens` | 16384 | |
| `reasoning_effort` | 从 LLM Preset 读取 | |

### 输出格式

Markdown 自由文本，通常是结构完整的诊断报告。

### 关键优化点

- Temperature 0.4 是4次调用中最高的，用于在审计基础上做创造性融合
- `fused_context` 的拼接格式在 `main.py:984` 中拼接，可调整各段权重

---

## AI-4: 精简卡片（老板视图）

**函数**: `call_simplified_ai(settings, detailed_report_text)`  
**文件**: `packages/ai/ai_caller.py:197`

### 触发时机

AI-3 深度报告生成后，作为最后一步。

### 输入结构

**System Prompt** (`SIMPLIFIED_SYSTEM_PROMPT`, 行 75-98):

```
为老板提取核心信息。严格按照 JSON 格式输出。

输出格式：
{
  "health_status": "1-2词整体状态",
  "overview_text": "一句大白话",
  "cards": [{
    "title": "问题标题",
    "explanation": "大白话",
    "suggestion": "咋办",
    "evidence": "优先 Markdown 迷你表格",
    "color": "red/yellow/green/blue/pink"
  }]
}

要求：cards最多7个，大白话，禁止"不是，而是"句式
color: red=报警 yellow=关注 green=正常 blue=信息 pink=数据口径不一致
```

**User Content** 组装逻辑 (行 197-209):

```
【当前分析环境】...
【详细报告内容】
{AI-3 输出}
```

### 内部调用参数

| 参数 | 值 | 说明 |
|------|----|------|
| `temperature` | 0.3 | |
| `max_tokens` | 8192 | |
| `response_format` | `{"type": "json_object"}` | 强制 JSON 输出 |
| `reasoning_effort` | 从 LLM Preset 读取 | |

### 输出格式

严格 JSON（`response_format: json_object` 强制）。schema 见上。

### 关键优化点

- 唯一使用 `response_format: json_object` 的调用
- cards color 字段前端直接映射颜色
- evidence 字段建议用 Markdown 迷你表格增强可读性

---

## LLM 辅助模块（留桩）

以下模块目前已用规则实现，LLM 版本已留出桩函数但未接入主流程：

| 模块 | 规则函数 | LLM 桩 | 触发阈值 |
|------|---------|--------|---------|
| 场景识别 | `scene_classifier.classify_scene()` | `llm_classify_scene()` | confidence < 0.6 |
| 语义映射 | `semantic_mapper.map_profiles()` | `llm_map_profiles()` | confidence < 0.5 |

**接入时需注意**:
- 两者都在 `run_multifile_analysis` 的早期阶段（engine 之前），先执行场景识别，再执行语义映射；LLM 映射需传入 scene 上下文
- LLM 映射应输出 `SemanticMapping[]` 格式，与规则版兼容
- 需加缓存（`storage/mappings/`），同一客户复用历史结果
- 建议 temperature 设 0.1-0.2，因为字段映射是确定性任务
