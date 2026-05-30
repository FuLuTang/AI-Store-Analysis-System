"""共享的任务计划模板，供 custom 管线使用。"""
PLAN_TEMPLATE = [
    {"title": "扫描并解析上传文件",
     "detail": (
         "扫描 input/ 目录，先用 list_files 查看文件类型和大小，再用 read_document_structure 看内容摘要。"
         "只对小型文本文件用 read_file 分页查看；大文件、压缩包、数据库或二进制文件要用 run_python 分块读取和清洗，"
         "存为 parquet/JSON 到 tables/ 再 duckdb_register_parquet 注册。"
         "如果已有注册好的表，直接验证后跳过。"
         "确认每张表行数差不多，数据量基本完整，数据链路通了再往下走。"
         "完成后调 check_plan(0) 验证。"
     ),
     "status": "pending",
     "check": (
         "# 检查至少有一张表已注册\n"
         "import duckdb\n"
         "con = duckdb.connect('analysis.duckdb')\n"
         "tables = con.execute(\"SELECT table_name FROM information_schema.tables WHERE table_schema='main'\").fetchall()\n"
         "assert tables, 'DuckDB 中没有注册任何表'\n"
         "for (t,) in tables:\n"
         "    cnt = con.execute(f'SELECT COUNT(*) FROM \"{t}\"').fetchone()[0]\n"
         "    assert cnt > 0, f'{t} 行数为 0'\n"
     ),
     "errors": []},
    {"title": "算指标，写入 指标.json",
     "detail": (
         "读 指标计算文档.md 了解公式，DESCRIBE 表结构看实际列名，直接写 SQL 用实际列名（双引号包裹）算指标。"
         "每算几个就用 write_file 追加到 指标.json 里存着。"
         "每条 metric 要有 metric_id/name/value/unit/status/reason/evidence，"
         "status 只能是 pass/attention/warning/uncountable 四种。"
         "完成后调 check_plan(1) 验证。"
     ),
     "status": "pending",
     "check": (
         "# 检查 指标.json 存在\n"
         "import os\n"
         "assert os.path.exists('指标.json'), '指标.json 还没写'\n"
     ),
     "errors": []},
    {"title": "分析深层原因，写诊断报告 → output/summary.md",
     "detail": (
         "根据算出来的指标，分析背后的原因——哪些指标异常？为什么？有什么趋势？"
         "结合行业常识和数据证据，写一篇完整的 Markdown 经营诊断报告，用 write_file 保存到 output/summary.md。"
         "报告格式分两部分："
         "# 第一部分：现状诊断报告 — "
         "1) 核心经营判断（涨跌稳定波动，可能原因，带emoji如📈📉）；"
         "2) 核心指标逐项解读（优先关注 attention/warning 项）；"
         "3) 关联分析（多指标交叉解读，如营收涨变毛利跌说明什么）；"
         "4) 风险预警。"
         "# 第二部分：优化行动方案 — "
         "1) 紧急事项（高风险指标对应的动作）；"
         "2) 中期改善（结构性问题的优化方向）；"
         "3) 基于行业经验的其他建议。"
         "要求：用 # 现状诊断报告 ... --- ... # 优化行动方案 分隔；"
         "全文约1000字；bullet 不超45字；最多4个核心问题；"
         "优先使用证据中的数据，禁止编造数值；禁止结尾客套话。"
         "完成后调 check_plan(2) 验证。\n"
         "但是注意，以下是来自用户传入的要求（需要优先遵循）：\nget_param"
         "如果有要求生成其他产物，先进行生成，再写最终完整报告（于是可以在summary.md中插入相对路径链接来展示对应文件或图片）"
     ),
     "status": "pending",
     "check": (
         "# 检查 output/summary.md 存在\n"
         "import os\n"
         "assert os.path.exists('output/summary.md'), 'summary.md 还没写'\n"
     ),
     "errors": []},
    {"title": "输出精简视图",
     "detail": (
         "写 output/summary_short.json — 给管理层看的精简视图，严格 JSON 格式："
         '{\"health_status\": \"1-2词整体状态\", \"overview_text\": \"大白话说当前状况\", '
         '\"cards\": [{\"title\": \"问题标题\", \"explanation\": \"咋回事\", '
         '\"suggestion\": \"咋办\", \"evidence\": \"数据证据（优先 Markdown 表格，即使是单列数据也可以用表格，但是不推荐列数超过5，行数也别太多，可以用有代表性的举例。可以用 文字+表格）\", '
         '\"color\": \"red/yellow/green/blue/pink\"}]}。'
         "cards 尽量别超过 7 个。"
         "完成后调 check_plan(3) 验证。"
     ),
     "status": "pending",
     "check": (
         "# 检查报告产物\n"
         "import os\n"
         "assert os.path.exists('output/summary_short.json'), 'summary_short.json 不存在'\n"
     ),
     "errors": []},
]

