"""共享的任务计划模板，custom / smol 管线共用。"""
PLAN_TEMPLATE = [
    {"title": "扫描并解析上传文件",
     "detail": (
         "用 list_files('input') 看 input/ 目录下有哪些文件。"
         "对数据文件（xlsx/csv/pdf/docx），调 extract_document_tables 提取表格数据，"
         "用 write_file 将提取结果保存为 JSON 到 tables/（不要写 Python 脚本），"
         "再 duckdb_register_parquet 注册。DuckDB 支持直接读 JSON，不需要转 parquet。"
         "如果 input/ 里已经有 parquet 或 JSON 文件，直接注册。"
         "确认每张表都注册到了 DuckDB、行数 > 0，数据链路通了再往下走。"
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
    {"title": "算指标，写入 output/指标.json",
     "detail": (
         "先调 read_context('指标计算文档.md') 了解标准指标公式。"
         "然后用 duckdb_query('DESCRIBE \"表名\"') 看看实际列名，"
         "直接写 SQL 用实际列名（双引号包裹）算指标。"
         "可以一批一批算，每算几个就用 write_file 追加到 output/指标.json 里存着。"
         "每条 metric 要有 metric_id/name/value/unit/status/reason/evidence，"
         "status 只能是 pass/attention/warning/uncountable 四种。"
         "完成后调 check_plan(1) 验证。"
     ),
     "status": "pending",
     "check": (
         "# 检查 output/指标.json 存在\n"
         "import os\n"
         "assert os.path.exists('output/指标.json'), '指标.json 还没写'\n"
     ),
     "errors": []},
    {"title": "分析深层原因，写诊断报告 → summary.md",
     "detail": (
         "根据算出来的指标，分析背后的原因——哪些指标异常？为什么？有什么趋势？"
         "结合行业常识和数据证据，写一篇完整的 Markdown 经营诊断报告，用 write_file 保存到 summary.md。"
         "报告格式分两大部分："
         "# 第一部分：现状诊断报告 — "
         "1) 核心经营判断（涨跌稳定波动，可能原因，带emoji如📈📉）；"
         "2) 核心指标逐项解读（优先关注 attention/warning 项）；"
         "3) 关联分析（多指标交叉解读，如营收涨但毛利跌说明什么）；"
         "4) 风险预警。"
         "# 第二部分：优化行动方案 — "
         "1) 紧急事项（高风险指标对应的动作）；"
         "2) 中期改善（结构性问题的优化方向）；"
         "3) 基于行业经验的其他建议。"
         "要求：用 # 现状诊断报告 ... --- ... # 优化行动方案 分隔；"
         "全文约1000字；bullet 不超45字；最多4个核心问题；"
         "优先使用证据中的数据，禁止编造数值；禁止结尾客套话。"
         "完成后调 check_plan(2) 验证。"
     ),
     "status": "pending",
     "check": (
         "# 检查 summary.md 存在\n"
         "import os\n"
         "assert os.path.exists('summary.md'), 'summary.md 还没写'\n"
     ),
     "errors": []},
    {"title": "输出精简视图 + 最终产物",
     "detail": (
         "写 summary_short.json — 给管理层看的精简视图，严格 JSON 格式："
         '{\"health_status\": \"1-2词整体状态\", \"overview_text\": \"一句大白话总结当前经营状况\", '
         '\"cards\": [{\"title\": \"问题标题\", \"explanation\": \"大白话说怎么回事\", '
         '\"suggestion\": \"咋办\", \"evidence\": \"数据证据（优先 Markdown 迷你表格）\", '
         '\"color\": \"red/yellow/green/blue/pink\"}]}。'
         "cards 最多 7 个；color: red=报警 yellow=关注 green=正常 blue=信息 pink=数据口径不一致。"
         "output/result.json 由系统自动组装，无需手动写入。"
         "完成后调 check_plan(3) 验证。"
     ),
     "status": "pending",
     "check": (
         "# 检查三个产物都存在\n"
         "import os\n"
         "assert os.path.exists('summary.md'), 'summary.md 不存在'\n"
         "assert os.path.exists('summary_short.json'), 'summary_short.json 不存在'\n"
         "assert os.path.exists('output/result.json'), 'result.json 不存在'\n"
     ),
     "errors": []},
]
