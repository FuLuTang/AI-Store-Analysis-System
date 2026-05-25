"""共享的任务计划模板，custom / smol 管线共用。"""
PLAN_TEMPLATE = [
    {"title": "扫描并解析上传文件",
     "detail": (
         "扫描 input/ 目录，用 read_document 看各文件内容概要。"
         "对数据文件（xlsx/csv 等），调 extract_document_tables 提取表格数据，"
         "存为 parquet/JSON 到 tables/ 再 duckdb_register_parquet 注册。"
         "如果已有注册好的表，直接验证后跳过。"
         "确认每张表行数 > 0，数据链路通了再往下走。"
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
         "读指标计算文档了解公式，DESCRIBE 表结构看实际列名，直接写 SQL 用实际列名（双引号包裹）算指标。"
         "每算几个就用 write_file 追加到 output/指标.json 里存着。"
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
         "结合指标和数据证据，写 Markdown 经营诊断报告用 write_file 保存到 summary.md。"
         "可分为现状诊断和优化行动两部分。"
         "数据可视化可以用 Mermaid xyChart 图表嵌入 md 中。"
         "优先用真实数据，不编造数值。"
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
         "写 summary_short.json — 给管理层看的精简视图："
         '{\"health_status\": \"1-2词整体状态\", \"overview_text\": \"大白话说当前状况\", '
         '\"cards\": [{\"title\": \"问题标题\", \"explanation\": \"咋回事\", '
         '\"suggestion\": \"咋办\", \"evidence\": \"数据证据\", '
         '\"color\": \"red/yellow/green/blue/pink\"}]}。'
         "cards 最多 7 个。"
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
