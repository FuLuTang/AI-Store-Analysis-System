"""Plan template reserved for the price recommendation Agent Runner."""

PRICE_PLAN_TEMPLATE = [
    {
        "title": "扫描并解析价格相关文件",
        "detail": "先 list_files，再做结构侦察。大文件和复杂格式必须用 run_python 处理，不要直接全文读取。",
        "status": "pending",
        "check": "import os\nassert os.path.exists('output/field_mapping.json') or os.path.exists('output/price_recommendation.json')\n",
        "errors": [],
    },
    {
        "title": "定位商品与关键字段",
        "detail": "识别目标商品记录、价格字段、销量字段和时间字段，证据写入 output/field_mapping.json。",
        "status": "pending",
        "check": "import os\nassert os.path.exists('output/field_mapping.json'), 'field_mapping.json 不存在'\n",
        "errors": [],
    },
    {
        "title": "生成候选价格与证据",
        "detail": "基于真实数据生成候选价格，写入 output/price_candidates.json。",
        "status": "pending",
        "check": "import os\nassert os.path.exists('output/price_candidates.json'), 'price_candidates.json 不存在'\n",
        "errors": [],
    },
    {
        "title": "输出最终价格推荐 JSON",
        "detail": "最终产物必须是 output/price_recommendation.json，不能只输出自然语言。",
        "status": "pending",
        "check": "import json, os\np='output/price_recommendation.json'\nassert os.path.exists(p), 'price_recommendation.json 不存在'\ndata=json.load(open(p, encoding='utf-8'))\nassert data.get('taskType') == 'price_recommendation'\nassert data.get('recommendations')\n",
        "errors": [],
    },
]
