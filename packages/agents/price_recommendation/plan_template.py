"""Plan template for the price recommendation Agent Runner."""

PRICE_PLAN_TEMPLATE = [
    {
        "title": "扫描并解析价格相关文件",
        "detail": (
            "确认 input/ 里有哪些文件、每个文件大概是什么结构、哪些内容和目标商品有关。"
            "为了看清数据来源、目标商品记录和后面入库要保留的关键字段。"
            "大文件和复杂格式必须用现有脚本或 run_python 处理，便于更好理解文件结构"
        ),
        "status": "pending",
        "check": (
            "import os\n"
            "assert os.path.isdir('input'), 'input 目录不存在'\n"
            "assert os.listdir('input'), 'input 目录为空'\n"
        ),
        "errors": [],
    },
    {
        "title": "对数据进行预处理，建立临时数据库",
        "detail": (
            "把目标商品相关数据整理成可直接计算的结构化表，写入 analysis.duckdb。"
            "无关字段可以丢掉，保留商品、门店、价格、销量、时间及后续换算需要的关键列。"
            "如果原文件不适合直接入库，可以先整理成中间表再入库。"
            "产物示例：DuckDB 中至少有一张价格分析表，列可类似 "
            '["shop_name","product_name","sale_price","sale_qty","sale_date","source_file"]。'
        ),
        "status": "pending",
        "check": (
            "import os, duckdb\n"
            "assert os.path.exists('analysis.duckdb'), 'analysis.duckdb 不存在'\n"
            "con = duckdb.connect('analysis.duckdb')\n"
            "tables = con.execute(\"SELECT table_name FROM information_schema.tables WHERE table_schema='main'\").fetchall()\n"
            "assert tables, 'DuckDB 中没有表'\n"
        ),
        "errors": [],
    },
    {
        "title": "数据整理、归一与换算",
        "detail": (
            "基于数据库，写参数&算法，把其他店面的销量数据换算成适配于本店计算的，一般来说可以考虑店铺规模，所以是直接成比例的。门店折算和本店适配销量换算。如果真的实在部分无法直接使用本店和他店共有的商品的话，可以尝试用其他商品和店间接中转的方式来算，就需要你开动脑筋了。"
            "描述所有参考的点时，每个店一套points，在json的对应字段中用[]放起来即可，产出 output/normalized_price_points.json。"
            "示例：{\"productName\":\"小葵花金银花露\",\"cost\":18.8,\"points\":[{\"store\":\"A店\",\"price&quantity\":[[35.8,66],[39.9,42]]},{...}]}"
        ),
        "status": "pending",
        "check": (
            "import json, os\n"
            "raw='output/raw_price_points.json'\n"
            "norm='output/normalized_price_points.json'\n"
            "assert os.path.exists(raw), 'raw_price_points.json 不存在'\n"
            "assert os.path.exists(norm), 'normalized_price_points.json 不存在'\n"
            "data=json.load(open(norm, encoding='utf-8'))\n"
            "assert isinstance(data, dict), 'normalized_price_points.json 最外层必须是 JSON 对象 {}，不能是列表 []'\n"
            "assert 'points' in data, 'normalized_price_points.json 中必须包含 points 字段'\n"
            "assert isinstance(data.get('points'), list), 'points 字段必须是一个列表'\n"
            "assert data.get('points'), 'points 不能为空'\n"
            "for p in data['points']:\n"
            "    assert isinstance(p, dict), 'points 列表项必须是字典对象'\n"
            "    assert 'store' in p or 'shop' in p, '每个门店数据必须包含 store 或 shop 字段'\n"
            "    assert 'price&quantity' in p, '每个门店数据必须包含 price&quantity 字段'\n"
            "    assert isinstance(p['price&quantity'], list), 'price&quantity 必须是嵌套列表'\n"
        ),
        "errors": [],
    }
]
