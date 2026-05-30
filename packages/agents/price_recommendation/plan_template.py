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
            "注意：对应的 price&quantity 列表必须仅包含从原始数据中清洗并经规模换算后得到的真实历史价格点，严禁使用价格弹性公式或估算模型凭空虚构/模拟不存在的价格点（例如，如果原始数据中某店该商品只卖过 6.0 元，那么经过换算后该店的价格点列表中应该只有对应的折算价格点，绝对不能自行拓展出 5.0, 5.5, 6.5, 7.0 等虚构的价格点）。"
            "示例：{\"productName\":\"小葵花金银花露\",\"cost\":18.8,\"points\":[{\"store\":\"A店\",\"price&quantity\":[[35.8,66],[39.9,42]]},{...}]}"
        ),
        "status": "pending",
        "check": (
            "import json, os\n"
            "raw='output/raw_price_points.json'\n"
            "norm='output/normalized_price_points.json'\n"
            "assert os.path.exists(raw), 'raw_price_points.json 不存在'\n"
            "assert os.path.exists(norm), 'normalized_price_points.json 不存在'\n"
            "raw_data=json.load(open(raw, encoding='utf-8'))\n"
            "norm_data=json.load(open(norm, encoding='utf-8'))\n"
            "assert isinstance(norm_data, dict), 'normalized_price_points.json 最外层必须是 JSON 对象 {}，不能是列表 []'\n"
            "assert 'points' in norm_data, 'normalized_price_points.json 中必须包含 points 字段'\n"
            "assert isinstance(norm_data.get('points'), list), 'points 字段必须是一个列表'\n"
            "assert norm_data.get('points'), 'points 不能为空'\n"
            "cost=norm_data.get('cost') or raw_data.get('cost')\n"
            "if cost:\n"
            "    for p in norm_data['points']:\n"
            "        assert isinstance(p, dict), 'points 列表项必须是字典对象'\n"
            "        assert 'store' in p or 'shop' in p, '每个门店数据必须包含 store 或 shop 字段'\n"
            "        assert 'price&quantity' in p, '每个门店数据必须包含 price&quantity 字段'\n"
            "        assert isinstance(p['price&quantity'], list), 'price&quantity 必须是嵌套列表'\n"
            "        for item in p['price&quantity']:\n"
            "            price = item[0]\n"
            "            assert cost * 0.5 <= price <= cost * 4.0, f'价格 {price} 与商品成本 {cost} 不匹配，请确保 points 中只包含目标商品的价格点，不要混入其他商品（如泰诺等）的价格'\n"
            "    if 'rawPoints' in raw_data:\n        for p in raw_data['rawPoints']:\n            price = p.get('price')\n            if price is not None:\n                assert cost * 0.5 <= price <= cost * 4.0, f'原始价格 {price} 与成本 {cost} 不匹配，请确保 rawPoints 只包含目标商品的价格点'\n    # 限制不允许凭空模拟/虚构价格点数量\n    raw_points_count = len(raw_data.get('rawPoints') or raw_data.get('points') or [])\n    norm_points_count = sum(len(p.get('price&quantity') or p.get('price_quantity') or []) for p in norm_data['points'])\n    assert norm_points_count <= raw_points_count, f'归一化价格点数 ({norm_points_count}) 多于原始价格点数 ({raw_points_count})，禁止凭空虚构或模拟不存在的价格点。请仅保留并归一化原始数据中真实存在的价格点。'\n"
        ),
        "errors": [],
    }
]
