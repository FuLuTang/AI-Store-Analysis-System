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
             "title": "对数据进行预处理，建立临时数据库",
        "detail": (
            "把目标商品相关数据整理成可直接计算的结构化表，写入 analysis.duckdb。"
            "无关字段可以丢掉，保留商品、门店、价格、销量、时间及后续换算需要的关键列。"
            "为了后续能区分日常原价与促销优惠销售数据，必须在表中保留能够识别促销/优惠的字段（例如：原价、实付价、折扣率、促销/活动标记、优惠券金额等）。"
            "如果原文件不适合直接入库，可以先整理成中间表再入库。"
            "产物示例：DuckDB 中至少有一张价格分析表，列可类似 "
            '["shop_name","product_name","sale_price","sale_qty","sale_date","source_file","is_promotion"]。'
        ),
        "status": "pending",
        "check": (
            "import duckdb\n"
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
            "统计时需将数据分为两类："
            "1. 未使用促销的原价购买数据（日常销售，没有使用促销/优惠券/折扣等，promotion 字段设为 \"false\"）；"
            "2. 促销后的销售数据（使用了限时折扣/优惠券/满减活动折算后的成交价数据，promotion 字段设为 \"true\"，如果没有相关促销数据则该类别为空）。"
            '在归一时，需要先统一时间颗粒度，也就是确定是以周/月/x天为时间颗粒度来计算（你来找合适的单位，主要是考虑最后是否容易观测出数据之间的关系，更具有统计性，所以如果过小的颗粒度会导致购买量过于稀疏导致难以分析，便需要使用更大的时间颗粒度来统计）否则会出现长时间同价格销售数叠加导致的统计出现偏差。（其中比如某个价格在某家店只卖了4天，但如果按照周颗粒度来算，那销售数量得* 7/4 才是正常的"周颗粒度"）'
            "然后再尝试通过店铺规模对其他店铺的销售表现进行换算，比如另一家店规模是当前店的三倍，或者是对面店可能是某种药的专卖店，于是都需要在换算/归一时对那家店的销售表现进行变幻。"
            "描述所有参考的点时，每个店一套points，在json the points中以字典表示点，包含 price、qty/normalizedQty 和 promotion 字段，产出 raw_price_points.json 和 normalized_price_points.json。"
            "同时写清楚本次归一化用的时间颗粒度，字段名为 timeGranularity，取值例如 日 / 周 / 月。"
            "示例一（门店嵌套）：{\"productName\":\"小葵花金银花露\",\"cost\":18.8,\"timeGranularity\":\"日\",\"points\":[{\"store\":\"A店\",\"price&quantity\":[{\"price\":35.8,\"qty\":66,\"promotion\":\"false\"},{\"price\":28.0,\"qty\":120,\"promotion\":\"true\"}]},{...}]}"
            "示例二（扁平）：{\"productName\":\"小葵花金银花露\",\"cost\":18.8,\"timeGranularity\":\"日\",\"points\":[{\"price\":35.8,\"normalizedQty\":66,\"promotion\":\"false\"},{\"price\":28.0,\"normalizedQty\":120,\"promotion\":\"true\"}]}"
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
            "assert 'timeGranularity' in norm_data, 'normalized_price_points.json 中必须包含 timeGranularity 字段'\n"
            "assert isinstance(norm_data.get('points'), list), 'points 字段必须是一个列表'\n"
            "assert norm_data.get('points'), 'points 不能为空'\n"
            "\n"
            "pts=norm_data['points']\n"
            "has_price_qty=any(isinstance(p,dict) and 'price&quantity' in p for p in pts)\n"
            "has_flat_price=all(isinstance(p,dict) and 'price' in p and 'normalizedQty' in p for p in pts)\n"
            "\n"
            "cost=norm_data.get('cost') or raw_data.get('cost')\n"
            "\n"
            "if has_price_qty:\n"
            "    for p in pts:\n"
            "        assert isinstance(p,dict), 'points 列表项必须是字典对象'\n"
            "        assert 'price&quantity' in p, '每个点必须包含 price&quantity 字段'\n"
            "        assert isinstance(p['price&quantity'],list), 'price&quantity 必须是嵌套列表'\n"
            "        for item in p['price&quantity']:\n"
            "            assert isinstance(item,dict) and 'price' in item and 'qty' in item, 'price&quantity 子项必须是包含 price 和 qty 的字典'\n"
            "    total_pairs=0; low_qty_pairs=0\n"
            "    for p in pts:\n"
            "        for item in p.get('price&quantity',[]):\n"
            "            if isinstance(item,dict) and 'price' in item and 'qty' in item:\n"
            "                try:\n"
            "                    qty_val=float(item['qty'])\n"
            "                    total_pairs+=1\n"
            "                    if qty_val<2.0: low_qty_pairs+=1\n"
            "                except (ValueError,TypeError): pass\n"
            "    assert total_pairs>0, 'price&quantity 中未解析出可用 (价格, 销量) 对'\n"
            "    if cost:\n"
            "        assert low_qty_pairs/total_pairs<=0.3, f'低销量点占比过高：{low_qty_pairs}/{total_pairs}，请使用更大的时间颗粒度'\n"
            "        for p in pts:\n"
            "            for item in p['price&quantity']:\n"
            "                price=float(item['price'])\n"
            "                is_promo=str(item.get('promotion','')).lower()=='true' or item.get('promotion') is True\n"
            "                if not is_promo:\n"
            "                    assert cost*0.5<=price<=cost*4.0, f'价格 {price} 与成本 {cost} 不匹配'\n"
            "elif has_flat_price:\n"
            "    for p in pts:\n"
            "        assert isinstance(p,dict), 'points 列表项必须是字典对象'\n"
            "        assert 'price' in p, '扁平格式中每个点必须有 price 字段'\n"
            "        assert 'normalizedQty' in p, '扁平格式中每个点必须有 normalizedQty 字段'\n"
            "        price=float(p['price']); qty=float(p['normalizedQty'])\n"
            "        assert price>0, f'价格必须为正数: {price}'\n"
            "        is_promo=str(p.get('promotion','')).lower()=='true' or p.get('promotion') is True\n"
            "        if cost and not is_promo:\n"
            "            assert cost*0.5<=price<=cost*4.0, f'价格 {price} 与成本 {cost} 不匹配'\n"
            "else:\n"
            "    assert False, 'points 格式无法被 data_fitting 消费。必须为：格式一（门店嵌套）[{\"store\":\"..\",\"price&quantity\":[{\"price\":价,\"qty\":量,\"promotion\":\"false\"}]}]；格式二（扁平）[{\"price\":数值,\"normalizedQty\":数值,\"promotion\":\"false\",...}]'\n"
            "\n"
            "if 'rawPoints' in raw_data and cost:\n"
            "    for p in raw_data['rawPoints']:\n"
            "        price=p.get('price')\n"
            "        is_promo=str(p.get('promotion','')).lower()=='true' or p.get('promotion') is True\n"
            "        if price is not None and not is_promo:\n"
            "            assert cost*0.5<=price<=cost*4.0, f'原始价格 {price} 与成本 {cost} 不匹配'\n"
        ),
        "errors": [],
    },
]
