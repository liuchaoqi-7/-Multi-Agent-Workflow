INDUSTRY_CATEGORY = [
    {"行业类目": '图书教育', "一级类目": "图书教育", "二级类目": "书籍/杂志/报纸", "三级类目": "全部"},
    {"行业类目": '学习用品', "一级类目": "图书教育", "二级类目": "学习用品/办公用品", "三级类目": "全部"},
    {"行业类目": '电子教育', "一级类目": "3C数码家电", "二级类目": "电子教育", "三级类目": "全部"},
    {"行业类目": '玩具乐器', "一级类目": "玩具乐器", "二级类目": "玩具/童车/益智/积木/模型", "三级类目": "全部"},
    {"行业类目": '母婴童装', "一级类目": "母婴宠物", "二级类目": "童装/婴儿装/亲子装", "三级类目": "全部"},
    {"行业类目": '母婴童鞋', "一级类目": "母婴宠物", "二级类目": "童鞋/婴儿鞋/亲子鞋", "三级类目": "全部"},
    {"行业类目": '母婴奶辅', "一级类目": "母婴宠物", "二级类目": "奶粉/辅食/营养品/零食", "三级类目": "全部"},
    {"行业类目": '婴童用品', "一级类目": "母婴宠物", "二级类目": "婴童用品", "三级类目": "全部"},
    {"行业类目": '儿童家纺', "一级类目": "母婴宠物", "二级类目": "儿童床品/家纺", "三级类目": "全部"},
    {"行业类目": '婴童尿裤', "一级类目": "母婴宠物", "二级类目": "婴童尿裤", "三级类目": "全部"},
]

RANK_CONFIGS = {
    "rank_content": {
        "TARGET_URL": "https://compass.jinritemai.com/shop/chance/rank-video",
        "RANK_CATEGORY": ["直播交易榜", "视频销量榜", "引流直播榜"],
        "AMOUNT_CLASS": ["自营", "合作"],
        "MAX_PAGES": 10,
    },
    "rank_kol": {
        "TARGET_URL": "https://compass.jinritemai.com/shop/chance/rank-talent",
        "RANK_CATEGORY": ["直播带货榜", "短视频带货榜"],
        "AMOUNT_CLASS": ["自营账号", "合作账号"],
        "MAX_PAGES": 10,
    },
    "rank_product": {
        "TARGET_URL": "https://compass.jinritemai.com/shop/chance/rank-product",
        "RANK_CATEGORY": ["直播带货榜", "短视频带货榜"],
        "AMOUNT_CLASS": None,
        "MAX_PAGES": 100,
    },
    "rank_serach": {
        "TARGET_URL": "https://compass.jinritemai.com/shop/chance/rank-search",
        "RANK_CATEGORY": None,
        "AMOUNT_CLASS": None,
        "MAX_PAGES": 100,
    },
    "rank_shop": {
        "TARGET_URL": "https://compass.jinritemai.com/shop/chance/rank-shop",
        "RANK_CATEGORY": ["直播带货榜", "短视频带货榜"],
        "AMOUNT_CLASS": ["自营账号", "合作账号"],
        "MAX_PAGES": 10,
    },
}
