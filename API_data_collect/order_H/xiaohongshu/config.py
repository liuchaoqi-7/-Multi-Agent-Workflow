ORDER_TYPE_MAP = {
    1: "现货",
    2: "定金预售",
    3: "全款预售(废弃)",
    4: "全款预售(新)",
    5: "补发"
}

ORDER_STATUS_MAP = {
    1: "已下单待付款",
    2: "已支付处理中",
    3: "清关中",
    4: "待发货",
    5: "部分发货",
    6: "待收货",
    7: "已完成",
    8: "已关闭",
    9: "已取消",
    10: "换货申请中"
}

AFTER_SALES_STATUS_MAP = {
    1: "无售后",
    2: "售后处理中",
    3: "售后完成",
    4: "售后拒绝",
    5: "售后关闭",
    6: "平台介入中",
    7: "售后取消"
}

SKU_TAG_MAP = {
    1: "赠品",
    0: "普通商品"
}

ORDER_TAG_MAP = {
    "NEW_YEAR": "新年礼",
    "PLATFORM_DECLARE": "平台报备",
    "SELLER_DECLARE": "商家报备",
    "CONSULT": "协商发货",
    "MODIFIED_ADDR": "已改地址",
    "MODIFIED_PRICE": "已改价",
    "NO_LOGISTICS_SHIP": "无物流发货",
    "PRINTED": "部分打单/已打单",
    "URGENT_SHIP": "催发货",
    "QIC": "QIC质检",
    "SAMPLE": "拿样",
    "HOME_DELIVERY": "送货上门",
    "LACK_GOOD": "缺货",
    "EXPLODE": "发生现货爆单的订单",
    "EXEMPT": "发生现货爆单享受豁免",
    "CERTIFICATION_WAREHOUSE": "认证仓",
    "COUNTRY_SUBSIDY": "国家补贴",
    "CITY_SUBSIDY": "城市补贴",
    "COUNTRY_SUBSIDY_SUPPLY_SALE": "国补供销",
    "BUY_AGENT": "代购",
    "RDS": "跨境出海订单",
    "BANK_ASSET_CUSTODY": "银行资产托管",
    "SAME_DAY_DELIVERY": "包含当日发商品"
}

PAYMENT_TYPE_MAP = {
    1: "支付宝",
    2: "微信",
    3: "apple 内购",
    4: "apple pay",
    5: "花呗分期",
    7: "支付宝免密支付",
    8: "云闪付",
    -1: "其他"
}

SELLER_REMARK_FLAG_MAP = {
    1: "灰旗",
    2: "红旗",
    3: "黄旗",
    4: "绿旗",
    5: "蓝旗",
    6: "紫旗"
}

RETURN_TYPE_MAP = {
    1: "退货退款",
    2: "换货",
    3: "仅退款",
    4: "仅退款",
    5: "未发货仅退款"
}

AFTER_SALE_STATUS_MAP_2 = {
    1: "待审核",
    2: "待用户寄回",
    3: "待收货",
    4: "完成",
    5: "取消",
    6: "关闭",
    9: "拒绝",
    11: "换货转退款",
    12: "包裹商家已确认收货，等待商家发货",
    13: "包裹商家已发货，等待用户确认收货",
    14: "平台介入中",
    9001: "商家收货拒绝"
}

SUB_STATUS_MAP = {
    301: "待审核",
    302: "快递已签收",
    304: "收货异常"
}

RECEIVE_ABNORMAL_TYPE_MAP = {
    1: "商家已开工单",
    2: "仓库质检异常",
    3: "收货地异常",
    4: "寄回包裹物流超时",
    5: "仓库反向创建退货",
    6: "快递轨迹异常",
    7: "拒收退仓超时",
    8: "退款金额超限",
    9: "收货地不一致",
    10: "仓库质检假货",
    11: "已退款，未收货",
    21: "未收到货",
    22: "退货数量不符",
    23: "退货商品不符",
    24: "退货质检异常",
    25: "其他"
}

SHIP_NEEDED_MAP = {
    0: "否",
    1: "是",
    -1: "全部"
}

SETTLE_BIZ_TYPE_MAP = {
    0: "结算入账",
    1: "退款"
}

ERQING_TYPE_MAP = {
    0: "店铺余额",
    1: "支付宝",
    2: "微信"
}

STATEMENT_TYPE_MAP = {
    0: "订单结算",
    1: "预约单打包结算",
    2: "履约单单笔结算"
}

TRANSACTION_BIZ_TYPE_MAP = {
    0: "销售",
    1: "退货退款",
    2: "客服退款",
    3: "售后退款"
}

TRANSACTION_SETTLE_STATUS_MAP = {
    0: "不需要结算",
    1: "初始态",
    2: "可结算",
    3: "结算中",
    4: "已结算",
    5: "结算异常",
    6: "正逆冲抵无需结算"
}

COMMON_SETTLE_STATUS_MAP = {
    0: "未结算",
    1: "已结算"
}

DEFAULT_TABLE_NAMES = {
    "order": "ods_红书_订单_全量",
    "aftersale": "ods_红书_售后_全量",
    "finance": "ods_红书_结算_全量"
}

API_CONFIG = {
    "base_url": "https://ark.xiaohongshu.com/ark/open_api/v3/common_controller",
    "version": "2.0",
    "timeout": 15
}

MODULE_CONFIG = {
    "order": {
        "max_workers": 20,
        "max_retries": 5,
        "base_delay": 0.3,
        "random_delay_range": (1.0, 2.0),
    },
    "aftersale": {
        "max_workers": 10,
        "max_retries": 5,
        "base_delay": 0.3,
        "random_delay_range": (1, 2),
    },
    "finance": {
        "max_workers": 10,
        "max_retries": 5,
        "base_delay": 0.3,
        "random_delay_range": (1.0, 2.0),
        "page_delay": 0.1,
    }
}
