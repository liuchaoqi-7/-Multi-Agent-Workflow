STATUS_DESC_MAP = {
    10: "待付款",
    12: "礼物待收下",
    13: "凑单买凑团中",
    20: "待发货",
    21: "部分发货",
    30: "待收货",
    100: "完成",
    200: "全部商品售后之后，订单取消",
    250: "未付款用户主动取消或超时未付款订单自动取消"
}

PRESENT_SEND_TYPE_MAP = {
    0: "普通单聊",
    1: "普通群聊",
    2: "公众号/服务号送礼",
    3: "视频号送礼",
    4: "企微单聊",
    5: "企微群聊",
    6: "小程序/小游戏送礼"
}

PAYMENT_METHOD_MAP = {
    1: "微信支付",
    2: "先用后付",
    3: "抽奖商品0元订单",
    4: "会员积分兑换订单"
}

DELIVER_METHOD_MAP = {
    0: "普通物流",
    1: "虚拟发货",
    3: "虚拟发货"
}

DELIVER_TYPE_MAP = {
    1: "自寄快递",
    2: "在线签约快递单（已废弃）",
    3: "虚拟商品无需物流发货",
    4: "在线快递散单（已废弃）"
}

ORDER_SCENE_MAP = {
    1: "其他",
    2: "直播间",
    3: "短视频",
    4: "商品分享",
    5: "商品橱窗主页",
    6: "公众号文章商品卡片"
}

SHARE_SCENE_MAP = {
    1: "直播间",
    2: "橱窗",
    3: "短视频",
    4: "视频号主页",
    5: "商品详情页",
    6: "带商品的公众号文章",
    7: "商品链接",
    8: "商品二维码",
    9: "商品短链",
    10: "分享直播间",
    11: "分享直播间",
    12: "视频号橱窗的短链",
    13: "视频号橱窗的二维码"
}

CUSTOM_TYPE_MAP = {
    1: "文字定制",
    2: "图片定制",
    3: "小程序定制"
}

PREDICT_ARRIVE_TIME_TYPE_MAP = {
    0: "立即配送",
    1: "预约配送"
}

COUPON_TYPE_MAP = {
    1: "商家优惠",
    2: "达人优惠",
    3: "平台优惠",
    4: "国家补贴",
    5: "地方补贴",
    6: "活动商家补贴"
}

QUALITY_INSPECT_STATUS_JEWELRY = {
    0: "待录入送检信息",
    1: "待送检",
    2: "未入库已取消",
    3: "入库异常",
    4: "已入库",
    5: "质检中",
    6: "待出库",
    7: "出库异常",
    8: "待自提",
    10: "已取消已自提",
    11: "已发货",
    12: "待重新送检",
    13: "已达送检上限",
    14: "待驿站入库"
}

QUALITY_INSPECT_STATUS_FRESH = {
    100: "待上传打包信息",
    200: "质检中",
    201: "质检不通过",
    202: "质检通过"
}

SALE_CHANNEL_MAP = {
    0: "关联账号",
    1: "合作账号",
    100: "联盟达人带货",
    101: "联盟带货机构推广"
}

ACCOUNT_TYPE_MAP = {
    1: "视频号",
    2: "公众号",
    3: "小程序"
}

COMMISSION_TYPE_MAP = {
    0: "达人【对应的可能为 finder_id，或者为 talent_id】",
    1: "带货机构"
}

COMMISSION_STATUS_MAP = {
    1: "未结算",
    2: "已结算"
}

SHARER_TYPE_MAP = {
    0: "普通分享员",
    1: "店铺分享员"
}

DELIVERY_TIME_TYPE_MAP = {
    1: "正常发货",
    2: "发货协商",
    3: "发货报备"
}

CHANGE_SKU_STATE_MAP = {
    3: "等待商家处理",
    4: "商家审核通过",
    5: "商家拒绝",
    6: "用户主动取消",
    7: "超时默认拒绝"
}

QUALITY_INSPECT_TYPE_MAP = {
    0: "不需要",
    2: "生鲜类质检",
    1: "珠宝玉石类质检"
}

DROPSHIP_FLAG_MAP = {
    0: "不可代发",
    1: "未分配代发",
    2: "部分分配",
    3: "已分配"
}

AFTERSALE_STATUS_MAP = {
    "USER_CANCELD": "用户取消申请",
    "MERCHANT_PROCESSING": "商家受理中",
    "MERCHANT_REJECT_REFUND": "商家拒绝退款",
    "MERCHANT_REJECT_RETURN": "商家拒绝退货退款",
    "USER_WAIT_RETURN": "待买家退货",
    "RETURN_CLOSED": "退货退款关闭",
    "MERCHANT_WAIT_RECEIPT": "待商家收货",
    "MERCHANT_OVERDUE_REFUND": "商家逾期未退款",
    "MERCHANT_REFUND_SUCCESS": "退款完成",
    "MERCHANT_RETURN_SUCCESS": "退货退款完成",
    "PLATFORM_REFUNDING": "平台退款中",
    "PLATFORM_REFUND_FAIL": "平台退款失败",
    "USER_WAIT_CONFIRM": "待用户确认",
    "MERCHANT_REFUND_RETRY_FAIL": "商家打款失败，客服关闭售后",
    "MERCHANT_FAIL": "售后关闭",
    "USER_WAIT_CONFIRM_UPDATE": "待用户处理商家协商",
    "USER_WAIT_HANDLE_MERCHANT_AFTER_SALE": "待用户处理商家代发起的售后申请",
    "WAIT_PACKAGE_INTERCEPT": "物流线上拦截中",
    "MERCHANT_REJECT_EXCHANGE": "商家拒绝换货",
    "MERCHANT_REJECT_RESHIP": "商家拒绝发货",
    "USER_WAIT_RECEIPT": "待用户收货",
    "MERCHANT_EXCHANGE_SUCCESS": "换货完成"
}

REFUND_REASON_MAP = {
    1: "商家通过店铺管理页或者小助手发起退款",
    2: "退货退款场景，商家同意买家未上传物流单号情况下确认收货并退款（无运费险）",
    3: "商家通过后台api发起退款",
    4: "未发货售后平台自动同意",
    5: "平台介入纠纷退款",
    6: "特殊场景下平台强制退款",
    7: "退货退款场景，买家同意未上传物流单号情况下商家确认收货并退款（含运费险无法理赔）",
    8: "商家发货超时，平台退款",
    9: "商家处理买家售后申请超时，平台自动同意退款",
    10: "用户确认收货超时，平台退款",
    11: "商家确认收货超时，平台退款"
}

AFTERSALE_TYPE_MAP = {
    "REFUND": "退款",
    "RETURN": "退货退款",
    "EXCHANGE": "换货"
}

AFTERSALE_SUB_TYPE_MAP = {
    "DEFAULT": "普通售后",
    "REFUND_PRICE_DIFF": "退差价售后"
}

AFTERSALE_REASON_MAP = {
    "INCORRECT_SELECTION": "拍错/多拍",
    "NO_LONGER_WANT": "不想要了",
    "NO_EXPRESS_INFO": "无快递信息",
    "EMPTY_PACKAGE": "包裹为空",
    "REJECT_RECEIVE_PACKAGE": "已拒签包裹",
    "NOT_DELIVERED_TOO_LONG": "快递长时间未送达",
    "NOT_MATCH_PRODUCT_DESC": "与商品描述不符",
    "QUALITY_ISSUE": "质量问题",
    "SEND_WRONG_GOODS": "卖家发错货",
    "THREE_NO_PRODUCT": "三无产品",
    "FAKE_PRODUCT": "假冒产品",
    "NO_REASON_7_DAYS": "七天无理由",
    "INITIATE_BY_PLATFORM": "平台代发起",
    "OTHERS": "其它"
}

ORDER_SETTLE_STATE_MAP = {
    0: "无，查询全部",
    1: "待结算",
    2: "无需结算",
    60: "结算完成",
    100: "部分结算"
}

ORDER_STATE_MAP = {
    0: "全部",
    20: "待发货",
    30: "待收货",
    100: "订单完成"
}

ORDER_PAY_METHOD_MAP = {
    0: "全部",
    1: "普通支付",
    2: "先用后付"
}

SETTLE_STATE_MAP = {
    0: "未结算",
    1: "待结算",
    2: "无需结算",
    60: "结算完成",
    100: "部分结算"
}

DEFAULT_TABLE_NAMES = {
    "order": "ods_微信_订单_全量",
    "aftersale": "ods_微信_售后_全量",
    "settle": "ods_微信_结算_全量"
}

API_CONFIG = {
    "base_url": "https://api.weixin.qq.com",
    "timeout": 15
}

MODULE_CONFIG = {
    "order": {
        "max_workers": 20,
        "max_retries": 3,
        "base_delay": 0.1,
        "chunk_size": 10,
    },
    "aftersale": {
        "max_workers": 10,
        "max_retries": 3,
        "base_delay": 0.1,
    },
    "settle": {
        "max_workers": 5,
        "max_retries": 3,
        "base_delay": 0.05,
        "page_size": 100,
    }
}
