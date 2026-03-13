import time
import pandas as pd
from datetime import datetime
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from .client import WeChatAPIClient
from .config import (
    STATUS_DESC_MAP, PRESENT_SEND_TYPE_MAP, PAYMENT_METHOD_MAP,
    DELIVER_METHOD_MAP, DELIVER_TYPE_MAP, ORDER_SCENE_MAP,
    SHARE_SCENE_MAP, CUSTOM_TYPE_MAP, PREDICT_ARRIVE_TIME_TYPE_MAP,
    COUPON_TYPE_MAP, QUALITY_INSPECT_STATUS_JEWELRY, QUALITY_INSPECT_STATUS_FRESH,
    SALE_CHANNEL_MAP, ACCOUNT_TYPE_MAP, COMMISSION_TYPE_MAP, COMMISSION_STATUS_MAP,
    SHARER_TYPE_MAP, DELIVERY_TIME_TYPE_MAP, CHANGE_SKU_STATE_MAP,
    QUALITY_INSPECT_TYPE_MAP, DROPSHIP_FLAG_MAP, MODULE_CONFIG
)
from .utils import format_time, format_amount, get_mapped_value, safe_get, safe_get_first


class OrderService:
    def __init__(self, client: WeChatAPIClient, max_workers: int = None):
        self.client = client
        config = MODULE_CONFIG["order"]
        self.max_workers = max_workers or config["max_workers"]
        self.max_retries = config["max_retries"]
        self.base_delay = config["base_delay"]

    def get_order_list(self, start_time: int, end_time: int) -> List[str]:
        print("开始获取订单列表...")
        all_order_ids = []
        next_key = ""
        
        while True:
            request_data = {
                "create_time_range": {
                    "start_time": start_time,
                    "end_time": end_time
                },
                "page_size": 100,
                "next_key": next_key
            }
            
            result = self.client.request_with_path("channels/ec/order/list/get", request_data)
            
            if not result:
                break
            
            order_ids = result.get("order_id_list", [])
            all_order_ids.extend(order_ids)
            
            has_more = result.get("has_more", False)
            if not has_more:
                break
            
            next_key = result.get("next_key", "")
            print(f"已获取{len(all_order_ids)}个订单ID，继续获取下一页...")
            time.sleep(0.1)
        
        print(f"\n✅ 订单列表获取完成！共提取{len(all_order_ids)}个订单ID")
        return all_order_ids

    def get_order_detail(self, order_id: str) -> Optional[Dict[str, Any]]:
        if not order_id:
            return None
        
        try:
            result = self.client.request_with_path("channels/ec/order/get", {"order_id": order_id})
            return result
        except Exception as e:
            print(f"❌ 订单{order_id}获取失败: {str(e)}")
            return None

    def _process_order(self, order_id: str) -> Optional[Dict[str, Any]]:
        result = self.get_order_detail(order_id)
        if not result:
            return None
        
        def format_ts(ts):
            return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S") if ts and isinstance(ts, (int, float)) else ""
        
        def format_price(cents):
            return round(cents / 100, 2) if cents and isinstance(cents, (int, float)) else 0.0
        
        def get_enum_desc(enum_map, value, default=""):
            return enum_map.get(value, f"{default}({value})" if value is not None else "")
        
        order = result.get("order", {})
        order_detail = order.get("order_detail", {})
        aftersale_detail = order.get("aftersale_detail", {})
        order_present_info = order.get("order_present_info", {})
        intra_city_order_info = order.get("intra_city_order_info", {})
        
        product_infos = order_detail.get("product_infos", [])
        pay_info = order_detail.get("pay_info", {})
        price_info = order_detail.get("price_info", {})
        delivery_info = order_detail.get("delivery_info", {})
        ext_info = order_detail.get("ext_info", {})
        coupon_info = order_detail.get("coupon_info", {})
        commission_infos = order_detail.get("commission_infos", [])
        sharer_info = order_detail.get("sharer_info", {})
        settle_info = order_detail.get("settle_info", {})
        sku_sharer_infos = order_detail.get("sku_sharer_infos", [])
        agent_info = order_detail.get("agent_info", {})
        source_infos = order_detail.get("source_infos", [])
        refund_info = order_detail.get("refund_info", {})
        greeting_card_info = order_detail.get("greeting_card_info", {})
        custom_info = order_detail.get("custom_info", {})
        
        address_info = delivery_info.get("address_info", {})
        delivery_product_info = delivery_info.get("delivery_product_info", [])
        address_under_review = delivery_info.get("address_under_review", {})
        recharge_info = delivery_info.get("recharge_info", {})
        quality_inspect_info = delivery_info.get("quality_inspect_info", {})
        
        tel_number_ext_info = address_info.get("tel_number_ext_info", {})
        review_tel_ext_info = address_under_review.get("tel_number_ext_info", {})
        
        first_product = safe_get_first(product_infos)
        product_sku_attrs = first_product.get("sku_attrs", [])
        first_sku_attr = safe_get_first(product_sku_attrs)
        
        product_coupons = first_product.get("order_product_coupon_info_list", [])
        first_product_coupon = safe_get_first(product_coupons)
        
        gift_info = first_product.get("free_gift_info", {})
        gift_list = gift_info.get("main_product_list", [])
        first_product_gift = safe_get_first(gift_list)
        
        first_product_sku_deliver = first_product.get("sku_deliver_info", {})
        first_product_extra_service = first_product.get("extra_service", {})
        first_product_change_sku = first_product.get("change_sku_info", {})
        first_product_dropship = first_product.get("dropship_info", {})
        
        first_logistics = safe_get_first(delivery_product_info)
        logistics_products = first_logistics.get("product_infos", [])
        first_logistics_product = safe_get_first(logistics_products)
        
        first_commission = safe_get_first(commission_infos)
        
        first_source = safe_get_first(source_infos)
        
        first_sku_sharer = safe_get_first(sku_sharer_infos)
        
        aftersale_list = aftersale_detail.get("aftersale_order_list", [])
        first_aftersale = safe_get_first(aftersale_list)
        
        order_data = {
            "订单ID": str(order.get("order_id", "")),
            "创建时间": str(format_ts(order.get("create_time"))),
            "更新时间": str(format_ts(order.get("update_time"))),
            "订单状态": str(order.get("status", "")),
            "订单状态描述": str(get_enum_desc(STATUS_DESC_MAP, order.get("status"), "未知状态")),
            "平台": str("微信"),
            "店铺ID": str(order.get("shop_id", "")),
            "订单归属人OpenID": str(order.get("openid", "")),
            "订单归属人UnionID": str(order.get("unionid", "")),
            "是否礼物订单": str(order.get("is_present", False)),
            "礼物订单留言": str(order.get("present_note", "")),
            "礼物订单赠送者OpenID": str(order.get("present_giver_openid", "")),
            "礼物订单赠送者UnionID": str(order.get("present_giver_unionid", "")),
            "礼物订单ID": str(order.get("present_order_id_str", "")),
            "礼物单类型": str(order.get("present_send_type", "")),
            "礼物单类型描述": str(get_enum_desc(PRESENT_SEND_TYPE_MAP, order.get("present_send_type"))),
            "是否闪购订单": str(order.get("is_flash_sale_order", False)),
            
            "礼物订单留言_详细": str(order_present_info.get("present_note", "")),
            "礼物赠送者OpenID_详细": str(order_present_info.get("present_giver_openid", "")),
            "礼物赠送者UnionID_详细": str(order_present_info.get("present_giver_unionid", "")),
            "礼物订单ID_详细": str(order_present_info.get("present_order_id_str", "")),
            "礼物单类型_详细": str(order_present_info.get("present_send_type", "")),
            "礼物单是否付款": str(order_present_info.get("is_b2c_free_present", "")),
            "礼物单付款状态描述": str(get_enum_desc({0: "已付款", 1: "无需付款"}, order_present_info.get("is_b2c_free_present"))),
            
            "门店ID": str(intra_city_order_info.get("shop_id", "")),
            "预计送达开始时间": str(format_ts(intra_city_order_info.get("predict_arrive_start_time"))),
            "预计送达结束时间": str(format_ts(intra_city_order_info.get("predict_arrive_end_time"))),
            "配送类型": str(intra_city_order_info.get("predict_arrive_time_type", "")),
            "配送类型描述": str(get_enum_desc(PREDICT_ARRIVE_TIME_TYPE_MAP, intra_city_order_info.get("predict_arrive_time_type"))),
            
            "支付方式": str(pay_info.get("payment_method", "")),
            "支付方式描述": str(get_enum_desc(PAYMENT_METHOD_MAP, pay_info.get("payment_method"))),
            "支付时间": str(format_ts(pay_info.get("pay_time"))),
            "支付订单号": str(pay_info.get("transaction_id", "")),
            
            "商品总价(元)": str(format_price(price_info.get("product_price", 0))),
            "用户实付金额(元)": str(format_price(price_info.get("order_price", 0))),
            "运费(元)": str(format_price(price_info.get("freight", 0))),
            "商家优惠金额(元)": str(format_price(price_info.get("discounted_price", 0))),
            "是否有商家优惠券优惠": str(price_info.get("is_discounted", False)),
            "订单原始价格(元)": str(format_price(price_info.get("original_order_price", 0))),
            "商品预估价格(元)": str(format_price(price_info.get("estimate_product_price", 0))),
            "改价后降低金额(元)": str(format_price(price_info.get("change_down_price", 0))),
            "改价后运费(元)": str(format_price(price_info.get("change_freight", 0))),
            "是否修改运费": str(price_info.get("is_change_freight", False)),
            "是否使用会员积分抵扣": str(price_info.get("use_deduction", False)),
            "会员积分抵扣金额(元)": str(format_price(price_info.get("deduction_price", 0))),
            "商家实收金额(元)": str(format_price(price_info.get("merchant_receieve_price", 0))),
            "达人优惠金额(元)": str(format_price(price_info.get("finder_discounted_price", 0))),
            "会员权益优惠金额(元)": str(format_price(price_info.get("vip_discounted_price", 0))),
            "一起买优惠金额(元)": str(format_price(price_info.get("bulkbuy_discounted_price", 0))),
            "国补优惠金额(元)": str(format_price(price_info.get("national_subsidy_discounted_price", 0))),
            "平台券优惠金额(元)": str(format_price(price_info.get("cash_coupon_discounted_price", 0))),
            "地方补贴优惠(元)": str(format_price(price_info.get("national_subsidy_merchant_discounted_price", 0))),
            "活动商家补贴(元)": str(format_price(price_info.get("platform_activity_merchant_discounted_price", 0))),
            
            "订单发货方式": str(delivery_info.get("deliver_method", "")),
            "订单发货方式描述": str(get_enum_desc(DELIVER_METHOD_MAP, delivery_info.get("deliver_method"))),
            "发货完成时间": str(format_ts(delivery_info.get("ship_done_time"))),
            "修改地址申请时间": str(format_ts(delivery_info.get("address_apply_time"))),
            "电子面单跨店铺取号密钥": str(delivery_info.get("ewaybill_order_code", "")),
            "订单质检类型": str(delivery_info.get("quality_inspect_type", "")),
            "订单质检类型描述": str(get_enum_desc(QUALITY_INSPECT_TYPE_MAP, delivery_info.get("quality_inspect_type"))),
            "预计发货时间": str(format_ts(delivery_info.get("predict_delivery_time"))),
            "发货时效类型": str(delivery_info.get("delivery_time_type", "")),
            "发货时效类型描述": str(get_enum_desc(DELIVERY_TIME_TYPE_MAP, delivery_info.get("delivery_time_type"))),
            "供货商代发标记": str(delivery_info.get("dropship_flag", "")),
            "供货商代发状态": str(get_enum_desc(DROPSHIP_FLAG_MAP, delivery_info.get("dropship_flag"))),
            
            "收货人姓名": str(address_info.get("user_name", "")),
            "邮编": str(address_info.get("postal_code", "")),
            "省份": str(address_info.get("province_name", "")),
            "城市": str(address_info.get("city_name", "")),
            "区/县": str(address_info.get("county_name", "")),
            "详细地址": str(address_info.get("detail_info", "")),
            "联系方式": str(address_info.get("tel_number", "")),
            "门牌号码": str(address_info.get("house_number", "")),
            "虚拟发货联系方式": str(address_info.get("virtual_order_tel_number", "")),
            "是否使用虚拟号码": str(address_info.get("use_tel_number", 0)),
            "用户地址唯一标识": str(address_info.get("hash_code", "")),
            "脱敏手机号": str(tel_number_ext_info.get("real_tel_number", "")),
            "完整虚拟号码": str(tel_number_ext_info.get("virtual_tel_number", "")),
            "主动兑换虚拟号码次数": str(tel_number_ext_info.get("get_virtual_tel_cnt", 0)),
            "虚拟号码过期时间": str(format_ts(tel_number_ext_info.get("virtual_tel_expire_time"))),
            
            "修改后收货人姓名": str(address_under_review.get("user_name", "")),
            "修改后邮编": str(address_under_review.get("postal_code", "")),
            "修改后省份": str(address_under_review.get("province_name", "")),
            "修改后城市": str(address_under_review.get("city_name", "")),
            "修改后区县": str(address_under_review.get("county_name", "")),
            "修改后详细地址": str(address_under_review.get("detail_info", "")),
            "修改后联系方式": str(address_under_review.get("tel_number", "")),
            "修改后门牌号码": str(address_under_review.get("house_number", "")),
            "修改后虚拟联系方式": str(address_under_review.get("virtual_order_tel_number", "")),
            "修改后是否用虚拟号": str(address_under_review.get("use_tel_number", 0)),
            "修改后地址唯一标识": str(address_under_review.get("hash_code", "")),
            "修改后脱敏手机号": str(review_tel_ext_info.get("real_tel_number", "")),
            "修改后虚拟号码": str(review_tel_ext_info.get("virtual_tel_number", "")),
            
            "充值账号": str(recharge_info.get("account_no", "")),
            "账号充值类型": str(recharge_info.get("account_type", "")),
            "微信OpenID": str(recharge_info.get("wx_openid", "")),
            
            "质检状态": str(quality_inspect_info.get("inspect_status", "")),
            "质检状态描述": str(get_enum_desc({**QUALITY_INSPECT_STATUS_JEWELRY, **QUALITY_INSPECT_STATUS_FRESH}, 
                                            quality_inspect_info.get("inspect_status"), "未知质检状态")),
            
            "用户备注": str(ext_info.get("customer_notes", "")),
            "商家备注": str(ext_info.get("merchant_notes", "")),
            "确认收货时间": str(format_ts(ext_info.get("confirm_receipt_time"))),
            "视频号ID": str(ext_info.get("finder_id", "")),
            "直播ID": str(ext_info.get("live_id", "")),
            "下单场景": str(ext_info.get("order_scene", "")),
            "下单场景描述": str(get_enum_desc(ORDER_SCENE_MAP, ext_info.get("order_scene"))),
            "会员权益SessionID": str(ext_info.get("vip_order_session_id", "")),
            "分佣单生成状态": str(ext_info.get("commission_handling_progress", "")),
            "分佣单状态描述": str(get_enum_desc({0: "未生成", 1: "已生成"}, ext_info.get("commission_handling_progress"))),
            
            "用户优惠券ID": str(coupon_info.get("user_coupon_id", "")),
            
            "分享员OpenID": str(sharer_info.get("sharer_openid", "")),
            "分享员UnionID": str(sharer_info.get("sharer_unionid", "")),
            "分享员类型": str(sharer_info.get("sharer_type", "")),
            "分享员类型描述": str(get_enum_desc(SHARER_TYPE_MAP, sharer_info.get("sharer_type"))),
            "分享场景": str(sharer_info.get("share_scene", "")),
            "分享场景描述": str(get_enum_desc(SHARE_SCENE_MAP, sharer_info.get("share_scene"))),
            "分享员数据解析状态": str(sharer_info.get("handling_progress", "")),
            "解析状态描述": str(get_enum_desc({1: "解析完成", 0: "解析中"}, sharer_info.get("handling_progress"))),
            
            "预计技术服务费(元)": str(format_price(settle_info.get("predict_commission_fee", 0))),
            "技术服务费(元)": str(format_price(settle_info.get("commission_fee", 0))),
            "预计人气卡返佣(元)": str(format_price(settle_info.get("predict_wecoin_commission", 0))),
            "人气卡返佣金额(元)": str(format_price(settle_info.get("wecoin_commission", 0))),
            "商家结算时间": str(format_ts(settle_info.get("settle_time"))),
            
            "授权视频号ID": str(agent_info.get("agent_finder_id", "")),
            "授权视频号昵称": str(agent_info.get("agent_finder_nickname", "")),
            
            "退还运费金额(元)": str(format_price(refund_info.get("refund_freight", 0))),
            
            "贺卡落款": str(greeting_card_info.get("giver_name", "")),
            "贺卡称谓": str(greeting_card_info.get("receiver_name", "")),
            "贺卡内容": str(greeting_card_info.get("greeting_message", "")),
            
            "定制图片URL": str(custom_info.get("custom_img_url", "")),
            "定制文字": str(custom_info.get("custom_word", "")),
            "定制类型": str(custom_info.get("custom_type", "")),
            "定制类型描述": str(get_enum_desc(CUSTOM_TYPE_MAP, custom_info.get("custom_type"))),
            "定制预览图片URL": str(custom_info.get("custom_preview_img_url", "")),
            
            "正在售后流程的售后单数": str(aftersale_detail.get("on_aftersale_order_cnt", 0)),
            "售后订单ID": str(first_aftersale.get("aftersale_order_id", "")),
            "售后订单状态": str(first_aftersale.get("status", "")),
            
            "商品ID": str(first_product.get("product_id", "")),
            "SKU_ID": str(first_product.get("sku_id", "")),
            "SKU小图URL": str(first_product.get("thumb_img", "")),
            "售卖单价(元)": str(format_price(first_product.get("sale_price", 0))),
            "SKU数量": str(first_product.get("sku_cnt", 0)),
            "商品标题": str(first_product.get("title", "")),
            "正在售后SKU数量": str(first_product.get("on_aftersale_sku_cnt", 0)),
            "完成售后SKU数量": str(first_product.get("finish_aftersale_sku_cnt", 0)),
            "商品编码": str(first_product.get("sku_code", "")),
            "市场单价(元)": str(format_price(first_product.get("market_price", 0))),
            "SKU属性键": str(first_sku_attr.get("attr_key", "")),
            "SKU属性值": str(first_sku_attr.get("attr_value", "")),
            "SKU实付总价(元)": str(format_price(first_product.get("real_price", 0))),
            "商品外部SPUID": str(first_product.get("out_product_id", "")),
            "商品外部SKUID": str(first_product.get("out_sku_id", "")),
            "是否有商家优惠": str(first_product.get("is_discounted", False)),
            "使用优惠后SKU总价(元)": str(format_price(first_product.get("estimate_price", 0))),
            "是否修改过价格": str(first_product.get("is_change_price", False)),
            "改价后SKU总价(元)": str(format_price(first_product.get("change_price", 0))),
            "区域库存ID": str(first_product.get("out_warehouse_id", "")),
            "是否使用会员积分抵扣": str(first_product.get("use_deduction", False)),
            "会员积分抵扣金额(元)": str(format_price(first_product.get("deduction_price", 0))),
            "商品发货时效(秒)": str(first_product.get("delivery_deadline", 0)),
            "商家优惠金额(元)": str(format_price(first_product.get("merchant_discounted_price", 0))),
            "达人优惠金额(元)": str(format_price(first_product.get("finder_discounted_price", 0))),
            "是否赠品": str(first_product.get("is_free_gift", 0)),
            "会员权益优惠金额(元)": str(format_price(first_product.get("vip_discounted_price", 0))),
            "商品常量编号": str(first_product.get("product_unique_id", "")),
            "一起买优惠金额(元)": str(format_price(first_product.get("bulkbuy_discounted_price", 0))),
            "国补优惠金额(元)": str(format_price(first_product.get("national_subsidy_discounted_price", 0))),
            "是否闪购商品": str(first_product.get("is_flash_sale", False)),
            "地方补贴优惠金额(元)": str(format_price(first_product.get("national_subsidy_merchant_discounted_price", 0))),
            "活动商家补贴(元)": str(format_price(first_product.get("platform_activity_merchant_discounted_price", 0))),
            "平台券优惠金额(元)": str(format_price(first_product.get("cash_coupon_discounted_price", 0))),
            "代发单号": str(first_product_dropship.get("ds_order_id", "")),
            "改价状态": str(get_enum_desc(CHANGE_SKU_STATE_MAP, first_product_change_sku.get("preshipment_change_sku_state"))),
            "原SKU_ID": str(first_product_change_sku.get("old_sku_id", "")),
            "新SKU_ID": str(first_product_change_sku.get("new_sku_id", "")),
            "商家处理截止时间": str(format_ts(first_product_change_sku.get("ddl_time_stamp"))),
            "7天无理由": str(get_enum_desc({0: "不支持", 1: "支持"}, first_product_extra_service.get("seven_day_return"))),
            "商家运费险": str(get_enum_desc({0: "不支持", 1: "支持"}, first_product_extra_service.get("freight_insurance"))),
            "商品发货类型": str(get_enum_desc({0: "现货", 1: "全款预售"}, first_product_sku_deliver.get("stock_type"))),
            "预计发货时间_商品": str(format_ts(first_product_sku_deliver.get("predict_delivery_time"))),
            "预售类型": str(get_enum_desc({0: "付款后n天发货", 1: "预售结束后n天发货"}, first_product_sku_deliver.get("full_payment_presale_delivery_type"))),
            
            "商品优惠券ID": str(first_product_coupon.get("user_coupon_id", "")),
            "优惠券类型": str(get_enum_desc(COUPON_TYPE_MAP, first_product_coupon.get("coupon_type"))),
            "优惠金额(元)": str(format_price(first_product_coupon.get("discounted_price", 0))),
            "优惠券ID": str(first_product_coupon.get("coupon_id", "")),
            
            "赠品数量": str(first_product_gift.get("gift_cnt", 0)),
            "活动ID": str(first_product_gift.get("task_id", "")),
            "赠品商品ID": str(first_product_gift.get("product_id", "")),
            "主品SKU_ID": str(first_product_gift.get("sku_id", "")),
            
            "快递单号": str(first_logistics.get("waybill_id", "")),
            "快递公司编码": str(first_logistics.get("delivery_id", "")),
            "快递公司名称": str(first_logistics.get("delivery_name", "")),
            "发货时间": str(format_ts(first_logistics.get("delivery_time"))),
            "配送方式": str(get_enum_desc(DELIVER_TYPE_MAP, first_logistics.get("deliver_type"))),
            "物流商品ID": str(first_logistics_product.get("product_id", "")),
            "物流SKU_ID": str(first_logistics_product.get("sku_id", "")),
            "物流商品数量": str(first_logistics_product.get("product_cnt", 0)),
            
            "分账方昵称": str(first_commission.get("nickname", "")),
            "分账方类型": str(get_enum_desc(COMMISSION_TYPE_MAP, first_commission.get("type"))),
            "分账状态": str(get_enum_desc(COMMISSION_STATUS_MAP, first_commission.get("status"))),
            "分账金额(元)": str(format_price(first_commission.get("amount", 0))),
            "达人视频号ID": str(first_commission.get("finder_id", "")),
            "达人openfinderid": str(first_commission.get("openfinderid", "")),
            "新带货达人ID": str(first_commission.get("talent_id", "")),
            "带货机构ID": str(first_commission.get("agency_id", "")),
            
            "带货账户类型": str(get_enum_desc(ACCOUNT_TYPE_MAP, first_source.get("account_type"))),
            "带货账户ID": str(first_source.get("account_id", "")),
            "销售渠道": str(get_enum_desc(SALE_CHANNEL_MAP, first_source.get("sale_channel"))),
            "带货账户昵称": str(first_source.get("account_nickname", "")),
            "带货内容类型": str(first_source.get("content_type", "")),
            "带货内容ID": str(first_source.get("content_id", "")),
            
            "分享员类型_SKU": str(get_enum_desc(SHARER_TYPE_MAP, first_sku_sharer.get("sharer_type"))),
            "分享场景_SKU": str(get_enum_desc(SHARE_SCENE_MAP, first_sku_sharer.get("share_scene"))),
            "是否来自企微分享": str(first_sku_sharer.get("from_wecom", False))
        }
        
        return order_data

    def batch_get_order_details(self, order_ids: List[str]) -> pd.DataFrame:
        if not order_ids:
            print("❌ 无有效订单ID")
            return pd.DataFrame()

        print(f"📌 开始获取{len(order_ids)}个订单详情...")
        all_order_data = []
        total = len(order_ids)

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_id = {
                executor.submit(self._process_order, order_id): order_id
                for order_id in order_ids
            }

            for idx, future in enumerate(as_completed(future_to_id), 1):
                order_id = future_to_id[future]
                try:
                    result = future.result()
                    if result:
                        all_order_data.append(result)
                    if idx % 100 == 0 or idx == total:
                        print(f"进度：{idx}/{total}，已获取有效数据：{len(all_order_data)}条")
                except Exception as e:
                    print(f"❌ 订单{order_id}处理异常：{str(e)}")
                time.sleep(0.01)

        df = pd.DataFrame(all_order_data)
        print(f"\n✅ 完成！共获取{len(df)}条订单数据，包含{len(df.columns)}个字段")
        return df
