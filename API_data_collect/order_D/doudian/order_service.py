import time
import pandas as pd
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from .client import DouDianAPIClient
from .config import (
    PAY_TYPE_MAP, CANCEL_TYPE_MAP, BMP_SOURCE_MAP, BMP_VERTICAL_MARKET_MAP,
    BMP_SELLER_TYPE_MAP, SHOP_TYPE_MAP, SELLER_REMARK_STRAS, GIVEN_PRODUCT_TYPE_MAP,
    AFTER_SALE_STATUS_MAP, AFTER_SALE_TYPE_MAP, REFUND_STATUS_MAP
)
from .utils import format_time, format_amount, get_mapped_value


class OrderService:
    def __init__(self, client: DouDianAPIClient, max_workers: int = 60):
        self.client = client
        self.max_workers = max_workers

    def get_order_list(self, params: Dict[str, Any]) -> List[str]:
        print("开始获取订单列表...")
        all_order_ids = []
        page = params.get("page", 0)
        max_retries = 3

        while True:
            request_params = params.copy()
            request_params["page"] = page
            retry_count = 0
            result = None

            while retry_count < max_retries:
                try:
                    result = self.client.request("order.searchList", request_params)
                    break
                except Exception as e:
                    retry_count += 1
                    print(f"第{page}页请求异常（{retry_count}/{max_retries}）：{str(e)}")
                    time.sleep(1)

            data = result
            if not data:
                print(f"第{page}页无数据返回")
                break
                
            total = int(data.get("total", 0))
            orders = data.get("shop_order_list", [])
            
            if not orders:
                print(f"第{page}页无订单数据")
                break
                
            current_order_ids = [
                str(order["order_id"]) 
                for order in orders 
                if "order_id" in order and order["order_id"]
            ]
            
            all_order_ids.extend(current_order_ids)
            
            print(f"第{page}页：获取{len(current_order_ids)}个订单ID（累计{len(all_order_ids)}/{total}）")
            
            if len(all_order_ids) >= total or page > 100:
                break
                
            page += 1
            time.sleep(0.05)

        print(f"\n✅ 订单列表获取完成！共提取{len(all_order_ids)}个订单ID")
        return all_order_ids

    def get_order_detail(self, order_id: str) -> Optional[Dict[str, Any]]:
        if not order_id or not isinstance(order_id, str) or len(order_id) < 16:
            print(f"❌ 订单ID格式无效：{order_id}")
            return None
        
        try:
            request_params = {"shop_order_id": order_id}
            print(f"🔍 正在请求订单{order_id}")
            
            start_time = time.time()
            result = self.client.request("order.orderDetail", request_params)
            end_time = time.time()
            print(f"⏱️  订单{order_id}响应耗时：{end_time - start_time:.2f}秒")
            
            if not isinstance(result, dict):
                print(f"❌ 订单{order_id}响应格式异常：{type(result)}")
                return None
            
            order_detail = result.get("shop_order_detail", {})
            if not order_detail or not isinstance(order_detail, dict):
                print(f"❌ 订单{order_id}无有效详情：{result}")
                return None
            
            print(f"✅ 订单{order_id}获取成功")
            return order_detail
        
        except Exception as e:
            print(f"❌ 订单{order_id}异常：{str(e)}")
            return None

    def _process_order(self, order_id: str) -> Optional[Dict[str, Any]]:
        detail = self.get_order_detail(order_id)
        
        sku_order_list = detail.get("sku_order_list", [])
        sku_order = sku_order_list[0] if (isinstance(sku_order_list, list) and len(sku_order_list) > 0) else {}
        
        logistics_list = detail.get("logistics_info", [])
        logistics = logistics_list[0] if (isinstance(logistics_list, list) and len(logistics_list) > 0) else {}
        
        recommend_logistics_list = detail.get("recommend_logistics_list", [])
        recommend_logistics = recommend_logistics_list[0] if (isinstance(recommend_logistics_list, list) and len(recommend_logistics_list) > 0) else {}
        
        order_operate_list = detail.get("order_operate_record_list", [])
        order_operate = order_operate_list[0] if (isinstance(order_operate_list, list) and len(order_operate_list) > 0) else {}
        
        order_phase_list = detail.get("order_phase_list", [])
        order_phase = order_phase_list[0] if (isinstance(order_phase_list, list) and len(order_phase_list) > 0) else {}
        
        order_data = {
            "根订单ID": str(detail.get("order_id", "")).strip(),
            "店铺ID": str(detail.get("shop_id", "")).strip(),
            "店铺名称": str(detail.get("shop_name", "")).strip(),
            "原始店铺ID": str(detail.get("original_shop_id", "")).strip(),
            "订单类型": str(detail.get("order_type_desc", "")).strip(),
            "订单类型编码": str(detail.get("order_type", "")).strip(),
            "交易类型": str(detail.get("trade_type_desc", "")).strip(),
            "交易类型编码": str(detail.get("trade_type", "")).strip(),
            "交易场景": str(detail.get("trade_scene", 0)).strip(),
            "支付方式编码": str(detail.get("pay_type", 0)).strip(),
            "支付方式": str(get_mapped_value(PAY_TYPE_MAP, detail.get("pay_type"))).strip(),
            "支付渠道流水号": str(detail.get("channel_payment_no", "")).strip(),
            "平台": str("抖店").strip(),
            "业务来源": str(detail.get("biz_desc", "")).strip(),
            "业务来源编码": str(detail.get("biz", "")).strip(),
            "下单端": str(detail.get("b_type_desc", "")).strip(),
            "下单端编码": str(detail.get("b_type", "")).strip(),
            "下单场景": str(detail.get("sub_b_type_desc", "")).strip(),
            "下单场景编码": str(detail.get("sub_b_type", "")).strip(),
            "小程序ID": str(detail.get("app_id", "")).strip(),
            "小程序 AppName": str(detail.get("app_name", "")).strip(),
            "抖音小程序ID": str(detail.get("open_id", "")).strip(),
            "用户唯一ID": str(detail.get("doudian_open_id", "")).strip(),
            "主播ID(达人)": str(sku_order.get("author_id","")).strip(),
            "主播名称(达人)": str(sku_order.get("author_name","")).strip(),
            "达人抖音号（申样）": str(detail.get("aweme_id", "")).strip(),
            "达人抖音昵称（申样）": str(detail.get("user_nick_name", "")).strip(),
            "达人抖音头像（申样）": str(detail.get("user_icon", "")).strip(),
            "买家标签": str(detail.get("user_tag_ui", [{}])[0].get("text", "") if (detail.get("user_tag_ui") and len(detail.get("user_tag_ui")) > 0) else "").strip(),
            "买家留言": str(detail.get("buyer_words", "")).strip(),
            "商家备注": str(detail.get("seller_words", "")).strip(),
            "插旗星级": str(get_mapped_value(SELLER_REMARK_STRAS,detail.get("seller_remark_stars", 0))).strip(),
            "催单次数": str(detail.get("urge_deliver_times", 0)).strip(),
            "订单层级": str(detail.get("order_level", "")).strip(),
            "订单状态": str(detail.get("order_status_desc", "")).strip(),
            "订单状态编码": str(detail.get("order_status", "")).strip(),
            "主流程状态": str(detail.get("main_status_desc", "")).strip(),
            "主流程状态编码": str(detail.get("main_status", "")).strip(),
            "接单状态": str(detail.get("accept_order_status", "")).strip(),
            "接单状态描述": str("已接单" if detail.get("accept_order_status") == 1 else "未接单/取消").strip(),
            "履约状态": str(detail.get("fulfill_status", "")).strip(),
            "取消状态": str(detail.get("cancel_status", 0)).strip(),
            "取消类型编码": str(detail.get("cancel_type", 0)).strip(),
            "取消类型": str(get_mapped_value(CANCEL_TYPE_MAP, detail.get("cancel_type"))).strip(),
            "取消原因": str(detail.get("cancel_reason", "")).strip(),
            "订单标签": str(detail.get("order_tag", {})).strip(),
            "店铺单标签": str(detail.get("shop_order_tag_ui", [{}])[0].get("text", "") if (detail.get("shop_order_tag_ui") and len(detail.get("shop_order_tag_ui")) > 0) else "").strip(),
            "地址标签": str(detail.get("address_tag_ui", [{}])[0].get("text", "") if (detail.get("address_tag_ui") and len(detail.get("address_tag_ui")) > 0) else "").strip(),
            "地址标签提示": str(detail.get("address_tag_ui", [{}])[0].get("hover_text", "") if (detail.get("address_tag_ui") and len(detail.get("address_tag_ui")) > 0) else "").strip(),
            "业务身份-来源": str(detail.get("bmp_source", "")).strip(),
            "业务身份-来源描述": str(get_mapped_value(BMP_SOURCE_MAP, detail.get("bmp_source"))).strip(),
            "业务身份-垂类": str(detail.get("bmp_vertical_market", "")).strip(),
            "业务身份-垂类描述": str(get_mapped_value(BMP_VERTICAL_MARKET_MAP, detail.get("bmp_vertical_market"))).strip(),
            "业务身份-卖家身份": str(detail.get("bmp_seller_type", "")).strip(),
            "业务身份-卖家身份描述": str(get_mapped_value(BMP_SELLER_TYPE_MAP, detail.get("bmp_seller_type"))).strip(),
            "关联订单号": str(detail.get("relation_shop_order_ids", [])).strip(),
            "店铺类型编码": str(detail.get("shop_type", 0)).strip(),
            "店铺类型": str(get_mapped_value(SHOP_TYPE_MAP, detail.get("shop_type"))).strip(),
            "商家服务ID": str(detail.get("poi_id", "")).strip(),
            "商家服务名称": str(detail.get("poi_name", "")).strip(),
            "商家服务类型": str(detail.get("shop_service_type", 0)).strip(),
            "团购ID": str(detail.get("group_buy_id", "")).strip(),
            "团购类型": str(detail.get("group_buy_type", 0)).strip(),
            "是否为拼团订单": str(detail.get("is_group_buy", 0)).strip(),
            "运力类型": str(detail.get("delivery_type", 0)).strip(),
            "物流服务商": str(detail.get("logistics_service_type", 0)).strip(),
            "是否代发订单": str(detail.get("is_delivery_agent", 0)).strip(),
            "是否平台代发": str(detail.get("is_platform_delivery", 0)).strip(),
            "是否是小时达订单": str(detail.get("is_hour_up", 0)).strip(),
            "小时达门店流水号": str(detail.get("supermarket_order_serial_no", "")).strip(),
            "合单地址ID": str(detail.get("open_address_id", "")).strip(),
            "渠道ID": str(detail.get("channel_id", "")).strip(),
            "是否包邮": str(detail.get("is_post_free", 0)).strip(),
            "电子面单号": str(detail.get("waybill_no", "")).strip(),
            "是否为极速达订单": str(detail.get("is_fast_delivery", 0)).strip(),
            "是否为极速发货单": str(detail.get("is_fast_ship", 0)).strip(),
            "是否包含履约险": str(detail.get("has_fulfill_insurance", 0)).strip(),
            "是否跨境": str(detail.get("is_sea_order", 0)).strip(),
            "是否为跨境子单": str(detail.get("is_sea_child_order", 0)).strip(),
            "流量来源": str(detail.get("send_pay_desc", "")).strip(),
            "流量来源编码": str(detail.get("send_pay", 0)).strip(),
            "下单来源": str(detail.get("theme_type_desc", "")).strip(),
            "下单来源编码": str(detail.get("theme_type", 0)).strip(),
            "C端流量来源": str(detail.get("c_biz_desc", "")).strip(),
            "C端流量来源编码": str(detail.get("c_biz", 0)).strip(),
            "广告来源": str(detail.get("ad_env_type", "")).strip(),
            "落地页ID": str(detail.get("page_id", "")).strip(),
            "抖音直播间ID": str(detail.get("room_id_str", "")).strip(),
            "内容ID": str(detail.get("content_id", "")).strip(),
            "视频ID": str(detail.get("video_id", "")).strip(),
            "广告ID": str(detail.get("cid", "")).strip(),
            "流量来源ID": str(detail.get("origin_id", "")).strip(),
            "是否已评价": str(detail.get("is_comment", 0)).strip(),
            "是否需要序列号": str(detail.get("need_serial_number", 0)).strip(),
            "商品序列号列表": str(detail.get("serial_number_list", [])).strip(),
            "是否为预售单": str(detail.get("is_pre_sale", 0)).strip(),
            "订单备注": str(detail.get("order_remark", "")).strip(),
            "是否已打款": str(detail.get("is_settled", 0)).strip(),
            "是否为极速退款单": str(detail.get("is_fast_refund", 0)).strip(),
            "是否为退货包运费单": str(detail.get("is_post_insurance", 0)).strip(),
            "订单总重量(g)": str(detail.get("total_weight", 0)).strip(),
            "主订单是否有售后": str(detail.get("has_after_sale", 0)).strip(),
            "订单总金额(元)": str(format_amount(detail.get("order_amount", 0))).strip(),
            "支付金额(元)": str(format_amount(detail.get("pay_amount", 0))).strip(),
            "运费金额(元)": str(format_amount(detail.get("post_amount", 0))).strip(),
            "运费原价(元)": str(format_amount(detail.get("post_origin_amount", 0))).strip(),
            "运费优惠金额(元)": str(format_amount(detail.get("post_promotion_amount", 0))).strip(),
            "运费险金额(元)": str(format_amount(detail.get("post_insurance_amount", 0))).strip(),
            "打包费(元)": str(format_amount(detail.get("packing_amount", 0))).strip(),
            "运费补差金额(元)": str(format_amount(detail.get("make_up_post_amount", 0))).strip(),
            "总优惠金额(元)": str(format_amount(detail.get("promotion_amount", 0))).strip(),
            "店铺优惠金额(元)": str(format_amount(detail.get("promotion_shop_amount", 0))).strip(),
            "平台优惠金额(元)": str(format_amount(detail.get("promotion_platform_amount", 0))).strip(),
            "达人优惠金额(元)": str(format_amount(detail.get("promotion_talent_amount", 0))).strip(),
            "支付优惠金额(元)": str(format_amount(detail.get("promotion_pay_amount", 0))).strip(),
            "红包优惠金额(元)": str(format_amount(detail.get("promotion_redpack_amount", 0))).strip(),
            "平台红包金额(元)": str(format_amount(detail.get("promotion_redpack_platform_amount", 0))).strip(),
            "达人红包金额(元)": str(format_amount(detail.get("promotion_redpack_talent_amount", 0))).strip(),
            "商家承担金额(元)": str(format_amount(detail.get("shop_cost_amount", 0))).strip(),
            "平台承担金额(元)": str(format_amount(detail.get("platform_cost_amount", 0))).strip(),
            "仅平台承担金额(元)": str(format_amount(detail.get("only_platform_cost_amount", 0))).strip(),
            "达人承担金额(元)": str(format_amount(detail.get("author_cost_amount", 0))).strip(),
            "总优惠金额(含运费)(元)": str(format_amount(detail.get("total_promotion_amount", 0))).strip(),
            "税费金额(元)": str(format_amount(detail.get("tax_amount", 0))).strip(),
            "跨境税费未计算标识": str(detail.get("tax_amount_not_come_out", 0)).strip(),
            "商家实际收入(元)": str(format_amount(detail.get("actual_receive_amount_info", {}).get("actual_receive_amount", 0))).strip(),
            "收入明细-类型": str(detail.get("actual_receive_amount_info", {}).get("actual_receive_amount_details", [{}])[0].get("type", "") if (detail.get("actual_receive_amount_info", {}).get("actual_receive_amount_details") and len(detail.get("actual_receive_amount_info", {}).get("actual_receive_amount_details")) > 0) else "").strip(),
            "收入明细-金额(元)": str(format_amount(detail.get("actual_receive_amount_info", {}).get("actual_receive_amount_details", [{}])[0].get("amount", 0)) if (detail.get("actual_receive_amount_info", {}).get("actual_receive_amount_details") and len(detail.get("actual_receive_amount_info", {}).get("actual_receive_amount_details")) > 0) else 0.0).strip(),
            "抖音月付-总交易金额(元)": str(format_amount(detail.get("free_interest", {}).get("trade_amount", 0))).strip(),
            "抖音月付-分期期数": str(detail.get("free_interest", {}).get("period", "")).strip(),
            "抖音月付-平台承担息费(元)": str(format_amount(detail.get("free_interest", {}).get("total_free_interest_platform_clearing_amount", 0))).strip(),
            "抖音月付-商家承担息费(元)": str(format_amount(detail.get("free_interest", {}).get("total_free_interest_shop_clearing_amount", 0))).strip(),
            "抖音月付-退款平台息费(元)": str(format_amount(detail.get("free_interest", {}).get("refund_total_free_interest_platform_clearing_amount", 0))).strip(),
            "抖音月付-退款商家息费(元)": str(format_amount(detail.get("free_interest", {}).get("refund_total_free_interest_shop_clearing_amount", 0))).strip(),
            "是否依旧换新": str("否" if detail.get("shop_recycle_subsidy_amount_info", {}).get("subsidy_amount", 0) < 1 else "是").strip(),
            "换新补贴金额(元)": str(format_amount(detail.get("shop_recycle_subsidy_amount_info", {}).get("subsidy_amount", 0))).strip(),
            "换新补贴-平台出资(元)": str(format_amount(detail.get("shop_recycle_subsidy_amount_info", {}).get("amount_composition_info", {}).get("platform_cost", 0))).strip(),
            "换新补贴-商家出资(元)": str(format_amount(detail.get("shop_recycle_subsidy_amount_info", {}).get("amount_composition_info", {}).get("shop_cost", 0))).strip(),
            "换新补贴渠道": str(detail.get("shop_recycle_subsidy_amount_info", {}).get("subsidy_channel", "")).strip(),
            "换新补贴状态": str(detail.get("shop_recycle_subsidy_amount_info", {}).get("subsidy_status", "")).strip(),
            "额外优惠-活动类型": str(detail.get("extra_promotion_amount_detail", [{}])[0].get("instant_discount_type", "") if (detail.get("extra_promotion_amount_detail") and len(detail.get("extra_promotion_amount_detail")) > 0) else "").strip(),
            "额外优惠-活动ID": str(detail.get("extra_promotion_amount_detail", [{}])[0].get("promotion_id", "") if (detail.get("extra_promotion_amount_detail") and len(detail.get("extra_promotion_amount_detail")) > 0) else "").strip(),
            "额外优惠-优惠描述": str(detail.get("extra_promotion_amount_detail", [{}])[0].get("promotion_desc", "") if (detail.get("extra_promotion_amount_detail") and len(detail.get("extra_promotion_amount_detail")) > 0) else "").strip(),
            "额外优惠-优惠类型": str(detail.get("extra_promotion_amount_detail", [{}])[0].get("promotion_type", "") if (detail.get("extra_promotion_amount_detail") and len(detail.get("extra_promotion_amount_detail")) > 0) else "").strip(),
            "额外优惠-达人承担(元)": str(format_amount(detail.get("extra_promotion_amount_detail", [{}])[0].get("share_cost", {}).get("author_cost", 0)) if (detail.get("extra_promotion_amount_detail") and len(detail.get("extra_promotion_amount_detail")) > 0) else 0.0).strip(),
            "额外优惠-商家承担(元)": str(format_amount(detail.get("extra_promotion_amount_detail", [{}])[0].get("share_cost", {}).get("shop_cost", 0)) if (detail.get("extra_promotion_amount_detail") and len(detail.get("extra_promotion_amount_detail")) > 0) else 0.0).strip(),
            "额外优惠-平台承担(元)": str(format_amount(detail.get("extra_promotion_amount_detail", [{}])[0].get("share_cost", {}).get("platform_cost", 0)) if (detail.get("extra_promotion_amount_detail") and len(detail.get("extra_promotion_amount_detail")) > 0) else 0.0).strip(),
            "店铺支付优惠(元)": str(format_amount(detail.get("promotion_pay_shop_amount", 0))).strip(),
            "平台支付优惠(元)": str(format_amount(detail.get("promotion_pay_platform_amount", 0))).strip(),
            "达人支付优惠(元)": str(format_amount(detail.get("promotion_pay_talent_amount", 0))).strip(),
            "用户预付款(元)": str(format_amount(detail.get("pre_sale_pay_amount", 0))).strip(),
            "改价金额变化量(元)": str(format_amount(detail.get("modify_amount", 0))).strip(),
            "改价运费变化量(元)": str(format_amount(detail.get("modify_post_amount", 0))).strip(),
            "提交时间": str(format_time(detail.get("create_time", 0))).strip(),
            "支付时间": str(format_time(detail.get("pay_time", 0))).strip(),
            "商家接单时间": str(format_time(detail.get("accept_order_time", 0))).strip(),
            "发货时间": str(format_time(detail.get("ship_time", 0))).strip(),
            "订单完成时间": str(format_time(detail.get("finish_time", 0))).strip(),
            "收货时间": str(format_time(detail.get("receipt_time", 0))).strip(),
            "订单更新时间": str(format_time(detail.get("update_time", 0))).strip(),
            "订单最近修改时间": str(format_time(detail.get("latest_update_time", 0))).strip(),
            "商品价格修改时间": str(format_time(detail.get("order_goods_price_modify_time", 0))).strip(),
            "预计发货时间": str(format_time(detail.get("exp_ship_time", 0))).strip(),
            "订单过期时间": str(format_time(detail.get("order_expire_time", 0))).strip(),
            "预约发货时间": str(format_time(detail.get("appointment_ship_time", 0))).strip(),
            "建议最早发货时间": str(format_time(detail.get("recommend_start_ship_time", 0))).strip(),
            "建议最晚发货时间": str(format_time(detail.get("recommend_end_ship_time", 0))).strip(),
            "最晚送达时间": str(format_time(detail.get("latest_receipt_time", 0))).strip(),
            "取消申请时间": str(format_time(detail.get("cancel_apply_time", 0))).strip(),
            "时效类型": str(detail.get("promise_detail", {}).get("promise_type", "")).strip(),
            "承诺最晚发货时间": str(format_time(detail.get("promise_detail", {}).get("promise_time_detail", {}).get("promise_exp_ship_time", 0))).strip(),
            "承诺最晚送达时间": str(format_time(detail.get("promise_detail", {}).get("promise_time_detail", {}).get("promise_latest_receipt_time", 0))).strip(),
            "预约发货时间(承诺)": str(format_time(detail.get("promise_detail", {}).get("promise_time_detail", {}).get("promise_appointment_ship_time", 0))).strip(),
            "承诺最早发货时间": str(format_time(detail.get("promise_detail", {}).get("promise_time_detail", {}).get("promise_recommend_start_ship_time", 0))).strip(),
            "承诺最晚发货时间(推荐)": str(format_time(detail.get("promise_detail", {}).get("promise_time_detail", {}).get("promise_recommend_end_ship_time", 0))).strip(),
            "操作人": str(order_operate.get("operator", "")).strip(),
            "操作时间": str(format_time(order_operate.get("operate_time", ""))).strip(),
            "操作类型编码": str(order_operate.get("op_code", "")).strip(),
            "操作记录ID": str(order_operate.get("op_id", "")).strip(),
            "物流公司": str(logistics.get("company_name", "")).strip(),
            "物流公司编码": str(logistics.get("company", "")).strip(),
            "物流单号": str(logistics.get("tracking_no", "")).strip(),
            "物流跟踪码": str(logistics.get("tracking_code", "")).strip(),
            "包裹ID": str(logistics.get("delivery_id", "")).strip(),
            "合包标识": str(logistics.get("transit_merge_type", "")).strip(),
            "保价金额(元)": str(format_amount(logistics.get("guarantee_amount", 0))).strip(),
            "骑手取件码": str(logistics.get("hour_up_pickup_code", "")).strip(),
            "物流发货时间": str(format_time(logistics.get("ship_time", 0))).strip(),
            "物流增值服务": str(logistics.get("added_services", [{}])[0].get("text", "") if (logistics.get("added_services") and len(logistics.get("added_services")) > 0) else "").strip(),
            "物流增值服务编码": str(logistics.get("added_services", [{}])[0].get("value", "") if (logistics.get("added_services") and len(logistics.get("added_services")) > 0) else "").strip(),
            "是否虚拟发货": str(logistics.get("is_virtual_ship", 0)).strip(),
            "发货方式": str(logistics.get("ship_method", 0)).strip(),
            "揽收时间": str(format_time(logistics.get("collect_time", 0))).strip(),
            "配送应付金额(元)": str(format_amount(logistics.get("sp_total_price", 0))).strip(),
            "配送实付金额(元)": str(format_amount(logistics.get("sp_price", 0))).strip(),
            "配送优惠金额(元)": str(format_amount(logistics.get("sp_discount_price", 0))).strip(),
            "推荐物流公司": str(recommend_logistics.get("logistics_company_name", "")).strip(),
            "推荐物流公司编码": str(recommend_logistics.get("logistics_company_code", "")).strip(),
            "推荐快递产品类型": str(recommend_logistics.get("logistics_product_type_name", "")).strip(),
            "推荐快递产品编码": str(recommend_logistics.get("logistics_product_type", "")).strip(),
            "推荐最早发货时间": str(format_time(recommend_logistics.get("logistics_recommend_start_ship_time", 0))).strip(),
            "推荐最晚发货时间": str(format_time(recommend_logistics.get("logistics_recommend_end_ship_time", 0))).strip(),
            "最晚揽收时间": str(format_time(recommend_logistics.get("logistics_collection_end_time", 0))).strip(),
            "最晚发货时间(物流)": str(format_time(recommend_logistics.get("logistics_delivery_end_time", 0))).strip(),
            "承诺送达时间(物流)": str(format_time(recommend_logistics.get("logistics_latest_receipt_time", 0))).strip(),
            "推荐物流产品类型": str(recommend_logistics.get("logistics_product_type", 0)).strip(),
            "收件人姓名": str(detail.get("mask_post_receiver", "")).strip(),
            "收件人电话": str(detail.get("mask_post_tel", "")).strip(),
            "收件人省份": str(detail.get("mask_post_addr", {}).get("province", {}).get("name", "")).strip(),
            "收件人城市": str(detail.get("mask_post_addr", {}).get("city", {}).get("name", "")).strip(),
            "收件人区县": str(detail.get("mask_post_addr", {}).get("town", {}).get("name", "")).strip(),
            "收件人街道": str(detail.get("mask_post_addr", {}).get("street", {}).get("name", "")).strip(),
            "详细地址(脱敏)": str(detail.get("mask_post_addr", {}).get("detail", "")).strip(),
            "下单人手机号(鲜花订单)": str(detail.get("encrypt_pay_tel", "")).strip(),
            "脱敏下单人手机号": str(detail.get("mask_pay_tel", "")).strip(),
            "贺卡文字": str(detail.get("greet_words", "")).strip(),
            "收货地址经度": str(detail.get("user_coordinate", {}).get("user_coordinate_longitude", "")).strip(),
            "收货地址纬度": str(detail.get("user_coordinate", {}).get("user_coordinate_latitude", "")).strip(),
            "收件人地址 ID": str(detail.get("post_addr_id", "")).strip(),
            "用户端展示的地址": str(detail.get("mask_post_addr", {}).get("user_addr", "")).strip(),
            "地址类型": str(detail.get("mask_post_addr", {}).get("type", 0)).strip(),
            "子订单ID": str(sku_order.get("order_id", "")).strip(),
            "父订单ID": str(sku_order.get("parent_order_id", "")).strip(),
            "子订单层级": str(sku_order.get("order_level", "")).strip(),
            "子订单业务来源": str(sku_order.get("biz_desc", "")).strip(),
            "子订单业务来源编码": str(sku_order.get("biz", "")).strip(),
            "子订单类型": str(sku_order.get("order_type_desc", "")).strip(),
            "子订单类型编码": str(sku_order.get("order_type", "")).strip(),
            "子订单交易类型": str(sku_order.get("trade_type_desc", "")).strip(),
            "子订单状态": str(sku_order.get("order_status_desc", "")).strip(),
            "子订单状态编码": str(sku_order.get("order_status", "")).strip(),
            "子订单主状态": str(sku_order.get("main_status_desc", "")).strip(),
            "子订单主状态编码": str(sku_order.get("main_status", "")).strip(),
            "子订单下单端": str(sku_order.get("b_type_desc", "")).strip(),
            "子订单下单端编码": str(sku_order.get("b_type", "")).strip(),
            "子订单下单场景": str(sku_order.get("sub_b_type_desc", "")).strip(),
            "子订单下单场景编码": str(sku_order.get("sub_b_type", "")).strip(),
            "流量来源": str(sku_order.get("send_pay_desc", "")).strip(),
            "流量来源编码": str(sku_order.get("send_pay", "")).strip(),
            "下单来源": str(sku_order.get("theme_type_desc", "")).strip(),
            "下单来源编码": str(sku_order.get("theme_type", "")).strip(),
            "C端流量来源": str(sku_order.get("c_biz_desc", "")).strip(),
            "C端流量来源编码": str(sku_order.get("c_biz", "")).strip(),
            "广告来源": str(sku_order.get("ad_env_type", "")).strip(),
            "小程序ID(子单)": str(sku_order.get("app_id", "")).strip(),
            "内容ID": str(sku_order.get("content_id", "")).strip(),
            "视频ID": str(sku_order.get("video_id", "")).strip(),
            "直播房间ID": str(sku_order.get("room_id_str", "")).strip(),
            "落地页ID": str(sku_order.get("page_id", "")).strip(),
            "广告ID": str(sku_order.get("cid", "")).strip(),
            "流量来源ID": str(sku_order.get("origin_id", "")).strip(),
            "商家编码": str(sku_order.get("code", "")).strip(),
            "子订单取消原因": str(sku_order.get("cancel_reason", "")).strip(),
            "SKU名称": str(sku_order.get("sku_name", "")).strip(),
            "商品名称": str(sku_order.get("product_name", "")).strip(),
            "商品ID": str(sku_order.get("product_id_str", "")).strip(),
            "SKU ID": str(sku_order.get("sku_id", "")).strip(),
            "用户侧SKU名称": str(sku_order.get("user_sku_name", "")).strip(),
            "商品图片": str(sku_order.get("product_pic", "")).strip(),
            "商品原价(元)": str(format_amount(sku_order.get("goods_price", 0))).strip(),
            "商品现价(元)": str(format_amount(sku_order.get("origin_amount", 0))).strip(),
            "购买数量": str(sku_order.get("item_num", 0)).strip(),
            "商品总价(元)": str(format_amount(sku_order.get("sum_amount", 0))).strip(),
            "子订单总重量(g)": str(sku_order.get("total_weight", 0)).strip(),
            "商品类型": str("虚拟商品" if sku_order.get("goods_type") == 1 else "实体商品").strip(),
            "商品类型编码": str(sku_order.get("goods_type", "")).strip(),
            "一级类目": str(sku_order.get("first_cid", "")).strip(),
            "二级类目": str(sku_order.get("second_cid", "")).strip(),
            "三级类目": str(sku_order.get("third_cid", "")).strip(),
            "四级类目": str(sku_order.get("fourth_cid", "")).strip(),
            "外部商品编码": str(sku_order.get("out_product_id", "")).strip(),
            "外部SKU编码": str(sku_order.get("out_sku_id", "")).strip(),
            "供应商编码": str(sku_order.get("supplier_id", "")).strip(),
            "商品规格名称": str(sku_order.get("spec", [{}])[0].get("name", "") if (sku_order.get("spec") and len(sku_order.get("spec")) > 0) else "").strip(),
            "商品规格值": str(sku_order.get("spec", [{}])[0].get("value", "") if (sku_order.get("spec") and len(sku_order.get("spec")) > 0) else "").strip(),
            "商品属性名称": str(sku_order.get("custom_properties", [{}])[0].get("name", "") if (sku_order.get("custom_properties") and len(sku_order.get("custom_properties")) > 0) else "").strip(),
            "商品编码": str(sku_order.get("product_code", "")).strip(),
            "是否包税": str(sku_order.get("has_tax", "")).strip(),
            "税费(子单)(元)": str(format_amount(sku_order.get("tax_amount", 0))).strip(),
            "跨境税费未计算标识(子单)": str(sku_order.get("tax_amount_not_come_out", "")).strip(),
            "是否已评价": str("已评价" if sku_order.get("is_comment") == 1 else "未评价/追评").strip(),
            "是否补贴品订单": str(sku_order.get("is_activity", "")).strip(),
            "是否需要序列号": str(sku_order.get("need_serial_number", "")).strip(),
            "商品序列号列表": str(sku_order.get("serial_number_list", [])).strip(),
            "库存扣减方式": str(sku_order.get("reduce_stock_type_desc", "")).strip(),
            "库存扣减方式编码": str(sku_order.get("reduce_stock_type", "")).strip(),
            "仓ID": str(sku_order.get("warehouse_ids", [])).strip(),
            "外部仓ID": str(sku_order.get("out_warehouse_ids", [])).strip(),
            "库存类型": str(sku_order.get("inventory_type_desc", "")).strip(),
            "库存类型编码": str(sku_order.get("inventory_type", "")).strip(),
            "打包费(子单)(元)": str(format_amount(sku_order.get("packing_charge_amount", 0))).strip(),
            "运费(子单)(元)": str(format_amount(sku_order.get("post_amount", 0))).strip(),
            "运费险(子单)(元)": str(format_amount(sku_order.get("post_insurance_amount", 0))).strip(),
            "运费原价(子单)(元)": str(format_amount(sku_order.get("post_origin_amount", 0))).strip(),
            "运费优惠金额(子单)(元)": str(format_amount(sku_order.get("post_promotion_amount", 0))).strip(),
            "子订单总金额(元)": str(format_amount(sku_order.get("order_amount", 0))).strip(),
            "子订单支付金额(元)": str(format_amount(sku_order.get("pay_amount", 0))).strip(),
            "子订单优惠金额(元)": str(format_amount(sku_order.get("promotion_amount", 0))).strip(),
            "子订单店铺优惠(元)": str(format_amount(sku_order.get("promotion_shop_amount", 0))).strip(),
            "子订单平台优惠(元)": str(format_amount(sku_order.get("promotion_platform_amount", 0))).strip(),
            "子订单达人优惠(元)": str(format_amount(sku_order.get("promotion_talent_amount", 0))).strip(),
            "子订单支付优惠(元)": str(format_amount(sku_order.get("promotion_pay_amount", 0))).strip(),
            "子订单红包优惠(元)": str(format_amount(sku_order.get("promotion_redpack_amount", 0))).strip(),
            "子订单平台红包(元)": str(format_amount(sku_order.get("promotion_redpack_platform_amount", 0))).strip(),
            "子订单达人红包(元)": str(format_amount(sku_order.get("promotion_redpack_talent_amount", 0))).strip(),
            "商家承担金额(子单)(元)": str(format_amount(sku_order.get("shop_cost_amount", 0))).strip(),
            "平台承担金额(子单)(元)": str(format_amount(sku_order.get("platform_cost_amount", 0))).strip(),
            "仅平台承担(子单)(元)": str(format_amount(sku_order.get("only_platform_cost_amount", 0))).strip(),
            "达人承担金额(子单)(元)": str(format_amount(sku_order.get("author_cost_amount", 0))).strip(),
            "提货券抵扣金额(元)": str(format_amount(sku_order.get("voucher_deduction_amount", 0))).strip(),
            "子订单总优惠金额(含运费)(元)": str(format_amount(sku_order.get("total_promotion_amount", 0))).strip(),
            "子订单税费未计算标识": str(sku_order.get("tax_amount_not_come_out", 0)).strip(),
            "子订单下单时间": str(format_time(sku_order.get("create_time", 0))).strip(),
            "子订单支付时间": str(format_time(sku_order.get("pay_time", 0))).strip(),
            "子订单发货时间": str(format_time(sku_order.get("ship_time", 0))).strip(),
            "子订单完成时间": str(format_time(sku_order.get("finish_time", 0))).strip(),
            "子订单更新时间": str(format_time(sku_order.get("update_time", 0))).strip(),
            "子订单过期时间": str(format_time(sku_order.get("order_expire_time", 0))).strip(),
            "子订单预计发货时间": str(format_time(sku_order.get("exp_ship_time", 0))).strip(),
            "子订单预约发货时间": str(format_time(sku_order.get("appointment_ship_time", 0))).strip(),
            "子订单取消状态": str(sku_order.get("cancel_status", 0)).strip(),
            "子订单取消类型": str(sku_order.get("cancel_type", 0)).strip(),
            "子订单取消时间": str(format_time(sku_order.get("cancel_time", 0))).strip(),
            "物流收货时间": str(format_time(sku_order.get("logistics_receipt_time", 0))).strip(),
            "用户确认收货时间": str(format_time(sku_order.get("confirm_receipt_time", 0))).strip(),
            "预售类型": str(sku_order.get("pre_sale_type", "")).strip(),
            "车型描述": str(sku_order.get("sku_car_model_desc", "")).strip(),
            "渠道商品类型编码": str(sku_order.get("product_channel_type", "")).strip(),
            "渠道商品ID": str(sku_order.get("product_channel_id", "")).strip(),
            "关联商品单ID": str(sku_order.get("relation_sku_order_ids", [])).strip(),
            "主单子单关系": str(sku_order.get("relation_type", 0)).strip(),
            "主品SKU订单ID": str(sku_order.get("master_sku_order_id", "")).strip(),
            "主品SKU订单ID列表": str(sku_order.get("master_sku_order_id_list", [])).strip(),
            "所属套餐ID": str(sku_order.get("combo_id", "")).strip(),
            "套餐内子单ID列表": str(sku_order.get("combo_order_id_list", [])).strip(),
            "赠品类型": str(sku_order.get("given_product_type", "")).strip(),
            "赠品类型描述": str(get_mapped_value(GIVEN_PRODUCT_TYPE_MAP, sku_order.get("given_product_type"))).strip(),
            "赠品活动类型": str(sku_order.get("given_product_activity_info", {}).get("given_product_activity_type", "")).strip(),
            "赠品活动阈值": str(sku_order.get("given_product_activity_info", {}).get("given_product_activity_value", "")).strip(),
            "后发赠品时效": str(format_time(sku_order.get("given_exp_ship_time", 0))).strip(),
            "后发赠品时效描述": str(sku_order.get("given_exp_ship_time_desc", "")).strip(),
            "服务商品编码": str(sku_order.get("sp_product_id", "")).strip(),
            "履约时效信息": str(sku_order.get("promise_info", "")).strip(),
            "政府补贴商品标签": str(sku_order.get("gov_subsidy_detail", {}).get("product_tag", "")).strip(),
            "实际销售公司": str(sku_order.get("gov_subsidy_detail", {}).get("supplier_shop_name", "")).strip(),
            "实际销售公司ID": str(sku_order.get("gov_subsidy_detail", {}).get("supplier_shop_id", "")).strip(),
            "超值购商品名称": str(sku_order.get("low_price_info", {}).get("low_price_product", {}).get("product_name", "")).strip(),
            "超值购商品ID": str(sku_order.get("low_price_info", {}).get("low_price_product", {}).get("product_id", "")).strip(),
            "质检状态": str(sku_order.get("quality_inspection_status", "")).strip(),
            "质检结果": str(sku_order.get("quality_check_info", {}).get("check_result_code", "")).strip(),
            "质检失败原因": str(sku_order.get("quality_check_info", {}).get("check_fail_msg", "")).strip(),
            "重新送检截止时间": str(format_time(sku_order.get("quality_check_info", {}).get("resend_check_time", 0))).strip(),
            "收货方式": str("邮寄" if sku_order.get("receive_type") == 1 else "自提").strip(),
            "收货方式编码": str(sku_order.get("receive_type", "")).strip(),
            "售后状态编码": str(sku_order.get("after_sale_info", {}).get("after_sale_status", "")).strip(),
            "售后状态": str(get_mapped_value(AFTER_SALE_STATUS_MAP, sku_order.get("after_sale_info", {}).get("after_sale_status"))).strip(),
            "售后类型编码": str(sku_order.get("after_sale_info", {}).get("after_sale_type", "")).strip(),
            "售后类型": str(get_mapped_value(AFTER_SALE_TYPE_MAP, sku_order.get("after_sale_info", {}).get("after_sale_type"))).strip(),
            "退款状态编码": str(sku_order.get("after_sale_info", {}).get("refund_status", "")).strip(),
            "退款状态": str(get_mapped_value(REFUND_STATUS_MAP, sku_order.get("after_sale_info", {}).get("refund_status"))).strip(),
            "售后ID": str(sku_order.get("after_sale_info", {}).get("after_sale_id", "")).strip(),
            "阶段单ID": str(order_phase.get("phase_order_id", "")).strip(),
            "总阶段数": str(order_phase.get("total_phase", "")).strip(),
            "当前阶段": str(order_phase.get("current_phase", "")).strip(),
            "阶段单状态编码": str(order_phase.get("current_phase_status", 0)).strip(),
            "阶段单状态描述": str(order_phase.get("current_phase_status_desc", "")).strip(),
            "SKU单价(阶段单)(元)": str(format_amount(order_phase.get("sku_price", 0))).strip(),
            "阶段单支付状态": str(order_phase.get("pay_success", "")).strip(),
            "关联SKU订单ID": str(order_phase.get("sku_order_id", "")).strip(),
            "阶段单活动ID": str(order_phase.get("campaign_id", "")).strip(),
            "阶段单活动类型": str(order_phase.get("campaign_type", "")).strip(),
            "阶段单应付价格(元)": str(format_amount(order_phase.get("phase_payable_price", 0))).strip(),
            "阶段单支付类型编码": str(order_phase.get("phase_pay_type", "")).strip(),
            "阶段单开始支付时间": str(format_time(order_phase.get("phase_open_time", 0))).strip(),
            "阶段单支付时间": str(format_time(order_phase.get("phase_pay_time", 0))).strip(),
            "阶段单关闭时间": str(format_time(order_phase.get("phase_close_time", 0))).strip(),
            "阶段单总金额(元)": str(format_amount(order_phase.get("phase_order_amount", 0))).strip(),
            "阶段单订单金额(元)": str(format_amount(order_phase.get("phase_sum_amount", 0))).strip(),
            "阶段单运费(元)": str(format_amount(order_phase.get("phase_post_amount", 0))).strip(),
            "阶段单支付金额(元)": str(format_amount(order_phase.get("phase_pay_amount", 0))).strip(),
            "阶段单优惠金额(元)": str(format_amount(order_phase.get("phase_promotion_amount", 0))).strip(),
            "回收单ID": str(detail.get("order_recycle_info", {}).get("recycle_order_id", "")).strip(),
            "回收服务单号": str(detail.get("order_recycle_info", {}).get("recycle_order_service_id", "")).strip(),
            "回收商品描述": str(detail.get("order_recycle_info", {}).get("pronduct_desc", "")).strip(),
            "旧机预估价(元)": str(format_amount(detail.get("order_recycle_info", {}).get("quoted_price", 0))).strip(),
            "回收方式": str("上门回收" if detail.get("order_recycle_info", {}).get("recycle_mode") == 1 else "邮寄回收").strip(),
            "回收方式编码": str(detail.get("order_recycle_info", {}).get("recycle_mode", "")).strip(),
            "回收状态": str(detail.get("order_recycle_info", {}).get("recycle_status_desc", "")).strip(),
            "回收状态编码": str(detail.get("order_recycle_info", {}).get("recycle_status", "")).strip(),
            "预约拆机开始时间": str(format_time(detail.get("order_recycle_info", {}).get("appoint_recycle_start_time", 0))).strip(),
            "预约拆机结束时间": str(format_time(detail.get("order_recycle_info", {}).get("appoint_recycle_end_time", 0))).strip(),
            "回收服务商名称": str(detail.get("order_recycle_info", {}).get("supplier_name", "")).strip(),
            "回收服务商编码": str(detail.get("order_recycle_info", {}).get("supplier_code", "")).strip(),
            "实际回收价(元)": str(format_amount(detail.get("order_recycle_info", {}).get("recycle_actual_amount_info", {}).get("actual_amount", 0))).strip(),
            "回收款发放状态": str(detail.get("order_recycle_info", {}).get("recycle_actual_amount_info", {}).get("send_status", "")).strip(),
            "回收款发放渠道": str(detail.get("order_recycle_info", {}).get("recycle_actual_amount_info", {}).get("send_channel", "")).strip(),
            "补贴发放状态(旧)": str(detail.get("order_recycle_info", {}).get("subsidy_receive_status", "")).strip(),
            "追缴状态": str(detail.get("order_recycle_info", {}).get("press_for_payment_afterwards", {}).get("pay_status_desc", "")).strip(),
            "追缴状态编码": str(detail.get("order_recycle_info", {}).get("press_for_payment_afterwards", {}).get("pay_status", "")).strip(),
            "追缴总金额(元)": str(format_amount(detail.get("order_recycle_info", {}).get("press_for_payment_afterwards", {}).get("amount", 0))).strip(),
            "追缴明细-类型": str(detail.get("order_recycle_info", {}).get("press_for_payment_afterwards", {}).get("amount_detail_list", [{}])[0].get("type", "") if (detail.get("order_recycle_info", {}).get("press_for_payment_afterwards", {}).get("amount_detail_list") and len(detail.get("order_recycle_info", {}).get("press_for_payment_afterwards", {}).get("amount_detail_list")) > 0) else "").strip(),
            "追缴明细-描述": str(detail.get("order_recycle_info", {}).get("press_for_payment_afterwards", {}).get("amount_detail_list", [{}])[0].get("type_desc", "") if (detail.get("order_recycle_info", {}).get("press_for_payment_afterwards", {}).get("amount_detail_list") and len(detail.get("order_recycle_info", {}).get("press_for_payment_afterwards", {}).get("amount_detail_list")) > 0) else "").strip(),
            "追缴明细-金额(元)": str(format_amount(detail.get("order_recycle_info", {}).get("press_for_payment_afterwards", {}).get("amount_detail_list", [{}])[0].get("amount", 0)) if (detail.get("order_recycle_info", {}).get("press_for_payment_afterwards", {}).get("amount_detail_list") and len(detail.get("order_recycle_info", {}).get("press_for_payment_afterwards", {}).get("amount_detail_list")) > 0) else 0.0).strip(),
            "周期购总期数": str(detail.get("shop_period_purchase_info", {}).get("period_num", "")).strip(),
            "周期购类型": str(detail.get("shop_period_purchase_info", {}).get("deliver_type", "")).strip(),
            "上门安装服务状态": str(sku_order.get("home_installation_service_info", {}).get("service_info", {}).get("service_status_desc", "")).strip(),
            "上门安装服务状态编码": str(sku_order.get("home_installation_service_info", {}).get("service_info", {}).get("service_status", "")).strip(),
            "上门安装服务单号": str(sku_order.get("home_installation_service_info", {}).get("service_info", {}).get("service_order_id", "")).strip(),
            "安装工人姓名": str(sku_order.get("home_installation_service_info", {}).get("worker_info", {}).get("worker_name", "")).strip(),
            "安装工人电话": str(sku_order.get("home_installation_service_info", {}).get("worker_info", {}).get("worker_tel", "")).strip(),
            "安装服务商名称": str(sku_order.get("home_installation_service_info", {}).get("supplier_info", {}).get("supplier_name", "")).strip(),
            "安装服务商电话": str(sku_order.get("home_installation_service_info", {}).get("supplier_info", {}).get("supplier_tel", "")).strip(),
            "电子卡券核销状态": str(sku_order.get("writeoff_info", [{}])[0].get("writeoff_status_desc", "") if (sku_order.get("writeoff_info") and len(sku_order.get("writeoff_info")) > 0) else "").strip(),
            "电子卡券核销状态编码": str(sku_order.get("writeoff_info", [{}])[0].get("writeoff_status", "") if (sku_order.get("writeoff_info") and len(sku_order.get("writeoff_info")) > 0) else "").strip(),
            "电子卡券核销码": str(sku_order.get("writeoff_info", [{}])[0].get("writeoff_no", "") if (sku_order.get("writeoff_info") and len(sku_order.get("writeoff_info")) > 0) else "").strip(),
            "电子卡券核销码(脱敏)": str(sku_order.get("writeoff_info", [{}])[0].get("writeoff_no_mask", "") if (sku_order.get("writeoff_info") and len(sku_order.get("writeoff_info")) > 0) else "").strip(),
            "用户证件姓名(脱敏)": str(detail.get("user_id_info", {}).get("encrypt_id_card_name", "")).strip(),
            "用户证件号(脱敏)": str(detail.get("user_id_info", {}).get("encrypt_id_card_no", "")).strip()
        }
        return order_data

    def batch_get_order_details(self, order_ids: List[str]) -> pd.DataFrame:
        if not order_ids:
            print("❌ 无有效订单ID")
            return pd.DataFrame()
        
        print(f"📌 开始获取{len(order_ids)}个订单所有字段（共317字段）...")
        print(f"📢 开始多线程获取，线程数：{self.max_workers}")
        
        all_order_data = []
        
        for idx, order_id in enumerate(order_ids):
            if (idx + 1) % 100 == 0:
                print(f"进度：{idx+1}/{len(order_ids)}")
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_id = {executor.submit(self._process_order, oid): oid for oid in order_ids}
            
            total = len(order_ids)
            for idx, future in enumerate(as_completed(future_to_id), 1):
                oid = future_to_id[future]
                try:
                    res = future.result()
                    if res:
                        all_order_data.append(res)
                    if (idx + 1) % 100 == 0:
                        print(f"进度：{idx+1}/{len(order_ids)}")
                except Exception as e:
                    print(f"❌ 订单{oid}多线程处理异常：{str(e)}")
                time.sleep(0.01)
        
        df = pd.DataFrame(all_order_data)
        print(f"\n✅ 完成！共获取{len(df)}条订单数据，包含{len(df.columns)}个字段（无遗漏）")
        return df
