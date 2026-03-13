import time
import pandas as pd
from datetime import datetime
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

from .client import WeChatAPIClient
from .config import (
    AFTERSALE_STATUS_MAP, REFUND_REASON_MAP, AFTERSALE_TYPE_MAP,
    AFTERSALE_SUB_TYPE_MAP, AFTERSALE_REASON_MAP, MODULE_CONFIG
)
from .utils import format_time, format_amount, get_mapped_value, safe_get


thread_local = threading.local()
result_lock = threading.Lock()


def get_session():
    if not hasattr(thread_local, "session"):
        import requests
        thread_local.session = requests.Session()
        thread_local.session.headers.update({"Content-Type": "application/json"})
    return thread_local.session


class AftersaleService:
    def __init__(self, client: WeChatAPIClient, max_workers: int = None):
        self.client = client
        config = MODULE_CONFIG["aftersale"]
        self.max_workers = max_workers or config["max_workers"]
        self.max_retries = config["max_retries"]
        self.base_delay = config["base_delay"]

    def get_aftersale_list(self, begin_time: int, end_time: int) -> List[str]:
        print("\n开始获取售后列表...")
        aftersale_ids = []
        next_key = None
        
        while True:
            data = {
                "begin_create_time": begin_time,
                "end_create_time": end_time
            }
            
            if next_key:
                data["next_key"] = next_key
            
            result = self.client.request_with_path("channels/ec/aftersale/getaftersalelist", data)
            
            if not result:
                break
            
            batch_ids = result.get("after_sale_order_id_list", [])
            aftersale_ids.extend(batch_ids)
            
            print(f"获取到售后单ID数量：{len(batch_ids)}，累计：{len(aftersale_ids)}")
            
            has_more = result.get("has_more", False)
            if not has_more:
                break
            
            next_key = result.get("next_key")
            if not next_key:
                break
            
            time.sleep(0.1)
        
        print(f"售后列表获取完成，共{len(aftersale_ids)}个售后ID")
        return aftersale_ids

    def get_aftersale_detail(self, aftersale_id: str, access_token: str = None) -> Optional[Dict[str, Any]]:
        if not aftersale_id:
            return None
        
        def ts_to_str(timestamp: int) -> str:
            return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S") if timestamp else ""
        
        def cents_to_yuan(cents: int) -> float:
            return round(cents / 100, 2) if cents and isinstance(cents, (int, float)) else 0.0
        
        def get_enum_desc(enum_map, value, default="未知"):
            return enum_map.get(value, f"{default}({value})" if value is not None else default)
        
        try:
            session = get_session()
            url = f"https://api.weixin.qq.com/channels/ec/aftersale/getaftersaleorder?access_token={self.client.access_token}"
            data = {"after_sale_order_id": aftersale_id}
            
            response = session.post(url, json=data, timeout=15)
            result = response.json()
            
            if result.get("errcode") != 0:
                print(f"售后单{aftersale_id}获取失败：{result.get('errmsg')}")
                return None
            
            aftersale = result.get("after_sale_order", {})
            if not aftersale:
                print(f"售后单{aftersale_id}无返回数据")
                return None
            
            product_info = aftersale.get("product_info", {})
            refund_info = aftersale.get("refund_info", {})
            return_info = aftersale.get("return_info", {})
            merchant_upload_info = aftersale.get("merchant_upload_info", {})
            refund_resp = aftersale.get("refund_resp", {})
            exchange_product_info = aftersale.get("exchange_product_info", {})
            exchange_delivery_info = aftersale.get("exchange_delivery_info", {})
            virtual_tel_num_info = aftersale.get("virtual_tel_num_info", {})
            details = aftersale.get("details", {})
            gift_product_list = product_info.get("gift_product_list", [])
            exchange_address_info = exchange_delivery_info.get("address_info", {})
            return_address_info = return_info.get("address_info", {})
            
            gift_details = []
            for gift in gift_product_list:
                gift_details.append({
                    "赠品商品SPUID": str(gift.get("product_id", "")).strip(),
                    "赠品SKUID": str(gift.get("sku_id", "")).strip(),
                    "赠品数量": str(gift.get("count", 0))
                })
            
            detail_data = {
                "售后单ID": str(aftersale.get("after_sale_order_id", "")).strip(),
                "售后单状态": str(aftersale.get("status", "")).strip(),
                "售后单状态描述": str(get_enum_desc(AFTERSALE_STATUS_MAP, aftersale.get("status"))).strip(),
                "订单归属人OpenID": str(aftersale.get("openid", "")).strip(),
                "订单归属人UnionID": str(aftersale.get("unionid", "")).strip(),
                "礼物订单赠送者OpenID": str(aftersale.get("present_giver_openid", "")).strip(),
                "礼物订单赠送者UnionID": str(aftersale.get("present_giver_unionid", "")).strip(),
                "纠纷ID": str(aftersale.get("complaint_id", "")).strip(),
                "原订单号": str(aftersale.get("order_id", "")).strip(),
                "售后单创建时间": str(ts_to_str(aftersale.get("create_time"))).strip(),
                "售后单更新时间": str(ts_to_str(aftersale.get("update_time"))).strip(),
                "当前状态截止时间": str(ts_to_str(aftersale.get("deadline"))).strip(),
                "商责额外赔付金额(元)": str(cents_to_yuan(aftersale.get("compensation_liability_amount", 0))).strip(),
                "实际退款金额(分)": str(aftersale.get("refund_amount", 0)).strip(),
                "实际退款金额(元)": str(cents_to_yuan(aftersale.get("refund_amount", 0))).strip(),
                
                "售后类型": str(aftersale.get("type", "")).strip(),
                "售后类型描述": str(get_enum_desc(AFTERSALE_TYPE_MAP, aftersale.get("type"))).strip(),
                "售后子类型": str(aftersale.get("sub_type", "")).strip(),
                "售后子类型描述": str(get_enum_desc(AFTERSALE_SUB_TYPE_MAP, aftersale.get("sub_type"))).strip(),

                "商品SPUID": str(product_info.get("product_id", "")).strip(),
                "商品SKUID": str(product_info.get("sku_id", "")).strip(),
                "售后数量": str(product_info.get("count", 0)).strip(),
                "是否极速退款": str(product_info.get("fast_refund", False)).strip(),
                "赠品信息": str(gift_details).strip(),
                "SKU实际价格(分)": str(product_info.get("sku_real_price", 0)).strip(),
                "SKU实际价格(元)": str(cents_to_yuan(product_info.get("sku_real_price", 0))).strip(),
                "商品原始价格(分)": str(product_info.get("product_price", 0)).strip(),
                "商品原始价格(元)": str(cents_to_yuan(product_info.get("product_price", 0))).strip(),
                "SKU图片列表": str(";".join(product_info.get("sku_imgs", []))).strip(),
                "商家自定义商品ID": str(product_info.get("out_product_id", "")).strip(),
                "商家自定义SKUID": str(product_info.get("out_sku_id", "")).strip(),
                
                "退款金额(分)": str(refund_info.get("amount", 0)).strip(),
                "退款金额(元)": str(cents_to_yuan(refund_info.get("amount", 0))).strip(),
                "退款直接原因": str(refund_info.get("refund_reason", "")).strip(),
                "退款直接原因描述": str(get_enum_desc(REFUND_REASON_MAP, refund_info.get("refund_reason"))).strip(),
                "平台优惠退款金额(分)": str(refund_info.get("platform_discount_return_amount", 0)).strip(),
                "平台优惠退款金额(元)": str(cents_to_yuan(refund_info.get("platform_discount_return_amount", 0))).strip(),
                "退款原因编码": str(aftersale.get("reason", "")).strip(),
                "退款原因描述": str(get_enum_desc(AFTERSALE_REASON_MAP, aftersale.get("reason"))).strip(),
                "退款原因解释": str(aftersale.get("reason_text", "")).strip(),

                "快递单号": str(return_info.get("waybill_id", "")).strip(),
                "物流公司ID": str(return_info.get("delivery_id", "")).strip(),
                "物流公司名称": str(return_info.get("delivery_name", "")).strip(),
                "退货地址-收货人姓名": str(return_address_info.get("user_name", "")).strip(),
                "退货地址-联系方式": str(return_address_info.get("tel_number", "")).strip(),
                "退货地址-详细地址": str(return_address_info.get("detail_info", "")).strip(),

                "商家拒绝原因": str(merchant_upload_info.get("reject_reason", "")).strip(),
                "退款凭证数量": str(len(merchant_upload_info.get("refund_certificates", []))).strip(),
                "退款凭证列表": str(";".join(merchant_upload_info.get("refund_certificates", []))).strip(),

                "退款响应错误码": str(refund_resp.get("code", "")).strip(),
                "退款响应状态码": str(refund_resp.get("ret", "")).strip(),
                "退款响应描述": str(refund_resp.get("message", "")).strip(),
                
                "换货商品SPUID": str(exchange_product_info.get("product_id", "")).strip(),
                "原商品SKUID": str(exchange_product_info.get("old_sku_id", "")).strip(),
                "新商品SKUID": str(exchange_product_info.get("new_sku_id", "")).strip(),
                "换货数量": str(exchange_product_info.get("product_cnt", 0)).strip(),
                "原商品价格(元)": str(cents_to_yuan(exchange_product_info.get("old_sku_price", 0))).strip(),
                "新商品价格(元)": str(cents_to_yuan(exchange_product_info.get("new_sku_price", 0))).strip(),
                "换货快递单号": str(exchange_delivery_info.get("waybill_id", "")).strip(),
                "换货物流公司ID": str(exchange_delivery_info.get("delivery_id", "")).strip(),
                "换货物流公司名称": str(exchange_delivery_info.get("delivery_name", "")).strip(),

                "换货收货人姓名": str(exchange_address_info.get("user_name", "")).strip(),
                "换货邮编": str(exchange_address_info.get("postal_code", "")).strip(),
                "换货省份": str(exchange_address_info.get("province_name", "")).strip(),
                "换货城市": str(exchange_address_info.get("city_name", "")).strip(),
                "换货区县": str(exchange_address_info.get("county_name", "")).strip(),
                "换货详细地址": str(exchange_address_info.get("detail_info", "")).strip(),
                "换货国家码": str(exchange_address_info.get("national_code", "")).strip(),
                "换货联系方式": str(exchange_address_info.get("tel_number", "")).strip(),
                "换货门牌号码": str(exchange_address_info.get("house_number", "")).strip(),
                "换货虚拟商品联系方式": str(exchange_address_info.get("virtual_order_tel_number", "")).strip(),

                "虚拟号码": str(virtual_tel_num_info.get("virtual_tel_number", "")).strip(),
                "虚拟号码过期时间": str(ts_to_str(virtual_tel_num_info.get("virtual_tel_expire_time"))).strip(),
                
                "用户申请描述": str(details.get("desc", "")).strip(),
                "用户是否收到货": str(details.get("receive_product", False)).strip(),
                "用户是否收到货描述": str("收到货" if details.get("receive_product") else "未收到货").strip(),
                "取消时间": str(ts_to_str(details.get("cancel_time"))).strip(),
                "联系方式_售后": str(details.get("tel_number", "")).strip(),
                "媒体ID列表": str(";".join(details.get("media id list", []))).strip(),
                "详情中的售后类型": str(details.get("after_sale_type", 0)).strip(),
                
                "平台": "微信",
            }
            
            return detail_data
            
        except Exception as e:
            print(f"处理售后单{aftersale_id}异常：{str(e)}")
            return None

    def batch_get_aftersale_details(self, aftersale_ids: List[str]) -> pd.DataFrame:
        if not aftersale_ids:
            print("❌ 无有效售后ID")
            return pd.DataFrame()

        print(f"📌 开始获取{len(aftersale_ids)}个售后单详情...")
        all_aftersale_data = []
        total = len(aftersale_ids)

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_id = {
                executor.submit(self.get_aftersale_detail, aftersale_id): aftersale_id
                for aftersale_id in aftersale_ids
            }

            for idx, future in enumerate(as_completed(future_to_id), 1):
                aftersale_id = future_to_id[future]
                try:
                    result = future.result()
                    if result:
                        with result_lock:
                            all_aftersale_data.append(result)
                    if idx % 100 == 0 or idx == total:
                        print(f"进度：{idx}/{total}，已获取有效数据：{len(all_aftersale_data)}条")
                except Exception as e:
                    print(f"❌ 售后单{aftersale_id}处理异常：{str(e)}")

        df = pd.DataFrame(all_aftersale_data)
        print(f"\n✅ 完成！共获取{len(df)}条售后数据，包含{len(df.columns)}个字段")
        return df
