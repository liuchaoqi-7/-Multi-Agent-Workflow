import time
import pandas as pd
from datetime import datetime
from typing import List, Dict, Any, Optional, Union

from .client import WeChatAPIClient
from .config import (
    ORDER_SETTLE_STATE_MAP, ORDER_STATE_MAP, ORDER_PAY_METHOD_MAP,
    SETTLE_STATE_MAP, MODULE_CONFIG
)
from .utils import format_time, format_amount, get_mapped_value, safe_get


class SettleService:
    def __init__(self, client: WeChatAPIClient, max_workers: int = None):
        self.client = client
        config = MODULE_CONFIG["settle"]
        self.max_workers = max_workers or config["max_workers"]
        self.max_retries = config["max_retries"]
        self.base_delay = config["base_delay"]
        self.page_size = config["page_size"]

    def get_settle_list(self, order_settle_state: int = 0) -> pd.DataFrame:
        print("📌 开始获取结算流水列表...")
        all_flows = []
        
        def format_ts(ts: Union[int, float, None]) -> str:
            if ts and isinstance(ts, (int, float)) and ts > 0:
                return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
            return ""
        
        def format_price(cents: Union[int, float, None]) -> float:
            if cents and isinstance(cents, (int, float)):
                return round(cents / 100, 2)
            return 0.0
        
        def get_enum_desc(enum_map: Dict[int, str], value: Union[int, None], default: str = "") -> str:
            if value is None:
                return default
            return enum_map.get(value, f"{default}({value})")
        
        payload = {
            "order_settle_state": order_settle_state,
            "pagination_info": {
                "use_page_ctx": True,
                "page_ctx": ""
            }
        }
        
        page_num = 1
        total_count = 0
        
        while True:
            print(f"正在获取第 {page_num} 页数据...")
            
            result = self.client.request_with_path("channels/ec/funds/listorderflow", payload)
            
            if not result:
                break
            
            data_list = result.get("data_list", [])
            current_page_count = len(data_list)
            total_count = result.get("total_count", 0)
            
            print(f"第 {page_num} 页获取到 {current_page_count} 条数据，累计 {len(all_flows) + current_page_count}/{total_count}")
            
            for flow in data_list:
                product_list = flow.get("product_list", [])
                products_info = []
                
                for product in product_list:
                    param_list = product.get("param_list", [])
                    sku_attrs = {}
                    for param in param_list:
                        sku_attrs[param.get("key", "")] = param.get("value", "")
                    
                    product_info = {
                        "商品ID": product.get("product_id", ""),
                        "商品名称": product.get("product_name", ""),
                        "销售价格(分)": product.get("sale_price", 0),
                        "销售价格(元)": format_price(product.get("sale_price", 0)),
                        "商品数量": product.get("count", 0),
                        "是否赠品": product.get("is_gift", False),
                        "商品规格": sku_attrs,
                        "商品规格字符串": "; ".join([f"{k}:{v}" for k, v in sku_attrs.items()])
                    }
                    products_info.append(product_info)
                
                post_settle = flow.get("post_settlement_expense", {})
                
                flow_record = {
                    "订单ID": str(flow.get("order_id", "")),
                    "订单状态": str(flow.get("order_state", "")),
                    "订单状态描述": str(get_enum_desc(ORDER_STATE_MAP, flow.get("order_state"))),
                    "结算状态": str(flow.get("order_settle_state", "")),
                    "结算状态描述": str(get_enum_desc(ORDER_SETTLE_STATE_MAP, flow.get("order_settle_state"))),
                    "订单创建时间": str(format_ts(flow.get("order_create_time"))),
                    "订单支付时间": str(format_ts(flow.get("order_paid_time"))),
                    "支付方式": str(flow.get("order_pay_method", "")),
                    "支付方式描述": str(get_enum_desc(ORDER_PAY_METHOD_MAP, flow.get("order_pay_method"))),
                    "订单类型": str(flow.get("order_type", "")),
                    "同城配送门店ID": str(flow.get("intra_city_shop_id", "")),
                    
                    "商户实收金额(分)": str(flow.get("mch_received_amount", 0)),
                    "支出金额(分)": str(flow.get("expense_amount", 0)),
                    "预计结算金额(分)": str(flow.get("mch_settle_amount", 0)),
                    "商品总金额(分)": str(flow.get("product_total_amount", 0)),
                    "运费金额(分)": str(flow.get("freight_amount", 0)),
                    "改价金额(分)": str(flow.get("change_down_price", 0)),
                    "商户优惠金额(分)": str(flow.get("mch_discount_amount", 0)),
                    "积分抵扣金额(分)": str(flow.get("score_discount_amount", 0)),
                    "用户实付金额(分)": str(flow.get("buyer_paid_amount", 0)),
                    "达人优惠金额(分)": str(flow.get("promoter_discount_amount", 0)),
                    "平台优惠金额(分)": str(flow.get("platform_discount_amount", 0)),
                    "国家补贴金额(分)": str(flow.get("national_subsidy_discount_amount", 0)),
                    "补交运费(分)": str(flow.get("freight_make_up_amount", 0)),
                    "跨店优惠金额(分)": str(flow.get("cross_shop_discount_amount", 0)),
                    "用户退款金额(分)": str(flow.get("buyer_refund_amount", 0)),
                    "平台优惠退款金额(分)": str(flow.get("platform_discount_refund_amount", 0)),
                    "达人优惠退款金额(分)": str(flow.get("promoter_discount_refund_amount", 0)),
                    "原技术服务费(分)": str(flow.get("original_platform_commission_amount", 0)),
                    "预计技术服务费(分)": str(flow.get("platform_commission_amount", 0)),
                    "运费险补贴减免技术服务费(分)": str(flow.get("freight_insurance_subsidy_amount", 0)),
                    "预计机构服务费(分)": str(flow.get("supplier_commission_amount", 0)),
                    "预计达人服务费(分)": str(flow.get("promoter_commission_amount", 0)),
                    "预计运费险金额(分)": str(flow.get("freight_insurance_amount", 0)),
                    "运费险补缴他单金额(分)": str(flow.get("freight_insurance_make_up_amount", 0)),
                    "预付运费退回金额(分)": str(flow.get("pre_freight_refund_amount", 0)),
                    "其他支出(分)": str(flow.get("other_expense_amount", 0)),
                    "结算前退款(分)": str(flow.get("refund_before_settlement", 0)),
                    
                    "商户实收金额(元)": str(format_price(flow.get("mch_received_amount", 0))),
                    "支出金额(元)": str(format_price(flow.get("expense_amount", 0))),
                    "预计结算金额(元)": str(format_price(flow.get("mch_settle_amount", 0))),
                    "商品总金额(元)": str(format_price(flow.get("product_total_amount", 0))),
                    "运费金额(元)": str(format_price(flow.get("freight_amount", 0))),
                    "改价金额(元)": str(format_price(flow.get("change_down_price", 0))),
                    "商户优惠金额(元)": str(format_price(flow.get("mch_discount_amount", 0))),
                    "积分抵扣金额(元)": str(format_price(flow.get("score_discount_amount", 0))),
                    "用户实付金额(元)": str(format_price(flow.get("buyer_paid_amount", 0))),
                    "达人优惠金额(元)": str(format_price(flow.get("promoter_discount_amount", 0))),
                    "平台优惠金额(元)": str(format_price(flow.get("platform_discount_amount", 0))),
                    "国家补贴金额(元)": str(format_price(flow.get("national_subsidy_discount_amount", 0))),
                    "补交运费(元)": str(format_price(flow.get("freight_make_up_amount", 0))),
                    "跨店优惠金额(元)": str(format_price(flow.get("cross_shop_discount_amount", 0))),
                    "用户退款金额(元)": str(format_price(flow.get("buyer_refund_amount", 0))),
                    "平台优惠退款金额(元)": str(format_price(flow.get("platform_discount_refund_amount", 0))),
                    "达人优惠退款金额(元)": str(format_price(flow.get("promoter_discount_refund_amount", 0))),
                    "原技术服务费(元)": str(format_price(flow.get("original_platform_commission_amount", 0))),
                    "预计技术服务费(元)": str(format_price(flow.get("platform_commission_amount", 0))),
                    "运费险补贴减免技术服务费(元)": str(format_price(flow.get("freight_insurance_subsidy_amount", 0))),
                    "预计机构服务费(元)": str(format_price(flow.get("supplier_commission_amount", 0))),
                    "预计达人服务费(元)": str(format_price(flow.get("promoter_commission_amount", 0))),
                    "预计运费险金额(元)": str(format_price(flow.get("freight_insurance_amount", 0))),
                    "运费险补缴他单金额(元)": str(format_price(flow.get("freight_insurance_make_up_amount", 0))),
                    "预付运费退回金额(元)": str(format_price(flow.get("pre_freight_refund_amount", 0))),
                    "其他支出(元)": str(format_price(flow.get("other_expense_amount", 0))),
                    "结算前退款(元)": str(format_price(flow.get("refund_before_settlement", 0))),
                    
                    "预计商家货款结算时间": str(format_ts(flow.get("mch_settle_time"))),
                    "预计技术服务费结算时间": str(format_ts(flow.get("platform_commission_settle_time"))),
                    "预计达人服务费结算时间": str(format_ts(flow.get("promoter_commission_settle_time"))),
                    "预计机构服务费结算时间": str(format_ts(flow.get("supplier_commission_settle_time"))),
                    "预计运费险结算时间": str(format_ts(flow.get("freight_insurance_settle_time"))),
                    "预计运费险补缴他单结算时间": str(format_ts(flow.get("freight_insurance_make_up_settle_time"))),
                    
                    "机构服务费结算状态": str(flow.get("supplier_commission_settle_state", "")),
                    "机构服务费结算状态描述": str(get_enum_desc(SETTLE_STATE_MAP, flow.get("supplier_commission_settle_state"))),
                    "达人服务费结算状态": str(flow.get("promoter_commission_settle_state", "")),
                    "达人服务费结算状态描述": str(get_enum_desc(SETTLE_STATE_MAP, flow.get("promoter_commission_settle_state"))),
                    "运费险结算状态": str(flow.get("freight_insurance_settle_state", "")),
                    "运费险结算状态描述": str(get_enum_desc(SETTLE_STATE_MAP, flow.get("freight_insurance_settle_state"))),
                    "平台服务费结算状态": str(flow.get("platform_commission_settle_state", "")),
                    "平台服务费结算状态描述": str(get_enum_desc(SETTLE_STATE_MAP, flow.get("platform_commission_settle_state"))),
                    "运费险补缴他单结算状态": str(flow.get("freight_insurance_make_up_settle_state", "")),
                    "运费险补缴他单结算状态描述": str(get_enum_desc(SETTLE_STATE_MAP, flow.get("freight_insurance_make_up_settle_state"))),
                    
                    "运费险补缴他单订单ID列表": str(",".join([str(id) for id in flow.get("freight_insurance_make_up_order_id_list", [])])),
                    
                    "结算后买家退款(分)": str(safe_get(post_settle, ["buyer_refund_amount"], 0)),
                    "结算后平台优惠退款(分)": str(safe_get(post_settle, ["platform_discount_refund_amount"], 0)),
                    "结算后达人退款(分)": str(safe_get(post_settle, ["promoter_refund_amount"], 0)),
                    "结算后运费险保费(分)": str(safe_get(post_settle, ["freight_insurance_make_up_amount"], 0)),
                    
                    "结算后买家退款(元)": str(format_price(safe_get(post_settle, ["buyer_refund_amount"], 0))),
                    "结算后平台优惠退款(元)": str(format_price(safe_get(post_settle, ["platform_discount_refund_amount"], 0))),
                    "结算后达人退款(元)": str(format_price(safe_get(post_settle, ["promoter_refund_amount"], 0))),
                    "结算后运费险保费(元)": str(format_price(safe_get(post_settle, ["freight_insurance_make_up_amount"], 0))),
                    
                    "运费险补缴本单结算状态": str(safe_get(post_settle, ["freight_insurance_make_up_settle_state"], "")),
                    "运费险补缴本单结算状态描述": str(get_enum_desc(SETTLE_STATE_MAP, safe_get(post_settle, ["freight_insurance_make_up_settle_state"]))),
                    "运费险补缴本单订单ID": str(safe_get(post_settle, ["freight_insurance_make_up_order_id"], "")),
                    
                    "商品数量": str(len(product_list)),
                    "商品名称列表": str(",".join([p.get("product_name", "") for p in product_list])),
                    "商品ID列表": str(",".join([str(p.get("product_id", "")) for p in product_list])),
                    "商品总数量": str(sum([p.get("count", 0) for p in product_list])),
                    
                    "平台": "微信",
                }
                all_flows.append(flow_record)
            
            page_ctx = result.get("page_ctx")
            if not page_ctx:
                break
            
            payload["pagination_info"]["page_ctx"] = page_ctx
            page_num += 1
            
            time.sleep(0.05)
        
        df = pd.DataFrame(all_flows)
        print(f"\n✅ 结算流水获取完成！共{len(df)}条记录，{len(df.columns)}个字段")
        return df
