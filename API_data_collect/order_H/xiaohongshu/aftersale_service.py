import time
import random
import pandas as pd
from datetime import datetime
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from .client import XiaoHongShuAPIClient
from .config import (
    RETURN_TYPE_MAP, AFTER_SALE_STATUS_MAP_2, SUB_STATUS_MAP,
    RECEIVE_ABNORMAL_TYPE_MAP, SHIP_NEEDED_MAP, MODULE_CONFIG
)
from .utils import format_time, format_amount, get_mapped_value


class AftersaleService:
    def __init__(self, client: XiaoHongShuAPIClient, max_workers: int = None):
        self.client = client
        config = MODULE_CONFIG["aftersale"]
        self.max_workers = max_workers or config["max_workers"]
        self.max_retries = config["max_retries"]
        self.base_delay = config["base_delay"]
        self.random_delay_range = config["random_delay_range"]

    def get_aftersale_list(self, params: Dict[str, Any]) -> List[str]:
        print("\n开始获取售后列表...")
        after_sale_ids = []
        
        result = self.client.request("afterSale.listAfterSaleApi", params)
        
        if not result or not isinstance(result, dict):
            print("接口返回数据格式异常")
            return after_sale_ids
        
        after_sale_list = result.get("simpleAfterSaleList", [])
        
        for after_sale in after_sale_list:
            returns_id = after_sale.get("returnsId")
            if returns_id:
                after_sale_ids.append(returns_id)
        
        print(f"售后列表获取完成，共{len(after_sale_ids)}个售后ID")
        return after_sale_ids

    def get_aftersale_detail(self, after_sale_id: str) -> Optional[Dict[str, Any]]:
        if not after_sale_id:
            return None

        try:
            result = self.client.request("afterSale.getAfterSaleDetail", {"afterSaleId": after_sale_id})
            return result
        except Exception as e:
            print(f"❌ 售后单{after_sale_id}获取失败: {str(e)}")
            return None

    def _process_aftersale(self, after_sale_id: str) -> Optional[Dict[str, Any]]:
        detail = self.get_aftersale_detail(after_sale_id)
        if not detail:
            return None
        
        def format_ts(ts):
            try:
                return datetime.fromtimestamp(ts/1000).strftime("%Y-%m-%d %H:%M:%S") if (ts and ts != 0) else ""
            except:
                return ""
        
        skus = detail.get("skus", [])
        
        sku_names = []
        sku_codes = []
        sku_barcodes = []
        sku_prices = []
        sku_quantities = []
        total_amount = 0
        
        for sku in skus:
            sku_names.append(sku.get("skuName", ""))
            sku_codes.append(sku.get("scSkucode", ""))
            sku_barcodes.append(sku.get("barcode", ""))
            sku_prices.append(f"{sku.get('price', 0)/100:.2f}")
            sku_quantities.append(str(sku.get("appliedCount", 0)))
            total_amount += sku.get("price", 0) * sku.get("appliedCount", 0)
        
        aftersale_data = {
            "售后ID": detail.get("returnsId", ""),
            "订单ID": detail.get("orderId", ""),
            "换货订单ID": detail.get("exchangeOrderId", ""),
            "用户ID": detail.get("userId", ""),
            "创建时间": format_ts(detail.get("createdAt")),
            "更新时间": format_ts(detail.get("updatedAt")),
            
            "退货类型": RETURN_TYPE_MAP.get(detail.get("returnType"), f"未知({detail.get('returnType')})"),
            "售后状态": AFTER_SALE_STATUS_MAP_2.get(detail.get("status"), f"未知({detail.get('status')})"),
            "售后子状态": SUB_STATUS_MAP.get(detail.get("subStatus"), f"未知({detail.get('subStatus')})"),
            "收货异常类型": RECEIVE_ABNORMAL_TYPE_MAP.get(detail.get("receiveAbnormalType"), f"未知({detail.get('receiveAbnormalType')})"),
            
            "售后原因ID": detail.get("reasonId", ""),
            "售后原因说明": detail.get("reason", ""),
            "用户描述": detail.get("desc", ""),
            "商家备注": detail.get("note", ""),
            
            "退货快递单号": detail.get("returnExpressNo", ""),
            "退货快递公司": detail.get("returnExpressCompany", ""),
            "退货快递公司编号": detail.get("returnExpressCompanyCode", ""),
            "售后寄回地址": detail.get("returnAddress", ""),
            "是否需要寄回": SHIP_NEEDED_MAP.get(detail.get("shipNeeded"), f"未知({detail.get('shipNeeded')})"),
            "填写退货快递单时间": format_ts(detail.get("fillExpressTime")),
            "退货快递签收时间": format_ts(detail.get("expressSignTime")),
            "超时自动确认收货时间": format_ts(detail.get("autoReceiveDeadline")),
            
            "是否已退款": detail.get("refunded", False),
            "退款状态": detail.get("refundStatus", ""),
            "退款时间": format_ts(detail.get("refundTime")),
            "退款金额(元)": round(detail.get("refundFee", 0)/100, 2),
            "预期退款金额(元)": round(detail.get("expectedRefundAmount", 0)/100, 2),
            "退货运费是否可退": detail.get("returnExpressRefundable", False),
            "退货运费是否已退": detail.get("returnExpressRefunded", False),
            "是否急速退款": detail.get("useFastRefund", False),
            
            "商品名称": " | ".join(sku_names),
            "小红书编码": " | ".join(sku_codes),
            "商品条码": " | ".join(sku_barcodes),
            "商品单价(元)": " | ".join(sku_prices),
            "申请退货数量": " | ".join(sku_quantities),
            "商品总金额(元)": round(total_amount/100, 2),
            
            "主商品名称": skus[0].get("skuName", "") if skus else "",
            "主商品编码": skus[0].get("scSkucode", "") if skus else "",
            "主商品条码": skus[0].get("barcode", "") if skus else "",
            "主商品单价(元)": round(skus[0].get("price", 0)/100, 2) if skus else 0,
            "主商品申请数量": skus[0].get("appliedCount", 0) if skus else 0,
            "主商品实际收货数量": skus[0].get("returnedCount", 0) if skus else 0,
            "主商品退款数量": skus[0].get("refundedCount", 0) if skus else 0,
            
            "照片凭证数量": len(detail.get("proofPhotos", [])),
            "照片凭证链接": ";".join(detail.get("proofPhotos", [])),
            "收件信息ID": detail.get("openAddressId", ""),
            "订单创建时间": "",
            "订单支付时间": "",
            "拦截状态": "",
            "拦截发起时间": "",
            "拦截异常原因": "",
            "处理建议": "",
            
            "平台": "小红书",
        }
        
        return aftersale_data

    def batch_get_aftersale_details(self, aftersale_ids: List[str]) -> pd.DataFrame:
        if not aftersale_ids:
            print("❌ 无有效售后ID")
            return pd.DataFrame()

        print(f"📌 开始获取{len(aftersale_ids)}个售后单详情...")
        all_aftersale_data = []
        total = len(aftersale_ids)

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_id = {
                executor.submit(self._process_aftersale, after_sale_id): after_sale_id
                for after_sale_id in aftersale_ids
            }

            for idx, future in enumerate(as_completed(future_to_id), 1):
                after_sale_id = future_to_id[future]
                try:
                    result = future.result()
                    if result:
                        all_aftersale_data.append(result)
                    if idx % 100 == 0 or idx == total:
                        print(f"进度：{idx}/{total}，已获取有效数据：{len(all_aftersale_data)}条")
                except Exception as e:
                    print(f"❌ 售后单{after_sale_id}处理异常：{str(e)}")

        df = pd.DataFrame(all_aftersale_data)
        print(f"\n✅ 完成！共获取{len(df)}条售后数据，包含{len(df.columns)}个字段")
        return df
