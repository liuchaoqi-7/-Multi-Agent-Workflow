import time
import random
import pandas as pd
from datetime import datetime
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from .client import XiaoHongShuAPIClient
from .config import (
    ORDER_TYPE_MAP, ORDER_STATUS_MAP, AFTER_SALES_STATUS_MAP, SKU_TAG_MAP,
    ORDER_TAG_MAP, PAYMENT_TYPE_MAP, SELLER_REMARK_FLAG_MAP, MODULE_CONFIG
)
from .utils import format_time, format_amount, get_mapped_value


def convert_order_tags(tags_list):
    if not tags_list:
        return ""
    chinese_tags = []
    for tag in tags_list:
        tag_key = tag.split(":")[0] if ":" in tag else tag
        chinese_tag = ORDER_TAG_MAP.get(tag_key, tag)
        chinese_tags.append(chinese_tag)
    return "、".join(chinese_tags)


class OrderService:
    def __init__(self, client: XiaoHongShuAPIClient, max_workers: int = None):
        self.client = client
        config = MODULE_CONFIG["order"]
        self.max_workers = max_workers or config["max_workers"]
        self.max_retries = config["max_retries"]
        self.base_delay = config["base_delay"]
        self.random_delay_range = config["random_delay_range"]

    def get_order_list(self, params: Dict[str, Any]) -> List[str]:
        print("开始获取订单列表...")
        all_order_ids = []
        current_page = 1
        page_size = params.get('pageSize', 50)
        
        while True:
            current_params = params.copy()
            current_params['pageNo'] = current_page
            current_params['pageSize'] = page_size
            
            result = self.client.request("order.getOrderList", current_params)
            
            if not result:
                print(f"第{current_page}页：响应结果为空")
                break
            
            if not isinstance(result, dict):
                print(f"第{current_page}页：响应格式不是JSON")
                break
            
            if 'orderList' not in result:
                print(f"第{current_page}页：响应中缺少orderList字段")
                break
            
            max_page_no = result.get("maxPageNo", 1)
            total = result.get("total", 0)
            
            current_order_ids = [o.get("orderId") for o in result.get("orderList", []) if o and o.get("orderId")]
            all_order_ids.extend(current_order_ids)
            
            print(f"第{current_page}页：获取{len(current_order_ids)}/{total}个订单ID")
            
            if current_page >= max_page_no or not current_order_ids:
                break
                
            current_page += 1
            time.sleep(0.5)
            
        print(f"\n✅ 订单列表获取完成！共提取{len(all_order_ids)}个订单ID")
        return all_order_ids

    def get_order_detail(self, order_id: str) -> Optional[Dict[str, Any]]:
        if not order_id:
            return None

        try:
            result = self.client.request("order.getOrderDetail", {"orderId": order_id})
            return result
        except Exception as e:
            print(f"❌ 订单{order_id}获取失败: {str(e)}")
            return None

    def get_order_receiver_info(self, order_id: str, open_address_id: str) -> Optional[Dict[str, Any]]:
        if not order_id or not open_address_id:
            return None
        
        try:
            business_params = {
                "receiverQueries": [{
                    "orderId": order_id,
                    "openAddressId": open_address_id
                }],
                "isReturn": False
            }
            result = self.client.request("order.getOrderReceiverInfo", business_params)
            return result
        except Exception as e:
            print(f"❌ 订单{order_id}收件人信息获取失败: {str(e)}")
            return None

    def get_kol_settle_info(self, order_id: str) -> Optional[Dict[str, Any]]:
        if not order_id:
            return None
        
        try:
            result = self.client.request("bill.queryCpsSettle", {"orderId": order_id})
            return result
        except Exception as e:
            print(f"❌ 订单{order_id}达人结算信息获取失败: {str(e)}")
            return None

    def _process_order(self, order_id: str) -> Optional[Dict[str, Any]]:
        detail = self.get_order_detail(order_id)
        if not detail:
            return None
        
        def format_ts(ts):
            try:
                return datetime.fromtimestamp(int(ts)/1000).strftime("%Y-%m-%d %H:%M:%S") if (ts and ts != 0) else ""
            except:
                return ""
        
        def format_amt(amt):
            try:
                return "{0:.2f}".format(float(amt) / 100) if amt else "0.00"
            except:
                return "0.00"
        
        sku_list = detail.get("skuList", [])
        sku_info = sku_list[0] if sku_list else {}
        sku_detail_list = sku_info.get("skuDetailList", [])
        sku_detail = sku_detail_list[0] if sku_detail_list else {}
        
        simple_delivery_list = detail.get("simpleDeliveryOrderList", [])
        simple_delivery = simple_delivery_list[0] if simple_delivery_list else {}
        
        bound_extend_info = detail.get("boundExtendInfo", {})
        if isinstance(bound_extend_info, list) and bound_extend_info:
            bound_extend_info = bound_extend_info[0]
        bound_extend_info = bound_extend_info or {}
        
        transfer_extend_info = detail.get("transferExtendInfo", {}) or {}
        
        subsidy_info = detail.get("subsidySkuIdentifyCodeRequiredInfo", {}) or {}
        sku_identify_info = detail.get("skuIdentifyCodeInfo", {}) or {}
        
        order_data = {
            "订单ID": detail.get("orderId"),
            "原始关联订单号": detail.get("originalOrderId"),
            "订单类型": ORDER_TYPE_MAP.get(detail.get("orderType", 0), f"未知类型({detail.get('orderType')})"),
            "订单状态": ORDER_STATUS_MAP.get(detail.get("orderStatus", 0), f"未知状态({detail.get('orderStatus')})"),
            "售后状态": AFTER_SALES_STATUS_MAP.get(detail.get("orderAfterSalesStatus", 0), f"未知售后状态({detail.get('orderAfterSalesStatus')})"),
            "申请取消状态": "未申请取消" if detail.get("cancelStatus") == 0 else "取消处理中",
            "订单标记": convert_order_tags(detail.get("orderTagList", [])),
            "订单平台": "小红书",
            "是否拆包": detail.get("unpack", False),
            
            "收件人姓名": "",
            "收件人电话": "",
            "收件人地址": "",
            "省": detail.get("receiverProvinceName"),
            "市": detail.get("receiverCityName"),
            "区": detail.get("receiverDistrictName"),
            "镇/街道": "",
            "收件人国家": detail.get("receiverCountryName"),
            "收件人国家ID": detail.get("receiverCountryId"),
            "省ID": detail.get("receiverProvinceId"),
            "市ID": detail.get("receiverCityId"),
            "区ID": detail.get("receiverDistrictId"),
            "区域编码": (detail.get("receiverProvinceId") or "") + (detail.get("receiverCityId") or "") + (detail.get("receiverDistrictId") or ""),
            "openAddressId": detail.get("openAddressId"),
            
            "商品ID": sku_info.get("itemId", ""),
            "商品名称": sku_info.get("itemName", ""),
            
            "主SKU ID": sku_info.get("skuId", ""),
            "主SKU名称": sku_info.get("skuName", ""),
            "主SKU商家编码": sku_info.get("erpcode", ""),
            "主SKU规格": sku_info.get("skuSpec", ""),
            "主SKU图片URL": sku_info.get("skuImage", ""),
            "主SKU数量": sku_info.get("skuQuantity", ""),
            
            "单品SKU ID": sku_detail.get("skuId", ""),
            "单品商家编码": sku_detail.get("erpCode", ""),
            "单品条码": sku_detail.get("barcode", ""),
            "小红书编码": sku_detail.get("scSkuCode", ""),
            "单品购买数量": sku_detail.get("quantity", ""),
            "商品备案名称": sku_detail.get("registerName", ""),
            "单品名称": sku_detail.get("skuName", ""),
            "是否赠品": SKU_TAG_MAP.get(sku_detail.get("skuTag", 0), f"未知({sku_detail.get('skuTag')})"),
            "是否渠道商品": sku_detail.get("isChannel", False),
            "是否支持无物流发货": "支持" if sku_detail.get("deliveryMode") == 1 else "不支持",
            "SKU售后状态": AFTER_SALES_STATUS_MAP.get(sku_detail.get("skuAfterSaleStatus", 0), sku_detail.get("skuAfterSaleStatus")),
            
            "达人ID": sku_info.get("kolId", ""),
            "达人名称": sku_info.get("kolName", ""),
            
            "单个SKU价格(元)": format_amt(sku_detail.get("pricePerSku", 0)),
            "单个SKU税金(元)": format_amt(sku_detail.get("taxPerSku", 0)),
            "单个SKU实付(元)": format_amt(sku_detail.get("paidAmountPerSku", 0)),
            "单个SKU定金(元)": format_amt(sku_detail.get("depositAmountPerSku", 0)),
            "单个SKU商家优惠(元)": format_amt(sku_detail.get("merchantDiscountPerSku", 0)),
            "单个SKU平台优惠(元)": format_amt(sku_detail.get("redDiscountPerSku", 0)),
            "单个SKU原价(元)": format_amt(sku_detail.get("rawPricePerSku", 0)),
            "商品总价(元)": format_amt(sku_info.get("totalPaidAmount", 0)),
            
            "订单实付金额(包含运费和定金)": format_amt(detail.get("totalPayAmount", 0)),
            "平台承担总优惠(元)": format_amt(detail.get("totalRedDiscount", 0)),
            "商家承担总优惠(元)": format_amt(detail.get("totalMerchantDiscount", 0)),
            "支付渠道优惠(元)": format_amt(detail.get("outPromotionAmount", 0)),
            "SKU商家改价(元)": format_amt(detail.get("totalChangePriceAmount", 0)),
            "商家应收金额(元)": format_amt(detail.get("merchantActualReceiveAmount", 0)),
            "订单定金(元)": format_amt(detail.get("totalDepositAmount", 0)),
            "包裹总运费(元)": format_amt(detail.get("totalShippingFree", 0)),
            "商品税金总计(元)": format_amt(detail.get("totalTaxAmount", 0)),
            
            "支付方式": PAYMENT_TYPE_MAP.get(detail.get("paymentType", -1), f"未知({detail.get('paymentType')})"),
            "三方支付渠道订单号": detail.get("outTradeNo"),
            "三方保税交易流水号": bound_extend_info.get("payNo", ""),
            "三方保税交易渠道": bound_extend_info.get("payChannel", ""),
            "三方保税节点订单支付金额（含运费）": bound_extend_info.get("payAmount", ""),
            
            "快递公司": detail.get("expressCompanyCode"),
            "快递单号": detail.get("expressTrackingNo"),
            "物流方案ID": detail.get("planInfoId"),
            "物流方案名称": detail.get("planInfoName"),
            "物流模式": detail.get("logistics"),
            "物流模式编码": detail.get("logisticsMode"),
            "口岸code": detail.get("customsCode"),
            "发货仓库": detail.get("whcode"),
            
            "拆包快递单号": simple_delivery.get("expressTrackingNo", ""),
            "拆包快递公司代码": simple_delivery.get("expressCompanyCode", ""),
            "拆包发货订单状态": ORDER_STATUS_MAP.get(simple_delivery.get("status", 0), simple_delivery.get("status")),
            "拆包发货订单索引": simple_delivery.get("deliveryOrderIndex", ""),
            "拆包SKU列表": ",".join(simple_delivery.get("skuIdList", [])),
            
            "国际快递单号": transfer_extend_info.get("internationalExpressNo"),
            "订单申报金额": transfer_extend_info.get("orderDeclaredAmount"),
            "大头笔": transfer_extend_info.get("paintMarker"),
            "集包地": transfer_extend_info.get("collectionPlace"),
            "三段码": transfer_extend_info.get("threeSegmentCode"),
            
            "订单创建时间": format_ts(detail.get("createdTime")),
            "订单支付时间": format_ts(detail.get("paidTime")),
            "订单更新时间": format_ts(detail.get("updateTime")),
            "订单发货时间": format_ts(detail.get("deliveryTime")),
            "订单取消时间": format_ts(detail.get("cancelTime")),
            "订单完成时间": format_ts(detail.get("finishTime")),
            "承诺最晚发货时间": format_ts(detail.get("promiseLastDeliveryTime")),
            "预售最早发货时间": format_ts(detail.get("presaleDeliveryStartTime")),
            "预售最晚发货时间": format_ts(detail.get("presaleDeliveryEndTime")),
            
            "商品总净重": detail.get("totalNetWeight"),
            "单品总净重": sku_info.get("totalNetWeight", ""),
            "包裹总净重": detail.get("totalNetWeightAmount"),
            
            "订单价值(元)": format_amt(bound_extend_info.get("productValue", 0)),
            "保税订单税金(元)": format_amt(bound_extend_info.get("taxAmount", 0)),
            "运费(含运费税)(元)": format_amt(bound_extend_info.get("shippingFee", 0)),
            "保税订单优惠(元)": format_amt(bound_extend_info.get("discountAmount", 0)),
            "海关三级地址区域编码": ",".join(bound_extend_info.get("zoneCodes", [])),
            
            "用户编号": detail.get("userId", ""),
            "用户备注": detail.get("customerRemark"),
            "订购人身份证姓名/ID Name": "",
            "订购人身份证号/ID Number": "",
            "收件人身份证姓名": "",
            "收件人身份证号": "",
            
            "店铺ID": detail.get("shopId"),
            "店铺名称": detail.get("shopName"),
            "包裹备注标记": SELLER_REMARK_FLAG_MAP.get(detail.get("sellerRemarkFlag"), f"未知({detail.get('sellerRemarkFlag')})"),
            "包裹备注信息": detail.get("sellerRemark"),
            
            "国补供应商ID": detail.get("subsidySupplierId"),
            "国补供应商名称": detail.get("subsidySupplierName"),
            "国补顺丰微派任务编码": detail.get("subsidyWpServiceCode"),
            "SN码是否必填": subsidy_info.get("snRequired"),
            "BarCode是否必填": subsidy_info.get("barCodeRequired"),
            "IMEI1是否必填": subsidy_info.get("imei1Required"),
            "IMEI2是否必填": subsidy_info.get("imei2Required"),
            
            "商品序列号(SN)": sku_identify_info.get("sNCode"),
            "商品条码": sku_identify_info.get("barCode"),
            "IMEI1": sku_identify_info.get("iMEI1Code"),
            "IMEI2": sku_identify_info.get("iMEI2Code"),
            
            "发货链接": simple_delivery.get("expressUrlProofList", {}).get("list", [""])[0] if isinstance(simple_delivery.get("expressUrlProofList"), dict) else "",
        }
        
        open_address_id = detail.get("openAddressId")
        if open_address_id and "NO_LOGISTICS_SHIP" not in str(order_data.get("订单标记", "")):
            receiver_result = self.get_order_receiver_info(order_id, open_address_id)
            if receiver_result and receiver_result.get("receiverInfos"):
                receiver_info = receiver_result["receiverInfos"][0]
                order_data["收件人姓名"] = receiver_info.get("receiverName", "")
                order_data["收件人电话"] = receiver_info.get("receiverPhone", "")
                order_data["镇/街道"] = receiver_info.get("receiverTownName", "")
                receiver_address = receiver_info.get("receiverAddress", "") or receiver_info.get("receiverTownName", "")
                full_address = f"{order_data['省']}{order_data['市']}{order_data['区']}{order_data['镇/街道']}{receiver_address}"
                order_data["收件人地址"] = full_address
        
        return order_data

    def _process_kol_info(self, order_id: str, df_order: pd.DataFrame) -> pd.DataFrame:
        settle_data_list = []
        
        detail = self.get_kol_settle_info(order_id)
        if not detail or not isinstance(detail, dict):
            empty_kol_info = {
                "订单ID": order_id,
                "达人ID": "hongshu0",
                "达人昵称": "小店自卖",
                "成交总金额(分)": 0,
                "退货总金额(分)": 0,
                "税费总金额(分)": 0,
                "带货总金额(分)": 0,
                "商家分成比例(万分比)": 0,
                "达人分成比例(万分比)": 0,
                "达人费率(万分比)": 0,
                "达人佣金金额(分)": 0,
                "结算状态": "无达人",
                "下单时间": "",
                "完成时间": "",
                "可结算时间": "",
                "结算时间": ""
            }
            settle_data_list.append(empty_kol_info)
        else:
            cps_settle_details = detail.get("cpsUserSettleDetails", [])
            
            if not cps_settle_details:
                empty_kol_info = {
                    "订单ID": order_id,
                    "达人ID": "hongshu0",
                    "达人昵称": "小店自卖",
                    "成交总金额(分)": 0,
                    "退货总金额(分)": 0,
                    "税费总金额(分)": 0,
                    "带货总金额(分)": 0,
                    "商家分成比例(万分比)": 0,
                    "达人分成比例(万分比)": 0,
                    "达人费率(万分比)": 0,
                    "达人佣金金额(分)": 0,
                    "结算状态": "无达人",
                    "下单时间": "",
                    "完成时间": "",
                    "可结算时间": "",
                    "结算时间": ""
                }
                settle_data_list.append(empty_kol_info)
            else:
                for settle_item in cps_settle_details:
                    def safe_num(value):
                        return 0 if value is None or value == "" or pd.isna(value) else (float(value) if isinstance(value, (int, float)) else 0)

                    def safe_time(timestamp):
                        if timestamp is None or timestamp == 0 or pd.isna(timestamp):
                            return ""
                        try:
                            return pd.to_datetime(timestamp, unit='ms').strftime("%Y-%m-%d %H:%M:%S")
                        except:
                            return ""

                    settle_info = {
                        "订单ID": order_id,
                        "达人ID": settle_item.get("kolUserId", ""),
                        "达人昵称": settle_item.get("kolUserName", ""),
                        "成交总金额(分)": safe_num(settle_item.get("dealTotalAmount")),
                        "退货总金额(分)": safe_num(settle_item.get("returnTotalAmount")),
                        "税费总金额(分)": safe_num(settle_item.get("taxTotalAmount")),
                        "带货总金额(分)": safe_num(settle_item.get("carryingTotalAmount")),
                        "商家分成比例(万分比)": safe_num(settle_item.get("sellerRate")),
                        "达人分成比例(万分比)": safe_num(settle_item.get("kolUserShareRatio")),
                        "达人费率(万分比)": safe_num(settle_item.get("kolUserRate")),
                        "达人佣金金额(分)": safe_num(settle_item.get("kolUserCommissionAmount")),
                        "结算状态": settle_item.get("settleStatus", ""),
                        "下单时间": safe_time(settle_item.get("orderTime")),
                        "完成时间": safe_time(settle_item.get("finishTime")),
                        "可结算时间": safe_time(settle_item.get("canSettleTime")),
                        "结算时间": safe_time(settle_item.get("settleTime"))
                    }
                    settle_data_list.append(settle_info)
        
        if settle_data_list:
            df_settle = pd.DataFrame(settle_data_list)
            df_settle = df_settle.drop_duplicates(subset=["订单ID"], keep="last")
            df_merged = pd.merge(df_order, df_settle, on="订单ID", how="left")
            return df_merged
        
        return df_order

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

        df = pd.DataFrame(all_order_data)
        print(f"\n✅ 完成！共获取{len(df)}条订单数据，包含{len(df.columns)}个字段")
        return df
