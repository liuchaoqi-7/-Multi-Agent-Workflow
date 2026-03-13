import time
import pandas as pd
from typing import List, Dict, Any, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

from .client import DouDianAPIClient
from .config import AFTER_SALE_STATUS_MAP, AFTER_SALE_TYPE_MAP, REFUND_STATUS_MAP
from .utils import format_time, format_amount, get_mapped_value


class AftersaleService:
    def __init__(self, client: DouDianAPIClient, max_workers: int = 40):
        self.client = client
        self.max_workers = max_workers

    def get_aftersale_list(self, params: Dict[str, Any]) -> Tuple[List[str], List[str]]:
        print("\n开始获取售后列表...")
        all_ids = []
        all_orders = []
        
        while True:
            result = self.client.request("afterSale.List", params)
            if not result:
                break
                
            items = result.get("items", [])
            if not items:
                break
            total = result.get("total", 0)
            
            ids = []
            orders = []
            for item in items:
                aftersale_info = item.get("aftersale_info", {})
                aftersale_id = aftersale_info.get("aftersale_id")
                order_id = aftersale_info.get("related_id")
                if aftersale_id:
                    ids.append(aftersale_id)
                    orders.append(order_id)
            
            all_ids.extend(ids)
            all_orders.extend(orders)
            
            print(f"第{params['page']}页: 获取{len(ids)}个售后单 (累计{len(all_ids)}/{total})")
            
            if len(all_ids) >= total:
                break
            params["page"] += 1
            time.sleep(0.2)
        
        print(f"售后列表获取完成，共{len(all_ids)}个售后ID")
        print(f"售后列表获取完成，共{len(all_ids)}个关联订单ID")
        return all_ids, all_orders

    def get_aftersale_detail(self, after_sale_id: str) -> Optional[Dict[str, Any]]:
        try:
            result = self.client.request("afterSale.Detail", {
                "after_sale_id": after_sale_id,
                "need_operation_record": True
            })
            
            if not isinstance(result, dict):
                print(f"❌ 售后单{after_sale_id}响应格式异常：非字典类型")
                return None
            
            return result
        except Exception as e:
            print(f"❌ 售后单{after_sale_id}异常：{str(e)}")
            return None

    def _process_aftersale(self, after_sale_id: str) -> Optional[Dict[str, Any]]:
        detail = self.get_aftersale_detail(after_sale_id)
        detail = detail or {}
        order_info = detail.get("order_info", {})
        process_info = detail.get("process_info", {})
        after_sale_info = process_info.get("after_sale_info", {})
        logistics_info = process_info.get("logistics_info", {})
        arbitrate_info = process_info.get("arbitrate_info", {})
        price_protection = process_info.get("price_protection_detail", {})
        
        sku_order_list = order_info.get("sku_order_infos", [])
        sku_order = sku_order_list[0] if (isinstance(sku_order_list, list) and len(sku_order_list) > 0) else {}
        
        after_sale_remark_list = process_info.get("after_sale_shop_remarks", [])
        after_sale_remark = after_sale_remark_list[0] if (isinstance(after_sale_remark_list, list) and len(after_sale_remark_list) > 0) else {}

        record_log_list = process_info.get("record_logs_list", [])
        record_log = record_log_list[0] if (isinstance(record_log_list, list) and len(record_log_list) > 0) else {}

        logistics_order_list = logistics_info.get("order", [])
        logistics_order = logistics_order_list[0] if (isinstance(logistics_order_list, list) and len(logistics_order_list) > 0) else {}
        
        aftersale_data = {
            "售后单号": str((after_sale_info or {}).get("after_sale_id", "")).strip(),
            "店铺订单ID": str((order_info or {}).get("shop_order_id", "")).strip(),
            "SKU订单ID": str((sku_order or {}).get("sku_order_id", "")).strip(),
            "售后订单类型": str((after_sale_info or {}).get("after_sale_order_type", "")).strip(),
            "售后订单类型描述": str("历史订单" if (after_sale_info or {}).get("after_sale_order_type") == -1 else 
                                "商品单" if (after_sale_info or {}).get("after_sale_order_type") == 1 else 
                                "店铺单" if (after_sale_info or {}).get("after_sale_order_type") == 2 else "").strip(),
            "售后类型": str((after_sale_info or {}).get("after_sale_type_text", "")).strip(),
            "售后类型编码": str((after_sale_info or {}).get("after_sale_type", "")).strip(),
            "售后子类型编码": str((after_sale_info or {}).get("after_sale_sub_type", "")).strip(),
            "售后状态": str((after_sale_info or {}).get("after_sale_status_desc", "")).strip(),
            "售后状态编码": str((after_sale_info or {}).get("after_sale_status", "")).strip(),
            "退款状态": str((after_sale_info or {}).get("refund_status", "")).strip(),
            "退款状态描述": str("待退款" if (after_sale_info or {}).get("refund_status") == 1 else 
                            "退款成功" if (after_sale_info or {}).get("refund_status") == 3 else 
                            "退款失败" if (after_sale_info or {}).get("refund_status") == 4 else "").strip(),
            "申请角色": str("买家" if (after_sale_info or {}).get("apply_role") == 1 else "商家" if (after_sale_info or {}).get("apply_role") == 2 else "").strip(),
            "申请角色编码": str((after_sale_info or {}).get("apply_role", "")).strip(),
            "是否需要退货": str("否" if (after_sale_info or {}).get("need_return_count", 0) == 0 else "是").strip(),
            "需退货数量": str((after_sale_info or {}).get("need_return_count", 0)).strip(),
            "是否收到包裹": str("是" if (after_sale_info or {}).get("got_pkg") == 1 else "否").strip(),
            "收到包裹标识": str((after_sale_info or {}).get("got_pkg", "")).strip(),
            "售后完结时间": str(format_time((after_sale_info or {}).get("aftersale_status_to_final_time", 0))).strip(),
            "状态截止时间": str(format_time((after_sale_info or {}).get("status_deadline", 0))).strip(),
            "售后标签": str((after_sale_info or {}).get("aftersale_tags", [])).strip(),
            "风险描述": str((after_sale_info or {}).get("risk_decsison_description", "")).strip(),
            "风险原因": str((after_sale_info or {}).get("risk_decsison_reason", "")).strip(),
            "风险决策编码": str((after_sale_info or {}).get("risk_decsison_code", "")).strip(),
            "商品名称": str((sku_order or {}).get("product_name", "")).strip(),
            "商品ID": str((sku_order or {}).get("product_id", "")).strip(),
            "SKU ID": str((sku_order or {}).get("sku_id", "")).strip(),
            "商家SKU编码": str((sku_order or {}).get("shop_sku_code", "")).strip(),
            "商品规格": str((sku_order or {}).get("sku_spec", [])).strip(),
            "商品属性": str((sku_order or {}).get("custom_property_list", [])).strip(),
            "购买数量": str((sku_order or {}).get("item_quantity", 0)).strip(),
            "商品总价(元)": str(format_amount((sku_order or {}).get("item_sum_amount", 0))).strip(),
            "商品图片": str((sku_order or {}).get("product_image", "")).strip(),
            "是否跨境订单": str("是" if (sku_order or {}).get("is_oversea_order") == "1" else "否").strip(),
            "跨境订单标识": str((sku_order or {}).get("is_oversea_order", "")).strip(),
            "赠品信息": str((sku_order or {}).get("given_sku_details", [])).strip(),
            "商品标签": str((sku_order or {}).get("tags", [])).strip(),
            "运费险标签": str((sku_order or {}).get("insurance_tags", [])).strip(),
            "实付金额(元)": str(format_amount((sku_order or {}).get("pay_amount", 0))).strip(),
            "支付金额(子单)(元)": str(format_amount((sku_order or {}).get("sku_pay_amount", 0))).strip(),
            "运费金额(元)": str(format_amount((sku_order or {}).get("post_amount", 0))).strip(),
            "优惠总金额(元)": str(format_amount((sku_order or {}).get("promotion_amount", 0))).strip(),
            "税费金额(元)": str(format_amount((sku_order or {}).get("tax_amount", 0))).strip(),
            "退款总金额(元)": str(format_amount((after_sale_info or {}).get("refund_total_amount", 0))).strip(),
            "实际退款金额(元)": str(format_amount((after_sale_info or {}).get("real_refund_amount", 0))).strip(),
            "抵扣金额(元)": str(format_amount((after_sale_info or {}).get("deduction_amount", 0))).strip(),
            "运费退款(元)": str(format_amount((after_sale_info or {}).get("refund_post_amount", 0))).strip(),
            "打包费退款(元)": str(format_amount((after_sale_info or {}).get("refund_packing_charge_amount", 0))).strip(),
            "优惠退款(元)": str(format_amount((after_sale_info or {}).get("refund_promotion_amount", 0))).strip(),
            "金币返还金额(元)": str(format_amount((after_sale_info or {}).get("gold_coin_return_amount", 0))).strip(),
            "政府渠道补贴(元)": str(format_amount((after_sale_info or {}).get("government_channel_subsidy_amount", 0))).strip(),
            "达人优惠返还(元)": str(format_amount((after_sale_info or {}).get("kol_discount_return_amount", 0))).strip(),
            "达人运费优惠返还(元)": str(format_amount((after_sale_info or {}).get("kol_post_discount_return_amount", 0))).strip(),
            "平台优惠返还(元)": str(format_amount((after_sale_info or {}).get("platform_discount_return_amount", 0))).strip(),
            "店铺优惠返还(元)": str(format_amount((after_sale_info or {}).get("shop_discount_return_amount", 0))).strip(),
            "店铺运费优惠返还(元)": str(format_amount((after_sale_info or {}).get("shop_post_discount_return_amount", 0))).strip(),
            "运费优惠返还(元)": str(format_amount((after_sale_info or {}).get("post_discount_return_amount", 0))).strip(),
            "价保平台回收金额(元)": str(format_amount((price_protection or {}).get("platform_recycle_amount", 0))).strip(),
            "价保实退差价(元)": str(format_amount(((price_protection or {}).get("refund_detail", {}) or {}).get("actual_amount", {}).get("amount", 0))).strip(),
            "申请时间": str(format_time((after_sale_info or {}).get("apply_time", 0))).strip(),
            "退款时间": str(format_time((after_sale_info or {}).get("refund_time", 0))).strip(),
            "商品创建时间": str(format_time((sku_order or {}).get("create_time", 0))).strip(),
            "仲裁创建时间": str(format_time((arbitrate_info or {}).get("arbitrate_create_time", 0))).strip(),
            "仲裁更新时间": str(format_time((arbitrate_info or {}).get("arbitrate_update_time", 0))).strip(),
            "仲裁截止时间": str(format_time((arbitrate_info or {}).get("arbitrate_status_deadline", 0))).strip(),
            "操作记录时间": str(format_time((record_log or {}).get("time", ""))).strip(),
            "商家备注创建时间": str(format_time((after_sale_remark or {}).get("create_time", 0))).strip(),
            "卡券生效时间": str(format_time(((sku_order or {}).get("card_voucher", {}) or {}).get("valid_start", 0))).strip(),
            "卡券失效时间": str(format_time(((sku_order or {}).get("card_voucher", {}) or {}).get("valid_end", 0))).strip(),
            "原订单物流公司": str((logistics_order or {}).get("company_name", "")).strip(),
            "原订单物流编码": str((logistics_order or {}).get("company_code", "")).strip(),
            "原订单物流单号": str((logistics_order or {}).get("tracking_no", "")).strip(),
            "原订单物流状态": str((logistics_order or {}).get("logistics_state", "")).strip(),
            "原订单物流时间": str(format_time((logistics_order or {}).get("logistics_time", 0))).strip(),
            "退货物流公司": str(((logistics_info or {}).get("return", {}) or {}).get("company_name", "")).strip(),
            "退货物流编码": str(((logistics_info or {}).get("return", {}) or {}).get("company_code", "")).strip(),
            "退货物流单号": str(((logistics_info or {}).get("return", {}) or {}).get("tracking_no", "")).strip(),
            "退货物流时间": str(format_time(((logistics_info or {}).get("return", {}) or {}).get("logistics_time", 0))).strip(),
            "换货物流公司": str(((logistics_info or {}).get("exchange", {}) or {}).get("company_name", "")).strip(),
            "换货物流单号": str(((logistics_info or {}).get("exchange", {}) or {}).get("tracking_no", "")).strip(),
            "补发物流公司": str(((logistics_info or {}).get("resend", {}) or {}).get("company_name", "")).strip(),
            "补发物流单号": str(((logistics_info or {}).get("resend", {}) or {}).get("tracking_no", "")).strip(),
            "物流增值服务": str((logistics_order or {}).get("value_added_services", [])).strip(),
            "是否偏远服务": str("是" if (logistics_order or {}).get("is_remote_service") == 1 else "否").strip(),
            "是否中转": str("是" if (logistics_order or {}).get("is_transit") == 1 else "否").strip(),
            "退货方式": str("快递退货" if (after_sale_info or {}).get("return_method") == 1 else "上门取件" if (after_sale_info or {}).get("return_method") == 2 else "").strip(),
            "退货方式编码": str((after_sale_info or {}).get("return_method", "")).strip(),
            "收件人姓名": str((after_sale_info or {}).get("encrypt_post_receiver", "")).strip(),
            "收件人电话": str((after_sale_info or {}).get("encrypt_post_tel_sec", "")).strip(),
            "收件人省份": str((((after_sale_info or {}).get("post_address", {}) or {}).get("province", {}) or {}).get("name", "")).strip(),
            "收件人城市": str((((after_sale_info or {}).get("post_address", {}) or {}).get("city", {}) or {}).get("name", "")).strip(),
            "收件人区县": str((((after_sale_info or {}).get("post_address", {}) or {}).get("town", {}) or {}).get("name", "")).strip(),
            "收件人街道": str((((after_sale_info or {}).get("post_address", {}) or {}).get("street", {}) or {}).get("name", "")).strip(),
            "收件人省份id": str((((after_sale_info or {}).get("post_address", {}) or {}).get("province", {}) or {}).get("id", "")).strip(),
            "收件人城市id": str((((after_sale_info or {}).get("post_address", {}) or {}).get("city", {}) or {}).get("id", "")).strip(),
            "收件人区县id": str((((after_sale_info or {}).get("post_address", {}) or {}).get("town", {}) or {}).get("id", "")).strip(),
            "收件人街道id": str((((after_sale_info or {}).get("post_address", {}) or {}).get("street", {}) or {}).get("id", "")).strip(),
            "收件人详细地址": str(((after_sale_info or {}).get("post_address", {}) or {}).get("encrypt_detail", "")).strip(),
            "退货地址省份": str((((after_sale_info or {}).get("return_address", {}) or {}).get("province", {}) or {}).get("name", "")).strip(),
            "退货地址城市": str((((after_sale_info or {}).get("return_address", {}) or {}).get("city", {}) or {}).get("name", "")).strip(),
            "退货地址区县": str((((after_sale_info or {}).get("return_address", {}) or {}).get("town", {}) or {}).get("name", "")).strip(),
            "退货地址街道": str((((after_sale_info or {}).get("return_address", {}) or {}).get("street", {}) or {}).get("name", "")).strip(),
            "退货地址详细地址": str(((after_sale_info or {}).get("return_address", {}) or {}).get("detail", "")).strip(),
            "退货地址ID": str((after_sale_info or {}).get("return_address_id", "")).strip(),
            "预约取件信息": str((after_sale_info or {}).get("return_book_info", {})).strip(),
            "售后原因": str((after_sale_info or {}).get("reason", "")).strip(),
            "售后原因编码": str((after_sale_info or {}).get("reason_code", "")).strip(),
            "售后备注": str((after_sale_info or {}).get("reason_remark", "")).strip(),
            "二级原因标签": str((after_sale_info or {}).get("reason_second_labels", [])).strip(),
            "退款失败原因": str((after_sale_info or {}).get("refund_fail_reason", "")).strip(),
            "退款类型": str((after_sale_info or {}).get("refund_type_text", "")).strip(),
            "退款类型编码": str((after_sale_info or {}).get("refund_type", "")).strip(),
            "仲裁ID": str((arbitrate_info or {}).get("arbitrate_id", "")).strip(),
            "仲裁状态": str("无需仲裁" if (arbitrate_info or {}).get("arbitrate_status") == 0 else 
                        "仲裁中" if (arbitrate_info or {}).get("arbitrate_status") == 1 else 
                        "仲裁完成" if (arbitrate_info or {}).get("arbitrate_status") == 2 else "").strip(),
            "仲裁状态编码": str((arbitrate_info or {}).get("arbitrate_status", "")).strip(),
            "仲裁责任方": str("商家责任" if (arbitrate_info or {}).get("arbitrate_blame") == 1 else 
                        "买家责任" if (arbitrate_info or {}).get("arbitrate_blame") == 2 else 
                        "双方有责" if (arbitrate_info or {}).get("arbitrate_blame") == 3 else 
                        "平台责任" if (arbitrate_info or {}).get("arbitrate_blame") == 4 else 
                        "达人责任" if (arbitrate_info or {}).get("arbitrate_blame") == 5 else "").strip(),
            "仲裁结论": str((arbitrate_info or {}).get("arbitrate_conclusion", "")).strip(),
            "仲裁意见": str((arbitrate_info or {}).get("arbitrate_opinion", "")).strip(),
            "是否需要举证": str("是" if (arbitrate_info or {}).get("is_required_evidence") == 1 else "否").strip(),
            "仲裁举证描述": str(((arbitrate_info or {}).get("arbitrate_evidence", {}) or {}).get("describe", "")).strip(),
            "仲裁举证图片": str(((arbitrate_info or {}).get("arbitrate_evidence", {}) or {}).get("images", [])).strip(),
            "商家备注内容": str((after_sale_remark or {}).get("remark", "")).strip(),
            "备注操作人": str((after_sale_remark or {}).get("operator", "")).strip(),
            "备注创建时间": str((after_sale_remark or {}).get("create_time", "")).strip(),
            "备注关联订单号": str((after_sale_remark or {}).get("order_id", "")).strip(),
            "备注关联售后单号": str((after_sale_remark or {}).get("after_sale_id", "")).strip(),
            "操作记录类型": str((record_log or {}).get("action", "")).strip(),
            "操作记录描述": str((record_log or {}).get("text", "")).strip(),
            "操作人角色": str("买家" if (record_log or {}).get("role") == 1 else "商家" if (record_log or {}).get("role") == 2 else "平台" if (record_log or {}).get("role") == 3 else "").strip(),
            "操作人名称": str((record_log or {}).get("operator", "")).strip(),
            "操作举证图片": str((record_log or {}).get("all_evidence", [])).strip(),
            "售后凭证图片": str((after_sale_info or {}).get("evidence", [])).strip(),
            "服务标签": str((process_info or {}).get("after_sale_service_tag", {})).strip(),
            "禁用优惠券ID": str((after_sale_info or {}).get("disable_coupon_id", "")).strip(),
            "换货SKU信息": str((after_sale_info or {}).get("exchange_sku_info", {})).strip(),
            "自动审核标识": str((after_sale_info or {}).get("auto_audit_bits", [])).strip(),
            "同意拒绝标识": str((after_sale_info or {}).get("agree_refuse_sign", "")).strip(),
            "部分售后类型": str((after_sale_info or {}).get("part_type", "")).strip(),
            "门店ID": str((after_sale_info or {}).get("store_id", "")).strip(),
            "门店名称": str((after_sale_info or {}).get("store_name", "")).strip(),
            "价保计算明细": str((price_protection or {}).get("price_protection_formulas", "")).strip(),
            "退款承担方明细": str((price_protection or {}).get("refund_bearer_list", [])).strip(),
            "售后更新时间": str(format_time((after_sale_info or {}).get("update_time", 0))).strip(),
            "合同ID": str(((after_sale_info or {}).get("contract_info", {}) or {}).get("contract_id", "")).strip(),
            "办理手机号": str(((after_sale_info or {}).get("contract_info", {}) or {}).get("encrypt_mobile_no", "")).strip()
        }
        return aftersale_data

    def batch_get_aftersale_details(self, aftersale_ids: List[str]) -> pd.DataFrame:
        if not aftersale_ids:
            print("❌ 无有效售后ID")
            return pd.DataFrame()
        
        print(f"📌 开始获取{len(aftersale_ids)}个售后单所有字段...")
        all_aftersale_data = []
        total = len(aftersale_ids)
        print(f"📢 开始多线程获取 {total} 个售后详情，线程数：{self.max_workers}")
        
        for idx, after_sale_id in enumerate(aftersale_ids):
            if (idx + 1) % 100 == 0:
                print(f"进度：{idx+1}/{len(aftersale_ids)}")
        
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
                    if (idx + 1) % 100 == 0 or idx == total:
                        print(f"进度：{idx}/{total}，已获取有效数据：{len(all_aftersale_data)} 条")
                except Exception as e:
                    print(f"❌ 售后单{after_sale_id}处理异常：{str(e)}")
                time.sleep(0.01)
        
        df = pd.DataFrame(all_aftersale_data)
        print(f"\n✅ 完成！共获取{len(df)}条售后数据，包含{len(df.columns)}个字段")
        return df
