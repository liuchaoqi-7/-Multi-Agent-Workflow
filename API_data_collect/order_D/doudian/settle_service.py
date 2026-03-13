import time
import pandas as pd
from typing import Dict, Any, List

from .client import DouDianAPIClient
from .utils import format_time, format_amount


class SettleService:
    def __init__(self, client: DouDianAPIClient):
        self.client = client

    def batch_get_settle_details(self, params: Dict[str, Any], max_pages: int = 5000) -> pd.DataFrame:
        print("📌 开始获取结算单详情...")
        all_settle_data = []
        current_page = 0
        start_index = ""
        next_start_time = ""
        history_order_ids = set()
        
        while current_page < max_pages:
            current_page += 1
            new_order_count = 0

            try:
                request_params = params.copy()
                if start_index:
                    request_params["start_index"] = start_index
                if next_start_time:
                    request_params["start_time"] = next_start_time

                response = self.client.request("order.getSettleBillDetailV3", request_params)
                settle_data = response or {}
                records = settle_data.get("data", [])
                is_end = settle_data.get("is_end", 1)
                start_index = str(settle_data.get("next_start_index", "")).strip()
                next_start_time = settle_data.get("next_start_time", "")
                page_size = len(records)

                print(f"第{current_page}页：{page_size}条记录 | 下一页索引：{start_index[:8]}... | 下一页时间：{next_start_time}")

                for record in records:
                    order_id = record.get("order_id", "")
                    if order_id not in history_order_ids:
                        history_order_ids.add(order_id)
                        new_order_count += 1
                    settle_item = {
                        "结算请求号": str(record.get("request_no", "")).strip(),
                        "订单ID": str(record.get("shop_order_id", "")).strip(),
                        "店铺子订单ID": str(record.get("order_id", "")).strip(),
                        "订单类型": str(record.get("order_type", "")).strip(),
                        "交易类型编码": str(record.get("trade_type", "")).strip(),
                        "流量来源": str(record.get("flow_type_desc", "")).strip(),
                        "支付方式": str(record.get("pay_type_desc", "")).strip(),
                        "备注信息": str(record.get("remark", "")).strip(),
                        "是否包含结算前退款": str("是" if record.get("is_contains_refund_before_settle") == 1 else "否").strip(),
                        "商品ID": str(record.get("product_id", "")).strip(),
                        "商品名称": str(record.get("product_name", "")).strip(),
                        "商品数量": str(record.get("goods_count", 0)).strip(),
                        "达人ID": str(record.get("author_id", "")).strip(),
                        "达人名称": str(record.get("author_name", "")).strip(),
                        "是否免佣金": str(record.get("free_commission_flag", "")).strip(),
                        "订单总金额(元)": str(format_amount(record.get("total_amount", 0))).strip(),
                        "商品总金额(元)": str(format_amount(record.get("total_goods_amount", 0))).strip(),
                        "实际支付金额(元)": str(format_amount(record.get("real_pay_amount", 0))).strip(),
                        "结算金额(元)": str(format_amount(record.get("settle_amount", 0))).strip(),
                        "总收入(元)": str(format_amount(record.get("total_income", 0))).strip(),
                        "总支出(元)": str(format_amount(record.get("total_outcome", 0))).strip(),
                        "运费金额(元)": str(format_amount(record.get("post_amount", 0))).strip(),
                        "运费优惠金额(元)": str(format_amount(record.get("post_promotion_amount", 0))).strip(),
                        "打包费(元)": str(format_amount(record.get("packing_amount", 0))).strip(),
                        "结算前退款金额(元)": str(format_amount(record.get("refund_before_settle", 0))).strip(),
                        "平台优惠券金额(元)": str(format_amount(record.get("platform_coupon", 0))).strip(),
                        "店铺优惠券金额(元)": str(format_amount(record.get("shop_coupon", 0))).strip(),
                        "达人优惠券金额(元)": str(format_amount(record.get("author_coupon", 0))).strip(),
                        "政府补贴金额(元)": str(format_amount(record.get("gov_promotion_amount", 0))).strip(),
                        "政府补贴店铺减免(元)": str(format_amount(record.get("gov_promotion_shop_reduce_amount", 0))).strip(),
                        "银行补贴金额(元)": str(format_amount(record.get("bank_promotion", 0))).strip(),
                        "换新补贴金额(元)": str(format_amount(record.get("old_for_new_promotion", 0))).strip(),
                        "ZR支付补贴(元)": str(format_amount(record.get("zr_pay_promotion", 0))).strip(),
                        "ZT支付补贴(元)": str(format_amount(record.get("zt_pay_promotion", 0))).strip(),
                        "其他平台补贴(元)": str(format_amount(record.get("other_platform_promotion_amount", 0))).strip(),
                        "达人佣金(元)": str(format_amount(record.get("commission", 0))).strip(),
                        "合作伙伴佣金(元)": str(format_amount(record.get("partner_commission", 0))).strip(),
                        "实际免佣金金额(元)": str(format_amount(record.get("real_free_commission_amount", 0))).strip(),
                        "平台服务费(元)": str(format_amount(record.get("platform_service_fee", 0))).strip(),
                        "上校服务费(元)": str(format_amount(record.get("colonel_service_fee", 0))).strip(),
                        "渠道推广费(元)": str(format_amount(record.get("channel_promotion_fee", 0))).strip(),
                        "上校渠道费(元)": str(format_amount(record.get("good_learn_channel_fee", 0))).strip(),
                        "其他分摊金额(元)": str(format_amount(record.get("other_sharing_amount", 0))).strip(),
                        "下单时间": str(format_time(record.get("order_time", ""))).strip(),
                        "结算时间": str(format_time(record.get("settle_time", ""))).strip()
                    }
                    all_settle_data.append(settle_item)

                if int(is_end) == 1:
                    print("🎉 API返回is_end=1，正常结束")
                    break
                if new_order_count == 0:
                    print("⚠️ 本页无新增记录，终止分页")
                    break
                if not start_index and not next_start_time:
                    print("⚠️ 无分页参数，终止")
                    break

                time.sleep(0.5)

            except Exception as e:
                print(f"❌ 第{current_page}页失败：{str(e)}")
                break

        df = pd.DataFrame(all_settle_data)
        print(f"\n✅ 结算单获取完成！共{len(df)}条不重复记录，{len(df.columns)}个字段")
        return df
