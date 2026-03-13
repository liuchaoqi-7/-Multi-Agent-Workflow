import time
import random
import pandas as pd
from datetime import datetime
from typing import Dict, Any, List

from .client import XiaoHongShuAPIClient
from .config import (
    SETTLE_BIZ_TYPE_MAP, ERQING_TYPE_MAP, STATEMENT_TYPE_MAP,
    TRANSACTION_BIZ_TYPE_MAP, TRANSACTION_SETTLE_STATUS_MAP,
    COMMON_SETTLE_STATUS_MAP, MODULE_CONFIG
)
from .utils import format_time, format_amount, get_mapped_value


class FinanceService:
    def __init__(self, client: XiaoHongShuAPIClient, max_workers: int = None):
        self.client = client
        config = MODULE_CONFIG["finance"]
        self.max_workers = max_workers or config["max_workers"]
        self.max_retries = config["max_retries"]
        self.base_delay = config["base_delay"]
        self.random_delay_range = config["random_delay_range"]
        self.page_delay = config["page_delay"]

    def get_finance_details(self, finance_params: Dict[str, Any]) -> pd.DataFrame:
        target_columns = [
            "订单号", "售后单号", "履约单号", "结算唯一ID", "下单时间", "可结算时间", 
            "结算时间", "预计结算时间", "交易类型", "结算账户", "结算类型", "账本业务类型",
            "账本结算状态", "结算状态", "动账金额", "结算金额", "商品实付/实退", 
            "运费实付/实退", "平台优惠补贴", "平台运费补贴", "商品税金", 
            "运费税金", "佣金", "支付渠道费", "分销佣金", "代运营服务商佣金", 
            "代开发服务商佣金", "花呗分期手续费", "附加费", "国补订单毛保金额", 
            "计费备注", "备注"
        ]
        
        if not finance_params.get("startTime") or not finance_params.get("endTime"):
            print("错误：缺少必要参数 startTime 或 endTime")
            return pd.DataFrame(columns=target_columns)
        
        time_diff = finance_params.get("endTime", 0) - finance_params.get("startTime", 0)
        if time_diff > 24 * 60 * 60 * 1000:
            print(f"警告：时间范围超过24小时({time_diff/1000/3600:.1f}小时)，可能返回空数据")
        
        try:
            print(f"查询财务数据：{datetime.fromtimestamp(finance_params['startTime']/1000)} 至 {datetime.fromtimestamp(finance_params['endTime']/1000)}")
            result = self.client.request("finance.pageQueryTransaction", finance_params)
            
            if not result:
                print("错误：接口返回空")
                return pd.DataFrame(columns=target_columns)
            
            data = result.get("data", result)
            total = data.get("total", 0)
            transactions = data.get("transactions", [])
            
            if total == 0 or len(transactions) == 0:
                print(f"提示：当前查询条件下没有找到财务数据（total={total}）")
                return pd.DataFrame(columns=target_columns)
            
            print(f"找到{len(transactions)}条交易记录（总计{total}条）")
            
            def format_ts(ts):
                try:
                    return datetime.fromtimestamp(int(ts)/1000).strftime("%Y-%m-%d %H:%M:%S") if (ts and ts != 0) else ""
                except:
                    return ""
            
            details = []
            for trans in transactions:
                if not isinstance(trans, dict):
                    continue
                    
                goods_details_list = trans.get("goodsDetails", [])
                goods = goods_details_list[0] if goods_details_list else {}
                
                details.append({
                    "订单号": str(trans.get("packageId", "")).strip(),
                    "售后单号": str(trans.get("returnId", "")).strip(),
                    "履约单号": str(trans.get("deliveryId", "")).strip(),
                    "结算唯一ID": str(trans.get("transactionId", "")).strip(),
                    "下单时间": str(format_ts(trans.get("orderTime"))).strip(),
                    "可结算时间": str(format_ts(trans.get("canSettleTime"))).strip(),
                    "结算时间": str(format_ts(trans.get("settledTime"))).strip(),
                    "预计结算时间": str(trans.get("predictableSettleTime", "")).strip(),
                    "交易类型": str(SETTLE_BIZ_TYPE_MAP.get(trans.get("settleBizType"), trans.get("settleBizType"))).strip(),
                    "结算账户": str(ERQING_TYPE_MAP.get(trans.get("erqingType"), trans.get("erqingType"))).strip(),
                    "结算类型": str(STATEMENT_TYPE_MAP.get(trans.get("statementType"), trans.get("statementType"))).strip(),
                    "账本业务类型": str(TRANSACTION_BIZ_TYPE_MAP.get(trans.get("transactionBizType"), trans.get("transactionBizType"))).strip(),
                    "账本结算状态": str(TRANSACTION_SETTLE_STATUS_MAP.get(trans.get("transactionSettleStatus"), trans.get("transactionSettleStatus"))).strip(),
                    "结算状态": str(COMMON_SETTLE_STATUS_MAP.get(trans.get("commonSettleStatus"), trans.get("commonSettleStatus"))).strip(),
                    "动账金额": str(trans.get("amount", "0")).strip(),
                    "结算金额": str(trans.get("goodsAmount", "0")).strip(),
                    "商品实付/实退": str(trans.get("payAmount", "0")).strip(),
                    "运费实付/实退": str(trans.get("freightAmount", "0")).strip(),
                    "平台优惠补贴": str(trans.get("appPromotion", "0")).strip(),
                    "平台运费补贴": str(goods.get("freightAppPromotion", "0")).strip(),
                    "商品税金": str(trans.get("taxAmount", "0")).strip(),
                    "运费税金": str(trans.get("freightTaxAmount", "0")).strip(),
                    "佣金": str(trans.get("commissionAmount", "0")).strip(),
                    "支付渠道费": str(trans.get("payChannelAmount", "0")).strip(),
                    "分销佣金": str(trans.get("cpsAmount", "0")).strip(),
                    "代运营服务商佣金": str(goods.get("serviceCommissionAmount", "0")).strip(),
                    "代开发服务商佣金": str(goods.get("redCommissionAmount", "0")).strip(),
                    "花呗分期手续费": str(trans.get("installmentAmount", "0")).strip(),
                    "附加费": str(trans.get("extraAmount", "0")).strip(),
                    "国补订单毛保金额": str("0").strip(),
                    "计费备注": str(trans.get("calculateRemark", "")).strip(),
                    "备注": str(trans.get("calculateRemark", "")).strip()
                })
            
            df = pd.DataFrame(details, columns=target_columns)
            print(f"成功提取{len(df)}条财务结算数据")
            return df
            
        except Exception as e:
            print(f"处理财务数据出错：{str(e)}")
            return pd.DataFrame(columns=target_columns)

    def batch_get_finance_data(self, params: Dict[str, Any]) -> pd.DataFrame:
        all_finance_data = []
        page_num = params.get("pageNum", 1)
        page_size = params.get("pageSize", 50)
        
        while True:
            request_params = params.copy()
            request_params["pageNum"] = page_num
            request_params["pageSize"] = page_size
            
            df_page = self.get_finance_details(request_params)
            if df_page.empty:
                break
            
            all_finance_data.append(df_page)
            
            if len(df_page) < page_size:
                break
            
            page_num += 1
            time.sleep(self.page_delay)
            
            if page_num >= 100:
                break
        
        if all_finance_data:
            df = pd.concat(all_finance_data, ignore_index=True)
            print(f"\n✅ 结算数据获取完成！共{len(df)}条记录，{len(df.columns)}个字段")
            return df
        
        return pd.DataFrame()
