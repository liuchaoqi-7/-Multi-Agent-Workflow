import time
import json
import threading
import pandas as pd
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from .client import QianChuanClient, get_session
from .config import OTHER_MAPPING, MODULE_CONFIG


data_lock = threading.Lock()


class OtherService:
    def __init__(self, client: QianChuanClient, max_workers: int = None):
        self.client = client
        config = MODULE_CONFIG["other"]
        self.max_workers = max_workers or config["max_workers"]
        self.max_retries = config["max_retries"]
        self.base_delay = config["base_delay"]
        self.page_size = config["page_size"]
        self.data_topic = config["data_topic"]

    def _request_single_minute(
        self, 
        advertiser_id: str,
        aweme_id: str,
        minute_range: tuple, 
        base_params: Dict[str, Any], 
        all_data: List[Dict[str, Any]]
    ) -> None:
        start_time, end_time = minute_range
        params = base_params.copy()
        params["start_time"] = start_time
        params["end_time"] = end_time
        params["page_size"] = self.page_size
        thread_name = threading.current_thread().name
        
        current_page = 1
        while True:
            params["page"] = current_page
            retry_count = 0
            retry_delay = self.base_delay
            success = False
            
            while retry_count <= self.max_retries and not success:
                try:
                    print(f"🔄 {thread_name} | 分钟：{start_time[:16]} | 页码：{current_page} (重试：{retry_count})")
                    
                    session = get_session()
                    headers = {
                        "Access-Token": self.client.access_token,
                        "Content-Type": "application/json"
                    }
                    
                    response = session.get(
                        url="https://api.oceanengine.com/open_api/v1.0/qianchuan/report/uni_promotion/data/get/",
                        params=params,
                        headers=headers,
                        timeout=30
                    )
                    response.raise_for_status()
                    result = response.json()
                    
                    if result.get("code") != 0:
                        if result.get("code") == 40000 and retry_count < self.max_retries:
                            raise Exception(f"下游依赖错误：{result.get('message')}")
                        else:
                            raise Exception(f"业务错误：{result.get('message')} (request_id: {result.get('request_id')})")
                    
                    data = result.get("data", {})
                    page_info = data.get("page_info", {})
                    total_page = page_info.get("total_page", 1)
                    total = page_info.get("total", 0)
                    rows = data.get("rows", [])
                    
                    if not rows and current_page == 1:
                        print(f"ℹ️ {thread_name} | 分钟 {start_time[:16]} 无数据")
                        return
                    elif not rows:
                        print(f"ℹ️ {thread_name} | 分钟 {start_time[:16]} | 页码 {current_page} 无数据")
                        break
                    
                    parsed_data = []
                    for row in rows:
                        single_row = {}
                        single_row['千川UID'] = advertiser_id
                        single_row['抖音UID'] = aweme_id
                        single_row['时间'] = start_time
                        metrics = row.get("metrics", {})
                        for en_field, value_dict in metrics.items():
                            cn_field = OTHER_MAPPING.get(en_field, en_field)
                            single_row[cn_field] = value_dict.get("ValueStr", "")
                        parsed_data.append(single_row)
                    
                    with data_lock:
                        all_data.extend(parsed_data)
                        print(f"✅ {thread_name} | 分钟 {start_time[:16]} | 页码 {current_page} | 解析到 {len(parsed_data)} 条数据（累计：{len(all_data)}）")
                    
                    success = True
                    if current_page >= total_page:
                        print(f"ℹ️ {thread_name} | 分钟 {start_time[:16]} 分页完成（总页数：{total_page}，总条数：{total}）")
                        return
                    else:
                        current_page += 1
                        
                except Exception as e:
                    retry_count += 1
                    if retry_count > self.max_retries:
                        print(f"❌ {thread_name} | 分钟 {start_time[:16]} | 页码 {current_page} 请求失败（已重试{self.max_retries}次）：{str(e)}")
                        return
                    print(f"⚠️ {thread_name} | 分钟 {start_time[:16]} | 页码 {current_page} 请求失败，{retry_delay}秒后重试：{str(e)}")
                    time.sleep(retry_delay)
                    retry_delay *= 2

    def collect_data(
        self, 
        advertiser_id: str, 
        aweme_id: str, 
        start_time: str, 
        end_time: str
    ) -> pd.DataFrame:
        from .utils import split_time_by_day_then_minute
        
        base_params = {
            "advertiser_id": advertiser_id,
            "data_topic": self.data_topic,
            "dimensions": json.dumps(["roi2_other_creative_name", "stat_time_day"]),
            "filters": json.dumps([
                {"field": "anchor_id", "operator": 7, "values": [aweme_id]},
                {"field": "ecp_app_id", "operator": 7, "values": ["1", "2"]},
                {"field": "aggregate_smart_bid_type", "operator": 7, "values": ["0"]}
            ]),
            "metrics": json.dumps(list(OTHER_MAPPING.keys())),
            "order_by": json.dumps([{"field": "stat_time_day", "type": 1}]),
            "page": 1,
            "page_size": self.page_size
        }
        
        minute_ranges = split_time_by_day_then_minute(start_time, end_time)
        print(f"===== 开始采集其他创意数据 =====\n总分钟数：{len(minute_ranges)} | 时间范围：{start_time} ~ {end_time}")
        
        all_data = []
        with ThreadPoolExecutor(max_workers=self.max_workers, thread_name_prefix="Other") as executor:
            futures = [
                executor.submit(self._request_single_minute, advertiser_id, aweme_id, minute_range, base_params, all_data)
                for minute_range in minute_ranges
            ]
            
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"❌ 任务执行异常：{str(e)}")
        
        if not all_data:
            print("===== 采集完成：未获取到任何数据 =====")
            return pd.DataFrame()
        
        df = pd.DataFrame(all_data)
        print(f"\n===== 采集完成 =====\n总数据条数：{len(df)}\n字段数：{len(df.columns)}")
        
        return df
