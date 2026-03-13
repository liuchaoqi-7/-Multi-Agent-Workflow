import json
import random
import time
import hashlib
import hmac
from datetime import datetime, timedelta
from typing import Any, Dict, Optional
from collections import OrderedDict


def flatten_json(data: Any, parent_key: str = "", sep: str = "_") -> Dict[str, Any]:
    items = {}
    
    if isinstance(data, dict):
        for k, v in data.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            items.update(flatten_json(v, new_key, sep))
    elif isinstance(data, list):
        for i, v in enumerate(data):
            new_key = f"{parent_key}{sep}{i}" if parent_key else str(i)
            items.update(flatten_json(v, new_key, sep))
    else:
        items[parent_key] = data if data is not None else ""
    
    return items


def datetime_to_timestamp(date_str: str, format_str: str = "%Y-%m-%d %H:%M:%S") -> int:
    if not date_str:
        return 0
    try:
        dt_object = datetime.strptime(date_str, format_str)
        return int(dt_object.timestamp())
    except Exception as e:
        print(f"日期转换失败: {e}")
        return 0


def timestamp_to_datetime(timestamp: int) -> str:
    if not timestamp:
        return ""
    try:
        ts = int(timestamp)
        if ts > 10000000000:
            ts = ts // 1000
        return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
    except Exception as e:
        return ""


def format_time(timestamp: Any) -> str:
    try:
        if not timestamp or str(timestamp).strip() in ["0", "", "None", "null", "NaN"]:
            return ""
        
        ts_str = str(timestamp).strip()
        ts = int(float(ts_str))
        
        ts_len = len(str(abs(ts)))
        if ts_len == 13:
            ts = ts // 1000
        elif ts_len < 10 or ts_len > 10 or ts < 0 or ts > 253402300799:
            return ""
        
        try:
            return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
        except (OSError, ValueError):
            return datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
    
    except:
        ts_str = str(timestamp).strip()
        if isinstance(timestamp, str) and len(ts_str) >= 8 and ("-" in ts_str or ":" in ts_str):
            return ts_str
        return ""


def format_amount(amount: Any) -> float:
    try:
        return round(float(amount) / 100, 2) if amount and str(amount).strip() != "0" else 0.00
    except:
        return 0.00


def get_mapped_value(map_dict: Dict, key: Any, default_value: str = "未知") -> str:
    return map_dict.get(key, str(key) if key is not None else default_value)


def sorted_json_dumps(data: Any) -> str:
    def _sort_dict(d: Any) -> Any:
        if isinstance(d, dict):
            sorted_items = sorted(d.items(), key=lambda x: x[0])
            ordered_dict = OrderedDict()
            for k, v in sorted_items:
                ordered_dict[k] = _sort_dict(v)
            return ordered_dict
        elif isinstance(d, list):
            return [_sort_dict(item) for item in d]
        elif isinstance(d, float) and d.is_integer():
            return int(d)
        else:
            return d
    
    sorted_data = _sort_dict(data)
    return json.dumps(sorted_data, ensure_ascii=False, separators=(',', ':'))


def generate_signature(app_key: str, app_secret: str, access_token: str, 
                       timestamp: str, method: str, param_json: str) -> str:
    sign_str = f"{app_key}{timestamp}{access_token}{method}{param_json}"
    signature = hmac.new(
        app_secret.encode('utf-8'),
        sign_str.encode('utf-8'),
        hashlib.sha256
    ).hexdigest()
    return signature


def get_yesterday_range() -> tuple:
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday = today - timedelta(days=1)
    return yesterday.strftime("%Y-%m-%d %H:%M:%S"), today.strftime("%Y-%m-%d %H:%M:%S")


def get_date_range(days: int = 7) -> tuple:
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    start_date = today - timedelta(days=days)
    return start_date.strftime("%Y-%m-%d %H:%M:%S"), today.strftime("%Y-%m-%d %H:%M:%S")


def random_delay(min_seconds: float = 1.0, max_seconds: float = 2.0) -> None:
    delay = random.uniform(min_seconds, max_seconds)
    time.sleep(delay)


def retry_with_backoff(func, max_retries: int = 3, base_delay: float = 0.3, 
                       random_delay_range: tuple = (1.0, 2.0)):
    def wrapper(*args, **kwargs):
        for attempt in range(max_retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if attempt == max_retries - 1:
                    raise e
                delay = base_delay * (2 ** attempt) + random.uniform(*random_delay_range)
                print(f"重试 {attempt + 1}/{max_retries}, 等待 {delay:.2f}s: {str(e)}")
                time.sleep(delay)
        return None
    return wrapper
