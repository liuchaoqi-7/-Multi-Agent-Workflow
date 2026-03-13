import json
from datetime import datetime
from typing import Any, Dict
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


def normalize_data(raw_data: list) -> "pd.DataFrame":
    import pandas as pd
    
    if not raw_data:
        return pd.DataFrame()
    
    flat_records = [flatten_json(record) for record in raw_data]
    df = pd.DataFrame(flat_records)
    
    return df


def datetime_to_timestamp(date_str, format_str="%Y-%m-%d %H:%M:%S", is_milliseconds=False):
    if not date_str:
        return 0
    
    try:
        dt_object = datetime.strptime(date_str, format_str)
        timestamp = dt_object.timestamp()
        if is_milliseconds:
            timestamp = int(timestamp * 1000)
        else:
            timestamp = int(timestamp)
        return timestamp
    except Exception as e:
        print(f"日期转换失败: {e}")
        return 0


def format_time(timestamp):
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


def format_amount(amount):
    try:
        return round(float(amount)/100, 2) if amount and str(amount).strip() != "0" else 0.00
    except:
        return 0.00


def get_mapped_value(map_dict, key, default_value="未知"):
    return map_dict.get(key, key) if key is not None else default_value


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
