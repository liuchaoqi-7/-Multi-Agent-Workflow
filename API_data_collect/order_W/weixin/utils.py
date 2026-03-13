from datetime import datetime
from typing import Any, Dict, Union


def format_time(timestamp: Union[int, float, str, None]) -> str:
    try:
        if not timestamp or str(timestamp).strip() in ["0", ""]:
            return ""
        ts = int(float(str(timestamp).strip()))
        return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
    except:
        return str(timestamp) if isinstance(timestamp, str) and len(str(timestamp)) > 5 else ""


def format_amount(amount: Union[int, float, str, None]) -> float:
    try:
        return round(float(amount) / 100, 2) if amount and str(amount).strip() != "0" else 0.00
    except:
        return 0.00


def datetime_to_timestamp(date_str: str, format_str: str = "%Y-%m-%d %H:%M:%S", is_milliseconds: bool = True) -> int:
    if not date_str:
        return 0
    try:
        dt_object = datetime.strptime(date_str, format_str)
        timestamp = datetime.timestamp(dt_object)
        if is_milliseconds:
            timestamp = int(timestamp * 1000)
        else:
            timestamp = int(timestamp)
        return timestamp
    except Exception as e:
        print(f"日期转换失败: {e}")
        return 0


def str_to_ts(date_str: str) -> int:
    return int(datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S").timestamp())


def ts_to_str(timestamp: int) -> str:
    return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S") if timestamp else ""


def cents_to_yuan(cents: int) -> float:
    return round(cents / 100, 2) if cents and isinstance(cents, (int, float)) else 0.0


def get_mapped_value(enum_map: Dict, value: Any, default: str = "") -> str:
    if value is None:
        return default
    return enum_map.get(value, f"{default}({value})" if value is not None else default)


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


def safe_get(data: Dict, keys: list, default: Any = "") -> Any:
    current = data
    for key in keys:
        if isinstance(current, dict) and key in current:
            current = current[key]
        else:
            return default
    return current


def safe_get_first(lst: list) -> Dict:
    return lst[0] if isinstance(lst, list) and len(lst) > 0 else {}
