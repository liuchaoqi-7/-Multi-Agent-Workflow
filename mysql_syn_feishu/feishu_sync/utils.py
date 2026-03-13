import json
import re
from datetime import datetime, date
from decimal import Decimal
from typing import Any, Dict, Optional, Union
import pandas as pd


def clean_value_for_sql(value: Any) -> Any:
    if value is None or (isinstance(value, str) and value.strip().lower() in ['nan', 'null', 'none', '']):
        return None
    if isinstance(value, (int, float, str, bool)):
        return value
    if isinstance(value, (list, dict)):
        try:
            return json.dumps(value, ensure_ascii=False)
        except Exception:
            return str(value)
    if isinstance(value, Decimal):
        return float(value)
    return str(value)


def clean_value_for_feishu(value: Any, field_type: int = 1) -> Any:
    TYPE_TEXT = 1
    TYPE_NUMBER = 2
    TYPE_DATE = 5
    TYPE_CHECKBOX = 7
    TYPE_URL = 15
    
    if value is None or (isinstance(value, str) and value.strip().lower() in ['nan', 'null', 'none', '']):
        if field_type == TYPE_NUMBER:
            return None
        if field_type == TYPE_CHECKBOX:
            return False
        return None
    
    try:
        if field_type == TYPE_DATE:
            if isinstance(value, (int, float, Decimal)):
                return int(value)
            if isinstance(value, (datetime, date)):
                if isinstance(value, date) and not isinstance(value, datetime):
                    value = datetime.combine(value, datetime.min.time())
                return int(value.timestamp() * 1000)
            if isinstance(value, str):
                val_str = value.strip()
                if not val_str or val_str == '0000-00-00':
                    return None
                if val_str.replace('.', '', 1).isdigit():
                    return int(float(val_str))
                try:
                    dt = None
                    if len(val_str) == 10:
                        dt = datetime.strptime(val_str, "%Y-%m-%d")
                    elif len(val_str) >= 19:
                        dt = datetime.strptime(val_str[:19], "%Y-%m-%d %H:%M:%S")
                    if dt:
                        return int(dt.timestamp() * 1000)
                except:
                    pass
                return None
        
        elif field_type == TYPE_NUMBER:
            if isinstance(value, Decimal):
                return float(value)
            if isinstance(value, str):
                clean_val = value.replace(",", "").strip()
                return float(clean_val) if clean_val else None
            return value
        
        elif field_type in [TYPE_TEXT, TYPE_URL]:
            if isinstance(value, (dict, list)):
                return json.dumps(value, ensure_ascii=False)
            return str(value)
        
        elif field_type == TYPE_CHECKBOX:
            return bool(value)
        
        else:
            return value
            
    except Exception:
        return None


def ms_timestamp_to_datetime_str(ms_timestamp: Union[int, float, None]) -> Optional[str]:
    if ms_timestamp is None or ms_timestamp == 0:
        return None
    try:
        return datetime.fromtimestamp(ms_timestamp / 1000).strftime('%Y-%m-%d %H:%M:%S')
    except (ValueError, TypeError):
        return None


def datetime_to_ms_timestamp(dt: Union[datetime, date, str, None]) -> Optional[int]:
    if dt is None:
        return None
    try:
        if isinstance(dt, str):
            if len(dt) == 10:
                dt = datetime.strptime(dt, "%Y-%m-%d")
            elif len(dt) >= 19:
                dt = datetime.strptime(dt[:19], "%Y-%m-%d %H:%M:%S")
            else:
                return None
        if isinstance(dt, date) and not isinstance(dt, datetime):
            dt = datetime.combine(dt, datetime.min.time())
        return int(dt.timestamp() * 1000)
    except:
        return None


def clean_primary_key(value: Any) -> str:
    if value is None or value == "":
        return ""
    clean_id = str(value).strip()
    clean_id = re.sub(r'[^0-9a-zA-Z]', '', clean_id)
    if clean_id.isdigit():
        clean_id = str(int(clean_id))
    return clean_id


def clean_dataframe_for_sql(df: pd.DataFrame, datetime_fields: list = None) -> pd.DataFrame:
    df = df.map(clean_value_for_sql)
    if datetime_fields:
        for field in datetime_fields:
            if field in df.columns:
                df[field] = df[field].apply(ms_timestamp_to_datetime_str)
    return df


def clean_record_for_feishu(record: Dict, field_type_mapping: Dict) -> Dict:
    fields = {}
    for key, value in record.items():
        if key not in field_type_mapping:
            continue
        f_type = field_type_mapping[key]
        val = clean_value_for_feishu(value, f_type)
        if val is not None:
            fields[key] = val
    return fields
