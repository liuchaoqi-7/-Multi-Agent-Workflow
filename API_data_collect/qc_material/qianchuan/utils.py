from datetime import datetime, timedelta
from typing import List, Tuple


def split_time_by_day_then_minute(total_start: str, total_end: str) -> List[Tuple[str, str]]:
    all_minute_list = []
    total_start_dt = datetime.strptime(total_start, "%Y-%m-%d %H:%M:%S")
    total_end_dt = datetime.strptime(total_end, "%Y-%m-%d %H:%M:%S")
    
    current_day = total_start_dt
    while current_day < total_end_dt:
        day_start = current_day.replace(hour=0, minute=0, second=0)
        day_end = day_start + timedelta(days=1) - timedelta(seconds=1)
        
        if day_end > total_end_dt:
            day_end = total_end_dt
        
        current_minute = day_start
        while current_minute <= day_end:
            minute_start = current_minute.replace(second=0)
            minute_end = minute_start + timedelta(minutes=1) - timedelta(seconds=1)
            
            if minute_end > total_end_dt:
                minute_end = total_end_dt
            
            start_str = minute_start.strftime("%Y-%m-%d %H:%M:%S")
            end_str = minute_end.strftime("%Y-%m-%d %H:%M:%S")
            all_minute_list.append((start_str, end_str))
            
            current_minute = minute_start + timedelta(minutes=1)
        
        current_day += timedelta(days=1)
    
    return all_minute_list


def split_time_by_hour(total_start: str, total_end: str) -> List[Tuple[str, str]]:
    all_hour_list = []
    
    try:
        total_start_dt = datetime.strptime(total_start, "%Y-%m-%d %H:%M:%S")
        total_end_dt = datetime.strptime(total_end, "%Y-%m-%d %H:%M:%S")
    except ValueError as e:
        print(f"时间格式解析错误：{e}")
        return all_hour_list
    
    if total_start_dt >= total_end_dt:
        return all_hour_list
    
    current_hour_start = total_start_dt.replace(minute=0, second=0)
    while current_hour_start < total_end_dt:
        current_hour_end = current_hour_start + timedelta(hours=1) - timedelta(seconds=1)
        
        if current_hour_end > total_end_dt:
            current_hour_end = total_end_dt
        
        if current_hour_start >= current_hour_end:
            break
        
        start_str = current_hour_start.strftime("%Y-%m-%d %H:%M:%S")
        end_str = current_hour_end.strftime("%Y-%m-%d %H:%M:%S")
        all_hour_list.append((start_str, end_str))
        
        current_hour_start += timedelta(hours=1)
    
    return all_hour_list


def split_time_by_day(total_start: str, total_end: str) -> List[Tuple[str, str]]:
    all_day_list = []
    
    try:
        total_start_dt = datetime.strptime(total_start, "%Y-%m-%d %H:%M:%S")
        total_end_dt = datetime.strptime(total_end, "%Y-%m-%d %H:%M:%S")
    except ValueError as e:
        print(f"时间格式解析错误：{e}")
        return all_day_list
    
    if total_start_dt >= total_end_dt:
        return all_day_list
    
    current_day_start = total_start_dt.replace(hour=0, minute=0, second=0)
    while current_day_start < total_end_dt:
        current_day_end = current_day_start + timedelta(days=1) - timedelta(seconds=1)
        
        if current_day_end > total_end_dt:
            current_day_end = total_end_dt
        
        if current_day_start >= current_day_end:
            break
        
        start_str = current_day_start.strftime("%Y-%m-%d %H:%M:%S")
        end_str = current_day_end.strftime("%Y-%m-%d %H:%M:%S")
        all_day_list.append((start_str, end_str))
        
        current_day_start += timedelta(days=1)
    
    return all_day_list


def get_default_time_range() -> Tuple[str, str]:
    now = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    one_day_ago = now - timedelta(days=1)
    return one_day_ago.strftime("%Y-%m-%d %H:%M:%S"), now.strftime("%Y-%m-%d %H:%M:%S")
