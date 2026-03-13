#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
商品维度表同步 - MySQL -> 飞书

MySQL表：dim.dim_商品纬度
飞书表：tble0SWNIt1EnBkv
主键：商品ID
"""

import sys
import os
import re
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from feishu_sync import FeishuConfig, MySQLConfig, MySQLToFeishuSync
from feishu_sync.utils import clean_primary_key

FEISHU_CONFIG = FeishuConfig(
    app_id="",
    app_secret="",
    app_token="",
    table_id="",
    primary_key_field="商品ID",
    batch_size=50,
    sleep_time=0.3
)

MYSQL_CONFIG = MySQLConfig(
    host="",
    port=3306,
    user="",
    password="",
    database="dim",
    target_table="dim_商品纬度",
    update_time_field="update_time",
    status_table="sync_status"
)

SYNC_SQL_TEMPLATE = """
    SELECT
        `商品ID来源` AS `平台`,
        `商品ID`,
        `商品名称`
    FROM dim.dim_商品纬度
"""


def custom_clean_func(key: str, value, field_type: int):
    if key == "商品ID":
        return clean_primary_key(value)
    from feishu_sync.utils import clean_value_for_feishu
    return clean_value_for_feishu(value, field_type)


def sync_order_product(full_sync: bool = False):
    syncer = MySQLToFeishuSync(FEISHU_CONFIG, MYSQL_CONFIG)
    return syncer.sync(
        sql_template=SYNC_SQL_TEMPLATE, 
        full_sync=full_sync,
        clean_func=custom_clean_func
    )


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="商品维度表同步 - MySQL->飞书")
    parser.add_argument("--full-sync", action="store_true", help="执行全量同步")
    args = parser.parse_args()
    
    sync_order_product(full_sync=args.full_sync)
