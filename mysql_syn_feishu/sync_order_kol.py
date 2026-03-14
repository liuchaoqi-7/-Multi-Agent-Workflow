#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
达人维度表同步 - MySQL -> 飞书

MySQL表：dim.dim_达人维度 
飞书表：tbljS1RFr11jJBTU
主键：达人UID
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from feishu_sync import FeishuConfig, MySQLConfig, MySQLToFeishuSync

FEISHU_CONFIG = FeishuConfig(
    app_id="",
    app_secret="",
    app_token="",
    table_id="",
    primary_key_field="达人UID",
    batch_size=50,
    sleep_time=0.3
)

MYSQL_CONFIG = MySQLConfig(
    host="",
    port=3306,
    user="",
    password="",
    database="dim",
    target_table="dim_达人维度",
    update_time_field="update_time",
    status_table="sync_status"
)

SYNC_SQL_TEMPLATE = """
    SELECT
        `UID来源`, `达人UID`, `达人名称`, `最早成交时间`, `最后成交时间`,
        `总实付金额` AS `总GMV`,
        `支付订单数`,
        CAST(
            COALESCE(
                REPLACE(REPLACE(TRIM(`总GSV`), ',', ''), '元', ''),
                0
            ) AS DECIMAL(10,2)
        ) AS `总GSV`,
        `最早投流时间`, `最后投流时间`,
        `是否投流`, `账号类型`, `create_time`, `update_time`,
        NOW() AS `最后更新时间`
    FROM dim.dim_达人维度
"""


def sync_order_kol(full_sync: bool = False):
    syncer = MySQLToFeishuSync(FEISHU_CONFIG, MYSQL_CONFIG)
    return syncer.sync(sql_template=SYNC_SQL_TEMPLATE, full_sync=full_sync)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="达人维度表同步 - MySQL->飞书")
    parser.add_argument("--full-sync", action="store_true", help="执行全量同步")
    args = parser.parse_args()
    
    sync_order_kol(full_sync=args.full_sync)
