#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
主播排期表同步 - 飞书 -> MySQL

飞书表：tblTwJXtn4HWRGiN
MySQL表：ods.ods_主播排期表
主键：索引
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from feishu_sync import FeishuConfig, MySQLConfig, FeishuToMySQLSync

FEISHU_CONFIG = FeishuConfig(
    app_id="",
    app_secret="",
    app_token="",
    table_id="",
    primary_key_field="索引",
    batch_size=100,
    sleep_time=0.5,
    datetime_fields=["开始时间", "日期", "结束时间"],
    field_rename_map={"索引": "索引"}
)

MYSQL_CONFIG = MySQLConfig(
    host="",
    port=3306,
    user="",
    password="",
    database="ods",
    target_table="ods_主播排期表",
    status_table="sync_status"
)


def sync_live_author_schedule(full_sync: bool = False):
    syncer = FeishuToMySQLSync(FEISHU_CONFIG, MYSQL_CONFIG)
    return syncer.sync(full_sync=full_sync)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="主播排期表同步 - 飞书->MySQL")
    parser.add_argument("--full-sync", action="store_true", help="执行全量同步")
    args = parser.parse_args()
    
    sync_live_author_schedule(full_sync=args.full_sync)
