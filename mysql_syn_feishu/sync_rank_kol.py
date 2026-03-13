#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
达人榜单表同步 - MySQL -> 飞书

MySQL表：dim.dim_达人榜单
飞书表：tblguTY7iu5fNzWi
主键：达人ID
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
    primary_key_field="达人ID",
    batch_size=50,
    sleep_time=0.3
)

MYSQL_CONFIG = MySQLConfig(
    host="",
    port=3306,
    user="",
    password="",
    database="dim",
    target_table="dim_达人榜单",
    update_time_field="update_time",
    status_table="sync_status"
)

SYNC_SQL_TEMPLATE = """
    SELECT
        (UNIX_TIMESTAMP(`日期`) * 1000) AS `日期`,
        `行业类目`,
        `榜单`,
        `账号类型`,
        `头像URL`,
        `达人ID`,
        `达人昵称`,
        `抖音号ID`,
        `品牌名称`,
        `品牌ID`,
        `机构名称`,
        `机构ID`,
        `粉丝数`,
        `直播今日排名`,
        `直播成交额下限`,
        `直播成交额上限`,
        `直播成交数下限`,
        `直播成交数上限`,
        `直播累计成交额下限`,
        `直播累计成交额上限`,
        `直播累计成交数下限`,
        `直播累计成交数上限`,
        `视频今日排名`,
        `视频成交额下限`,
        `视频成交额上限`,
        `视频成交数下限`,
        `视频成交数上限`,
        `视频累计成交额下限`,
        `视频累计成交额上限`,
        `视频累计成交数下限`,
        `视频累计成交数上限`
    FROM dim.dim_达人榜单
"""


def sync_rank_kol(full_sync: bool = False):
    syncer = MySQLToFeishuSync(FEISHU_CONFIG, MYSQL_CONFIG)
    return syncer.sync(sql_template=SYNC_SQL_TEMPLATE, full_sync=full_sync)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="达人榜单表同步 - MySQL->飞书")
    parser.add_argument("--full-sync", action="store_true", help="执行全量同步")
    args = parser.parse_args()
    
    sync_rank_kol(full_sync=args.full_sync)
