#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
微信视频号小店数据采集工具

功能：
- 订单数据采集（支持多线程）
- 售后数据采集（支持多线程）
- 结算流水数据采集（支持分页）

使用方法：
    python main.py --app-id <APP_ID> --app-secret <APP_SECRET> --data-type order --start-date "2024-01-01 00:00:00" --end-date "2024-01-31 23:59:59"

环境变量：
    WEIXIN_APP_ID: 微信小程序AppID
    WEIXIN_APP_SECRET: 微信小程序AppSecret
    DB_HOST: 数据库主机
    DB_PORT: 数据库端口
    DB_USER: 数据库用户名
    DB_PASSWORD: 数据库密码
    DB_NAME: 数据库名称
"""

import argparse
import os
import sys
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine

from weixin import (
    WeChatAPIClient, WeChatTokenManager, OrderService,
    AftersaleService, SettleService, DEFAULT_TABLE_NAMES
)
from weixin.utils import str_to_ts


def get_db_engine(db_host: str, db_port: int, db_user: str, db_password: str, db_name: str):
    return create_engine(
        f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}?charset=utf8mb4"
    )


def collect_orders(
    client: WeChatAPIClient,
    start_date: str,
    end_date: str,
    db_engine,
    table_name: str = DEFAULT_TABLE_NAMES["order"],
    max_workers: int = 20
):
    print(f"\n{'='*60}")
    print(f"开始采集订单数据")
    print(f"时间范围：{start_date} 至 {end_date}")
    print(f"{'='*60}")
    
    order_service = OrderService(client, max_workers=max_workers)
    
    start_ts = str_to_ts(start_date)
    end_ts = str_to_ts(end_date)
    
    order_ids = order_service.get_order_list(start_ts, end_ts)
    
    if not order_ids:
        print("未获取到订单ID")
        return
    
    df = order_service.batch_get_order_details(order_ids)
    
    if df.empty:
        print("未获取到订单详情")
        return
    
    df = df.drop_duplicates(subset=['订单ID'], keep='last')
    
    if db_engine:
        df.to_sql(
            name=table_name,
            con=db_engine,
            if_exists="append",
            index=False,
            chunksize=10000
        )
        print(f"✅ 订单数据已写入数据库表：{table_name}")
    
    print(f"📊 共采集订单数据：{len(df)}条")


def collect_aftersales(
    client: WeChatAPIClient,
    start_date: str,
    end_date: str,
    db_engine,
    table_name: str = DEFAULT_TABLE_NAMES["aftersale"],
    max_workers: int = 10
):
    print(f"\n{'='*60}")
    print(f"开始采集售后数据")
    print(f"时间范围：{start_date} 至 {end_date}")
    print(f"{'='*60}")
    
    aftersale_service = AftersaleService(client, max_workers=max_workers)
    
    start_ts = str_to_ts(start_date)
    end_ts = str_to_ts(end_date)
    
    aftersale_ids = aftersale_service.get_aftersale_list(start_ts, end_ts)
    
    if not aftersale_ids:
        print("未获取到售后ID")
        return
    
    df = aftersale_service.batch_get_aftersale_details(aftersale_ids)
    
    if df.empty:
        print("未获取到售后详情")
        return
    
    df = df.drop_duplicates(subset=['售后单ID'], keep='last')
    
    if db_engine:
        df.to_sql(
            name=table_name,
            con=db_engine,
            if_exists="append",
            index=False,
            chunksize=10000
        )
        print(f"✅ 售后数据已写入数据库表：{table_name}")
    
    print(f"📊 共采集售后数据：{len(df)}条")


def collect_settlements(
    client: WeChatAPIClient,
    db_engine,
    table_name: str = DEFAULT_TABLE_NAMES["settle"],
    order_settle_state: int = 0
):
    print(f"\n{'='*60}")
    print(f"开始采集结算流水数据")
    print(f"{'='*60}")
    
    settle_service = SettleService(client)
    
    df = settle_service.get_settle_list(order_settle_state=order_settle_state)
    
    if df.empty:
        print("未获取到结算流水数据")
        return
    
    df = df.drop_duplicates(subset=['订单ID'], keep='last')
    
    if db_engine:
        df.to_sql(
            name=table_name,
            con=db_engine,
            if_exists="append",
            index=False,
            chunksize=10000
        )
        print(f"✅ 结算数据已写入数据库表：{table_name}")
    
    print(f"📊 共采集结算数据：{len(df)}条")


def main():
    parser = argparse.ArgumentParser(
        description="微信视频号小店数据采集工具",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        "--app-id",
        type=str,
        default=os.environ.get("WEIXIN_APP_ID"),
        help="微信小程序AppID（可从环境变量WEIXIN_APP_ID获取）"
    )
    parser.add_argument(
        "--app-secret",
        type=str,
        default=os.environ.get("WEIXIN_APP_SECRET"),
        help="微信小程序AppSecret（可从环境变量WEIXIN_APP_SECRET获取）"
    )
    parser.add_argument(
        "--data-type",
        type=str,
        choices=["order", "aftersale", "settle", "all"],
        default="all",
        help="数据类型：order(订单)、aftersale(售后)、settle(结算)、all(全部)"
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default=None,
        help="开始日期，格式：YYYY-MM-DD HH:MM:SS"
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=None,
        help="结束日期，格式：YYYY-MM-DD HH:MM:SS"
    )
    parser.add_argument(
        "--db-host",
        type=str,
        default=os.environ.get("DB_HOST", "localhost"),
        help="数据库主机"
    )
    parser.add_argument(
        "--db-port",
        type=int,
        default=int(os.environ.get("DB_PORT", 3306)),
        help="数据库端口"
    )
    parser.add_argument(
        "--db-user",
        type=str,
        default=os.environ.get("DB_USER"),
        help="数据库用户名"
    )
    parser.add_argument(
        "--db-password",
        type=str,
        default=os.environ.get("DB_PASSWORD"),
        help="数据库密码"
    )
    parser.add_argument(
        "--db-name",
        type=str,
        default=os.environ.get("DB_NAME"),
        help="数据库名称"
    )
    parser.add_argument(
        "--table-order",
        type=str,
        default=DEFAULT_TABLE_NAMES["order"],
        help="订单表名"
    )
    parser.add_argument(
        "--table-aftersale",
        type=str,
        default=DEFAULT_TABLE_NAMES["aftersale"],
        help="售后表名"
    )
    parser.add_argument(
        "--table-settle",
        type=str,
        default=DEFAULT_TABLE_NAMES["settle"],
        help="结算表名"
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=20,
        help="最大线程数"
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="输出目录（不指定则不保存文件）"
    )
    
    args = parser.parse_args()
    
    if not args.app_id or not args.app_secret:
        print("❌ 错误：必须提供app-id和app-secret参数")
        sys.exit(1)
    
    token_manager = WeChatTokenManager(args.app_id, args.app_secret)
    access_token = token_manager.get_access_token()
    
    if not access_token:
        print("❌ 获取Access Token失败")
        sys.exit(1)
    
    client = WeChatAPIClient(access_token)
    
    db_engine = None
    if args.db_host and args.db_user and args.db_password and args.db_name:
        db_engine = get_db_engine(
            args.db_host, args.db_port,
            args.db_user, args.db_password, args.db_name
        )
    
    if args.start_date and args.end_date:
        start_date = args.start_date
        end_date = args.end_date
    else:
        today = datetime.now().date()
        yesterday = today - timedelta(days=1)
        start_date = f"{yesterday} 00:00:00"
        end_date = f"{today} 00:00:00"
    
    print(f"\n🚀 微信视频号小店数据采集工具启动")
    print(f"📅 采集时间范围：{start_date} 至 {end_date}")
    
    if args.data_type in ["order", "all"]:
        collect_orders(
            client, start_date, end_date, db_engine,
            args.table_order, args.max_workers
        )
    
    if args.data_type in ["aftersale", "all"]:
        collect_aftersales(
            client, start_date, end_date, db_engine,
            args.table_aftersale, args.max_workers
        )
    
    if args.data_type in ["settle", "all"]:
        collect_settlements(
            client, db_engine, args.table_settle
        )
    
    print(f"\n{'='*60}")
    print("🎉 数据采集完成！")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
