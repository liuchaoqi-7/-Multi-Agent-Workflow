#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
千川素材数据采集工具

功能：
- 直播间画面数据采集（支持多线程）
- 视频素材数据采集（支持多线程）
- 其他创意数据采集（支持多线程）

使用方法：
    python main.py --app-id <APP_ID> --app-secret <APP_SECRET> --refresh-token <REFRESH_TOKEN> --data-type live --start-date "2024-01-01 00:00:00" --end-date "2024-01-31 23:59:59"

环境变量：
    QIANCHUAN_APP_ID: 千川应用ID
    QIANCHUAN_APP_SECRET: 千川应用密钥
    QIANCHUAN_REFRESH_TOKEN: 刷新Token
    DB_HOST: 数据库主机
    DB_PORT: 数据库端口
    DB_USER: 数据库用户名
    DB_PASSWORD: 数据库密码
    DB_NAME: 数据库名称
"""

import argparse
import os
import sys
from datetime import datetime
from typing import Optional, List

import pandas as pd
from sqlalchemy import create_engine

from qianchuan import (
    QianChuanClient, QianChuanTokenManager, LiveService,
    VideoService, OtherService, DEFAULT_TABLE_NAMES, get_default_time_range
)


def get_db_engine(db_host: str, db_port: int, db_user: str, db_password: str, db_name: str):
    return create_engine(
        f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}?charset=utf8mb4"
    )


def parse_accounts(accounts_str: str) -> List[tuple]:
    accounts = []
    if accounts_str:
        for account in accounts_str.split(","):
            parts = account.strip().split(":")
            if len(parts) >= 3:
                advertiser_id, aweme_id, account_name = parts[0], parts[1], parts[2]
                accounts.append((advertiser_id, aweme_id, account_name))
            elif len(parts) == 2:
                accounts.append((parts[0], parts[1], ""))
    return accounts


def collect_live(
    client: QianChuanClient,
    accounts: List[tuple],
    start_date: str,
    end_date: str,
    db_engine,
    table_name: str = DEFAULT_TABLE_NAMES["live"],
    max_workers: int = 40
):
    print(f"\n{'='*60}")
    print(f"开始采集直播间画面数据")
    print(f"时间范围：{start_date} 至 {end_date}")
    print(f"{'='*60}")
    
    service = LiveService(client, max_workers=max_workers)
    
    for advertiser_id, aweme_id, account_name in accounts:
        try:
            print(f"\n===== 开始处理: {account_name} (ID: {advertiser_id}) =====")
            df = service.collect_data(advertiser_id, aweme_id, start_date, end_date)
            
            if df.empty:
                continue
            
            if db_engine:
                df.to_sql(
                    name=table_name,
                    con=db_engine,
                    if_exists="append",
                    index=False,
                    chunksize=10000
                )
                print(f"✅ 数据已写入数据库表：{table_name}")
            
            print(f"📊 共采集数据：{len(df)}条")
            
        except Exception as e:
            print(f"处理账户 {account_name} 时发生错误: {e}")


def collect_video(
    client: QianChuanClient,
    accounts: List[tuple],
    start_date: str,
    end_date: str,
    db_engine,
    table_name: str = DEFAULT_TABLE_NAMES["video"],
    max_workers: int = 40
):
    print(f"\n{'='*60}")
    print(f"开始采集视频素材数据")
    print(f"时间范围：{start_date} 至 {end_date}")
    print(f"{'='*60}")
    
    service = VideoService(client, max_workers=max_workers)
    
    for advertiser_id, aweme_id, account_name in accounts:
        try:
            print(f"\n===== 开始处理: {account_name} (ID: {advertiser_id}) =====")
            df = service.collect_data(advertiser_id, aweme_id, start_date, end_date)
            
            if df.empty:
                continue
            
            if db_engine:
                df.to_sql(
                    name=table_name,
                    con=db_engine,
                    if_exists="append",
                    index=False,
                    chunksize=10000
                )
                print(f"✅ 数据已写入数据库表：{table_name}")
            
            print(f"📊 共采集数据：{len(df)}条")
            
        except Exception as e:
            print(f"处理账户 {account_name} 时发生错误: {e}")


def collect_other(
    client: QianChuanClient,
    accounts: List[tuple],
    start_date: str,
    end_date: str,
    db_engine,
    table_name: str = DEFAULT_TABLE_NAMES["other"],
    max_workers: int = 40
):
    print(f"\n{'='*60}")
    print(f"开始采集其他创意数据")
    print(f"时间范围：{start_date} 至 {end_date}")
    print(f"{'='*60}")
    
    service = OtherService(client, max_workers=max_workers)
    
    for advertiser_id, aweme_id, account_name in accounts:
        try:
            print(f"\n===== 开始处理: {account_name} (ID: {advertiser_id}) =====")
            df = service.collect_data(advertiser_id, aweme_id, start_date, end_date)
            
            if df.empty:
                continue
            
            if db_engine:
                df.to_sql(
                    name=table_name,
                    con=db_engine,
                    if_exists="append",
                    index=False,
                    chunksize=10000
                )
                print(f"✅ 数据已写入数据库表：{table_name}")
            
            print(f"📊 共采集数据：{len(df)}条")
            
        except Exception as e:
            print(f"处理账户 {account_name} 时发生错误: {e}")


def main():
    parser = argparse.ArgumentParser(
        description="千川素材数据采集工具",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        "--app-id",
        type=str,
        default=os.environ.get("QIANCHUAN_APP_ID"),
        help="千川应用ID（可从环境变量QIANCHUAN_APP_ID获取）"
    )
    parser.add_argument(
        "--app-secret",
        type=str,
        default=os.environ.get("QIANCHUAN_APP_SECRET"),
        help="千川应用密钥（可从环境变量QIANCHUAN_APP_SECRET获取）"
    )
    parser.add_argument(
        "--refresh-token",
        type=str,
        default=os.environ.get("QIANCHUAN_REFRESH_TOKEN"),
        help="刷新Token（可从环境变量QIANCHUAN_REFRESH_TOKEN获取）"
    )
    parser.add_argument(
        "--token-file",
        type=str,
        default=None,
        help="Token存储文件路径"
    )
    parser.add_argument(
        "--data-type",
        type=str,
        choices=["live", "video", "other", "all"],
        default="all",
        help="数据类型：live(直播间画面)、video(视频)、other(其他创意)、all(全部)"
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
        "--accounts",
        type=str,
        default=None,
        help="账户列表，格式：advertiser_id:aweme_id:account_name,多个账户用逗号分隔"
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
        "--table-live",
        type=str,
        default=DEFAULT_TABLE_NAMES["live"],
        help="直播间画面表名"
    )
    parser.add_argument(
        "--table-video",
        type=str,
        default=DEFAULT_TABLE_NAMES["video"],
        help="视频素材表名"
    )
    parser.add_argument(
        "--table-other",
        type=str,
        default=DEFAULT_TABLE_NAMES["other"],
        help="其他创意表名"
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=40,
        help="最大线程数"
    )
    
    args = parser.parse_args()
    
    if not args.app_id or not args.app_secret:
        print("❌ 错误：必须提供app-id和app-secret参数")
        sys.exit(1)
    
    token_manager = QianChuanTokenManager(args.app_id, args.app_secret, args.token_file)
    
    access_token = None
    if args.refresh_token:
        token_info = token_manager.refresh_token(args.refresh_token)
        if token_info:
            access_token = token_info.get("access_token")
    elif args.token_file:
        token_info = token_manager.load_token()
        if token_info:
            access_token = token_info.get("access_token")
    
    if not access_token:
        print("❌ 获取Access Token失败")
        sys.exit(1)
    
    client = QianChuanClient(access_token)
    
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
        start_date, end_date = get_default_time_range()
    
    accounts = []
    if args.accounts:
        accounts = parse_accounts(args.accounts)
    
    if not accounts:
        print("❌ 错误：必须提供账户列表")
        sys.exit(1)
    
    print(f"\n🚀 千川素材数据采集工具启动")
    print(f"📅 采集时间范围：{start_date} 至 {end_date}")
    print(f"👥 账户数量：{len(accounts)}")
    
    start_total = datetime.now()
    
    if args.data_type in ["live", "all"]:
        collect_live(
            client, accounts, start_date, end_date, db_engine,
            args.table_live, args.max_workers
        )
    
    if args.data_type in ["video", "all"]:
        collect_video(
            client, accounts, start_date, end_date, db_engine,
            args.table_video, args.max_workers
        )
    
    if args.data_type in ["other", "all"]:
        collect_other(
            client, accounts, start_date, end_date, db_engine,
            args.table_other, args.max_workers
        )
    
    end_total = datetime.now()
    total_seconds = (end_total - start_total).total_seconds()
    
    print(f"\n{'='*60}")
    print(f"🎉 数据采集完成！总耗时：{total_seconds:.2f} 秒")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
