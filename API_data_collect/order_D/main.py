import os
import argparse
from datetime import datetime, timedelta
from pprint import pprint
from sqlalchemy import create_engine

from doudian import (
    DouDianAPIClient,
    DouDianTokenCreator,
    OrderService,
    AftersaleService,
    SettleService,
    DEFAULT_TABLE_NAMES,
)
from doudian.utils import datetime_to_timestamp


def parse_args():
    parser = argparse.ArgumentParser(
        description="抖店订单/售后/结算数据采集脚本",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  # 使用默认时间（昨天）采集数据
  python main.py --app_key YOUR_KEY --app_secret YOUR_SECRET --db_host localhost --db_user root --db_password pwd --db_name test

  # 指定时间范围采集数据
  python main.py --app_key YOUR_KEY --app_secret YOUR_SECRET \\
      --order_start "2024-01-01 00:00:00" --order_end "2024-01-02 00:00:00" \\
      --settle_start "2024-01-01 00:00:00" \\
      --db_host localhost --db_user root --db_password pwd --db_name test

  # 使用环境变量传入敏感参数
  export DOUDIAN_APP_KEY=YOUR_KEY
  export DOUDIAN_APP_SECRET=YOUR_SECRET
  python main.py --db_host localhost --db_user root --db_password pwd --db_name test
        """
    )
    
    parser.add_argument("--app_key", type=str, 
                        default=os.environ.get("DOUDIAN_APP_KEY"),
                        help="抖店应用App Key (也可通过环境变量DOUDIAN_APP_KEY传入)")
    parser.add_argument("--app_secret", type=str,
                        default=os.environ.get("DOUDIAN_APP_SECRET"),
                        help="抖店应用App Secret (也可通过环境变量DOUDIAN_APP_SECRET传入)")
    parser.add_argument("--access_token", type=str, default=None,
                        help="抖店Access Token (可选，不传则自动刷新token)")
    
    parser.add_argument("--order_start", type=str, default=None,
                        help="订单查询开始时间 (格式: YYYY-MM-DD HH:MM:SS，默认昨天凌晨)")
    parser.add_argument("--order_end", type=str, default=None,
                        help="订单查询结束时间 (格式: YYYY-MM-DD HH:MM:SS，默认今天凌晨)")
    parser.add_argument("--settle_start", type=str, default=None,
                        help="结算数据开始时间 (格式: YYYY-MM-DD HH:MM:SS，默认2024-04-01)")
    
    parser.add_argument("--db_host", type=str, required=True,
                        help="数据库主机地址")
    parser.add_argument("--db_port", type=int, default=3306,
                        help="数据库端口 (默认3306)")
    parser.add_argument("--db_user", type=str, required=True,
                        help="数据库用户名")
    parser.add_argument("--db_password", type=str, required=True,
                        help="数据库密码")
    parser.add_argument("--db_name", type=str, required=True,
                        help="数据库名称")
    
    parser.add_argument("--order_table", type=str, default=DEFAULT_TABLE_NAMES["order"],
                        help=f"订单数据表名 (默认: {DEFAULT_TABLE_NAMES['order']})")
    parser.add_argument("--aftersale_table", type=str, default=DEFAULT_TABLE_NAMES["aftersale"],
                        help=f"售后数据表名 (默认: {DEFAULT_TABLE_NAMES['aftersale']})")
    parser.add_argument("--settle_table", type=str, default=DEFAULT_TABLE_NAMES["settle"],
                        help=f"结算数据表名 (默认: {DEFAULT_TABLE_NAMES['settle']})")
    
    parser.add_argument("--token_file", type=str, 
                        default="/Users/test/Documents/python代码/数据处理/数据获取/AccessToken/D_api_accesstoken.json",
                        help="Token文件路径")
    
    parser.add_argument("--skip_orders", action="store_true",
                        help="跳过订单数据采集")
    parser.add_argument("--skip_aftersales", action="store_true",
                        help="跳过售后数据采集")
    parser.add_argument("--skip_settle", action="store_true",
                        help="跳过结算数据采集")
    
    parser.add_argument("--max_workers", type=int, default=60,
                        help="多线程最大线程数 (默认60)")
    
    args = parser.parse_args()
    
    if not args.app_key or not args.app_secret:
        parser.error("必须提供 --app_key 和 --app_secret 参数，或设置环境变量 DOUDIAN_APP_KEY 和 DOUDIAN_APP_SECRET")
    
    return args


def main():
    args = parse_args()
    
    APP_KEY = args.app_key
    APP_SECRET = args.app_secret
    
    BUSINESS_PARAMS = {
        "grant_type": "authorization_self",
        "code": "",
    }
    
    client_token = DouDianTokenCreator(APP_KEY, APP_SECRET)
    token_data = client_token.create_token(BUSINESS_PARAMS)
    
    if token_data:
        print("\n===== Token创建成功 =====")
        pprint(token_data)
        access_token = token_data.get("access_token")
        refresh_token = token_data.get("refresh_token")
        print(f"\naccess_token: {access_token}")
        print(f"refresh_token: {refresh_token}")
    else:
        print("\n===== Token创建失败 =====")
        exit(1)
    
    client = DouDianAPIClient(
        app_key=APP_KEY,
        app_secret=APP_SECRET,
        access_token=access_token
    )
    
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday = today - timedelta(days=1)
    
    if args.order_start:
        date_start = args.order_start
    else:
        date_start = yesterday.strftime("%Y-%m-%d %H:%M:%S")
    
    if args.order_end:
        date_end = args.order_end
    else:
        date_end = today.strftime("%Y-%m-%d %H:%M:%S")
    
    if args.settle_start:
        date_start2 = args.settle_start
    else:
        date_start2 = "2024-04-01 00:00:00"
    
    print(f"\n{'='*60}")
    print(f"数据采集时间范围:")
    print(f"  订单/售后: {date_start} ~ {date_end}")
    print(f"  结算数据: {date_start2} ~ {date_end}")
    print(f"{'='*60}\n")
    
    START_TIME = datetime_to_timestamp(date_start)
    END_TIME = datetime_to_timestamp(date_end)
    START_TIME2 = datetime_to_timestamp(date_start2)
    
    db_connection_str = f"mysql+pymysql://{args.db_user}:{args.db_password}@{args.db_host}:{args.db_port}/{args.db_name}?charset=utf8mb4"
    engine = create_engine(db_connection_str)
    
    if not args.skip_orders:
        print("\n" + "="*60)
        print("开始采集订单数据...")
        print("="*60)
        order_service = OrderService(client, max_workers=args.max_workers)
        order_params = {
            "create_time_start": START_TIME,
            "create_time_end": END_TIME,
            "page": 0,
            "size": 100,
            "order_by": "create_time",
            "order_asc": False
        }
        order_ids = order_service.get_order_list(order_params)
        pprint(len(order_ids))
        
        df_orders = order_service.batch_get_order_details(order_ids)
        df_orders['更新时间'] = date_end
        print(df_orders.head(1))
        print(f"\n共提取到 {len(order_ids)} 个唯一订单号")
        df_orders.to_sql(
            name=args.order_table,
            con=engine,
            if_exists="append",
            index=False,
            chunksize=10000
        )
        print("订单数据导入完成！")
    
    if not args.skip_aftersales:
        print("\n" + "="*60)
        print("开始采集售后数据...")
        print("="*60)
        aftersale_service = AftersaleService(client, max_workers=args.max_workers)
        aftersale_params = {
            "start_time": START_TIME,
            "end_time": END_TIME,
            "page": 0,
            "size": 100
        }
        aftersale_ids, aftersale_orders = aftersale_service.get_aftersale_list(aftersale_params)
        pprint(aftersale_orders)
        df_aftersales = aftersale_service.batch_get_aftersale_details(aftersale_ids)
        df_aftersales['更新时间'] = date_end
        print(df_aftersales.head(1))
        df_aftersales.to_sql(
            name=args.aftersale_table,
            con=engine,
            if_exists="append",
            index=False,
            chunksize=10000
        )
        print("售后数据导入完成！")
    
    if not args.skip_settle:
        print("\n" + "="*60)
        print("开始采集结算数据...")
        print("="*60)
        settle_service = SettleService(client)
        bill_params = {
            "start_time": date_start2,
            "end_time": date_end,
            "size": 100,
            "start_index": ""
        }
        df_settle = settle_service.batch_get_settle_details(bill_params)
        df_settle['更新时间'] = date_end
        print(df_settle.head(1))
        df_settle.to_sql(
            name=args.settle_table,
            con=engine,
            if_exists="replace",
            index=False,
            chunksize=10000
        )
        print("结算数据导入完成！")
    
    print("\n" + "="*60)
    print("所有数据采集任务完成！")
    print("="*60)


if __name__ == "__main__":
    main()
