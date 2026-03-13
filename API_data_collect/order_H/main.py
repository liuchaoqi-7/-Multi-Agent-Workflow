import os
import argparse
from datetime import datetime, timedelta
from sqlalchemy import create_engine

from xiaohongshu import (
    XiaoHongShuAPIClient,
    OrderService,
    AftersaleService,
    FinanceService,
    DEFAULT_TABLE_NAMES,
)
from xiaohongshu.utils import datetime_to_timestamp, get_yesterday_range


def parse_args():
    parser = argparse.ArgumentParser(
        description="小红书订单/售后/结算数据采集脚本",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  # 使用默认时间（昨天）采集数据
  python main.py --app_key YOUR_KEY --app_secret YOUR_SECRET --access_token YOUR_TOKEN \\
      --db_host localhost --db_user root --db_password pwd --db_name test

  # 指定时间范围采集数据
  python main.py --app_key YOUR_KEY --app_secret YOUR_SECRET --access_token YOUR_TOKEN \\
      --order_start "2024-01-01 00:00:00" --order_end "2024-01-02 00:00:00" \\
      --db_host localhost --db_user root --db_password pwd --db_name test

  # 使用环境变量传入敏感参数
  export XHS_APP_KEY=YOUR_KEY
  export XHS_APP_SECRET=YOUR_SECRET
  export XHS_ACCESS_TOKEN=YOUR_TOKEN
  python main.py --db_host localhost --db_user root --db_password pwd --db_name test
        """
    )
    
    parser.add_argument("--app_key", type=str, 
                        default=os.environ.get("XHS_APP_KEY"),
                        help="小红书应用App Key (也可通过环境变量XHS_APP_KEY传入)")
    parser.add_argument("--app_secret", type=str,
                        default=os.environ.get("XHS_APP_SECRET"),
                        help="小红书应用App Secret (也可通过环境变量XHS_APP_SECRET传入)")
    parser.add_argument("--access_token", type=str,
                        default=os.environ.get("XHS_ACCESS_TOKEN"),
                        help="小红书Access Token (也可通过环境变量XHS_ACCESS_TOKEN传入)")
    
    parser.add_argument("--order_start", type=str, default=None,
                        help="订单查询开始时间 (格式: YYYY-MM-DD HH:MM:SS，默认昨天凌晨)")
    parser.add_argument("--order_end", type=str, default=None,
                        help="订单查询结束时间 (格式: YYYY-MM-DD HH:MM:SS，默认今天凌晨)")
    parser.add_argument("--settle_start", type=str, default=None,
                        help="结算数据开始时间 (格式: YYYY-MM-DD HH:MM:SS，默认最近7天)")
    
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
    parser.add_argument("--finance_table", type=str, default=DEFAULT_TABLE_NAMES["finance"],
                        help=f"结算数据表名 (默认: {DEFAULT_TABLE_NAMES['finance']})")
    
    parser.add_argument("--token_file", type=str, default=None,
                        help="Token文件路径 (可选)")
    
    parser.add_argument("--skip_orders", action="store_true",
                        help="跳过订单数据采集")
    parser.add_argument("--skip_aftersales", action="store_true",
                        help="跳过售后数据采集")
    parser.add_argument("--skip_finance", action="store_true",
                        help="跳过结算数据采集")
    
    parser.add_argument("--max_workers", type=int, default=20,
                        help="多线程最大线程数 (默认20)")
    
    args = parser.parse_args()
    
    if not args.app_key or not args.app_secret or not args.access_token:
        parser.error("必须提供 --app_key, --app_secret 和 --access_token 参数，或设置对应环境变量")
    
    return args


def main():
    args = parse_args()
    
    client = XiaoHongShuAPIClient(
        app_key=args.app_key,
        app_secret=args.app_secret,
        access_token=args.access_token
    )
    
    date_start, date_end = get_yesterday_range()
    
    if args.order_start:
        date_start = args.order_start
    if args.order_end:
        date_end = args.order_end
    
    if args.settle_start:
        settle_start = args.settle_start
    else:
        settle_start = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
    
    print(f"\n{'='*60}")
    print(f"数据采集时间范围:")
    print(f"  订单/售后: {date_start} ~ {date_end}")
    print(f"  结算数据: {settle_start} ~ {date_end}")
    print(f"{'='*60}\n")
    
    start_timestamp = datetime_to_timestamp(date_start)
    end_timestamp = datetime_to_timestamp(date_end)
    settle_start_timestamp = datetime_to_timestamp(settle_start)
    
    db_connection_str = f"mysql+pymysql://{args.db_user}:{args.db_password}@{args.db_host}:{args.db_port}/{args.db_name}?charset=utf8mb4"
    engine = create_engine(db_connection_str)
    
    if not args.skip_orders:
        print("\n" + "="*60)
        print("开始采集订单数据...")
        print("="*60)
        order_service = OrderService(client, max_workers=args.max_workers)
        order_params = {
            "startTime": start_timestamp,
            "endTime": end_timestamp,
            "pageNo": 1,
            "pageSize": 100,
        }
        order_ids = order_service.get_order_list(order_params)
        
        if order_ids:
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
            "startTime": start_timestamp,
            "endTime": end_timestamp,
            "pageNo": 1,
            "pageSize": 100,
        }
        aftersale_ids, aftersale_orders = aftersale_service.get_aftersale_list(aftersale_params)
        
        if aftersale_ids:
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
    
    if not args.skip_finance:
        print("\n" + "="*60)
        print("开始采集结算数据...")
        print("="*60)
        finance_service = FinanceService(client, max_workers=args.max_workers)
        finance_params = {
            "startTime": settle_start_timestamp,
            "endTime": end_timestamp,
            "pageNo": 1,
            "pageSize": 100,
        }
        df_finance = finance_service.batch_get_finance_data(finance_params, data_type="settle")
        
        if not df_finance.empty:
            df_finance['更新时间'] = date_end
            print(df_finance.head(1))
            df_finance.to_sql(
                name=args.finance_table,
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
