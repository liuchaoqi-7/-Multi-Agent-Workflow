#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
飞书同步统一入口

支持四种同步任务：
1. 主播排期表 - 飞书->MySQL
2. 达人维度表 - MySQL->飞书
3. 商品维度表 - MySQL->飞书
4. 达人榜单表 - MySQL->飞书

使用方法：
    # 同步所有表
    python run_sync.py --all

    # 同步指定表
    python run_sync.py --tables schedule,kol,product,rank

    # 全量同步
    python run_sync.py --all --full-sync

    # 只同步达人维度表
    python run_sync.py --tables kol --full-sync
"""

import argparse
from datetime import datetime

from sync_live_author_schedule import sync_live_author_schedule
from sync_order_kol import sync_order_kol
from sync_order_product import sync_order_product
from sync_rank_kol import sync_rank_kol


SYNC_TASKS = {
    "schedule": {
        "name": "主播排期表",
        "direction": "飞书->MySQL",
        "func": sync_live_author_schedule
    },
    "kol": {
        "name": "达人维度表",
        "direction": "MySQL->飞书",
        "func": sync_order_kol
    },
    "product": {
        "name": "商品维度表",
        "direction": "MySQL->飞书",
        "func": sync_order_product
    },
    "rank": {
        "name": "达人榜单表",
        "direction": "MySQL->飞书",
        "func": sync_rank_kol
    }
}


def run_sync(tables: list, full_sync: bool = False):
    print(f"\n{'='*70}")
    print(f"🚀 飞书同步任务开始")
    print(f"📅 时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📋 同步模式: {'全量同步' if full_sync else '增量同步'}")
    print(f"📋 同步表: {', '.join(tables)}")
    print(f"{'='*70}")
    
    results = {}
    
    for table_key in tables:
        if table_key not in SYNC_TASKS:
            print(f"⚠️ 未知的表: {table_key}")
            continue
        
        task = SYNC_TASKS[table_key]
        print(f"\n{'─'*70}")
        print(f"📌 开始同步: {task['name']} ({task['direction']})")
        print(f"{'─'*70}")
        
        try:
            result = task['func'](full_sync=full_sync)
            results[table_key] = {"status": "success", "result": result}
            print(f"✅ {task['name']} 同步成功")
        except Exception as e:
            results[table_key] = {"status": "failed", "error": str(e)}
            print(f"❌ {task['name']} 同步失败: {e}")
    
    print(f"\n{'='*70}")
    print(f"📊 同步结果汇总")
    print(f"{'='*70}")
    
    for table_key, result in results.items():
        task_name = SYNC_TASKS[table_key]['name']
        if result['status'] == 'success':
            print(f"✅ {task_name}: 成功")
        else:
            print(f"❌ {task_name}: 失败 - {result['error']}")
    
    print(f"\n🎉 所有同步任务完成！")
    return results


def main():
    parser = argparse.ArgumentParser(
        description="飞书同步统一入口",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
    python run_sync.py --all                    # 增量同步所有表
    python run_sync.py --all --full-sync        # 全量同步所有表
    python run_sync.py --tables kol,product     # 同步指定表
    python run_sync.py --tables schedule        # 只同步主播排期表
        """
    )
    
    parser.add_argument(
        "--all",
        action="store_true",
        help="同步所有表"
    )
    parser.add_argument(
        "--tables",
        type=str,
        default="",
        help="指定同步的表，用逗号分隔: schedule,kol,product,rank"
    )
    parser.add_argument(
        "--full-sync",
        action="store_true",
        help="执行全量同步（默认增量同步）"
    )
    
    args = parser.parse_args()
    
    if args.all:
        tables = list(SYNC_TASKS.keys())
    elif args.tables:
        tables = [t.strip() for t in args.tables.split(",") if t.strip()]
    else:
        parser.print_help()
        return
    
    if not tables:
        print("❌ 请指定要同步的表")
        parser.print_help()
        return
    
    run_sync(tables=tables, full_sync=args.full_sync)


if __name__ == "__main__":
    main()
