import pandas as pd
import time
import random
import json
from DrissionPage import ChromiumPage,ChromiumOptions
from datetime import datetime
from sqlalchemy import create_engine
import argparse
import sys
import warnings
warnings.filterwarnings('ignore', message=".*The provided table name.*")

from config import INDUSTRY_CATEGORY, RANK_CONFIGS

_cfg = RANK_CONFIGS["rank_shop"]
TARGET_URL = _cfg["TARGET_URL"]
RANK_CATEGORY = _cfg["RANK_CATEGORY"]
AMOUNT_CLASS = _cfg["AMOUNT_CLASS"]
MAX_PAGES = _cfg["MAX_PAGES"]

# START_DATE_STR = "2026-01-30"

def parse_compass_json(json_content, current_industry, current_date):
    if not json_content or not isinstance(json_content, dict):
        return [],[]

    # 封装提取 range 值的逻辑，避免重复代码
    def get_range_values(row, key):
        extra = row.get(key, {}).get('index_values', {}).get('extra_value', {})
        # 兼容原逻辑：通过长度判断或直接取 key
        lower = extra.get('lower', {}).get('value', 0)
        upper = extra.get('upper', {}).get('value', 0)
        return lower, upper

    try:
        raw_data_list = (json_content.get('data', {})
                         .get('module_data', {})
                         .get('search_shop_rank', {})
                         .get('compass_general_table_value', {})
                         .get('data', [])) or []
        
        shoprank_list = []
        product_list = []
        # 定义字段映射：{中文键名: 原始JSON中的Key}
        metrics_map = {
            # "支付金额": "pay_amt",
            "成交订单数": "pay_cnt",
            "成交人数": "pay_ucnt",
            # "客单价": "pay_user_unit_price",
            # "商品曝光次数": "product_show_cnt",
            "商品曝光数": "product_show_ucnt",
            "商品点击数": "product_click_ucnt",
            "商品曝光点击率": "product_show_click_ratio",
            "商品点击成交转化率": "product_click_pay_ratio",
        }

        # 第一个是本店铺，结构不一致会出错
        for row in raw_data_list[1:]:
            # 基础信息提取
            row = row.get('cell_info', {})
            shop_info = row.get('shop', {}).get('shop', {})
            rank_info = row.get('rank', {}).get('index_values', {})
            product_info = row.get('product_list', {}).get('product_list', [])
            pay_amt = row.get('pay_amt',{}).get('index_values',{}).get('extra_value', {})
            pay_user_unit_price = row.get('pay_user_unit_price',{}).get('index_values',{}).get('extra_value', {})
            # 初始化记录
            author_record = {
                "日期": current_date,
                "行业类目": current_industry,
                "店铺名": shop_info.get('shop_name'),
                "店铺ID": shop_info.get('shop_id'),
                "店铺LOGO": shop_info.get('shop_logo'),
                "店铺二维码": shop_info.get('qr_code'),
                "是否首次上榜": rank_info.get('extra_value', {}).get('first_on_rank', {}).get('value', 0),
                "排名变化": rank_info.get('last_period_change', {}).get('value', 0),
                "当前排名": rank_info.get('value', {}).get('value', 0),
                "支付金额下限":pay_amt.get('lower', {}).get('value', 0)/100 if pay_amt else 0,
                "支付金额上限":pay_amt.get('upper', {}).get('value', 0)/100 if pay_amt else 0,
                "客单价下限":pay_user_unit_price.get('lower', {}).get('value', 0)/100 if pay_user_unit_price else 0,
                "客单价上限":pay_user_unit_price.get('upper', {}).get('value', 0)/100 if pay_user_unit_price else 0
            }

            # 批量提取指标 range 
            for label, json_key in metrics_map.items():
                lower, upper = get_range_values(row, json_key)
                author_record[f"{label}下限"] = lower
                author_record[f"{label}上限"] = upper
            
            shoprank_list.append(author_record)
            
            # 批量提取商品信息
            for pro in product_info:
                product_record = {
                    "日期": current_date,
                    "店铺名": shop_info.get('shop_name'),
                    "店铺ID": shop_info.get('shop_id'),
                    "商品名称": pro.get('product_name', ''),
                    "商品ID": pro.get('product_id', ''),
                    "封面": pro.get('product_image', ''),
                    "二维码": pro.get('qr_code', ''),
                    "H5详情页地址": pro.get('detail_h5_url', ''),
                    "是否低价商品": pro.get('is_low_price_product', 0),
                    "审核状态": pro.get('product_audit_status', 0),
                    "上架状态": pro.get('product_status', 0),
                }
                product_list.append(product_record)


    except Exception as e:
        # print(f"❌ 解析失败: {e}")
        return []
        
    return shoprank_list,product_list


def select_date_by_header(page, target_date_str):
    target_dt = datetime.strptime(target_date_str, "%Y-%m-%d")
    t_year_val = str(target_dt.year)
    t_month_val = f"{target_dt.month}月"
    try:
        page.ele('text=更多', timeout=2).click()
        time.sleep(0.5)
        page.ele('text=自然日', timeout=2).click()
        time.sleep(0.5) 
        # year_btn = page.ele('.ecom-picker-year-btn', timeout=2)
        # year_btn.click()
        # time.sleep(0.5)
        # page.ele(f'text={t_year_val}', timeout=2).click()
        # time.sleep(0.5)
        month_btn = page.ele('.ecom-picker-month-btn', timeout=2)
        month_btn.click()
        time.sleep(0.5)
        page.ele(f'text={t_month_val}', timeout=2).click()
        time.sleep(0.5)
        formatted_date = target_dt.strftime("%Y-%m-%d")
        day_cell = page.ele(f'@title={formatted_date}', timeout=3)
        if day_cell:
            day_cell.click()
            # page.run_js('arguments[0].click();', day_cell)
            # print(f"   🎯 成功切换到日期: {formatted_date}")
            time.sleep(2)
            return True
        
        return False
    except Exception as e:
        print(f"⚠️ 日期选择异常: {e}")
        return False


def get_rank_data(START_DATE_STR,END_DATE_STR,MY_COOKIES,DB_URL):
    
    # 1. 创建配置对象
    co = ChromiumOptions()
    
    # 2. 开启静默模式（无头模式）
    co.headless(True) 
    # co.headless(False) 
    co.set_argument('--window-size=1920,1080')
    co.no_imgs(True) # 无图模式省流加速
    # 3. 推荐添加：禁用 GPU 加速和沙盒模式（提高在服务器或后台运行的稳定性）
    co.set_argument('--no-sandbox')
    co.set_argument('--disable-gpu')
    # co.incognito(True) # 无痕模式
    co.set_user_agent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')    
    # 4. 初始化页面对象（不再使用 addr_or_opts='127.0.0.1:9222'）
    co.set_local_port(9225) # 端口接管
    page = ChromiumPage(addr_or_opts=co)  
        
    page.get('https://compass.jinritemai.com')
    time.sleep(2)
    page.set.cookies(MY_COOKIES)
    page.refresh()
    page.get(TARGET_URL)
    time.sleep(3)
    
    engine = create_engine(DB_URL)
    date_range = pd.date_range(start=START_DATE_STR, end=END_DATE_STR).strftime('%Y-%m-%d').tolist()

    try:

        for target_date in date_range:
            # print(f"--- 🚀 开始处理: {target_date} ---")
            if not select_date_by_header(page, target_date): continue

            for category in INDUSTRY_CATEGORY:
                # print(f"   🎯 开始处理类目: {category.get('一级类目')}-{category.get('二级类目')}")
                target = page.ele('t:span@class=ecom-cascader-picker')
                if target:
                    target.click()
                time.sleep(1)
                page.ele(f'@title={category.get("一级类目")}').click()
                time.sleep(0.5)
                page.ele(f'@title={category.get("二级类目")}').click()
                time.sleep(0.5)
                page.listen.start(targets = 'shop_rank?')
                page.ele('xpath://ul[3]//li[@title="全部"]').click()
                # page.ele(f'@title={category.get("三级类目")}').click() # ecom-cascader-menu-item ecom-cascader-menu-item-active
                time.sleep(1)
                category_info = f'{category.get("一级类目")}-{category.get("二级类目")}'

                for current_page in range(1, MAX_PAGES + 1):
                    # --- 保持重试机制 ---
                    res = None
                    for retry in range(3):
                        res = page.listen.wait(timeout=15)
                        if res and res.response.body: break
                        # print(f"  ⚠️ 第 {current_page} 页捕获超时，重试 {retry+1}/3...")
                        time.sleep(3) # 给页面一点缓冲时间
                    if res and res.response.body:
                        auths, videos = parse_compass_json(res.response.body,category_info, target_date)
                        if auths:
                            pd.DataFrame(auths).to_sql(name="ods_店铺榜单_day", con=engine, if_exists="append", index=False)
                            # print(f"  ✅ 第 {current_page} 页捕获成功")
                        if videos:
                            pd.DataFrame(videos).to_sql(name="ods_店铺top商品_day", con=engine, if_exists="append", index=False)
                            # print(f"  ✅ 第 {current_page} 页商品捕获成功")
                    else:
                        print(f"  ❌ 第 {current_page} 页多次捕获失败，跳过")

                    next_btn = page.ele('xpath://li[@title="下一页"]')
                    if not next_btn or 'disabled' in next_btn.attr('class') or next_btn.attr('aria-disabled') == 'true':
                        break
                    try:
                        next_btn.click(by_js=True)
                        time.sleep(1) 
                    except:
                        page.run_js('arguments[0].click();', next_btn)
                        time.sleep(2)
                page.listen.stop()
    except Exception as e:
        print(f"发生错误: {e}")   
        # page.quit()
    finally:
        # page.quit()
        pass
    page.quit()

if __name__ == "__main__":
# 1. 定义命令行参数接收器
    parser = argparse.ArgumentParser(description="店铺榜单采集脚本")
    parser.add_argument("--start_time", type=str, required=True, help="开始时间 YYYY-MM-DD")
    parser.add_argument("--end_time", type=str, required=True, help="结束时间 YYYY-MM-DD")
    parser.add_argument("--cookie", type=str, required=True, help="用户访问信息")
    parser.add_argument("--db_url", type=str, required=True, help="数据库凭证")
    
    # 2. 解析参数
    args = parser.parse_args()

    # 3. 覆盖全局变量 (让 n8n 传进来的参数生效)
    START_TIME = args.start_time
    END_TIME = args.end_time
    COOKIE = args.cookie  
    DB_URL = args.db_url  
    get_rank_data(START_TIME,END_TIME,COOKIE,DB_URL)
    # print('success')