import pandas as pd
import time
import random
import json
from DrissionPage import ChromiumPage,ChromiumOptions
from datetime import datetime
from sqlalchemy import create_engine
from sympy import false, true
import argparse
import sys
import warnings
warnings.filterwarnings('ignore', message=".*The provided table name.*")

from config import INDUSTRY_CATEGORY, RANK_CONFIGS

_cfg = RANK_CONFIGS["rank_product"]
TARGET_URL = _cfg["TARGET_URL"]
RANK_CATEGORY = _cfg["RANK_CATEGORY"]
MAX_PAGES = _cfg["MAX_PAGES"]

# ================= 配置区域 =================
MY_COOKIES = [
]

# START_DATE_STR = "2026-01-27"
# END_DATE_STR = datetime.now().strftime('%Y-%m-%d')
# END_DATE_STR = '2026-01-30'
# DB_URL = "mysql+pymysql://longyu:110112119longyuLY@rm-bp11v498kanwzkp7veo.mysql.rds.aliyuncs.com:3306/ods?charset=utf8mb4"
# ===========================================

def extract_douyin_product_data(json_data, category_info, target_date):
    """
    从复杂的嵌套 JSON 中提取商品全量字段并扁平化
    """
    extracted_rows = []
    
    # 防御性编程：获取 data_result，如果不存在则返回空列表
    data_result = json_data.get("data", {}).get("data_result", [])
    if not data_result:
        return []
    
    for item in data_result:
        # 1. 提取基础 Product Info
        p_info = item.get("product_info", {})
        
        # 2. 提取店铺和达人信息 (shop_list 是一个列表，通常取第一个)
        shops = p_info.get("shop_list", [])
        first_shop = shops[0] if shops else {}
        author_info = first_shop.get("author_info", {})
        
        # 3. 定义一个内部辅助函数来处理指标区间 (value_range)
        # 结构通常是: [{'value': min}, {'value': max}]
        def get_range_values(metric_key):
            metric_data = item.get(metric_key, {})
            ranges = metric_data.get("value_range", [])
            if ranges and len(ranges) >= 2:
                return ranges[0].get("value"), ranges[1].get("value")
            return None, None

        # 提取各项核心指标的 [下限, 上限]
        pay_amt_min, pay_amt_max = get_range_values("new_pay_amt") # 支付金额(GMV)
        pay_cnt_min, pay_cnt_max = get_range_values("pay_combo_cnt") # 支付件数/组合数
        click_cnt_min, click_cnt_max = get_range_values("product_click_cnt") # 商品点击数
        click_pay_ratio_min, click_pay_ratio_max = get_range_values("product_click_pay_cnt_ratio") # 点击-支付转化率

        # 4. 组装单行数据 (字典)
        row = {
            "日期": target_date,
            "行业类目": category_info,
            # --- 店铺与达人 ---
            "店铺ID": first_shop.get("shop_id"),
            "店铺名称": first_shop.get("shop_name"),
            "店铺logo": first_shop.get("image"),
            "达人昵称": author_info.get("author_nick_name"),
            "达人UID": author_info.get("author_id"),
            "抖音号ID": author_info.get("aweme_id"),
            "粉丝数": author_info.get("fans_count",0),
            "达人链接列表": str(author_info.get("url_list")) if author_info.get("url_list") else '',
            
            # --- 商品信息 ---
            "商品ID": p_info.get("id"),
            "SPU_ID": p_info.get("spu_id"),
            "商品名称": p_info.get("name"),
            "价格": p_info.get("price_bin"),
            "封面": p_info.get("image_url"),
            "H5详情页": p_info.get("product_detail_h5_url"),
            "一级类目ID": p_info.get("leaf_category_id"),
            "二级类目ID": p_info.get("second_category_id"),
            "三级类目ID": p_info.get("third_category_id"),
            "商品链接列表": str(p_info.get("url_list")) if p_info.get("url_list") else '',
            
            # --- 核心排名 ---
            "排名": p_info.get("rank"),
            "排名变化": p_info.get("rank_change"),
            "新上榜": p_info.get("newly_on_ranking"),
            
            # --- 核心数据指标 (区间值) ---
            "支付金额_下限": pay_amt_min/100 if pay_amt_min else 0,
            "支付金额_上限": pay_amt_max/100 if pay_amt_max else 0,
            "支付订单数_下限": pay_cnt_min,
            "支付订单数_上限": pay_cnt_max,
            "点击数_下限": click_cnt_min,
            "点击数_上限": click_cnt_max,
            "点击支付转化率_下限": click_pay_ratio_min,
            "点击支付转化率_上限": click_pay_ratio_max,
            
            # --- 状态与原因 ---
            "关键点": str(item.get("key_point_list")) if item.get("key_point_list") else '', # item.get("key_point_list",[]),
            "可添加竟对": item.get("is_addible"),
            "不可添加竞对原因": item.get("not_addible_reason"),
            "可对比": item.get("is_comparable"),
            "不可对比原因": item.get("not_comparable_reason"),
            "可查看":item.get("is_viewable"),
            "不可查看原因": item.get("not_viewable_reason"),
            # "商品状态(下架)": "是" if item.get("not_viewable_reason") == "目标商品已下架" else "否"
        }
        
        extracted_rows.append(row)
        
    return extracted_rows



def select_date_by_header(page, target_date_str):
    target_dt = datetime.strptime(target_date_str, "%Y-%m-%d")
    t_year_val = str(target_dt.year)
    t_month_val = f"{target_dt.month}月"
    try:
        page.ele('text=更多').click()
        time.sleep(0.5)
        page.ele('text=自然日').click()
        time.sleep(0.5) 
        year_btn = page.ele('.ecom-picker-year-btn')
        year_btn.click()
        time.sleep(0.5)
        page.ele(f'text={t_year_val}').click()
        time.sleep(0.5)
        month_btn = page.ele('.ecom-picker-month-btn')
        month_btn.click()
        time.sleep(0.5)
        page.ele(f'text={t_month_val}').click()
        time.sleep(0.5)
        formatted_date = target_dt.strftime("%Y-%m-%d")
        day_cell = page.ele(f'xpath://td[@title="{formatted_date}"]')
        if day_cell:
            page.run_js('arguments[0].click();', day_cell)
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
    co.set_argument('--window-size=1920,1080')
    co.no_imgs(True) # 无图模式省流加速
    # 3. 推荐添加：禁用 GPU 加速和沙盒模式（提高在服务器或后台运行的稳定性）
    co.set_argument('--no-sandbox')
    co.set_argument('--disable-gpu')
    # co.incognito(True) # 无痕模式
    co.set_user_agent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36')    
    # 4. 初始化页面对象（不再使用 addr_or_opts='127.0.0.1:9222'）
    co.set_local_port(9225) # 端口接管
    page = ChromiumPage(addr_or_opts=co)    
    
    # page = driver.latest_tab 
    
    page.get('https://compass.jinritemai.com')
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
                # print(f"--- 🚀 开始处理: {category} ---")
                target = page.ele('t:span@class=ecom-cascader-picker')
                if target:
                    target.click()
                time.sleep(1)
                page.ele(f'@title={category.get("一级类目")}').click()
                time.sleep(0.5)
                page.ele(f'@title={category.get("二级类目")}').click()
                time.sleep(0.5)
                page.listen.start(targets = 'market_hot_sale?')
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
                        auths = extract_douyin_product_data(res.response.body,category_info, target_date)
                        if auths:
                            pd.DataFrame(auths).to_sql(name="ods_商品榜单_总榜_day", con=engine, if_exists="append", index=False)
                            # print(f"  ✅ 第 {current_page} 页捕获成功")
                        # if videos:
                        #     pd.DataFrame(videos).to_sql(name="ods_店铺TOP商品_day", con=engine, if_exists="append", index=False)
                        #     print(f"  ✅ 第 {current_page} 页商品捕获成功")
                    else:
                        print(f"  ❌ 第 {current_page} 页多次捕获失败，跳过")

                    next_btn = page.ele('xpath://li[@title="下一页"]')
                    if not next_btn or 'disabled' in next_btn.attr('class') or next_btn.attr('aria-disabled') == 'true':
                        break
                    try:
                        next_btn.click(timeout=2)
                        time.sleep(2) 
                    except:
                        page.run_js('arguments[0].click();', next_btn)
                        time.sleep(2)
                page.listen.stop()
                        
    except Exception as e:
        print(f"发生错误: {e}")   
        page.quit()
    finally:
        page.quit()
        pass
    page.quit()



if __name__ == "__main__":
# 1. 定义命令行参数接收器
    parser = argparse.ArgumentParser(description="商品榜单采集脚本")
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