from openai import timeout
import pandas as pd
import time
import random
import json
from DrissionPage import ChromiumPage, ChromiumOptions
from datetime import datetime
from sqlalchemy import create_engine
from sympy import true
import argparse
import sys
import warnings
warnings.filterwarnings('ignore', message=".*The provided table name.*")

from config import INDUSTRY_CATEGORY, RANK_CONFIGS

_cfg = RANK_CONFIGS["rank_serach"]
TARGET_URL = _cfg["TARGET_URL"]
MAX_PAGES = _cfg["MAX_PAGES"]

# ================= 配置区域 =================
# MY_COOKIES = [
#     'qc_tt_tag=0; passport_csrf_token=5f8d89711f9f896ef93cab22748575d2; passport_csrf_token_default=5f8d89711f9f896ef93cab22748575d2; s_v_web_id=verify_mk4wr5ky_FBrlYRnS_XcKB_41Jr_9H11_XDAiiDYb478D; csrf_session_id=a1c07aede2c80c0e25720d200e26fed9; Hm_lvt_b6520b076191ab4b36812da4c90f7a5e=1767844116,1768358678,1768978704; HMACCOUNT=4AF7DBC6F274486D; gfkadpd=4499,20590; ttwid=1%7CWVTQlaa1rlcwu51d5vnGWf6BhYPyyZEylfUwf9STHDw%7C1769618528%7C924f3ec3397658b3c8941dceacd4377616ef426c36c637c99344d702d5b3b253; tt_scid=lV.sInJNZUAmZBmUbm4YCMj6lH4.enmrpTt9M5Cer8JuCGO4L.pJo9YqSrEXqI126939; passport_mfa_token=CjZ%2Bc5x2dPQ9wL80iquNK8eHKiiWOcM6Mlk1euTyj9qoJO%2BRBkmLQDXmygc3jwQYB3Rf4tjkeBgaSgo8AAAAAAAAAAAAAFACfGuyFxrjo5XS2Nt0DAu11t8q1NRZr7ImS4o5urnmhWKZZ%2BYma9j4K8kNvTM5Q5twEJ6XiA4Y9rHRbCACIgEDukoanA%3D%3D; passport_auth_status=e5101fa7940b1a723d56d60ba726ae2b%2Caf874041df51fb9aad1a3026583d4201; passport_auth_status_ss=e5101fa7940b1a723d56d60ba726ae2b%2Caf874041df51fb9aad1a3026583d4201; PHPSESSID=9977b8d2babee9e7076e9736c262789c; PHPSESSID_SS=9977b8d2babee9e7076e9736c262789c; ucas_c0=CkAKBTEuMC4wELOIgdDRhbG9aRjmJiDYuPCcwK2IASiwITCHq7CP0a1kQLCI68sGSLC8p84GUIC8zpr82JW4aVhvEhSpVYrbAIphFafK0Tu7Nwx_OCMhCA; ucas_c0_ss=CkAKBTEuMC4wELOIgdDRhbG9aRjmJiDYuPCcwK2IASiwITCHq7CP0a1kQLCI68sGSLC8p84GUIC8zpr82JW4aVhvEhSpVYrbAIphFafK0Tu7Nwx_OCMhCA; Hm_lpvt_b6520b076191ab4b36812da4c90f7a5e=1769653300; ucas_c0_compass=CkAKBTEuMC4wEIaIhJKfhrG9aRjmJiDYuPCcwK2IASiwITCHq7CP0a1kQLWI68sGSLW8p84GUIC8zpr82JW4aVhvEhT4G170nzFfO-zLw5xdnr4t70Xurw; ucas_c0_ss_compass=CkAKBTEuMC4wEIaIhJKfhrG9aRjmJiDYuPCcwK2IASiwITCHq7CP0a1kQLWI68sGSLW8p84GUIC8zpr82JW4aVhvEhT4G170nzFfO-zLw5xdnr4t70Xurw; LUOPAN_DT=session_7600602634576134440; COMPASS_LUOPAN_DT=session_7600602634576134440; ecom_us_lt_compass=0461e221acae8bcf642a50da1e24b17095d96fc60874b0c45d64c35df75961d9; ecom_us_lt_ss_compass=0461e221acae8bcf642a50da1e24b17095d96fc60874b0c45d64c35df75961d9; BUYIN_SASID=SID2_7600601651384746303; odin_tt=f4f1791d7b5ab97bca0417a143cedd7aadf3293ae9299de25c588b80a6783150; gd_random=eyJtYXRjaCI6dHJ1ZSwicGVyY2VudCI6MC43MzMyNTU0NTc2MzQ5MjMyfQ==.CsxXFTthDOfqnGZdRqlAy+2ZN94mItEy0iwUgBEm8lU='
# ]

# START_DATE_STR = "2026-01-27"
# END_DATE_STR = datetime.now().strftime('%Y-%m-%d')
# END_DATE_STR = '2026-01-30'
# DB_URL = "mysql+pymysql://longyu:110112119longyuLY@rm-bp11v498kanwzkp7veo.mysql.rds.aliyuncs.com:3306/ods?charset=utf8mb4"
# ===========================================

def parse_compass_json(json_input,category_info, target_date):
    """
    高度解耦的字段提取函数
    返回: (主表列表, 热点商品列表)
    """
    if isinstance(json_input, str):
        data = json.loads(json_input)
    else:
        data = json_input
    if data:
        raw_items = data.get('data', {}).get('module_data', {}).get('info_list', {}).get('compass_general_table_value', {}).get('data', [])
    else:
        return [],[]
    main_list = []
    hot_products = []

    for item in raw_items:
        cell = item.get('cell_info', {})
        
        # 1. 提取热点商品 (单独存储)
        current_query = cell.get('query', {}).get('value', {}).get('value_str', '')
        if 'hot_product' in cell:
            for p in cell['hot_product'].get('product_list', []):
                hot_products.append({
                    "日期":target_date,
                    "行业类目":category_info,
                    "搜索词": current_query,
                    "商品ID": p.get('product_id'),
                    "商品名称": p.get('product_name'),
                    "封面": p.get('product_image'),
                    "h5详情链接": p.get('detail_h5_url'),
                    "审核状态": p.get('product_audit_status'),
                    "商品状态": p.get('product_status',0),
                })

        # 2. 提取主表字段 (逐个指定，方便你随时调整逻辑)
        # 提取逻辑辅助工具：获取数值(val)、比例(ratio)、区间(low/up)
        def get_idx(key): return cell.get(key, {}).get('index_values', {})
        def get_val(key): return cell.get(key, {}).get('value', {})

        row = {
            "日期":target_date,
            "行业类目":category_info,
            "搜索词": current_query,
            "排名": get_val('rank').get('value'),
            "竞争指数": get_idx('compete_index').get('value', {}).get('value'),
            "竞争指数_增长率": get_idx('compete_index').get('out_period_ratio', {}).get('value'),
            "是否类目词": get_idx('is_cate_query').get('value', {}).get('value'),
            
            # 搜索数据
            "搜索人数_下限": get_idx('search_ucnt').get('extra_value', {}).get('lower',{}).get('value'),
            "搜索人数_上限": get_idx('search_ucnt').get('extra_value', {}).get('upper',{}).get('value'),
            "搜索人数_增长率": get_idx('search_ucnt').get('out_period_ratio', {}).get('value'),
            
            "曝光人数_下限": get_idx('search_show_ucnt').get('extra_value',{}).get('lower',{}).get('value',0),
            "曝光人数_上限": get_idx('search_show_ucnt').get('extra_value',{}).get('upper',{}).get('value',0),
            "曝光人数_增长率": get_idx('search_show_ucnt').get('out_period_ratio', {}).get('value',0),
            
            "点击人数_下限": get_idx('search_click_ucnt').get('extra_value', {}).get('lower', {}).get('value'),
            "点击人数_上限": get_idx('search_click_ucnt').get('extra_value', {}).get('upper', {}).get('value'),
            "点击人数_增长率": get_idx('search_click_ucnt').get('out_period_ratio', {}).get('value'),

            # 成交数据 (金额类 / 100)
            "用户支付金额_下限": (get_idx('pay_amt').get('extra_value', {}).get('lower', {}).get('value', 0) or 0) / 100,
            "用户支付金额_上限": (get_idx('pay_amt').get('extra_value', {}).get('upper', {}).get('value', 0) or 0) / 100,
            "用户支付金额_增长率": get_idx('pay_amt').get('out_period_ratio', {}).get('value'),

            "客单价_下限": (get_idx('pay_per_usr_price').get('extra_value', {}).get('lower', {}).get('value', 0) or 0) / 100,
            "客单价_上限": (get_idx('pay_per_usr_price').get('extra_value', {}).get('upper', {}).get('value', 0) or 0) / 100,
            "客单价_增长率": get_idx('pay_per_usr_price').get('out_period_ratio', {}).get('value'),

            "成交订单数_下限": get_idx('pay_cnt').get('extra_value', {}).get('lower', {}).get('value'),
            "成交订单数_上限": get_idx('pay_cnt').get('extra_value', {}).get('upper', {}).get('value'),
            "成交订单数_增长率": get_idx('pay_cnt').get('out_period_ratio', {}).get('value'),
            
            "支付人数_下限": get_idx('pay_ucnt').get('extra_value', {}).get('lower', {}).get('value'),
            "支付人数_上限": get_idx('pay_ucnt').get('extra_value', {}).get('upper', {}).get('value'),
            "支付人数_增长率": get_idx('pay_ucnt').get('out_period_ratio', {}).get('value'),

            # 商品数据
            "商品展现人数_下限": get_idx('product_show_ucnt').get('extra_value', {}).get('lower', {}).get('value'),
            "商品展现人数_上限": get_idx('product_show_ucnt').get('extra_value', {}).get('upper', {}).get('value'),
            "商品展现人数_增长率": get_idx('product_show_ucnt').get('out_period_ratio', {}).get('value'),
            
            "商品点击人数_下限": get_idx('product_click_ucnt').get('extra_value', {}).get('lower', {}).get('value'),
            "商品点击人数_上限": get_idx('product_click_ucnt').get('extra_value', {}).get('upper', {}).get('value'),
            "商品点击人数_增长率": get_idx('product_click_ucnt').get('out_period_ratio', {}).get('value'),
            
            "商品点击率_下限": get_idx('prod_show_click_ratio').get('extra_value', {}).get('lower', {}).get('value'),
            "商品点击率_上限": get_idx('prod_show_click_ratio').get('extra_value', {}).get('upper', {}).get('value'),
            "商品点击率_增长率": get_idx('prod_show_click_ratio').get('out_period_ratio', {}).get('value'),

            "商品点击转化率_下限": get_idx('prod_click_pay_ratio').get('extra_value', {}).get('lower', {}).get('value'),
            "商品点击转化率_上限": get_idx('prod_click_pay_ratio').get('extra_value', {}).get('upper', {}).get('value'),
            "商品点击转化率_增长率": get_idx('prod_click_pay_ratio').get('out_period_ratio', {}).get('value'),

            "平台曝光商品数_下限": get_idx('show_product_cnt').get('extra_value', {}).get('lower', {}).get('value'),
            "平台曝光商品数_上限": get_idx('show_product_cnt').get('extra_value', {}).get('upper', {}).get('value'),
            "平台曝光商品数_增长率": get_idx('show_product_cnt').get('out_period_ratio', {}).get('value'),
                
            # 关联信息
            "关联内容数":get_idx('related_content_cnt').get('value', {}).get('value', 0),
            "关联内容数_增长率":get_idx('related_content_cnt').get('out_period_ratio', {}).get('value', 0),
            "关联商品数":get_idx('related_product_cnt').get('value', {}).get('value', 0),
            "关联商品数_增长率":get_idx('related_product_cnt').get('out_period_ratio', {}).get('value', 0),
     
            
        }
        main_list.append(row)

    return main_list, hot_products


def select_date_by_header(page, target_date_str):
    target_dt = datetime.strptime(target_date_str, "%Y-%m-%d")
    t_year_val = str(target_dt.year)
    t_month_val = f"{target_dt.month}月"
    try:
        page.ele('text=更多').click()
        time.sleep(0.5)
        page.ele('text=自然日',timeout = 3).click()
        time.sleep(1) 
        # year_btn = page.ele('.ecom-picker-year-btn')
        # year_btn.click()
        # time.sleep(0.5)
        # page.ele(f'text={t_year_val}').click()
        # time.sleep(0.5)
        month_btn = page.ele('t:button@class=ecom-picker-month-btn')
        month_btn.click()
        time.sleep(0.5)
        page.ele(f'text={t_month_val}').click()
        time.sleep(0.5)
        formatted_date = target_dt.strftime("%Y-%m-%d")
        day_cell = page.ele(f't:td@title={formatted_date}')
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
    
    # 接管模式
    # page=ChromiumPage(addr_or_opts='127.0.0.1:9222')
    # driver = ChromiumPage(addr_or_opts='127.0.0.1:9222')
    # page = driver.latest_tab 
    
    page.get('https://compass.jinritemai.com')
    page.set.cookies(MY_COOKIES)
    page.refresh()
    page.get(TARGET_URL)
    time.sleep(3)
    engine = create_engine(DB_URL)
    date_range = pd.date_range(start=START_DATE_STR, end=END_DATE_STR).strftime('%Y-%m-%d').tolist()

    try:
        # 每次先点击到其他的排序
        page.ele('text=搜索结果曝光人数').click()
        time.sleep(2)
        page.ele('text=搜索用户支付金额').click()
        time.sleep(2)
        for target_date in date_range:
            # print(f"--- 🚀 开始处理: {target_date} ---")
            if not select_date_by_header(page, target_date): continue

            for category in INDUSTRY_CATEGORY:
                # print(f"   🎯 开始处理类目: {category.get('一级类目')}-{category.get('二级类目')}")
                page.ele('t:span@class=ecom-cascader-picker',timeout=2).click(by_js=true)
                # if target:
                #     target.click()
                time.sleep(1)
                page.ele(f'@title={category.get("一级类目")}').click()
                time.sleep(0.5)
                page.ele(f't:li@title={category.get("二级类目")}').click()
                time.sleep(0.5)
                page.listen.start(targets = 'compass_rank_v3?')
                page.ele(f'xpath://ul[3]//li[@title="全部"]').click()
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
                            pd.DataFrame(auths).to_sql(name="ods_搜索榜单_day", con=engine, if_exists="append", index=False)
                            # print(f"  ✅ 第 {current_page} 页捕获成功")
                        if videos:
                            pd.DataFrame(videos).to_sql(name="ods_搜索榜单_TOP商品_day", con=engine, if_exists="append", index=False)
                            # print(f"  ✅ 第 {current_page} 页商品捕获成功")
                    else:
                        print(f"  ❌ 第 {current_page} 页多次捕获失败，跳过")

                    next_btn = page.ele('xpath://li[@title="下一页"]')
                    if not next_btn or 'disabled' in next_btn.attr('class') or next_btn.attr('aria-disabled') == 'true':
                        break
                    try:
                        next_btn.click(timeout=2)
                        page.listen.get_response()
                        time.sleep(1) 
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
    parser = argparse.ArgumentParser(description="搜索榜单采集脚本")
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