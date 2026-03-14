import json
import time
import pandas as pd 
from DrissionPage import ChromiumPage, ChromiumOptions
from datetime import datetime
from sqlalchemy import create_engine
import argparse
import sys
import warnings
warnings.filterwarnings('ignore', message=".*The provided table name.*")

from config import INDUSTRY_CATEGORY, RANK_CONFIGS

_cfg = RANK_CONFIGS["rank_kol"]
TARGET_URL = _cfg["TARGET_URL"]
RANK_CATEGORY = _cfg["RANK_CATEGORY"]
AMOUNT_CLASS = _cfg["AMOUNT_CLASS"]
MAX_PAGES = _cfg["MAX_PAGES"]

# ================= 配置区域 =================
# MY_COOKIES = [
#     'qc_tt_tag=0; passport_csrf_token=5f8d89711f9f896ef93cab22748575d2; passport_csrf_token_default=5f8d89711f9f896ef93cab22748575d2; s_v_web_id=verify_mk4wr5ky_FBrlYRnS_XcKB_41Jr_9H11_XDAiiDYb478D; csrf_session_id=a1c07aede2c80c0e25720d200e26fed9; Hm_lvt_b6520b076191ab4b36812da4c90f7a5e=1767844116,1768358678,1768978704; HMACCOUNT=4AF7DBC6F274486D; passport_mfa_token=CjZ%2Bc5x2dPQ9wL80iquNK8eHKiiWOcM6Mlk1euTyj9qoJO%2BRBkmLQDXmygc3jwQYB3Rf4tjkeBgaSgo8AAAAAAAAAAAAAFACfGuyFxrjo5XS2Nt0DAu11t8q1NRZr7ImS4o5urnmhWKZZ%2BYma9j4K8kNvTM5Q5twEJ6XiA4Y9rHRbCACIgEDukoanA%3D%3D; is_staff_user=false; ucas_c0=CkAKBTEuMC4wEKiIjrb5k5LBaRjmJiDYuPCcwK2IASiwITCHq7CP0a1kQMiRicwGSMjFxc4GUIC8zpr82JW4aVhvEhROaf8q1S-_Q9c1uNi1rLS12sjOHA; ucas_c0_ss=CkAKBTEuMC4wEKiIjrb5k5LBaRjmJiDYuPCcwK2IASiwITCHq7CP0a1kQMiRicwGSMjFxc4GUIC8zpr82JW4aVhvEhROaf8q1S-_Q9c1uNi1rLS12sjOHA; PHPSESSID=9d9d14f267e62b4015c40299739b770f; PHPSESSID_SS=9d9d14f267e62b4015c40299739b770f; Hm_lpvt_b6520b076191ab4b36812da4c90f7a5e=1770145995; BUYIN_SASID=SID2_7602717288898330914; gfkadpd=4499,20590; ttwid=1%7CWVTQlaa1rlcwu51d5vnGWf6BhYPyyZEylfUwf9STHDw%7C1770189526%7Cef782946b2cdf7b6e1a452345c3cb47f87afb5384d45b4e4b4d7aaf95926a84c; ucas_c0_compass=CkAKBTEuMC4wEK6Iic6t2rzBaRjmJiDYuPCcwK2IASiwITCHq7CP0a1kQNbli8wGSNaZyM4GUIC8zpr82JW4aVhvEhRw6FsJ-k0KaO4oHR6ls1RuyH1BXg; ucas_c0_ss_compass=CkAKBTEuMC4wEK6Iic6t2rzBaRjmJiDYuPCcwK2IASiwITCHq7CP0a1kQNbli8wGSNaZyM4GUIC8zpr82JW4aVhvEhRw6FsJ-k0KaO4oHR6ls1RuyH1BXg; LUOPAN_DT=session_7602905562111623467; COMPASS_LUOPAN_DT=session_7602905562111623467; ecom_us_lt_compass=7d9e9f1adacbca13413831a450adbe1cc1379b60b68db00a9204bd8256f3641b; ecom_us_lt_ss_compass=7d9e9f1adacbca13413831a450adbe1cc1379b60b68db00a9204bd8256f3641b; odin_tt=c1fbcc25b9b3ce872cfe5710fa7567a3b278b87dcaac07d73d72b50a1a3edd65266c8957ffc4c6573f17d2e7e5cb6e904cd30d4df1dfbe2f005b3409e19f3e6a; passport_auth_status=a75d083942aa64331ebc025db387b19b%2C7fa761fbbd43d72d716558f8a4f646ef; passport_auth_status_ss=a75d083942aa64331ebc025db387b19b%2C7fa761fbbd43d72d716558f8a4f646ef; sid_guard=efdec75dbef9a517e7da6a45e63d68f7%7C1770195013%7C4742345%7CTue%2C+31-Mar-2026+06%3A09%3A18+GMT; uid_tt=edc42d16861a0b606ec5b74a8eda4307; uid_tt_ss=edc42d16861a0b606ec5b74a8eda4307; sid_tt=efdec75dbef9a517e7da6a45e63d68f7; sessionid=efdec75dbef9a517e7da6a45e63d68f7; sessionid_ss=efdec75dbef9a517e7da6a45e63d68f7; session_tlb_tag=sttt%7C11%7C797HXb75pRfn2mpF5j1o9__________eif7w3eCCxk59eHCXmCe1k6bqOrHQYV_L8vvCBFk35Cs%3D; sid_ucp_v1=1.0.0-KDEyZWI3MjU0MTE1ZDVlMDUxNDEzM2E3M2Q0MjBjNTQ1ZDhlYzI0ZDUKGQjz95DJyczbBBDFkIzMBhimDCAMOAJA8QcaAmxmIiBlZmRlYzc1ZGJlZjlhNTE3ZTdkYTZhNDVlNjNkNjhmNw; ssid_ucp_v1=1.0.0-KDEyZWI3MjU0MTE1ZDVlMDUxNDEzM2E3M2Q0MjBjNTQ1ZDhlYzI0ZDUKGQjz95DJyczbBBDFkIzMBhimDCAMOAJA8QcaAmxmIiBlZmRlYzc1ZGJlZjlhNTE3ZTdkYTZhNDVlNjNkNjhmNw; gd_random=eyJwZXJjZW50IjowLjczMzI1NTQ1NzYzNDkyMzIsIm1hdGNoIjp0cnVlfQ==.JlY6KnHwZMn70UxGA/Y8H5maXm8CT5ELv2IGuauPz5c='
# ]

# START_DATE_STR = "2026-02-10"
# END_DATE_STR = '2026-02-11'
# # START_DATE_STR = datetime.now().strftime('%Y-%m-%d')
# # END_DATE_STR = datetime.now().strftime('%Y-%m-%d')
# DB_URL = "mysql+pymysql://longyu:110112119longyuLY@rm-bp11v498kanwzkp7veo.mysql.rds.aliyuncs.com:3306/ods?charset=utf8mb4"
# ===========================================


def clean_title_and_tags(full_title):    
    if not full_title: return "", ""
    idx = full_title.find('#')
    if idx == -1: return full_title.strip(), ""
    return full_title[:idx].strip(), full_title[idx:].strip()


def parse_compass_json(json_content, account_type, rank_category, current_industry, current_date):
    author_list = []
    video_list = []
    
    if not json_content or not isinstance(json_content, dict):
        return author_list, video_list
    
    try:
        raw_data_list = json_content.get('data', [])
        if raw_data_list is None: raw_data_list = []
        
        for item in raw_data_list:
            row = item.get('row', {})
            author_dim_outer = row.get('author_dimension', {})
            author_dim = author_dim_outer.get('author_dimension', {}) if author_dim_outer else {}
            rank_info_outer = row.get('rank_info', {})
            rank_info = rank_info_outer.get('rank_info', {}) if rank_info_outer else {}
            author_id = author_dim.get('author_id')
            
            # --- 1. 达人核心指标 (严格还原所有原始字段) ---
            pay_amt_range = row.get('pay_amt', {}).get('index_values', {}).get('value_range', [])
            lower_amt = pay_amt_range[0].get('value', 0) if len(pay_amt_range) > 0 else 0
            upper_amt = pay_amt_range[1].get('value', 0) if len(pay_amt_range) > 1 else 0
            
            brand_list = row.get('brand_list', {}).get('brand_list', [])
            brand_name = brand_list[0].get('brand_name', '') if brand_list else ''
            brand_id = brand_list[0].get('brand_id', '') if brand_list else ''
            
            pays_cnt_range = row.get('pay_combo_cnt', {}).get('index_values', {}).get('value_range', [])
            lower_cnt = pays_cnt_range[0].get('value', 0) if len(pays_cnt_range) > 0 else 0
            upper_cnt = pays_cnt_range[1].get('value', 0) if len(pays_cnt_range) > 1 else 0
            
            product_cnt_range = row.get('product_click_cnt', {}).get('index_values', {}).get('value_range', [])
            lower_view = product_cnt_range[0].get('value', 0) if len(product_cnt_range) > 0 else 0
            upper_view = product_cnt_range[1].get('value', 0) if len(product_cnt_range) > 1 else 0
            
            pay_incr_val = row.get('pay_amt_incr', {}).get('index_values', {}).get('real_value', {}).get('value', 0)
            
            inst_list = row.get('institution_list', {}).get('institution_list', [])
            inst_name = inst_list[0].get('institution_name', '') if inst_list else ''
            inst_id = inst_list[0].get('institution_id', '') if inst_list else ''
            
            author_record = {
                "日期": current_date,
                "榜单": rank_category,
                "行业类目": current_industry,
                "账号类型": account_type,
                "达人昵称": author_dim.get('author_nick_name'),
                "达人ID": author_id,
                "抖音号ID": author_dim.get('author_short_id'),
                "粉丝数": author_dim.get('author_fans_cnt'),
                "当前排名": rank_info.get('rank_no'),
                "排名变化": rank_info.get('rank_change') or '0',
                "第一次入榜": "是" if rank_info.get('is_first_on_rank')=='1' else "否",
                "品牌名称": brand_name,
                "品牌ID": brand_id,
                "机构名称": inst_name,
                "机构ID": inst_id,
                "成交额下限": lower_amt / 100,
                "成交额上限": upper_amt / 100,
                "成交数下限": lower_cnt,
                "成交数上限": upper_cnt,
                "商品点击下限": lower_view,
                "商品点击上限": upper_view,
                "成交增长率": f"{round(float(pay_incr_val) * 100, 2)}%" if pay_incr_val else "0%",
                "头像": author_dim.get('author_avatar_url'),
                "二维码": author_dim.get('author_qr_code_url') # 补回二维码字段
            }
            author_list.append(author_record)

            # --- 2. 视频列表提取 (还原所有原始字段) ---
            v_list_obj = row.get('video_list', {})
            raw_videos = v_list_obj.get('video_list') if isinstance(v_list_obj, dict) else None
            if isinstance(raw_videos, list):
                for v in raw_videos:
                    pure_title, tags = clean_title_and_tags(v.get('video_title', ''))
                    video_list.append({
                        "日期": current_date,
                        "榜单": rank_category,
                        "行业类目": current_industry,
                        "账号类型": account_type,
                        "达人ID": author_id,
                        "视频ID": v.get('video_id'),
                        "视频标题": pure_title,
                        "话题标签": tags,
                        "视频时长": round(v.get('video_duration', 0), 2),
                        "发布时间": datetime.fromtimestamp(v.get('video_publish_time')).strftime('%Y-%m-%d %H:%M:%S') if v.get('video_publish_time') else None,
                        "封面图": v.get('video_image'),
                        "视频链接": v.get('video_url'),
                        "是否付费可见": v.get('is_ff_see'),
                        "视频状态": v.get('video_status')
                    })
    except Exception as e:
        print(f"❌ 解析失败: {e}")
    return author_list, video_list


# select_date_by_header 
def select_date_by_header(page, target_date_str):
    target_dt = datetime.strptime(target_date_str, "%Y-%m-%d")
    t_year_val = str(target_dt.year)
    t_month_val = f"{target_dt.month}月"
    try:
        page.ele('text=更多').click()
        time.sleep(0.3)
        page.ele('text=自然日').click()
        time.sleep(0.3) 
        year_btn = page.ele('.ecom-picker-year-btn')
        year_btn.click()
        time.sleep(0.3)
        page.ele(f'text={t_year_val}').click()
        time.sleep(0.3)
        month_btn = page.ele('.ecom-picker-month-btn')
        month_btn.click()
        time.sleep(0.3)
        page.ele(f'text={t_month_val}').click()
        time.sleep(0.5)
        formatted_date = target_dt.strftime("%Y-%m-%d")
        day_cell = page.ele(f'xpath://td[@title="{formatted_date}"]')
        # day_cell = page.ele(f'xpath://td[@title={formatted_date}]')
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
    # co = ChromiumOptions()
    # co.set_argument('--start-maximized')
    # page = ChromiumPage(addr_or_opts=co)
    # driver = ChromiumPage(addr_or_opts='127.0.0.1:9222')
    # page = driver.latest_tab 
    # page = ChromiumPage()
    
    # 1. 创建配置对象
    co = ChromiumOptions()
    # 2. 开启静默模式（无头模式）
    co.headless(True)
    co.set_argument('--window-size=1920,1080')
    # 3. 推荐添加：禁用 GPU 加速和沙盒模式（提高在服务器或后台运行的稳定性）
    co.set_argument('--no-sandbox')
    co.set_argument('--disable-gpu')
    co.no_imgs(True) # 无图模式省流加速
    co.set_local_port(9225) # 端口接管
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
    time.sleep(2)
    page.set.cookies(MY_COOKIES)
    page.refresh()
    page.get(TARGET_URL)
    time.sleep(3)
    
    
    date_range = pd.date_range(start=START_DATE_STR, end=END_DATE_STR).strftime('%Y-%m-%d').tolist()
    engine = create_engine(DB_URL)
    
    try:
        time.sleep(1) 
        for category in INDUSTRY_CATEGORY:
            # print(f"🚀 开始处理: {category.get('一级类目')}-{category.get('二级类目')}-{category.get('三级类目')}")
            target = page.ele('t:span@class=ecom-cascader-picker')
            if target:
                target.click()
            time.sleep(1)
            page.ele(f'@title={category.get("一级类目")}').click()
            time.sleep(1)
            page.ele(f'@title={category.get("二级类目")}').click()
            time.sleep(1)
            page.ele(f'@title={category.get("三级类目")}').click()
            time.sleep(1)
            category_info = f'{category.get("一级类目")}-{category.get("二级类目")}'
            for target_date in date_range:
                # print(f"-- 🚀 开始处理: {target_date} --")
                if not select_date_by_header(page, target_date): 
                    # print(f"   ⚠️ 日期选择失败：{target_date}")
                    continue

                for rank_cat in RANK_CATEGORY:
                    # print(f"   🚀 开始处理: {rank_cat}")
                    cat_btn = page.ele(f'text={rank_cat}')
                    if cat_btn: cat_btn.click(); time.sleep(2)

                    for amt_class in AMOUNT_CLASS:
                        # print(f"     🚀 开始处理: {amt_class}")
                        page.listen.start(targets=["author_rank"])
                        target_tab = page.ele(f'xpath://div[@role="tab"]//span[text()="{amt_class}"]')
                        if target_tab: target_tab.click(); time.sleep(2)

                        for current_page in range(1, MAX_PAGES + 1):
                            # --- 保持重试机制 ---
                            res = None
                            for retry in range(3):
                                res = page.listen.wait(timeout=15)
                                if res and res.response.body: break
                                # print(f"  ⚠️ 第 {current_page} 页捕获超时，重试 {retry+1}/3...")
                                if retry == 0:
                                    # 第一次失败：尝试重新点击一下当前的“账号类型”标签，触发刷新
                                    if target_tab:
                                        target_tab.click()
                                elif retry == 1:
                                    # 第二次失败：如果是第2页及以后，尝试点一下当前页码
                                    try:
                                        curr_page_btn = page.ele(f'xpath://li[@title="{current_page}"]')
                                        if curr_page_btn:
                                            curr_page_btn.click()
                                    except:
                                        pass
                                time.sleep(3) # 给页面一点缓冲时间
                            if res and res.response.body:
                                auths, videos = parse_compass_json(res.response.body, amt_class, rank_cat, category_info, target_date)
                                if auths:
                                    pd.DataFrame(auths).to_sql(name="ods_达人榜单_day", con=engine, if_exists="append", index=False)
                                    # print(f"  ✅ 第 {current_page} 页捕获成功")
                                if videos:
                                    pd.DataFrame(videos).to_sql(name="ods_达人视频_day2", con=engine, if_exists="append", index=False)
                                    # print(f"  ✅ 第 {current_page} 页视频捕获成功")
                            else:
                                print(f"  ❌ 第 {current_page} 页多次捕获失败，跳过")

                            next_btn = page.ele('xpath://li[@title="下一页"]')
                            if not next_btn or 'disabled' in next_btn.attr('class') or next_btn.attr('aria-disabled') == 'true':
                                break
                            try:
                                next_btn.click(timeout=2)
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
    parser = argparse.ArgumentParser(description="达人榜单采集脚本")
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
