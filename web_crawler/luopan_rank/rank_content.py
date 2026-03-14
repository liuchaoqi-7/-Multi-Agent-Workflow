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

_cfg = RANK_CONFIGS["rank_content"]
TARGET_URL = _cfg["TARGET_URL"]
RANK_CATEGORY = _cfg["RANK_CATEGORY"]
AMOUNT_CLASS = _cfg["AMOUNT_CLASS"]
MAX_PAGES = _cfg["MAX_PAGES"]

def clean_title_and_tags(full_title):
    if not full_title:
        return "", ""
    # partition 会返回 (分隔符前, 分隔符, 分隔符后) 的三元组
    title, sep, tags = full_title.partition('#')
    return title.strip(), (sep + tags).strip() 


def parse_compass_json(json_content ,category_info,amt_class,rank_category, current_date):
    if not json_content or not isinstance(json_content, dict):
        return [],[]

    # 封装提取 cell_info 中 range 值的逻辑
    def get_cell_range_values(cell_data, key):
        # 路径：cell_info -> key -> index_values -> extra_value -> lower/upper
        extra = cell_data.get(key, {}).get('index_values', {}).get('extra_value', {})
        if not extra:
            return 0, 0
        
        # 提取下限和上限
        lower = extra.get('lower', {}).get('value', 0)
        upper = extra.get('upper', {}).get('value', 0)
        return lower, upper

    try:
        # 1. 定位直播交易流向数据根目录
        raw_data_list = (json_content.get('data', {})
                         .get('module_data', {})
                         .get('live_trade_flow_rank', {})
                         .get('compass_general_table_value', {})
                         .get('data', [])) or []
        
        shoprank_list = []
        videos_list = []
        
        # 2. 定义该榜单对应的指标映射 (JSON 中的 Key)
        metrics_map = {
            # "支付金额": "pay_amt",
            "直播观看次数": "watch_cnt",
            "商品点击次数": "product_click_cnt",
            "商品点击转化率": "click_pay_rate"
        }

        for item in raw_data_list:
            # 罗盘这个榜单的数据全部在 cell_info 里面
            cell = item.get('cell_info', {})
            if not cell:
                continue

            # 3. 提取店铺及房源基础信息
            room_info = cell.get('room', {}).get('room', {})
            author_info = room_info.get('author', {})
            rank_info = cell.get('rank', {}).get('index_values', {})
            
            # 初始化记录
            author_record = {
                "日期": current_date,
                "行业类目": category_info,
                "榜单":rank_category,
                "账户类型":amt_class,
                "店铺ID": cell.get('shop_id', {}).get('value', {}).get('value', 0),
                "达人昵称": author_info.get('nick_name', ''),
                "达人UID": author_info.get('author_id', ''),
                "抖音号ID": author_info.get('short_id', ''),
                "粉丝数": author_info.get('fans_cnt', 0),
                "头像": author_info.get('cover_url', ''),
                "二维码": author_info.get('qr_code', ''),
                "直播间标题": room_info.get('live_room_title', ''),
                "直播间ID": room_info.get('live_room_id', ''),
                "直播间封面": room_info.get('cover_url', ''),
                "直播时长_小时": round(room_info.get('live_duration', 0)/3600,2),
                "直播开始时间": datetime.fromtimestamp(room_info.get('live_start_ts', 0)).strftime('%Y-%m-%d %H:%M:%S') if room_info.get('live_start_ts', 0) else '',
                "直播结束时间": datetime.fromtimestamp(room_info.get('live_end_ts', 0)).strftime('%Y-%m-%d %H:%M:%S') if room_info.get('live_end_ts', 0) else '',
                "最佳渠道": cell.get('top_channel', {}).get('value', {}).get('value_str', ''),
                "排名变化": rank_info.get('last_period_change', {}).get('value', 0),
                "当前排名": rank_info.get('value', {}).get('value', 0),
                "支付金额下限":cell.get('pay_amt', {}).get('index_values', {}).get('extra_value', {}).get('lower', {}).get('value', 0)/100,
                "支付金额上限":cell.get('pay_amt', {}).get('index_values', {}).get('extra_value', {}).get('upper', {}).get('value', 0)/100,
            }

            # 4. 循环提取指标数值
            for label, json_key in metrics_map.items():
                lower, upper = get_cell_range_values(cell, json_key)
                author_record[f"{label}下限"] = lower
                author_record[f"{label}上限"] = upper
            
            # 5. 提取引流短视频
            video_list = cell.get('drainage_video_list', {}).get('video_list', [])
            author_record["视频列表"] = str(video_list) if video_list else ''
            for video in video_list:
                video_id = video.get('video_id', '')
                video_title = video.get('video_title', '')
                video_record = {
                    "日期": current_date,
                    "店铺ID": cell.get('shop_id', {}).get('value', {}).get('value', 0),
                    "达人昵称": author_info.get('nick_name', ''),
                    "达人UID": author_info.get('author_id', ''),
                    "抖音号ID": author_info.get('short_id', ''),
                    "直播间ID": room_info.get('live_room_id', ''),
                    "视频标题": video_title,
                    "视频ID": video_id,
                    "发布时间": datetime.fromtimestamp(video.get('publish_time', 0)).strftime('%Y-%m-%d %H:%M:%S') if video.get('publish_time', 0) else video.get('publish_time', 0),
                    "视频封面": video.get('video_cover', ''),
                    "视频地址": video.get('play_url', ''),
                    "视频抖音地址": f"https://www.douyin.com/video/{video_id}",
                    "视频状态": video.get('video_status', 0),
                }
                videos_list.append(video_record)
            
            shoprank_list.append(author_record)

    except Exception as e:
        print(f"❌ 直播流向榜解析失败: {e}")
        return []
        
    return shoprank_list,videos_list


def extract_all_video_rank_data(json_data, category_info,amt_class,rank_category, target_date):
    """
    全量提取视频带货榜单的所有字段
    """
    try:
        # 定位到数据列表 (根据你提供的JSON结构)
        items = json_data.get('data', {}).get('module_data', {}) \
            .get('video_bring_goods_rank', {}).get('compass_general_table_value', {}) \
            .get('data', [])
    except Exception as e:
        print(f"❌ JSON 结构定位失败: {e}")
        return []

    results = []

    for item in items:
        # 每一个 item 对应一行数据，核心都在 cell_info 里
        cell = item.get('cell_info', {})
        
        # === 1. 排名相关 (Rank) ===
        rank_data = cell.get('rank', {}).get('index_values', {})
        # 提取各个维度的排名信息
        rank_current = rank_data.get('value', {}).get('value', 0)
        rank_last = rank_data.get('last_value', {}).get('value', 0)
        rank_change = rank_data.get('last_period_change', {}).get('value', 0)
        rank_label = rank_data.get('extra_value', {}).get('new_in_rank', {}).get('value_str', '') # 如"新上榜"

        # === 2. 视频与作者信息 (Video & Author) ===
        video_wrap = cell.get('video', {}).get('video', {})
        author_wrap = video_wrap.get('author', {})
        
        # 时间戳转换
        pub_ts = video_wrap.get('publish_time', 0)
        pub_time_str = datetime.fromtimestamp(pub_ts).strftime('%Y-%m-%d %H:%M:%S') if pub_ts else ""

        # === 3. 互动数据 (Interaction) ===
        # 辅助函数：快速提取 value 下的 value
        def get_val(key):
            return cell.get(key, {}).get('index_values', {}).get('value', {}).get('value', 0)

        like_cnt = get_val('like_cnt')
        comment_cnt = get_val('comment_cnt')
        share_cnt = get_val('share_video_cnt')

        # === 4. 区间数据 (Range Data: Play & Sales) ===
        # 播放量区间
        watch_data = cell.get('watch_cnt', {}).get('index_values', {}).get('extra_value', {})
        watch_min = watch_data.get('lower', {}).get('value', 0)
        watch_max = watch_data.get('upper', {}).get('value', 0)

        # 销售额区间 (注意：单位通常是分)
        pay_data = cell.get('pay_amt', {}).get('index_values', {}).get('extra_value', {})
        pay_min = pay_data.get('lower', {}).get('value', 0)
        pay_max = pay_data.get('upper', {}).get('value', 0)

        # === 5. 店铺与状态 (Shop & Status) ===
        # 注意：这里结构稍有不同，直接在 value 下
        shop_id = cell.get('shop_id', {}).get('value', {}).get('value',0)
        is_viewable = cell.get('is_viewable', {}).get('value', {}).get('value',0)
        not_viewable_reason = cell.get('not_viewable_reason', {}).get('value', {}).get('unit',0) # 盲猜是unit或value
        is_my_rank = cell.get('is_my_rank', {}).get('index_values', {}).get('value', {}).get('value')

        # === 6. 商品列表 (Products) ===
        # 将商品列表保留为结构化数据，后续可按需展开
        product_list = []
        raw_product_list = cell.get('product_list', {}).get('product_list', [])
        p = raw_product_list[0] if raw_product_list else {}
        # raw_products = cell.get('product_list', {}).get('product_list', [])
        # p=raw_products
        # if raw_products:
        #     for p in raw_products :
        #         product_list.append({
        #             "日期": target_date,
        #             "达人昵称": author_wrap.get('nick_name'),
        #             "达人UID": author_wrap.get('author_id'),
        #             "商品名称": p.get('product_name'),
        #             "商品ID": p.get('product_id'),
        #             "售价": p.get('sale_price', 0) ,
        #             "图片链接": p.get('product_image'),
        #             "H5详情页": p.get('detail_h5_url'),
        #             "审核状态": p.get('product_audit_status'),
        #             "上架状态": p.get('product_status')
        #         })

        # === 7. 组装最终的大宽表字典 ===
        row = {
            "日期": target_date,
            "行业类目":category_info,
            "榜单": rank_category,
            "账户类型": amt_class,
            "店铺ID": shop_id,
            # --- 作者信息 ---
            "达人昵称": author_wrap.get('nick_name',''),
            "达人UID": author_wrap.get('author_id',''),
            "抖音号ID": author_wrap.get('short_id',''),
            "粉丝数": author_wrap.get('fans_cnt',0),
            "头像": author_wrap.get('cover_url',''),
            "二维码": author_wrap.get('qr_code',''),

            # --- 排名 ---
            "当前排名": rank_current,
            "上期排名": rank_last,
            "排名变化": rank_change,
            "榜单标签": rank_label,
            # "是否我的排名": is_my_rank,

            # --- 视频信息 ---
            "视频标题": video_wrap.get('video_title',''),
            "视频ID": video_wrap.get('video_id',''),
            # "发布时间": datetime.fromtimestamp(pub_time_str).strftime('%Y-%m-%d %H:%M:%S') if pub_time_str else None,
            "发布时间": pub_time_str,
            "视频时长_秒": video_wrap.get('duration',0),
            "视频状态": video_wrap.get('video_status',0),
            "播放链接": video_wrap.get('play_url',''),
            "抖音播放地址": f'https://www.douyin.com/video/{video_wrap.get('video_id')}',
            "封面链接": video_wrap.get('video_cover',''),
            

            # --- 核心指标 ---
            "播放量下限": watch_min,
            "播放量上限": watch_max,
            "用户支付金额下限": pay_min/100  if pay_min else 0, 
            "用户支付金额上限": pay_max/100  if pay_max else 0, 
            "点赞数": like_cnt,
            "评论数": comment_cnt,
            "分享数": share_cnt,

            # --- 其他 ---
            "是否可见": is_viewable,
            "不可见原因": not_viewable_reason,
            
            # --- 商品 (存为列表对象，如果存Excel可能会显示为字符串) ---
            "商品名称": p.get('product_name', ""),
            "商品ID": p.get('product_id', ""),
            "售价": round(p.get('sale_price', 0)/100,2) if p.get('sale_price', 0)/100 else 0,
            "封面": p.get('product_image', ""),
            "H5详情页": p.get('detail_h5_url', ""),
            "审核状态": p.get('product_audit_status',0),
            "上架状态": p.get('product_status',0),
            
            
            # "关联商品列表": product_list,
            # # 为了方便Excel查看，可以提取第一个商品的信息作为主商品
            # "主推商品名称": product_list[0]['商品名称'] if product_list else "",
            # "主推商品价格": product_list[0]['价格(元)'] if product_list else "",
        }
        
        results.append(row)

    return results


def extract_video_rank_all_fields(json_data,category_info, amt_class, rank_category, target_date):
    """
    全量提取引流榜单的所有字段
    返回: (基础指标列表, 视频详情列表)
    """
    try:
        # 定位到数据列表
        raw_items = json_data.get("data", {}).get("module_data", {}) \
                             .get("video_bring_goods_rank", {}) \
                             .get("compass_general_table_value", {}) \
                             .get("data", [])
    except Exception as e:
        print(f"❌ 解析路径失败: {e}")
        return [], []

    main_metrics = [] # 存储排名和各项指标区间
    video_details = [] # 存储视频和作者的详细信息

    for item in raw_items:
        cell = item.get("cell_info", {})
        if not cell: continue

        # --- 1. 提取基础信息 (用于关联) ---
        video_wrap = cell.get("video", {}).get("video", {})
        video_id = video_wrap.get("video_id")
        author_info = video_wrap.get("author", {})
        author_name = author_info.get("nick_name")
        pub_ts = video_wrap.get("publish_time")
        
        # --- 2. 提取所有指标字段 (Flatten) ---
        # 辅助函数处理嵌套的区间值
        def get_range(key):
            idx_val = cell.get(key, {}).get("index_values", {}).get("extra_value", {})
            return idx_val.get("lower", {}).get("value"), idx_val.get("upper", {}).get("value")

        rank_info = cell.get("rank", {}).get("index_values", {})
        
        # 组装主表数据 (包含所有业务字段)
        metric_record = {
            "日期": target_date,
            "行业类目":category_info,
            "榜单类型": rank_category,
            "账户分类": amt_class,
            "店铺ID": cell.get("shop_id", {}).get("value", {}).get("value",0),
            "达人昵称": author_name,
            "达人UID": author_info.get("author_id",''),
            "抖音号ID": author_info.get("short_id",''),
            "达人粉丝数": author_info.get("fans_cnt",0),
            "头像": author_info.get("cover_url",''),
            "二维码": author_info.get("qr_code",''),
            "引流视频标题": video_wrap.get("video_title",''),
            "引流视频ID": video_id,
            "视频时长": video_wrap.get("duration",''),
            "发布时间": datetime.fromtimestamp(pub_ts).strftime('%Y-%m-%d %H:%M:%S') if pub_ts else None,
            "播放地址": video_wrap.get("play_url",''),
            "抖音播放地址": f'https://www.douyin.com/video/{video_id}',
            "视频封面": video_wrap.get("video_cover",''),
            "当前排名": rank_info.get("value", {}).get("value",0),
            "上期排名": rank_info.get("last_value", {}).get("value",0),
            "排名变化": rank_info.get("last_period_change", {}).get("value",0),
            "榜单标签": rank_info.get("extra_value", {}).get("new_in_rank", {}).get("value_str",''),
            
            # 播放/成交指标
            "视频播放量_下限": get_range("watch_cnt")[0],
            "视频播放量_上限": get_range("watch_cnt")[1],
            "引流次数_下限": get_range("live_watch_cnt")[0],
            "引流次数_上限": get_range("live_watch_cnt")[1],
            "直播支付金额_下限": get_range("live_pay_amt")[0]/100,
            "直播支付金额_上限": get_range("live_pay_amt")[1]/100,
            
            # 状态字段
            "视频状态": video_wrap.get("video_status",0),
            "是否可见": cell.get("is_viewable", {}).get("value", {}).get("value",0),
            "不可见原因码": cell.get("not_viewable_reason", {}).get("value", {}).get("unit",0)
        }
        main_metrics.append(metric_record)

        # # --- 3. 提取视频列表/详情 (单独参数) ---
        # video_record = {
        # }
        # video_details.append(video_record)

    return main_metrics


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
    # driver = ChromiumPage(addr_or_opts='127.0.0.1:9222')
    # page = driver.latest_tab 
    # page=ChromiumPage()
    # 先找到容器，再点击里面的选择器

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
    # page=ChromiumPage(addr_or_opts='127.0.0.1:9223')
    # driver = ChromiumPage(addr_or_opts='127.0.0.1:9222')
    # page = driver.latest_tab 
    
    page.get('https://compass.jinritemai.com')
    time.sleep(2)
    page.set.cookies(MY_COOKIES)
    page.refresh()
    page.get(TARGET_URL)
    time.sleep(5)

    # page = ChromiumPage(127.0.0.1:9222)
    engine = create_engine(DB_URL)
    date_range = pd.date_range(start=START_DATE_STR, end=END_DATE_STR).strftime('%Y-%m-%d').tolist()

    try:
        for rank_category in RANK_CATEGORY:
            # print(f"--- 🚀 开始处理: {rank_category} ---")
            cat_btn = page.ele(f'text={rank_category}')
            if cat_btn: cat_btn.click()
            time.sleep(2)
            if rank_category == '视频销量榜':
                # print(f"   🚀 开始处理: 用户支付金额 ---")
                sorter = page.ele('xpath://th[contains(., "用户支付金额")]//div[contains(@class, "sorters")]')
                sorter.click()
                # rank_btn = page.ele('t:div@class=ecom-table-column-sorters')
                # if rank_btn: cat_btn.click()
                time.sleep(2)
            for category in INDUSTRY_CATEGORY:
                # print(f"   🚀 开始处理: {category.get('一级类目')}-{category.get('二级类目')}-{category.get('三级类目')} ---")
                target = page.ele('t:span@class=ecom-cascader-picker')
                if target:
                    target.click()
                time.sleep(1)
                page.ele(f'@title={category.get("一级类目")}').click()
                time.sleep(0.5)
                page.ele(f'@title={category.get("二级类目")}').click()
                time.sleep(0.5)
                page.ele(f'@title={category.get("三级类目")}').click()
                time.sleep(1)
                category_info = f'{category.get("一级类目")}-{category.get("二级类目")}'
                
                for target_date in date_range:
                    # print(f"--- 🚀 开始处理: {target_date} ---")
                    if not select_date_by_header(page, target_date): continue
                    
                    for amt_class in AMOUNT_CLASS:
                    # =================================================
                            listen_target = ""
                            if rank_category == '直播交易榜': # 注意这里名字要和 RANK_CATEGORY 一致
                                listen_target = "board_list_v2" # 直播榜接口
                            elif rank_category in ['视频销量榜', '引流直播榜']:
                                listen_target = "bring_good_flow_hot_v2" # 视频/引流榜接口
                            
                            if not listen_target:
                                # print(f"   ❌ 未知的榜单类型: {rank_category}，跳过监听")
                                continue
                            # 启动监听
                            page.listen.start(listen_target)
                            # print(f"      👂 已启动监听: {listen_target} | 账号类型: {amt_class}")                        
                            # if rank_category == '直播交易榜':
                            #     page.listen.start(targets=["board_list_v2"])
                            # elif rank_category == '视频销量榜':
                            #     page.listen.start(targets=["bring_good_flow_hot_v2"])
                            # elif rank_category == '引流直播榜':
                            #     page.listen.start(targets=["bring_good_flow_hot_v2"])
                            if amt_class == '自营':
                                target_tab1 = page.ele(f't:div@text()=合作')
                                target_tab2 = page.ele(f't:div@text()=自营')
                                # target_tab = page.ele(f'xpath://div[@role="tab"]//span[text()={amt_class}]')
                                if target_tab1 and target_tab2: 
                                    target_tab1.click() 
                                    target_tab2.click()
                                else : print(f"⚠️ 未找到目标标签：{amt_class}")
                            else:
                                target_tab1 = page.ele(f't:div@text()=自营')
                                target_tab2 = page.ele(f't:div@text()=合作')
                                # target_tab = page.ele(f'xpath://div[@role="tab"]//span[text()={amt_class}]')
                                if target_tab1 and target_tab2: 
                                    target_tab1.click() 
                                    target_tab2.click() 
                                else : print(f"⚠️ 未找到目标标签：{amt_class}")
                            time.sleep(2)

                            for current_page in range(1, MAX_PAGES + 1):
                                # --- 保持重试机制 ---
                                res = None
                                for retry in range(3):
                                    res = page.listen.wait(timeout=15)
                                    if res and res.response.body: break
                                    # print(f"  ⚠️ 第 {current_page} 页捕获超时，重试 {retry+1}/3...")
                                    # --- 【核心改进：无刷新重试】 ---
                                    if retry == 0:
                                        # 第一次失败：尝试重新点击一下当前的“账号类型”标签，触发刷新
                                        if target_tab2:
                                            target_tab2.click()
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
                                    if rank_category == '直播交易榜':
                                        auths,videos = parse_compass_json(res.response.body,category_info,amt_class,rank_category, target_date)
                                        if auths:
                                            pd.DataFrame(auths).to_sql(name="ods_内容榜_直播交易榜_day2", con=engine, if_exists="append", index=False)
                                            # print(f"  ✅ 第 {current_page} 页捕获成功")
                                        if videos:
                                            pd.DataFrame(videos).to_sql(name="ods_内容榜_直播交易榜_视频_day2", con=engine, if_exists="append",index=False)
                                            # print(f"  ✅ 第 {current_page} 页视频捕获成功")
                                    elif rank_category == '视频销量榜':
                                        videos = extract_all_video_rank_data(res.response.body,category_info,amt_class,rank_category, target_date)
                                        if videos:
                                            pd.DataFrame(videos).to_sql(name="ods_内容榜_视频销量榜_day2", con=engine, if_exists="append", index=False)
                                            # print(f"  ✅ 第 {current_page} 页捕获成功")
                                    elif rank_category == '引流直播榜':
                                        lives = extract_video_rank_all_fields(res.response.body,category_info,amt_class,rank_category, target_date)
                                        if lives:
                                            pd.DataFrame(lives).to_sql(name="ods_内容榜_引流直播榜_day2", con=engine, if_exists="append", index=False)
                                            # print(f"  ✅ 第 {current_page} 页直播捕获成功")
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