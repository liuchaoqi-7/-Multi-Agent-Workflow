import polars as pl
from pathlib import Path
import re
from sqlalchemy import create_engine

def parse_duration(value):
    if not value or value in ['0', '0秒', '-', '--']:
        return 0
    # 提取所有数字，处理 "1分钟6秒" 或 "66秒"
    nums = re.findall(r'\d+', str(value))
    if len(nums) == 2:
        return int(nums[0]) * 60 + int(nums[1])
    elif len(nums) == 1:
        return int(nums[0])
    return 0

def read_minute_level_data(folder_path):
    folder = Path(folder_path)
    all_data_frames = []
    
    target_files = [f for f in folder.glob("*.xlsx") if f.stem.endswith("-分钟级数据")]
    
    if not target_files:
        print("未找到符合‘分钟级数据’命名规则的文件。")
        return None

    for file_path in target_files:
        file_name = file_path.stem
        parts = file_name.split("-")
        uid = parts[0]
        session_id = "-".join(parts[:-1]) 
        
        # 强制 String 读取以避免符号导致的类型推断失败
        df = pl.read_excel(file_path, engine="calamine", infer_schema_length=0)
        df.columns = [c.strip() for c in df.columns]
        
        numeric_keywords = ["率", "比", "金额", "数", "人", "次", "时长", "曝光", "点击"]
        calc_cols = [c for c in df.columns if any(k in c for k in numeric_keywords) and c != "时间"]
        rate_cols = [c for c in df.columns if "率" in c or "比" in c]

        df_cleaned = (
            df.with_columns([
                # 1. 基础转换
                pl.col("时间").str.to_datetime("%Y/%m/%d %H:%M"),
                pl.lit(uid).alias("直播UID"),
                pl.lit(session_id).alias("场次ID"),
                
                # 2. 数值预清洗
                *[
                    pl.col(c)
                    .str.replace_all(r"[¥,%秒分钟]", "")
                    .str.replace_all(r"^-+$", "0")
                    .cast(pl.Float64, strict=False)
                    .fill_null(0)
                    .alias(c)
                    for c in calc_cols
                ]
            ])
            .with_columns([
                # 3. 百分比还原为小数
                *[ (pl.col(c) / 100).alias(c) for c in rate_cols ],
                # 4. 时间维度扩展
                pl.col("时间").dt.date().alias("天"),
                pl.col("时间").dt.hour().alias("小时")
            ])
            .with_columns([
                # 5. 反推计算指标 (处理除以0导致的inf)
                (pl.col("进入直播间人数") / pl.col("曝光-观看率"))
                .map_elements(lambda x: 0.0 if x == float('inf') or x != x else x, return_dtype=pl.Float64)
                .alias("曝光人数"),
                
                (pl.col("进入直播间人数") * pl.col("互动率")).alias("互动人数"),
                (pl.col("进入直播间人数") * pl.col("负反馈率")).alias("负反馈人次"),
                
                # 观看时长秒数转换
                pl.col("人均观看时长")
                .map_elements(parse_duration, return_dtype=pl.Int64)
                .alias("人均观看时长_秒")
            ])
        )        
        all_data_frames.append(df_cleaned)

    return pl.concat(all_data_frames, how="diagonal")

# --- 执行 ---
folder_path = "/Users/test/Downloads/直播数据"
final_results = read_minute_level_data(folder_path)

# 最终重命名映射表
column_mapping = {
    "时间": "【直播M】minute",
    "天": "【直播M】Day",
    "小时": "【直播M】Hour",
    "直播UID": "【直播M】UID",
    "场次ID": "【直播M】场次ID",
    "进入直播间人数": "【直播M】进入直播间人数",
    "曝光人数": "【直播M】曝光人次_(曝光-观看率反推)",
    "成交金额": "【直播M】成交金额",
    "成交人数": "【直播M】成交人数",
    "成交订单数": "【直播M】成交订单数",
    "人均观看时长_秒": "【直播M】人均观看时长(秒)",
    "商品曝光人数": "【直播M】商品曝光人数",
    "商品点击人数": "【直播M】商品点击人数",
    "直播间离开人数": "【直播M】直播间离开人数",
    "实时在线人数": "【直播M】实时在线人数",
    "新增粉丝数": "【直播M】新增粉丝数",
    "点赞次数": "【直播M】点赞次数",
    "评论次数": "【直播M】评论次数",
    "新加直播团人数": "【直播M】新加直播团人数",
    "互动人数": "【直播M】互动人数_(互动率反推)",
    "负反馈人次": "【直播M】负反馈人次_(反馈率反推)"
}   

# 你要求的最终列顺序
ordered_columns = [
    "【直播M】场次ID",'【直播M】UID','【直播M】Day','【直播M】Hour','【直播M】minute','【直播M】曝光人次_(曝光-观看率反推)',
    '【直播M】进入直播间人数','【直播M】直播间离开人数','【直播M】实时在线人数','【直播M】人均观看时长(秒)','【直播M】商品曝光人数',
    '【直播M】商品点击人数','【直播M】成交人数','【直播M】成交订单数','【直播M】成交金额','【直播M】新增粉丝数','【直播M】点赞次数',
    '【直播M】评论次数','【直播M】新加直播团人数','【直播M】互动人数_(互动率反推)','【直播M】负反馈人次_(反馈率反推)'
]

if final_results is not None:
    # 1. 重命名
    final_results = final_results.rename({
        k: v for k, v in column_mapping.items() if k in final_results.columns
    })

    # 2. 规范化未在 mapping 里的列名（去掉减号避免 MySQL 报错）
    final_results.columns = [c.replace("-", "_") for c in final_results.columns]
    
    # 3. 列顺序重排与补全
    all_cols = final_results.columns
    final_cols = [c for c in ordered_columns if c in all_cols] + [c for c in all_cols if c not in ordered_columns]
    final_results = final_results.select(final_cols)

    print("📊 数据清洗及列重排完成，预览：")
    print(final_results.head())

    # 数据库导入
    engine = create_engine(
        ""
    )

    try:
        final_results.write_database(
            table_name="直播数据minute_v2", 
            connection=engine,
            if_table_exists="replace",  
            engine="sqlalchemy"
        )
        print("✅ 数据已入库，包含所有维度及反推指标。")
    except Exception as e:
        print(f"❌ 入库失败：{e}")
