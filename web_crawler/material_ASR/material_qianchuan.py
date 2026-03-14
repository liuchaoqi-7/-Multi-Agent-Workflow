import os
import time
import requests
import pandas as pd
import whisper
import cv2
import json
from openai import OpenAI
from DrissionPage import ChromiumPage, ChromiumOptions
from sympy import true 

# --- 1. 核心配置 ---
api_key = ""
whisper_model = whisper.load_model("base") 
qwen_client = OpenAI(api_key=api_key, base_url="https://dashscope.aliyuncs.com/compatible-mode/v1")

for folder in ['videos', 'snapshots', 'results']:
    os.makedirs(folder, exist_ok=True)

co = ChromiumOptions()
co.set_argument('--window-size=1920,1080')
page = ChromiumPage(addr_or_opts=co)

def get_ai_analysis(whisper_text, correct_script):
    """使用 OpenAI 协议调用千问进行深度分析"""
    try:
        completion = qwen_client.chat.completions.create(
            model="qwen-plus",
            messages=[
                {"role": "system", "content": "你是一个短视频专家。请参考准确脚本修正ASR文案。输出JSON格式：{'refined_text': '', 'sentiment': ''}"},
                {"role": "user", "content": f"【准确脚本】：{correct_script}\n【ASR文案】：{whisper_text}"}
            ],
            response_format={"type": "json_object"},
            timeout=60.0
        )
        return json.loads(completion.choices[0].message.content)
    except Exception as e:
        print(f"❌ AI分析失败: {e}")
        return None

def capture_extrema_frames(video_path, extrema_secs, video_id):
    """OpenCV 截取极值画面"""
    cap = cv2.VideoCapture(video_path)
    fps = cap.get(cv2.CAP_PROP_FPS)
    paths = {}
    for label, sec in extrema_secs.items():
        cap.set(cv2.CAP_PROP_POS_FRAMES, int(sec * fps))
        success, frame = cap.read()
        if success:
            path = f"snapshots/{video_id}_{label}.jpg"
            cv2.imwrite(path, frame)
            paths[label] = path
    cap.release()
    return paths

# --- 2. 主爬取逻辑（完全回归你原本的固定逻辑） ---
page.get('https://qianchuan.jinritemai.com/dataV2/roi2-material-analysis?aavid=1810151981662283#')
page.listen.start(['statQuery', 'getContentFormulaAndScript'])

all_summary_rows = [] # 存储所有视频的汇总数据

for page_num in range(1, 3):
    rows_count = len(page.eles('tag:tr@class:ovui-tr'))
    
    for index in range(1,  10):
        try:
            # 【固定逻辑】提取 ID 和 标题
            video_id = page.ele(f'x://table/tbody/tr[{index}]//div[@class="id"]').text.replace('ID:', '').strip()
            video_title = page.ele(f'x://table/tbody/tr[{index}]//div[contains(@class, "title")]').text
            
            # 【固定逻辑】点击内容按钮
            page.ele(f'x://table/tbody/tr[{index}]//div[2]/span/div').click(by_js=true)

            video_text, temp_list, video_url = "未抓取到", [], ""
            t1 = time.time()
            while time.time() - t1 < 15:
                res = page.listen.wait(timeout=1)
                if res and res.response.body.get('status_code') == 0:
                    data = res.response.body.get('data', {})
                    if 'text' in data: video_text = data['text']
                    if 'StatsData' in data:
                        for r in data['StatsData']['Rows']:
                            temp_list.append({
                                'second': int(r['Dimensions']['duration']['Value']),
                                'click': r['Metrics']['live_watch_count_for_roi2_v2']['Value']
                            })
                v_tag = page.ele('tag:video', timeout=1)
                if v_tag: video_url = v_tag.attrs.get('src')
                if temp_list and video_text != "未抓取到" and video_url: break

            # --- 核心处理逻辑 ---
            # 1. 下载与 Whisper
            video_path = f"videos/{video_id}.mp4"
            if video_url and not os.path.exists(video_path):
                with open(video_path, 'wb') as f: f.write(requests.get(video_url).content)
            
            whisper_res = whisper_model.transcribe(video_path, language='zh', fp16=False)
            sec_to_text = {int(s['start']): s['text'] for s in whisper_res['segments']}

            # 2. 计算极值点索引
            df_v = pd.DataFrame(temp_list)
            df_v['diff'] = df_v['click'].diff()
            
            extrema = {
                '最高点击': int(df_v.loc[df_v['click'].idxmax(), 'second']),
                '最低点击': int(df_v.loc[df_v['click'].idxmin(), 'second']),
                '最大上升': int(df_v.loc[df_v['diff'].idxmax(), 'second']),
                '最大下降': int(df_v.loc[df_v['diff'].idxmin(), 'second'])
            }
            # 翻转字典以便通过秒数反查类型: {秒数: "类型名称"}
            sec_to_label = {v: k for k, v in extrema.items()}
            
            # 3. 截图与 AI 分析
            img_paths = capture_extrema_frames(video_path, extrema, video_id)
            max_sec_text = sec_to_text.get(extrema['最高点击'], "")
            # ai_info = get_ai_analysis(max_sec_text, video_text)

            # 4. 【关键改动】保留所有秒级数据并标记极值
            for r in temp_list:
                curr_sec = r['second']
                label = sec_to_label.get(curr_sec, "") # 只有极值秒数才有 label
                
                all_summary_rows.append({
                    '视频ID': video_id,
                    '视频标题': video_title,
                    'second': curr_sec,
                    'click': r['click'],
                    'whisper_text': sec_to_text.get(curr_sec, ""),
                    '指标类型': label, # 非极值点为空
                    '画面截图': img_paths.get(label, "") if label else "", # 非极值点为空
                    # 'refined_text': ai_info['refined_text'] if label == '最高点击' and ai_info else "",
                    # '情感倾向': ai_info['sentiment'] if label == '最高点击' and ai_info else "",
                    '准确脚本': video_text
                })

            print(f"✅ 素材 {video_id} 全量数据(含极值标记)已并入")

            # --- 【固定逻辑】退出浮窗 ---
            close_btn = page.ele('@data-auto-id=drawer-close-btn') or page.ele('@class=oc-drawer-close')
            if close_btn: close_btn.click(by_js=true)
            else: page.run_js('document.querySelector("[data-auto-id=\'drawer-close-btn\']").click()')
            time.sleep(1.2)

        except Exception as e:
            print(f"❌ 出错: {e}")
            page.run_js('document.querySelector("[data-auto-id=\'drawer-close-btn\']").click()')
            
    # --- 翻页逻辑 ---
    next_btn = page.ele('@class=ovui-icon ovui-page-turner__next-icon', timeout=2)
    if not next_btn or 'disabled' in next_btn.parent('tag:li').attrs.get('class', ''):
        print("🏁 全部采集完成")
        break
    next_btn.click()
    time.sleep(3)

# --- 3. 汇总生成带截图的总表 Excel ---
df_final = pd.DataFrame(all_summary_rows)
output_excel = "results/千川全量诊断总表.xlsx"
writer = pd.ExcelWriter(output_excel, engine='xlsxwriter')
df_final.to_excel(writer, sheet_name='素材诊断表', index=False)

workbook = writer.book
worksheet = writer.sheets['素材诊断表']
worksheet.set_column('I:I', 30) # 设置截图列宽

for i, path in enumerate(df_final['画面截图']):
    row_idx = i + 1
    if path and os.path.exists(path):
        worksheet.set_row(row_idx, 100) # 设置行高以适应图片
        worksheet.insert_image(row_idx, 8, path, {'x_scale': 0.15, 'y_scale': 0.15, 'x_offset': 5, 'y_offset': 5})

writer.close()
print(f"📊 报告生成完毕: {output_excel}")
