#!/usr/bin/env python3
"""
单条视频：下载 + Whisper ASR，输出 JSON 到 stdout 供 n8n 千问节点使用。
用法: python3 run_single_video.py <视频ID> <达人昵称>
依赖: DrissionPage(浏览器)、whisper、与 material_rank 相同的环境。
"""
import os
import sys
import json
import time
import zhconv
import requests
import whisper
from DrissionPage import ChromiumPage

# 与 material_rank 一致
SAVE_DIR = os.environ.get("VIDEO_SAVE_DIR", "/Volumes/新加卷/刘0204")
DRISSION_ADDR = os.environ.get("DRISSION_ADDR", "127.0.0.1:9223")

def download_video(page, v_id, file_path):
    page.listen.clear()
    page.get(f'https://www.douyin.com/video/{v_id}')
    res = page.listen.wait(timeout=10)
    if res and res.response.body:
        try:
            data = res.response.body
            if 'aweme_detail' not in data:
                return False
            video_url = data['aweme_detail']['video']['play_addr']['url_list'][0]
            cookies = {c['name']: c['value'] for c in page.cookies()}
            headers = {'User-Agent': page.user_agent, 'Referer': 'https://www.douyin.com/', 'Range': 'bytes=0-'}
            resp = requests.get(video_url, headers=headers, cookies=cookies, stream=True, timeout=30)
            if resp.status_code in [200, 206]:
                with open(file_path, 'wb') as f:
                    for chunk in resp.iter_content(chunk_size=1024*1024):
                        f.write(chunk)
                return os.path.getsize(file_path) > 102400
        except Exception:
            pass
    return False

def main():
    if len(sys.argv) < 3:
        out = {"error": "usage: run_single_video.py <视频ID> <达人昵称>", "segments_text": ""}
        print(json.dumps(out, ensure_ascii=False))
        sys.exit(1)
    v_id = sys.argv[1].strip()
    author = "".join([c for c in str(sys.argv[2]) if c.isalnum()])
    if not v_id:
        out = {"error": "empty video_id", "segments_text": ""}
        print(json.dumps(out, ensure_ascii=False))
        sys.exit(1)
    os.makedirs(SAVE_DIR, exist_ok=True)
    file_path = f"{SAVE_DIR}/{v_id}_{author}.mp4"
    page = ChromiumPage(addr_or_opts=DRISSION_ADDR)
    page.listen.start('aweme/v1/web/aweme/detail')
    if not os.path.exists(file_path):
        if not download_video(page, v_id, file_path):
            page.listen.stop()
            out = {"error": "download_failed", "video_id": v_id, "segments_text": ""}
            print(json.dumps(out, ensure_ascii=False))
            sys.exit(0)
    try:
        asr_model = whisper.load_model("base")
        result = asr_model.transcribe(file_path, language='zh', fp16=False)
        raw_segments = []
        for seg in result.get('segments', []):
            start_time = time.strftime('%M:%S', time.gmtime(seg['start']))
            text_content = zhconv.convert(seg.get('text', ''), 'zh-cn')
            raw_segments.append(f"[{start_time}] {text_content}")
        segments_str = "\n".join(raw_segments)
        out = {"error": None, "video_id": v_id, "segments_text": segments_str, "segments": result.get('segments', [])}
        print(json.dumps(out, ensure_ascii=False))
    except Exception as e:
        out = {"error": str(e), "video_id": v_id, "segments_text": ""}
        print(json.dumps(out, ensure_ascii=False))
    finally:
        page.listen.stop()

if __name__ == "__main__":
    main()
