import requests
import json
import os
from datetime import datetime, timedelta
from typing import Optional, Dict, Any


class QianChuanTokenManager:
    def __init__(self, app_id: str, app_secret: str, token_file: str = None):
        self.app_id = app_id
        self.app_secret = app_secret
        self.token_file = token_file
        self.base_url = "https://api.oceanengine.com/open_api"

    def refresh_token(self, refresh_token: str) -> Optional[Dict[str, Any]]:
        url = f"{self.base_url}/oauth2/refresh_token/"
        payload = json.dumps({
            "app_id": int(self.app_id),
            "secret": self.app_secret,
            "refresh_token": refresh_token
        })
        headers = {"Content-Type": "application/json"}
        
        try:
            print("尝试刷新Access Token...")
            response = requests.post(url, headers=headers, data=payload, timeout=15)
            response.raise_for_status()
            result = response.json()
            
            if result.get("code") == 0:
                data = result.get("data", {})
                access_token = data.get("access_token")
                new_refresh_token = data.get("refresh_token")
                expires_in = data.get("expires_in", 0)
                refresh_token_expires_in = data.get("refresh_token_expires_in", 0)
                
                print(f"✅ AccessToken刷新成功！")
                print(f"⏳ access_token过期时间：{expires_in}秒（{datetime.now() + timedelta(seconds=expires_in)}）")
                
                token_info = {
                    "access_token": access_token,
                    "refresh_token": new_refresh_token,
                    "access_token_expires_at": (datetime.now() + timedelta(seconds=expires_in)).strftime("%Y-%m-%d %H:%M:%S"),
                    "refresh_token_expires_at": (datetime.now() + timedelta(seconds=refresh_token_expires_in)).strftime("%Y-%m-%d %H:%M:%S"),
                    "update_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                
                if self.token_file:
                    self._save_token(token_info)
                
                return token_info
            else:
                print(f"❌ AccessToken刷新接口请求失败：{result.get('message')}")
                return None
                
        except requests.exceptions.RequestException as e:
            print(f"❌ HTTP请求异常：{str(e)}")
            return None
        except json.JSONDecodeError:
            print(f"❌ 响应数据解析失败")
            return None

    def _save_token(self, token_data: Dict[str, Any]) -> None:
        if self.token_file:
            try:
                token_dir = os.path.dirname(self.token_file)
                if token_dir and not os.path.exists(token_dir):
                    os.makedirs(token_dir, exist_ok=True)
                
                with open(self.token_file, "w", encoding="utf-8") as f:
                    json.dump(token_data, f, indent=2, ensure_ascii=False)
                print("✅ 令牌信息已保存到文件")
            except Exception as e:
                print(f"保存Token失败：{e}")

    def load_token(self) -> Optional[Dict[str, Any]]:
        if self.token_file and os.path.exists(self.token_file):
            try:
                with open(self.token_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                print(f"读取Token失败：{e}")
        return None
