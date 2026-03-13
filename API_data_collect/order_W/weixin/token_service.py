import requests
import json
import os
from typing import Optional, Dict, Any


class WeChatTokenManager:
    def __init__(self, app_id: str, app_secret: str, token_file: str = None):
        self.app_id = app_id
        self.app_secret = app_secret
        self.token_file = token_file
        self.base_url = "https://api.weixin.qq.com"

    def get_access_token(self) -> Optional[str]:
        url = f"{self.base_url}/cgi-bin/token"
        params = {
            "grant_type": "client_credential",
            "appid": self.app_id,
            "secret": self.app_secret
        }
        
        try:
            response = requests.get(url, params=params, timeout=15)
            result = response.json()
            
            if "access_token" in result:
                print(f"✅ AccessToken获取成功：{result['access_token']}")
                print(f"⏳ 有效期：{result['expires_in']}秒（2小时）")
                
                if self.token_file:
                    self._save_token(result)
                
                return result["access_token"]
            else:
                print(f"❌ 获取失败：{result}")
                return None
        except Exception as e:
            print(f"❌ 请求异常：{str(e)}")
            return None

    def _save_token(self, token_data: Dict[str, Any]) -> None:
        if self.token_file:
            try:
                token_dir = os.path.dirname(self.token_file)
                if token_dir and not os.path.exists(token_dir):
                    os.makedirs(token_dir, exist_ok=True)
                
                with open(self.token_file, "w", encoding="utf-8") as f:
                    json.dump(token_data, f, ensure_ascii=False, indent=2)
            except Exception as e:
                print(f"保存Token失败：{e}")

    def load_token(self) -> Optional[str]:
        if self.token_file and os.path.exists(self.token_file):
            try:
                with open(self.token_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    return data.get("access_token")
            except Exception as e:
                print(f"读取Token失败：{e}")
        return None
