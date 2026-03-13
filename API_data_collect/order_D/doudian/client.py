import time
import hmac
import hashlib
import requests
from typing import Dict, Any, Optional
from pprint import pprint

from .config import API_CONFIG
from .utils import sorted_json_dumps


class DouDianAPIClient:
    def __init__(self, app_key: str, app_secret: str, access_token: str):
        self.app_key = app_key
        self.app_secret = app_secret
        self.access_token = access_token
        self.base_url = API_CONFIG["base_url"]
        self.version = API_CONFIG["version"]
        self.session = requests.Session()
        self.session.headers.update({
            "Content-Type": "application/json;charset=utf-8",
            "User-Agent": "DouDianAPIClient/1.0"
        })

    def generate_sign(self, method: str, param_json_str: str, timestamp: str) -> str:
        sign_base = (
            f"app_key{self.app_key}"
            f"method{method}"
            f"param_json{param_json_str}"
            f"timestamp{timestamp}"
            f"v{self.version}"
        )
        sign_base = f"{self.app_secret}{sign_base}{self.app_secret}"
        
        signature = hmac.new(
            self.app_secret.encode(),
            sign_base.encode(),
            hashlib.sha256
        ).hexdigest().lower()
        return signature
    
    def request(self, method: str, business_params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            param_json_str = sorted_json_dumps(business_params)
            sign = self.generate_sign(method, param_json_str, timestamp)
            
            api_path = method.replace(".", "/")
            url = f"{self.base_url}/{api_path}"
            
            query_params = {
                "method": method,
                "app_key": self.app_key,
                "access_token": self.access_token,
                "timestamp": timestamp,
                "v": self.version,
                "sign_method": API_CONFIG["sign_method"],
                "sign": sign
            }
            
            response = self.session.post(
                url=url,
                params=query_params,
                data=param_json_str.encode(),
                timeout=API_CONFIG["timeout"]
            )
            pprint(response)
            result = response.json()
            
            if result.get("code") == 0 or result.get("code") == 10000:
                return result.get("data", result)
            else:
                print(f"API错误 [{method}]: {result.get('msg', '未知错误')} - {result.get('sub_msg', '')}")
                return None
                
        except Exception as e:
            print(f"请求异常 [{method}]: {str(e)}")
            return None
