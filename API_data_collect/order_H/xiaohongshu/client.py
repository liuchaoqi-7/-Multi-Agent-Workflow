import time
import json
import requests
from typing import Dict, Any, Optional
from pprint import pprint

from .config import API_CONFIG
from .utils import generate_signature, sorted_json_dumps


class XiaoHongShuAPIClient:
    def __init__(self, app_key: str, app_secret: str, access_token: str):
        self.app_key = app_key
        self.app_secret = app_secret
        self.access_token = access_token
        self.base_url = API_CONFIG["base_url"]
        self.version = API_CONFIG["version"]
        self.timeout = API_CONFIG["timeout"]
        self.session = requests.Session()
        self.session.headers.update({
            "Content-Type": "application/json;charset=utf-8",
            "User-Agent": "XiaoHongShuAPIClient/1.0"
        })

    def request(self, method: str, business_params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            timestamp = str(int(time.time()))
            param_json = sorted_json_dumps(business_params)
            
            sign = generate_signature(
                self.app_key,
                self.app_secret,
                self.access_token,
                timestamp,
                method,
                param_json
            )
            
            query_params = {
                "appId": self.app_key,
                "accessToken": self.access_token,
                "timestamp": timestamp,
                "method": method,
                "version": self.version,
                "sign": sign
            }
            
            response = self.session.post(
                url=self.base_url,
                params=query_params,
                data=param_json.encode('utf-8'),
                timeout=self.timeout
            )
            
            result = response.json()
            
            if result.get("success"):
                return result.get("data", result)
            else:
                error_msg = result.get("errorMsg", "未知错误")
                error_code = result.get("errorCode", "")
                print(f"API错误 [{method}]: {error_code} - {error_msg}")
                return None
                
        except Exception as e:
            print(f"请求异常 [{method}]: {str(e)}")
            return None

    def request_with_retry(self, method: str, business_params: Dict[str, Any], 
                           max_retries: int = 3, delay: float = 1.0) -> Optional[Dict[str, Any]]:
        for attempt in range(max_retries):
            result = self.request(method, business_params)
            if result is not None:
                return result
            
            if attempt < max_retries - 1:
                print(f"请求失败，{delay}秒后重试 ({attempt + 1}/{max_retries})")
                time.sleep(delay)
                delay *= 2
        
        return None
