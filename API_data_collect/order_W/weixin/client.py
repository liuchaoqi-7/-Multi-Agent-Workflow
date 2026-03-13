import requests
import json
from typing import Dict, Any, Optional

from .config import API_CONFIG


class WeChatAPIClient:
    def __init__(self, access_token: str):
        self.access_token = access_token
        self.base_url = API_CONFIG["base_url"]
        self.timeout = API_CONFIG["timeout"]
        self.session = requests.Session()
        self.session.headers.update({
            "Content-Type": "application/json;charset=utf-8"
        })

    def request(self, method: str, business_params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        api_path = method.replace(".", "/")
        url = f"{self.base_url}/{api_path}?access_token={self.access_token}"
        
        request_params = {"access_token": self.access_token}
        request_params.update(business_params)
        
        try:
            response = self.session.post(
                url=url,
                json=request_params,
                timeout=self.timeout
            )
            
            print(f"请求URL: {url}")
            print(f"响应状态码: {response.status_code}")
            
            result = response.json()
            
            if result.get("errcode") == 0:
                return result
            else:
                print(f"API错误: {result.get('errmsg', '未知错误')}")
                return None
                
        except requests.exceptions.RequestException as e:
            print(f"网络请求异常: {e}")
            return None
        except json.JSONDecodeError as e:
            print(f"响应解析异常: {e}")
            return None

    def request_with_path(self, api_path: str, business_params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        url = f"{self.base_url}/{api_path}?access_token={self.access_token}"
        
        try:
            response = self.session.post(
                url=url,
                json=business_params,
                timeout=self.timeout
            )
            
            result = response.json()
            
            if result.get("errcode") == 0:
                return result
            else:
                print(f"API错误: {result.get('errmsg', '未知错误')}")
                return None
                
        except Exception as e:
            print(f"请求异常: {e}")
            return None
