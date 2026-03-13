import json
import time
import hmac
import hashlib
import requests
from datetime import datetime
from typing import Dict, Any, Optional

from .config import API_CONFIG
from .utils import sorted_json_dumps


class TokenRefreshError(Exception):
    pass


class DouDianTokenCreator:
    def __init__(self, app_key: str, app_secret: str):
        self.app_key = app_key
        self.app_secret = app_secret
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
            self.app_secret.encode("utf-8"),
            sign_base.encode("utf-8"),
            hashlib.sha256
        ).hexdigest().lower()
        return signature
    
    def create_token(self, business_params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        method = "token.create"
        try:
            timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            param_json_str = sorted_json_dumps(business_params)
            sign = self.generate_sign(method, param_json_str, timestamp)
            
            api_path = method.replace(".", "/")
            url = f"{self.base_url}/{api_path}"
            print(f"【调试】请求URL: {url}")
            
            query_params = {
                "method": method,
                "app_key": self.app_key,
                "timestamp": timestamp,
                "v": self.version,
                "sign_method": API_CONFIG["sign_method"],
                "sign": sign
            }
            print(f"【调试】查询参数: {query_params}")
            print(f"【调试】业务参数JSON: {param_json_str}")
            
            response = self.session.post(
                url=url,
                params=query_params,
                data=param_json_str.encode("utf-8"),
                timeout=API_CONFIG["timeout"]
            )
            print(f"\n【调试】响应状态码: {response.status_code}")
            print(f"【调试】响应原始内容: {response.text}")
            
            result = response.json()
            
            if result.get("code") == 10000:
                return result.get("data", result)
            else:
                print(f"Token创建失败 [{method}]: {result.get('msg', '未知错误')} - {result.get('sub_msg', '')}")
                return None
                
        except Exception as e:
            print(f"请求异常 [{method}]: {str(e)}")
            return None


class TokenManager:
    def __init__(self, token_file: str, app_key: str, app_secret: str, expire_threshold: int = 2 * 24 * 3600):
        self.token_file = token_file
        self.app_key = app_key
        self.app_secret = app_secret
        self.expire_threshold = expire_threshold

    def load_token_from_file(self) -> Optional[Dict[str, Any]]:
        try:
            with open(self.token_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return None

    def save_token_to_file(self, token_data: Dict[str, Any]) -> None:
        with open(self.token_file, "w", encoding="utf-8") as f:
            json.dump(token_data, f, ensure_ascii=False, indent=4)

    def is_token_expired(self, token_data: Dict[str, Any]) -> bool:
        if not token_data or "expires_in" not in token_data:
            return True
        remaining_seconds = token_data["expires_in"] - int(time.time())
        return remaining_seconds < self.expire_threshold

    def generate_sign(self, params: Dict[str, Any], sign_method: str = "md5") -> str:
        sorted_params = sorted(params.items(), key=lambda x: x[0])
        sign_str = "&".join([f"{k}={v}" for k, v in sorted_params if v is not None and v != ""])
        sign_str += self.app_secret

        if sign_method == "hmac-sha256":
            sign = hmac.new(self.app_secret.encode("utf-8"), sign_str.encode("utf-8"), hashlib.sha256).hexdigest()
        else:
            sign = hashlib.md5(sign_str.encode("utf-8")).hexdigest()
        return sign.upper()

    def refresh_token(self, refresh_token: str) -> Dict[str, Any]:
        param_json = {
            "refresh_token": refresh_token,
            "grant_type": "refresh_token1"
        }
        param_json_str = json.dumps(param_json, separators=(",", ":"))

        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        public_params = {
            "method": "token.refresh",
            "app_key": self.app_key,
            "param_json": param_json_str,
            "timestamp": timestamp,
            "v": API_CONFIG["version"],
            "sign_method": API_CONFIG["sign_method"],
        }

        sign = self.generate_sign(public_params, API_CONFIG["sign_method"])
        public_params["sign"] = sign

        try:
            response = requests.post(API_CONFIG["base_url"], data=public_params, timeout=30)
            response.raise_for_status()
            result = response.json()
        except requests.exceptions.RequestException as e:
            raise TokenRefreshError(f"接口请求失败: {str(e)}") from e

        if result.get("code") != 10000:
            raise TokenRefreshError(f"刷新失败: {result.get('msg')}({result.get('sub_msg')})")
        
        data = result["data"]
        token_info = {
            "access_token": data["access_token"],
            "refresh_token": data["refresh_token"],
            "expires_in": int(data["expires_in"]),
            "shop_id": data["shop_id"],
            "shop_name": data["shop_name"],
            "update_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        return token_info

    def get_valid_access_token(self) -> str:
        token_data = self.load_token_from_file()

        if token_data and not self.is_token_expired(token_data):
            print("使用本地有效token")
            return token_data["access_token"]
        
        if not token_data or "refresh_token" not in token_data:
            raise TokenRefreshError("本地无refresh_token，请先初始化token信息")
        
        print("token即将过期/已过期，执行刷新...")
        new_token_data = self.refresh_token(token_data["refresh_token"])
        
        self.save_token_to_file(new_token_data)
        print("token刷新成功，已保存到文件")
        return new_token_data["access_token"]
