import json
import time
import requests
from typing import Dict, Any, Optional

from .config import API_CONFIG
from .utils import generate_signature


class TokenRefreshError(Exception):
    pass


class XiaoHongShuTokenManager:
    def __init__(self, app_key: str, app_secret: str, token_file: str = None):
        self.app_key = app_key
        self.app_secret = app_secret
        self.token_file = token_file
        self.base_url = API_CONFIG["base_url"]

    def load_token_from_file(self) -> Optional[Dict[str, Any]]:
        if not self.token_file:
            return None
        try:
            with open(self.token_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return None

    def save_token_to_file(self, token_data: Dict[str, Any]) -> None:
        if not self.token_file:
            return
        with open(self.token_file, "w", encoding="utf-8") as f:
            json.dump(token_data, f, ensure_ascii=False, indent=4)

    def is_token_expired(self, token_data: Dict[str, Any], threshold: int = 3600) -> bool:
        if not token_data or "expires_at" not in token_data:
            return True
        return time.time() > (token_data["expires_at"] - threshold)

    def get_token(self, auth_code: str = None, refresh_token: str = None) -> Dict[str, Any]:
        if auth_code:
            return self._get_token_by_code(auth_code)
        elif refresh_token:
            return self._refresh_token(refresh_token)
        else:
            raise TokenRefreshError("需要提供 auth_code 或 refresh_token")

    def _get_token_by_code(self, auth_code: str) -> Dict[str, Any]:
        method = "oauth.token.create"
        timestamp = str(int(time.time()))
        
        business_params = {
            "code": auth_code,
            "grant_type": "authorization_code"
        }
        
        param_json = json.dumps(business_params, separators=(',', ':'))
        sign = generate_signature(
            self.app_key, self.app_secret, "", timestamp, method, param_json
        )
        
        query_params = {
            "appId": self.app_key,
            "timestamp": timestamp,
            "method": method,
            "version": API_CONFIG["version"],
            "sign": sign
        }
        
        try:
            response = requests.post(
                self.base_url,
                params=query_params,
                data=param_json.encode('utf-8'),
                timeout=API_CONFIG["timeout"]
            )
            result = response.json()
            
            if result.get("success"):
                data = result.get("data", {})
                token_data = {
                    "access_token": data.get("accessToken"),
                    "refresh_token": data.get("refreshToken"),
                    "expires_at": int(time.time()) + data.get("expiresIn", 86400),
                    "seller_id": data.get("sellerId"),
                    "seller_name": data.get("sellerName"),
                }
                return token_data
            else:
                raise TokenRefreshError(f"获取Token失败: {result.get('errorMsg')}")
                
        except Exception as e:
            raise TokenRefreshError(f"请求异常: {str(e)}")

    def _refresh_token(self, refresh_token: str) -> Dict[str, Any]:
        method = "oauth.token.refresh"
        timestamp = str(int(time.time()))
        
        business_params = {
            "refresh_token": refresh_token,
            "grant_type": "refresh_token"
        }
        
        param_json = json.dumps(business_params, separators=(',', ':'))
        sign = generate_signature(
            self.app_key, self.app_secret, "", timestamp, method, param_json
        )
        
        query_params = {
            "appId": self.app_key,
            "timestamp": timestamp,
            "method": method,
            "version": API_CONFIG["version"],
            "sign": sign
        }
        
        try:
            response = requests.post(
                self.base_url,
                params=query_params,
                data=param_json.encode('utf-8'),
                timeout=API_CONFIG["timeout"]
            )
            result = response.json()
            
            if result.get("success"):
                data = result.get("data", {})
                token_data = {
                    "access_token": data.get("accessToken"),
                    "refresh_token": data.get("refreshToken"),
                    "expires_at": int(time.time()) + data.get("expiresIn", 86400),
                    "seller_id": data.get("sellerId"),
                    "seller_name": data.get("sellerName"),
                }
                return token_data
            else:
                raise TokenRefreshError(f"刷新Token失败: {result.get('errorMsg')}")
                
        except Exception as e:
            raise TokenRefreshError(f"请求异常: {str(e)}")

    def get_valid_access_token(self, auth_code: str = None) -> str:
        token_data = self.load_token_from_file()
        
        if token_data and not self.is_token_expired(token_data):
            print("使用本地有效Token")
            return token_data["access_token"]
        
        if token_data and token_data.get("refresh_token"):
            print("Token已过期，正在刷新...")
            new_token_data = self._refresh_token(token_data["refresh_token"])
            self.save_token_to_file(new_token_data)
            print("Token刷新成功")
            return new_token_data["access_token"]
        
        if auth_code:
            print("使用授权码获取Token...")
            token_data = self._get_token_by_code(auth_code)
            self.save_token_to_file(token_data)
            print("Token获取成功")
            return token_data["access_token"]
        
        raise TokenRefreshError("无法获取有效Token，请提供 auth_code")
