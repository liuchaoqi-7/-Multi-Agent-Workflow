import requests
import json
import threading
from typing import Dict, Any, Optional

from .config import API_CONFIG


thread_local = threading.local()


def get_session():
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.Session()
        thread_local.session.headers.update({"Content-Type": "application/json"})
    return thread_local.session


class QianChuanClient:
    def __init__(self, access_token: str):
        self.access_token = access_token
        self.base_url = API_CONFIG["base_url"]
        self.timeout = API_CONFIG["timeout"]

    def request(self, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        session = get_session()
        headers = {
            "Access-Token": self.access_token,
            "Content-Type": "application/json"
        }
        
        url = f"{self.base_url}/v1.0/qianchuan/report/uni_promotion/data/get/"
        
        try:
            response = session.get(
                url=url,
                params=params,
                headers=headers,
                timeout=self.timeout
            )
            response.raise_for_status()
            result = response.json()
            
            if result.get("code") != 0:
                print(f"API错误: {result.get('message')} (request_id: {result.get('request_id')})")
                return None
            
            return result
        except requests.exceptions.RequestException as e:
            print(f"网络请求异常: {e}")
            return None
        except json.JSONDecodeError as e:
            print(f"响应解析异常: {e}")
            return None

    def request_with_retry(
        self, 
        params: Dict[str, Any], 
        max_retries: int = 10,
        base_delay: float = 1.0
    ) -> Optional[Dict[str, Any]]:
        retry_count = 0
        retry_delay = base_delay
        
        while retry_count <= max_retries:
            try:
                result = self.request(params)
                if result:
                    return result
                
                retry_count += 1
                if retry_count <= max_retries:
                    import time
                    time.sleep(retry_delay)
                    retry_delay *= 2
                    
            except Exception as e:
                retry_count += 1
                if retry_count > max_retries:
                    print(f"重试{max_retries}次后仍失败: {e}")
                    return None
                import time
                time.sleep(retry_delay)
                retry_delay *= 2
        
        return None
