import requests
import time
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass


@dataclass
class FeishuField:
    field_id: str
    field_name: str
    field_type: int


class FeishuClient:
    def __init__(self, app_id: str, app_secret: str, app_token: str, table_id: str):
        self.app_id = app_id
        self.app_secret = app_secret
        self.app_token = app_token
        self.table_id = table_id
        self.base_url = "https://open.feishu.cn/open-apis"
        self._access_token = None
        self._field_mapping = None
        self._field_type_mapping = None

    def get_access_token(self) -> str:
        if self._access_token:
            return self._access_token
        
        url = f"{self.base_url}/auth/v3/tenant_access_token/internal"
        payload = {
            "app_id": self.app_id,
            "app_secret": self.app_secret
        }
        
        try:
            response = requests.post(url, json=payload, timeout=15)
            response.raise_for_status()
            result = response.json()
            
            if result.get("code") != 0:
                raise Exception(f"获取Token失败: {result.get('msg')}")
            
            self._access_token = result.get("tenant_access_token")
            print(f"✅ 飞书Token获取成功")
            return self._access_token
            
        except requests.exceptions.RequestException as e:
            raise Exception(f"获取飞书Token失败: {e}")

    def _get_headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.get_access_token()}",
            "Content-Type": "application/json"
        }

    def get_field_mapping(self) -> Tuple[Dict[str, str], Dict[str, int]]:
        if self._field_mapping and self._field_type_mapping:
            return self._field_mapping, self._field_type_mapping
        
        url = f"{self.base_url}/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/fields"
        
        try:
            response = requests.get(url, headers=self._get_headers(), timeout=15)
            response.raise_for_status()
            result = response.json()
            
            if result.get("code") != 0:
                raise Exception(f"获取字段映射失败: {result.get('msg')}")
            
            self._field_mapping = {}
            self._field_type_mapping = {}
            
            for item in result.get("data", {}).get("items", []):
                field_name = item.get("field_name")
                field_id = item.get("field_id")
                field_type = item.get("type")
                
                if field_name and field_id:
                    self._field_mapping[field_name] = field_id
                if field_name and field_type is not None:
                    self._field_type_mapping[field_name] = field_type
            
            print(f"✅ 获取字段映射成功，共{len(self._field_mapping)}个字段")
            return self._field_mapping, self._field_type_mapping
            
        except requests.exceptions.RequestException as e:
            raise Exception(f"获取字段映射失败: {e}")

    def fetch_all_records(
        self, 
        page_size: int = 100,
        sleep_time: float = 0.3
    ) -> List[Dict[str, Any]]:
        url = f"{self.base_url}/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/records"
        params = {"page_size": page_size}
        
        all_records = []
        page_token = ""
        
        while True:
            params["page_token"] = page_token
            try:
                response = requests.get(url, headers=self._get_headers(), params=params, timeout=30)
                response.raise_for_status()
                result = response.json()
                
                if result.get("code") != 0:
                    print(f"⚠️ 获取记录失败: {result.get('msg')}")
                    break
                
                items = result.get("data", {}).get("items", [])
                for item in items:
                    all_records.append({
                        "record_id": item.get("record_id"),
                        "fields": item.get("fields", {})
                    })
                
                page_token = result.get("data", {}).get("page_token")
                if not page_token:
                    break
                
                time.sleep(sleep_time)
                
            except requests.exceptions.RequestException as e:
                print(f"❌ 请求飞书记录失败: {e}")
                break
        
        print(f"✅ 从飞书获取{len(all_records)}条记录")
        return all_records

    def get_existing_keys(
        self, 
        primary_key_field: str,
        page_size: int = 1000,
        sleep_time: float = 0.3
    ) -> Tuple[set, Dict[str, str]]:
        field_mapping, _ = self.get_field_mapping()
        
        if primary_key_field not in field_mapping:
            raise ValueError(f"飞书表中找不到字段: {primary_key_field}")
        
        url = f"{self.base_url}/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/records"
        params = {
            "page_size": page_size,
            "field_names": json.dumps([primary_key_field])
        }
        
        import json
        
        existing_keys = set()
        key_to_record_id = {}
        page_token = ""
        
        print(f"🔍 正在查询飞书表主键 '{primary_key_field}' ...")
        
        while True:
            params["page_token"] = page_token
            try:
                response = requests.get(url, headers=self._get_headers(), params=params, timeout=60)
                result = response.json()
                
                if result.get("code") != 0:
                    print(f"⚠️ 获取记录失败: {result.get('msg')}")
                    break
                
                items = result.get("data", {}).get("items", [])
                for item in items:
                    key_value = item.get("fields", {}).get(primary_key_field)
                    record_id = item.get("record_id")
                    if key_value and record_id:
                        existing_keys.add(key_value)
                        key_to_record_id[key_value] = record_id
                
                page_token = result.get("data", {}).get("page_token")
                if not page_token:
                    break
                
                time.sleep(sleep_time)
                
            except requests.exceptions.RequestException as e:
                print(f"❌ 请求失败: {e}")
                break
        
        print(f"✅ 飞书表共有{len(existing_keys)}条唯一记录")
        return existing_keys, key_to_record_id

    def batch_create_records(
        self, 
        records: List[Dict[str, Any]], 
        batch_size: int = 50,
        sleep_time: float = 0.3
    ) -> Tuple[int, List[Dict]]:
        url = f"{self.base_url}/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/records/batch_create"
        
        success_count = 0
        created_records = []
        
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            payload = {"records": [{"fields": r} for r in batch]}
            
            try:
                response = requests.post(url, headers=self._get_headers(), json=payload, timeout=30)
                result = response.json()
                
                if result.get("code") == 0:
                    success_count += len(batch)
                    created_records.extend(result.get("data", {}).get("records", []))
                    print(f"✅ 批次新增成功: {len(batch)}条")
                else:
                    print(f"❌ 批次新增失败: {result.get('msg')}")
                
                time.sleep(sleep_time)
                
            except requests.exceptions.RequestException as e:
                print(f"❌ 新增请求失败: {e}")
        
        return success_count, created_records

    def batch_update_records(
        self, 
        records: List[Dict[str, Any]], 
        batch_size: int = 50,
        sleep_time: float = 0.3
    ) -> int:
        url = f"{self.base_url}/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/records/batch_update"
        
        success_count = 0
        
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            payload = {"records": batch}
            
            try:
                response = requests.post(url, headers=self._get_headers(), json=payload, timeout=30)
                result = response.json()
                
                if result.get("code") == 0:
                    success_count += len(batch)
                    print(f"✅ 批次更新成功: {len(batch)}条")
                else:
                    print(f"❌ 批次更新失败: {result.get('msg')}")
                
                time.sleep(sleep_time)
                
            except requests.exceptions.RequestException as e:
                print(f"❌ 更新请求失败: {e}")
        
        return success_count


import json
