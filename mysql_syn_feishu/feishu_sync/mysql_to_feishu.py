import pymysql
import pandas as pd
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple, Callable

from .config import FeishuConfig, MySQLConfig
from .client import FeishuClient
from .utils import clean_value_for_feishu, clean_primary_key


class MySQLToFeishuSync:
    def __init__(
        self,
        feishu_config: FeishuConfig,
        mysql_config: MySQLConfig
    ):
        self.feishu_config = feishu_config
        self.mysql_config = mysql_config
        self.client = FeishuClient(
            app_id=feishu_config.app_id,
            app_secret=feishu_config.app_secret,
            app_token=feishu_config.app_token,
            table_id=feishu_config.table_id
        )
        self._existing_keys = None
        self._key_to_record_id = None
        self._field_type_mapping = None

    def _get_mysql_connection(self):
        return pymysql.connect(
            host=self.mysql_config.host,
            port=self.mysql_config.port,
            user=self.mysql_config.user,
            password=self.mysql_config.password,
            database=self.mysql_config.database,
            charset=self.mysql_config.charset
        )

    def get_last_sync_time(self) -> datetime:
        conn = None
        try:
            conn = self._get_mysql_connection()
            with conn.cursor() as cursor:
                sql = f"SELECT last_sync_time FROM {self.mysql_config.status_table} WHERE table_name = %s"
                cursor.execute(sql, (self.feishu_config.table_id,))
                result = cursor.fetchone()
                if result:
                    return result[0]
                else:
                    print(f"⚠️ 未找到同步记录，执行全量同步")
                    return datetime(1970, 1, 1)
        finally:
            if conn:
                conn.close()

    def update_sync_time(self, sync_time: datetime):
        conn = None
        try:
            conn = self._get_mysql_connection()
            with conn.cursor() as cursor:
                sql = f"REPLACE INTO {self.mysql_config.status_table} (table_name, last_sync_time) VALUES (%s, %s)"
                cursor.execute(sql, (self.feishu_config.table_id, sync_time))
            conn.commit()
            print(f"✅ 同步时间已更新: {sync_time}")
        finally:
            if conn:
                conn.close()

    def init_feishu_state(self):
        _, self._field_type_mapping = self.client.get_field_mapping()
        self._existing_keys, self._key_to_record_id = self.client.get_existing_keys(
            primary_key_field=self.feishu_config.primary_key_field,
            page_size=1000,
            sleep_time=self.feishu_config.sleep_time
        )

    def query_mysql_data(
        self, 
        sql: str, 
        params: tuple = (), 
        batch_size: int = 1000
    ) -> List[Dict]:
        conn = self._get_mysql_connection()
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        cursor.execute(sql, params)
        
        try:
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                yield rows
        finally:
            cursor.close()
            conn.close()

    def clean_record_for_feishu(self, record: Dict, clean_func: Callable = None) -> Dict:
        fields = {}
        for key, value in record.items():
            if key not in self._field_type_mapping:
                continue
            
            f_type = self._field_type_mapping[key]
            
            if clean_func:
                val = clean_func(key, value, f_type)
            else:
                val = clean_value_for_feishu(value, f_type)
            
            if val is not None:
                fields[key] = val
        
        return fields

    def batch_upsert_to_feishu(
        self,
        to_create: List[Dict],
        to_update: List[Dict],
        batch_num: int,
        clean_func: Callable = None
    ) -> Tuple[int, int]:
        create_count = 0
        update_count = 0
        
        if to_create:
            items_to_create = []
            for r in to_create:
                cleaned = self.clean_record_for_feishu(r, clean_func)
                if cleaned:
                    items_to_create.append({"fields": cleaned})
            
            if items_to_create:
                success, created = self.client.batch_create_records(
                    items_to_create,
                    batch_size=self.feishu_config.batch_size,
                    sleep_time=self.feishu_config.sleep_time
                )
                create_count = success
                
                for i, item in enumerate(items_to_create):
                    key_value = item["fields"].get(self.feishu_config.primary_key_field)
                    if key_value and i < len(created):
                        self._key_to_record_id[key_value] = created[i].get("record_id")
                        self._existing_keys.add(key_value)
        
        if to_update:
            items_to_update = []
            for r in to_update:
                key_value = r.get(self.feishu_config.primary_key_field)
                if not key_value:
                    continue
                
                record_id = self._key_to_record_id.get(key_value)
                if not record_id:
                    continue
                
                cleaned = self.clean_record_for_feishu(r, clean_func)
                if cleaned:
                    items_to_update.append({
                        "record_id": record_id,
                        "fields": cleaned
                    })
            
            if items_to_update:
                update_count = self.client.batch_update_records(
                    items_to_update,
                    batch_size=self.feishu_config.batch_size,
                    sleep_time=self.feishu_config.sleep_time
                )
        
        return create_count, update_count

    def sync(
        self,
        sql_template: str,
        full_sync: bool = False,
        clean_func: Callable = None
    ) -> Tuple[int, int, datetime]:
        task_start_time = datetime.now()
        print(f"\n{'='*60}")
        print(f"🚀 MySQL->飞书 同步任务开始: {task_start_time}")
        print(f"📋 MySQL表: {self.mysql_config.target_table}")
        print(f"📋 飞书表: {self.feishu_config.table_id}")
        print(f"📋 主键字段: {self.feishu_config.primary_key_field}")
        print(f"📋 同步模式: {'全量同步' if full_sync else '增量同步'}")
        print(f"{'='*60}")
        
        try:
            if full_sync:
                last_sync_time = datetime(1970, 1, 1)
                print(f"🕒 全量同步模式")
            else:
                last_sync_time = self.get_last_sync_time()
                print(f"🕒 增量同步，上次同步时间: {last_sync_time}")
            
            self.init_feishu_state()
            
            update_field = self.mysql_config.update_time_field
            incremental_sql = f"{sql_template} WHERE `{update_field}` > %s"
            print(f"🔍 执行查询: {incremental_sql}")
            
            total_create = 0
            total_update = 0
            batch_num = 0
            
            for batch_data in self.query_mysql_data(incremental_sql, params=(last_sync_time,)):
                print(f"\n📥 读取到 {len(batch_data)} 条MySQL数据")
                
                to_create = []
                to_update = []
                
                for record in batch_data:
                    key_value = record.get(self.feishu_config.primary_key_field)
                    if not key_value:
                        continue
                    
                    if key_value not in self._existing_keys:
                        to_create.append(record)
                    else:
                        to_update.append(record)
                
                if to_create or to_update:
                    batch_num += 1
                    c, u = self.batch_upsert_to_feishu(to_create, to_update, batch_num, clean_func)
                    total_create += c
                    total_update += u
            
            self.update_sync_time(task_start_time)
            
            print(f"\n🎉 同步完成！新增 {total_create} 条，更新 {total_update} 条")
            return total_create, total_update, task_start_time
            
        except Exception as e:
            print(f"\n❌ 同步失败: {e}")
            raise
