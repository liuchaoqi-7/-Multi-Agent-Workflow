import pymysql
import pandas as pd
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
from sqlalchemy import create_engine, text

from .config import FeishuConfig, MySQLConfig
from .client import FeishuClient
from .utils import clean_value_for_sql, ms_timestamp_to_datetime_str, clean_dataframe_for_sql


class FeishuToMySQLSync:
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
        self._engine = None

    def _get_engine(self):
        if not self._engine:
            db_url = f"mysql+pymysql://{self.mysql_config.user}:{self.mysql_config.password}@{self.mysql_config.host}:{self.mysql_config.port}/{self.mysql_config.database}"
            self._engine = create_engine(db_url)
        return self._engine

    def get_last_sync_time(self) -> datetime:
        engine = self._get_engine()
        with engine.connect() as conn:
            result = conn.execute(
                text(f"SELECT last_sync_time FROM {self.mysql_config.status_table} WHERE table_name = :name"),
                {"name": self.feishu_config.table_id}
            ).fetchone()
            if result:
                return result[0]
            else:
                print(f"⚠️ 未找到同步记录，执行全量同步")
                return datetime(1970, 1, 1)

    def update_sync_time(self, sync_time: datetime):
        engine = self._get_engine()
        with engine.connect() as conn:
            conn.execute(
                text(f"""
                    REPLACE INTO {self.mysql_config.status_table} (table_name, last_sync_time)
                    VALUES (:name, :time)
                """),
                {"name": self.feishu_config.table_id, "time": sync_time}
            )
            conn.commit()
        print(f"✅ 同步时间已更新: {sync_time}")

    def fetch_feishu_data(self) -> pd.DataFrame:
        records = self.client.fetch_all_records(
            page_size=self.feishu_config.batch_size,
            sleep_time=self.feishu_config.sleep_time
        )
        
        if not records:
            return pd.DataFrame()
        
        data = []
        for record in records:
            row = record.get("fields", {})
            row["record_id"] = record.get("record_id")
            data.append(row)
        
        df = pd.DataFrame(data)
        return df

    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        
        if self.feishu_config.field_rename_map:
            df = df.rename(columns=self.feishu_config.field_rename_map)
        
        df = clean_dataframe_for_sql(df, self.feishu_config.datetime_fields)
        
        return df

    def upsert_to_mysql(self, df: pd.DataFrame) -> int:
        if df.empty:
            print("📥 无数据需要写入MySQL")
            return 0
        
        engine = self._get_engine()
        primary_key = self.feishu_config.primary_key_field
        
        if primary_key not in df.columns:
            raise ValueError(f"主键字段 '{primary_key}' 不在数据中")
        
        df = df.drop_duplicates(subset=[primary_key], keep='last')
        
        data_to_insert = df.to_dict('records')
        columns = df.columns.tolist()
        
        placeholders = ', '.join([f':{col}' for col in columns])
        update_clause = ', '.join([f"{col}=VALUES({col})" for col in columns if col != primary_key])
        
        full_table_name = f"{self.mysql_config.database}.{self.mysql_config.target_table}"
        sql = f"""
            INSERT INTO {full_table_name} ({', '.join(columns)})
            VALUES ({placeholders})
            ON DUPLICATE KEY UPDATE {update_clause}
        """
        
        try:
            with engine.connect() as conn:
                conn.execute(text(sql), data_to_insert)
                conn.commit()
                print(f"✅ 成功写入MySQL: {len(data_to_insert)}条")
                return len(data_to_insert)
        except Exception as e:
            print(f"❌ 写入MySQL失败: {e}")
            raise

    def sync(self, full_sync: bool = False) -> Tuple[int, datetime]:
        task_start_time = datetime.now()
        print(f"\n{'='*60}")
        print(f"🚀 飞书->MySQL 同步任务开始: {task_start_time}")
        print(f"📋 飞书表: {self.feishu_config.table_id}")
        print(f"📋 MySQL表: {self.mysql_config.target_table}")
        print(f"📋 主键字段: {self.feishu_config.primary_key_field}")
        print(f"{'='*60}")
        
        try:
            df = self.fetch_feishu_data()
            
            if not df.empty:
                df = self.transform_data(df)
                count = self.upsert_to_mysql(df)
            else:
                count = 0
            
            self.update_sync_time(task_start_time)
            
            print(f"\n🎉 同步完成！共处理 {count} 条数据")
            return count, task_start_time
            
        except Exception as e:
            print(f"\n❌ 同步失败: {e}")
            raise
