from typing import Callable, Tuple, Optional
from datetime import datetime

from .config import FeishuConfig, MySQLConfig, SyncConfig, SyncDirection
from .feishu_to_mysql import FeishuToMySQLSync
from .mysql_to_feishu import MySQLToFeishuSync


class SyncManager:
    def __init__(self, config: SyncConfig):
        self.config = config
        self._feishu_to_mysql = None
        self._mysql_to_feishu = None

    def _get_feishu_to_mysql(self) -> FeishuToMySQLSync:
        if not self._feishu_to_mysql:
            self._feishu_to_mysql = FeishuToMySQLSync(
                feishu_config=self.config.feishu,
                mysql_config=self.config.mysql
            )
        return self._feishu_to_mysql

    def _get_mysql_to_feishu(self) -> MySQLToFeishuSync:
        if not self._mysql_to_feishu:
            self._mysql_to_feishu = MySQLToFeishuSync(
                feishu_config=self.config.feishu,
                mysql_config=self.config.mysql
            )
        return self._mysql_to_feishu

    def sync_feishu_to_mysql(self, full_sync: bool = False) -> Tuple[int, datetime]:
        syncer = self._get_feishu_to_mysql()
        return syncer.sync(full_sync=full_sync)

    def sync_mysql_to_feishu(
        self,
        sql_template: str,
        full_sync: bool = False,
        clean_func: Callable = None
    ) -> Tuple[int, int, datetime]:
        syncer = self._get_mysql_to_feishu()
        return syncer.sync(
            sql_template=sql_template,
            full_sync=full_sync,
            clean_func=clean_func
        )

    def sync(
        self,
        sql_template: Optional[str] = None,
        full_sync: bool = False,
        clean_func: Callable = None
    ):
        if self.config.direction == SyncDirection.FEISHU_TO_MYSQL:
            return self.sync_feishu_to_mysql(full_sync=full_sync)
        elif self.config.direction == SyncDirection.MYSQL_TO_FEISHU:
            if not sql_template:
                raise ValueError("MySQL->飞书同步需要提供sql_template参数")
            return self.sync_mysql_to_feishu(
                sql_template=sql_template,
                full_sync=full_sync,
                clean_func=clean_func
            )
        else:
            raise ValueError(f"不支持的同步方向: {self.config.direction}")


def create_sync_config(
    feishu_app_id: str,
    feishu_app_secret: str,
    feishu_app_token: str,
    feishu_table_id: str,
    primary_key_field: str,
    mysql_host: str,
    mysql_port: int,
    mysql_user: str,
    mysql_password: str,
    mysql_database: str,
    mysql_table: str,
    direction: str = "mysql_to_feishu",
    datetime_fields: list = None,
    field_rename_map: dict = None,
    update_time_field: str = "update_time",
    batch_size: int = 50,
    sleep_time: float = 0.3
) -> SyncConfig:
    feishu_config = FeishuConfig(
        app_id=feishu_app_id,
        app_secret=feishu_app_secret,
        app_token=feishu_app_token,
        table_id=feishu_table_id,
        primary_key_field=primary_key_field,
        batch_size=batch_size,
        sleep_time=sleep_time,
        datetime_fields=datetime_fields or [],
        field_rename_map=field_rename_map or {}
    )
    
    mysql_config = MySQLConfig(
        host=mysql_host,
        port=mysql_port,
        user=mysql_user,
        password=mysql_password,
        database=mysql_database,
        target_table=mysql_table,
        update_time_field=update_time_field
    )
    
    direction_enum = SyncDirection(direction)
    
    return SyncConfig(
        feishu=feishu_config,
        mysql=mysql_config,
        direction=direction_enum
    )


def quick_sync_feishu_to_mysql(
    feishu_app_id: str,
    feishu_app_secret: str,
    feishu_app_token: str,
    feishu_table_id: str,
    primary_key_field: str,
    mysql_host: str,
    mysql_port: int,
    mysql_user: str,
    mysql_password: str,
    mysql_database: str,
    mysql_table: str,
    datetime_fields: list = None,
    field_rename_map: dict = None,
    full_sync: bool = False
) -> Tuple[int, datetime]:
    config = create_sync_config(
        feishu_app_id=feishu_app_id,
        feishu_app_secret=feishu_app_secret,
        feishu_app_token=feishu_app_token,
        feishu_table_id=feishu_table_id,
        primary_key_field=primary_key_field,
        mysql_host=mysql_host,
        mysql_port=mysql_port,
        mysql_user=mysql_user,
        mysql_password=mysql_password,
        mysql_database=mysql_database,
        mysql_table=mysql_table,
        direction="feishu_to_mysql",
        datetime_fields=datetime_fields,
        field_rename_map=field_rename_map
    )
    
    manager = SyncManager(config)
    return manager.sync_feishu_to_mysql(full_sync=full_sync)


def quick_sync_mysql_to_feishu(
    feishu_app_id: str,
    feishu_app_secret: str,
    feishu_app_token: str,
    feishu_table_id: str,
    primary_key_field: str,
    mysql_host: str,
    mysql_port: int,
    mysql_user: str,
    mysql_password: str,
    mysql_database: str,
    mysql_table: str,
    sql_template: str,
    update_time_field: str = "update_time",
    full_sync: bool = False,
    clean_func: Callable = None
) -> Tuple[int, int, datetime]:
    config = create_sync_config(
        feishu_app_id=feishu_app_id,
        feishu_app_secret=feishu_app_secret,
        feishu_app_token=feishu_app_token,
        feishu_table_id=feishu_table_id,
        primary_key_field=primary_key_field,
        mysql_host=mysql_host,
        mysql_port=mysql_port,
        mysql_user=mysql_user,
        mysql_password=mysql_password,
        mysql_database=mysql_database,
        mysql_table=mysql_table,
        direction="mysql_to_feishu",
        update_time_field=update_time_field
    )
    
    manager = SyncManager(config)
    return manager.sync_mysql_to_feishu(
        sql_template=sql_template,
        full_sync=full_sync,
        clean_func=clean_func
    )
