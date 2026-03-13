from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Callable
from enum import Enum


class SyncDirection(Enum):
    FEISHU_TO_MYSQL = "feishu_to_mysql"
    MYSQL_TO_FEISHU = "mysql_to_feishu"


@dataclass
class FeishuConfig:
    app_id: str
    app_secret: str
    app_token: str
    table_id: str
    primary_key_field: str
    batch_size: int = 50
    sleep_time: float = 0.3
    max_retries: int = 3
    datetime_fields: List[str] = field(default_factory=list)
    field_rename_map: Dict[str, str] = field(default_factory=dict)


@dataclass
class MySQLConfig:
    host: str
    port: int
    user: str
    password: str
    database: str
    target_table: str
    charset: str = "utf8mb4"
    update_time_field: str = "update_time"
    status_table: str = "sync_status"


@dataclass
class SyncConfig:
    feishu: FeishuConfig
    mysql: MySQLConfig
    direction: SyncDirection
    full_sync: bool = False
    custom_clean_func: Optional[Callable] = None
    custom_sql_template: Optional[str] = None


DEFAULT_FIELD_TYPE_MAPPING = {
    1: "text",
    2: "number",
    3: "single_select",
    4: "multi_select",
    5: "date",
    7: "checkbox",
    11: "user",
    13: "phone",
    15: "url",
    17: "attachment",
    18: "association",
    19: "formula",
    20: "dual_link",
    21: "location",
    22: "currency",
    23: "progress",
    1001: "created_time",
    1002: "modified_time",
    1003: "created_user",
    1004: "modified_user",
    1005: "auto_number",
}
