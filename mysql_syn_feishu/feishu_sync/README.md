# 飞书多维表同步工具

通用的飞书多维表与MySQL数据库双向同步工具，支持按唯一主键进行增量/全量同步。

## 功能特性

- **双向同步**：支持飞书→MySQL、MySQL→飞书两个方向
- **主键更新**：按唯一主键判断新增/更新，避免重复数据
- **增量同步**：基于update_time字段实现增量同步
- **全量同步**：支持全量覆盖同步
- **字段映射**：支持字段重命名和类型转换
- **批量处理**：支持批量写入，提高同步效率
- **状态管理**：自动记录同步时间，支持断点续传

## 项目结构

```
feishu_sync/
├── __init__.py           # 包初始化
├── config.py             # 配置类定义
├── utils.py              # 工具函数
├── client.py             # 飞书API客户端
├── feishu_to_mysql.py    # 飞书->MySQL同步
├── mysql_to_feishu.py    # MySQL->飞书同步
└── sync_manager.py       # 同步管理器
```

## 安装依赖

```bash
pip install requests pymysql sqlalchemy pandas
```

## 快速使用

### 方式一：快速同步函数

```python
from feishu_sync import quick_sync_mysql_to_feishu, quick_sync_feishu_to_mysql

# MySQL -> 飞书 同步
quick_sync_mysql_to_feishu(
    feishu_app_id="cli_xxx",
    feishu_app_secret="xxx",
    feishu_app_token="xxx",
    feishu_table_id="tblxxx",
    primary_key_field="达人ID",
    mysql_host="localhost",
    mysql_port=3306,
    mysql_user="root",
    mysql_password="password",
    mysql_database="test_db",
    mysql_table="target_table",
    sql_template="SELECT * FROM source_table",
    update_time_field="update_time",
    full_sync=False
)

# 飞书 -> MySQL 同步
quick_sync_feishu_to_mysql(
    feishu_app_id="cli_xxx",
    feishu_app_secret="xxx",
    feishu_app_token="xxx",
    feishu_table_id="tblxxx",
    primary_key_field="索引",
    mysql_host="localhost",
    mysql_port=3306,
    mysql_user="root",
    mysql_password="password",
    mysql_database="test_db",
    mysql_table="target_table",
    datetime_fields=["创建时间", "更新时间"],
    full_sync=False
)
```

### 方式二：使用同步管理器

```python
from feishu_sync import SyncManager, create_sync_config

# 创建配置
config = create_sync_config(
    feishu_app_id="cli_xxx",
    feishu_app_secret="xxx",
    feishu_app_token="xxx",
    feishu_table_id="tblxxx",
    primary_key_field="达人ID",
    mysql_host="localhost",
    mysql_port=3306,
    mysql_user="root",
    mysql_password="password",
    mysql_database="test_db",
    mysql_table="target_table",
    direction="mysql_to_feishu"
)

# 创建同步管理器
manager = SyncManager(config)

# 执行同步
manager.sync_mysql_to_feishu(
    sql_template="SELECT * FROM source_table",
    full_sync=False
)
```

### 方式三：使用同步类

```python
from feishu_sync import FeishuConfig, MySQLConfig, MySQLToFeishuSync

# 配置
feishu_config = FeishuConfig(
    app_id="cli_xxx",
    app_secret="xxx",
    app_token="xxx",
    table_id="tblxxx",
    primary_key_field="达人ID",
    batch_size=50,
    sleep_time=0.3
)

mysql_config = MySQLConfig(
    host="localhost",
    port=3306,
    user="root",
    password="password",
    database="test_db",
    target_table="target_table",
    update_time_field="update_time"
)

# 创建同步器
syncer = MySQLToFeishuSync(feishu_config, mysql_config)

# 执行同步
syncer.sync(
    sql_template="SELECT * FROM source_table",
    full_sync=False
)
```

## 配置说明

### FeishuConfig

| 参数 | 说明 |
|------|------|
| app_id | 飞书应用ID |
| app_secret | 飞书应用密钥 |
| app_token | 多维表App Token |
| table_id | 数据表ID |
| primary_key_field | 主键字段名 |
| batch_size | 批量处理大小，默认50 |
| sleep_time | 请求间隔时间，默认0.3秒 |
| datetime_fields | 需要转换的时间字段列表 |
| field_rename_map | 字段重命名映射 |

### MySQLConfig

| 参数 | 说明 |
|------|------|
| host | MySQL主机 |
| port | MySQL端口 |
| user | 用户名 |
| password | 密码 |
| database | 数据库名 |
| target_table | 目标表名 |
| update_time_field | 更新时间字段，用于增量同步 |
| status_table | 同步状态表，默认sync_status |

## 同步逻辑说明

### MySQL -> 飞书

1. 从MySQL的sync_status表获取上次同步时间
2. 查询MySQL中update_time > 上次同步时间的数据
3. 获取飞书表中所有主键值和record_id映射
4. 判断每条数据是新增还是更新
5. 批量调用飞书API新增/更新记录
6. 更新同步状态表

### 飞书 -> MySQL

1. 获取飞书表中所有记录
2. 转换字段名和数据类型
3. 使用INSERT ... ON DUPLICATE KEY UPDATE写入MySQL
4. 更新同步状态表

## 数据库要求

MySQL需要创建同步状态表：

```sql
CREATE TABLE sync_status (
    table_name VARCHAR(100) PRIMARY KEY,
    last_sync_time DATETIME
);
```

## 注意事项

1. 主键字段必须在飞书表和MySQL表中都存在
2. 时间字段会自动在毫秒时间戳和datetime之间转换
3. 增量同步依赖update_time字段，确保该字段正确更新
4. 批量操作有频率限制，sleep_time不宜过小
