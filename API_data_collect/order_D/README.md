# 抖店数据采集工具

抖店订单、售后、结算数据采集模块化工具，支持命令行参数传入敏感信息和动态参数。

## 📁 项目结构

```
order_D/
├── doudian/                          # 核心模块包
│   ├── __init__.py                   # 模块导出
│   ├── config.py                     # 配置和常量（映射字典、API配置）
│   ├── utils.py                      # 工具函数（时间转换、金额格式化等）
│   ├── client.py                     # API客户端（DouDianAPIClient）
│   ├── token_service.py              # Token管理（创建、刷新）
│   ├── order_service.py              # 订单服务（订单列表、详情获取）
│   ├── aftersale_service.py          # 售后服务（售后列表、详情获取）
│   └── settle_service.py             # 结算服务（结算单获取）
├── main.py                           # 主入口（命令行参数解析、流程控制）
└── README.md                         # 本文档
```

## 📦 模块说明

| 模块 | 文件 | 职责 |
|------|------|------|
| 配置模块 | `config.py` | 存放所有映射字典（订单类型、支付方式等）和API配置常量 |
| 工具模块 | `utils.py` | 工具函数：时间戳转换、金额格式化、JSON扁平化处理等 |
| 客户端模块 | `client.py` | 抖店API客户端类，封装签名生成和HTTP请求逻辑 |
| Token服务 | `token_service.py` | Token创建和自动刷新管理，支持本地缓存 |
| 订单服务 | `order_service.py` | 订单数据采集服务，支持多线程批量获取 |
| 售后服务 | `aftersale_service.py` | 售后数据采集服务，支持多线程批量获取 |
| 结算服务 | `settle_service.py` | 结算数据采集服务，支持分页获取 |
| 主入口 | `main.py` | 命令行参数解析和流程调度 |

## 🚀 快速开始

### 1. 环境要求

- Python 3.7+
- 依赖包：
  - requests
  - pandas
  - sqlalchemy
  - pymysql

```bash
pip install requests pandas sqlalchemy pymysql
```

### 2. 基本使用

```bash
# 进入项目目录
cd /Users/test/Documents/data_analysis/API_data_collect/order_D

# 基本用法（采集昨天的数据）
python main.py \
    --app_key YOUR_APP_KEY \
    --app_secret YOUR_APP_SECRET \
    --db_host localhost \
    --db_user root \
    --db_password your_password \
    --db_name your_database
```

### 3. 使用环境变量（推荐）

将敏感信息设置为环境变量，避免在命令行中暴露：

```bash
# 设置环境变量
export DOUDIAN_APP_KEY=YOUR_APP_KEY
export DOUDIAN_APP_SECRET=YOUR_APP_SECRET

# 运行脚本
python main.py \
    --db_host localhost \
    --db_user root \
    --db_password your_password \
    --db_name your_database
```

## 📋 命令行参数

### 必需参数

| 参数 | 说明 | 示例 |
|------|------|------|
| `--app_key` | 抖店应用App Key（可通过环境变量`DOUDIAN_APP_KEY`传入） | `--app_key 7265598874851477032` |
| `--app_secret` | 抖店应用App Secret（可通过环境变量`DOUDIAN_APP_SECRET`传入） | `--app_secret xxx-xxx-xxx` |
| `--db_host` | 数据库主机地址 | `--db_host localhost` |
| `--db_user` | 数据库用户名 | `--db_user root` |
| `--db_password` | 数据库密码 | `--db_password 123456` |
| `--db_name` | 数据库名称 | `--db_name ods` |

### 可选参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--db_port` | 3306 | 数据库端口 |
| `--order_start` | 昨天凌晨 | 订单查询开始时间（格式：YYYY-MM-DD HH:MM:SS） |
| `--order_end` | 今天凌晨 | 订单查询结束时间（格式：YYYY-MM-DD HH:MM:SS） |
| `--settle_start` | 2024-04-01 | 结算数据开始时间 |
| `--order_table` | ods_抖店_订单_全量 | 订单数据表名 |
| `--aftersale_table` | ods_抖店_售后_全量 | 售后数据表名 |
| `--settle_table` | ods_抖店_结算_全量 | 结算数据表名 |
| `--token_file` | （见代码） | Token文件路径 |
| `--max_workers` | 60 | 多线程最大线程数 |

### 控制参数

| 参数 | 说明 |
|------|------|
| `--skip_orders` | 跳过订单数据采集 |
| `--skip_aftersales` | 跳过售后数据采集 |
| `--skip_settle` | 跳过结算数据采集 |

## 📖 使用示例

### 示例1：采集指定日期范围的数据

```bash
python main.py \
    --app_key YOUR_KEY \
    --app_secret YOUR_SECRET \
    --order_start "2024-01-01 00:00:00" \
    --order_end "2024-01-31 23:59:59" \
    --settle_start "2024-01-01 00:00:00" \
    --db_host localhost \
    --db_user root \
    --db_password pwd \
    --db_name test
```

### 示例2：只采集订单数据

```bash
python main.py \
    --app_key YOUR_KEY \
    --app_secret YOUR_SECRET \
    --db_host localhost \
    --db_user root \
    --db_password pwd \
    --db_name test \
    --skip_aftersales \
    --skip_settle
```

### 示例3：只采集结算数据

```bash
python main.py \
    --app_key YOUR_KEY \
    --app_secret YOUR_SECRET \
    --db_host localhost \
    --db_user root \
    --db_password pwd \
    --db_name test \
    --skip_orders \
    --skip_aftersales
```

### 示例4：自定义表名和线程数

```bash
python main.py \
    --app_key YOUR_KEY \
    --app_secret YOUR_SECRET \
    --db_host localhost \
    --db_user root \
    --db_password pwd \
    --db_name test \
    --order_table my_orders \
    --aftersale_table my_aftersales \
    --settle_table my_settle \
    --max_workers 30
```

### 示例5：查看帮助信息

```bash
python main.py --help
```

## 🔧 模块化使用

如果需要在其他Python脚本中使用各模块：

```python
from doudian import (
    DouDianAPIClient,
    DouDianTokenCreator,
    OrderService,
    AftersaleService,
    SettleService,
)

# 创建Token
token_creator = DouDianTokenCreator(app_key, app_secret)
token_data = token_creator.create_token({"grant_type": "authorization_self", "code": ""})
access_token = token_data.get("access_token")

# 创建API客户端
client = DouDianAPIClient(app_key, app_secret, access_token)

# 使用订单服务
order_service = OrderService(client, max_workers=30)
order_ids = order_service.get_order_list({
    "create_time_start": start_timestamp,
    "create_time_end": end_timestamp,
    "page": 0,
    "size": 100,
})
df_orders = order_service.batch_get_order_details(order_ids)

# 使用售后服务
aftersale_service = AftersaleService(client, max_workers=30)
aftersale_ids, order_ids = aftersale_service.get_aftersale_list({
    "start_time": start_timestamp,
    "end_time": end_timestamp,
    "page": 0,
    "size": 100,
})
df_aftersales = aftersale_service.batch_get_aftersale_details(aftersale_ids)

# 使用结算服务
settle_service = SettleService(client)
df_settle = settle_service.batch_get_settle_details({
    "start_time": "2024-01-01 00:00:00",
    "end_time": "2024-01-31 23:59:59",
    "size": 100,
    "start_index": "",
})
```

## 📊 数据字段说明

### 订单数据（300+字段）

包含订单基础信息、商品信息、金额信息、时间信息、物流信息、收件人信息、售后信息等。

### 售后数据（180+字段）

包含售后基础信息、商品信息、金额信息、物流信息、仲裁信息、操作记录等。

### 结算数据（50+字段）

包含结算基础信息、商品信息、达人信息、金额明细、佣金信息等。

## ⚠️ 注意事项

1. **敏感信息安全**：建议使用环境变量传入`app_key`和`app_secret`，避免在命令行中直接暴露
2. **API限流**：抖店API有请求频率限制，默认已内置限流控制，可根据需要调整`max_workers`
3. **Token有效期**：Token有效期为7天，脚本会自动刷新即将过期的Token
4. **数据库表**：确保目标数据库已创建，表会自动创建（if_exists="append"）
5. **大数据量**：订单数据量大时建议分批次采集，避免内存溢出

## 🔐 安全建议

1. 不要将`app_key`和`app_secret`硬编码在脚本中
2. 不要将敏感信息提交到版本控制系统
3. 使用环境变量或配置文件管理敏感信息
4. 定期更换API密钥

## 📝 更新日志

### v2.0.0 (2024-03-12)
- 重构为模块化架构
- 支持命令行参数传入
- 移除硬编码敏感信息
- 优化代码结构

### v1.0.0
- 初始版本（单文件脚本）

## 📄 License

MIT License
