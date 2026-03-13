# 微信视频号小店数据采集工具

模块化的微信视频号小店API数据采集工具，支持订单、售后、结算数据的采集。

## 项目结构

```
order_W/
├── main.py                 # 主入口文件
├── README.md               # 说明文档
└── weixin/                 # 核心模块包
    ├── __init__.py         # 包初始化
    ├── config.py           # 配置文件（枚举映射、模块配置）
    ├── utils.py            # 工具函数
    ├── client.py           # API客户端
    ├── token_service.py    # Token管理服务
    ├── order_service.py    # 订单服务
    ├── aftersale_service.py # 售后服务
    └── settle_service.py   # 结算服务
```

## 功能特性

- **订单数据采集**：支持多线程批量获取订单详情，包含100+字段
- **售后数据采集**：支持多线程批量获取售后详情，包含60+字段
- **结算流水采集**：支持分页获取结算流水，包含80+字段
- **自动Token管理**：自动获取和刷新Access Token
- **数据库支持**：支持MySQL数据库写入
- **命令行参数**：支持灵活的命令行参数配置

## 安装依赖

```bash
pip install requests pandas sqlalchemy pymysql
```

## 使用方法

### 命令行方式

```bash
# 采集全部数据（订单+售后+结算）
python main.py --app-id <APP_ID> --app-secret <APP_SECRET> --data-type all

# 只采集订单数据
python main.py --app-id <APP_ID> --app-secret <APP_SECRET> --data-type order --start-date "2024-01-01 00:00:00" --end-date "2024-01-31 23:59:59"

# 只采集售后数据
python main.py --app-id <APP_ID> --app-secret <APP_SECRET> --data-type aftersale

# 只采集结算数据
python main.py --app-id <APP_ID> --app-secret <APP_SECRET> --data-type settle

# 指定数据库写入
python main.py --app-id <APP_ID> --app-secret <APP_SECRET> \
    --db-host localhost --db-port 3306 \
    --db-user root --db-password password --db-name test_db
```

### 环境变量方式

```bash
# 设置环境变量
export WEIXIN_APP_ID="your_app_id"
export WEIXIN_APP_SECRET="your_app_secret"
export DB_HOST="localhost"
export DB_PORT="3306"
export DB_USER="root"
export DB_PASSWORD="password"
export DB_NAME="test_db"

# 运行
python main.py --data-type all
```

### 代码调用方式

```python
from weixin import WeChatAPIClient, WeChatTokenManager, OrderService, AftersaleService, SettleService
from weixin.utils import str_to_ts

# 获取Access Token
token_manager = WeChatTokenManager(app_id="your_app_id", app_secret="your_app_secret")
access_token = token_manager.get_access_token()

# 创建API客户端
client = WeChatAPIClient(access_token)

# 订单采集
order_service = OrderService(client, max_workers=20)
order_ids = order_service.get_order_list(
    start_time=str_to_ts("2024-01-01 00:00:00"),
    end_time=str_to_ts("2024-01-31 23:59:59")
)
order_df = order_service.batch_get_order_details(order_ids)

# 售后采集
aftersale_service = AftersaleService(client, max_workers=10)
aftersale_ids = aftersale_service.get_aftersale_list(
    begin_time=str_to_ts("2024-01-01 00:00:00"),
    end_time=str_to_ts("2024-01-31 23:59:59")
)
aftersale_df = aftersale_service.batch_get_aftersale_details(aftersale_ids)

# 结算采集
settle_service = SettleService(client)
settle_df = settle_service.get_settle_list(order_settle_state=0)
```

## 参数说明

| 参数 | 说明 | 默认值 |
|------|------|--------|
| --app-id | 微信小程序AppID | 环境变量WEIXIN_APP_ID |
| --app-secret | 微信小程序AppSecret | 环境变量WEIXIN_APP_SECRET |
| --data-type | 数据类型：order/aftersale/settle/all | all |
| --start-date | 开始日期 | 昨天凌晨 |
| --end-date | 结束日期 | 今天凌晨 |
| --db-host | 数据库主机 | localhost |
| --db-port | 数据库端口 | 3306 |
| --db-user | 数据库用户名 | 环境变量DB_USER |
| --db-password | 数据库密码 | 环境变量DB_PASSWORD |
| --db-name | 数据库名称 | 环境变量DB_NAME |
| --max-workers | 最大线程数 | 20 |

## 数据字段说明

### 订单数据（order_service.py）

包含100+字段，涵盖：
- 基础订单信息：订单ID、状态、创建时间等
- 礼物订单信息：赠送者信息、礼物类型等
- 同城配送信息：门店ID、预计送达时间等
- 支付信息：支付方式、支付时间、支付订单号等
- 价格信息：商品总价、实付金额、运费、优惠金额等
- 配送信息：发货方式、发货时间、质检信息等
- 收货地址信息：收货人、地址、虚拟号码等
- 商品信息：商品ID、SKU、价格、数量等
- 物流信息：快递单号、快递公司等
- 分佣信息：分账方、分账金额等
- 来源信息：带货账户、销售渠道等

### 售后数据（aftersale_service.py）

包含60+字段，涵盖：
- 基础信息：售后单ID、状态、创建时间等
- 商品信息：商品ID、SKU、售后数量等
- 退款信息：退款金额、退款原因等
- 退货信息：快递单号、物流公司等
- 换货信息：换货商品、换货地址等
- 商家上传信息：拒绝原因、退款凭证等

### 结算数据（settle_service.py）

包含80+字段，涵盖：
- 基础信息：订单ID、结算状态、创建时间等
- 金额信息：商户实收、支出金额、技术服务费等
- 结算时间信息：预计结算时间等
- 结算状态信息：各项费用的结算状态等
- 商品信息：商品ID、名称、数量等

## 注意事项

1. Access Token有效期为2小时，工具会自动获取新Token
2. 订单和售后接口有时间范围限制，建议按天或按周采集
3. 结算流水接口使用分页机制，会自动获取全部数据
4. 多线程采集时注意API限流，默认线程数已优化
5. 数据库写入使用append模式，请注意去重

## 数据库表结构

工具会自动创建表，字段名使用中文，字段类型为TEXT。建议预先创建表并设置合适的字段类型。

默认表名：
- 订单表：ods_微信_订单_全量
- 售后表：ods_微信_售后_全量
- 结算表：ods_微信_结算_全量
