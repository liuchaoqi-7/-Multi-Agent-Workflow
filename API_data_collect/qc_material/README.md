# 千川素材数据采集工具

模块化的巨量千川素材API数据采集工具，支持直播间画面、视频素材、其他创意数据的采集。

## 项目结构

```
qc_material/
├── main.py                 # 主入口文件
├── README.md               # 说明文档
└── qianchuan/              # 核心模块包
    ├── __init__.py         # 包初始化
    ├── config.py           # 配置文件（字段映射、模块配置）
    ├── utils.py            # 工具函数（时间拆分等）
    ├── client.py           # API客户端
    ├── token_service.py    # Token管理服务
    ├── live_service.py     # 直播间画面服务
    ├── video_service.py    # 视频素材服务
    └── other_service.py    # 其他创意服务
```

## 功能特性

- **直播间画面数据采集**：支持多线程分钟级数据采集，包含60+指标字段
- **视频素材数据采集**：支持多线程分钟级数据采集，包含80+指标字段
- **其他创意数据采集**：支持多线程分钟级数据采集，包含9个指标字段
- **自动Token管理**：支持Token刷新和本地存储
- **多账户支持**：支持同时采集多个账户数据
- **数据库支持**：支持MySQL数据库写入
- **命令行参数**：支持灵活的命令行参数配置

## 安装依赖

```bash
pip install requests pandas sqlalchemy pymysql
```

## 使用方法

### 命令行方式

```bash
# 采集全部数据（直播间画面+视频+其他创意）
python main.py --app-id <APP_ID> --app-secret <APP_SECRET> --refresh-token <REFRESH_TOKEN> --accounts "1810151981662283:95882467048:地理八分钟,1841239871244042:1748646316999804:历史八分钟" --data-type all

# 只采集直播间画面数据
python main.py --app-id <APP_ID> --app-secret <APP_SECRET> --refresh-token <REFRESH_TOKEN> --accounts "1810151981662283:95882467048:地理八分钟" --data-type live --start-date "2024-01-01 00:00:00" --end-date "2024-01-31 23:59:59"

# 只采集视频素材数据
python main.py --app-id <APP_ID> --app-secret <APP_SECRET> --refresh-token <REFRESH_TOKEN> --accounts "1810151981662283:95882467048:地理八分钟" --data-type video

# 只采集其他创意数据
python main.py --app-id <APP_ID> --app-secret <APP_SECRET> --refresh-token <REFRESH_TOKEN> --accounts "1810151981662283:95882467048:地理八分钟" --data-type other

# 指定数据库写入
python main.py --app-id <APP_ID> --app-secret <APP_SECRET> --refresh-token <REFRESH_TOKEN> \
    --accounts "1810151981662283:95882467048:地理八分钟" \
    --db-host localhost --db-port 3306 \
    --db-user root --db-password password --db-name test_db
```

### 环境变量方式

```bash
# 设置环境变量
export QIANCHUAN_APP_ID="your_app_id"
export QIANCHUAN_APP_SECRET="your_app_secret"
export QIANCHUAN_REFRESH_TOKEN="your_refresh_token"
export DB_HOST="localhost"
export DB_PORT="3306"
export DB_USER="root"
export DB_PASSWORD="password"
export DB_NAME="test_db"

# 运行
python main.py --accounts "1810151981662283:95882467048:地理八分钟" --data-type all
```

### 代码调用方式

```python
from qianchuan import QianChuanClient, QianChuanTokenManager, LiveService, VideoService, OtherService
from qianchuan.utils import get_default_time_range

# 刷新Token获取Access Token
token_manager = QianChuanTokenManager(app_id="your_app_id", app_secret="your_app_secret")
token_info = token_manager.refresh_token("your_refresh_token")
access_token = token_info.get("access_token")

# 创建API客户端
client = QianChuanClient(access_token)

# 直播间画面采集
live_service = LiveService(client, max_workers=40)
start_date, end_date = get_default_time_range()
live_df = live_service.collect_data(
    advertiser_id="1810151981662283",
    aweme_id="95882467048",
    start_time=start_date,
    end_time=end_date
)

# 视频素材采集
video_service = VideoService(client, max_workers=40)
video_df = video_service.collect_data(
    advertiser_id="1810151981662283",
    aweme_id="95882467048",
    start_time=start_date,
    end_time=end_date
)

# 其他创意采集
other_service = OtherService(client, max_workers=40)
other_df = other_service.collect_data(
    advertiser_id="1810151981662283",
    aweme_id="95882467048",
    start_time=start_date,
    end_time=end_date
)
```

## 参数说明

| 参数 | 说明 | 默认值 |
|------|------|--------|
| --app-id | 千川应用ID | 环境变量QIANCHUAN_APP_ID |
| --app-secret | 千川应用密钥 | 环境变量QIANCHUAN_APP_SECRET |
| --refresh-token | 刷新Token | 环境变量QIANCHUAN_REFRESH_TOKEN |
| --token-file | Token存储文件路径 | None |
| --data-type | 数据类型：live/video/other/all | all |
| --start-date | 开始日期 | 昨天凌晨 |
| --end-date | 结束日期 | 今天凌晨 |
| --accounts | 账户列表 | 必填 |
| --db-host | 数据库主机 | localhost |
| --db-port | 数据库端口 | 3306 |
| --db-user | 数据库用户名 | 环境变量DB_USER |
| --db-password | 数据库密码 | 环境变量DB_PASSWORD |
| --db-name | 数据库名称 | 环境变量DB_NAME |
| --max-workers | 最大线程数 | 40 |

### 账户列表格式

```
advertiser_id:aweme_id:account_name,advertiser_id2:aweme_id2:account_name2
```

多个账户用逗号分隔，每个账户包含：千川广告主ID、抖音号ID、账户名称（可选）

## 数据字段说明

### 直播间画面数据（live_service.py）

包含60+指标字段，涵盖：
- 基础指标：展示次数、点击次数、点击率、转化率
- 成交指标：成交订单数、成交金额、预售订单数等
- 消耗指标：整体消耗、基础消耗、千次展现费用等
- ROI指标：支付ROI、净成交ROI、结算ROI等
- 退款指标：1小时/7日/14日/30日/90日退款相关
- 追投调控指标：追投消耗、追投成交订单数等

### 视频素材数据（video_service.py）

包含80+指标字段，涵盖：
- 直播间画面所有指标
- 视频特有指标：视频点赞数、播放数、完播率、评论数等
- 追投指标：追投消耗、追投成交金额、追投ROI等

### 其他创意数据（other_service.py）

包含9个指标字段：
- 成交订单数、成交金额、成交金额占比
- 用户实际支付金额、智能优惠券金额
- 电商平台补贴金额、预售订单数/金额
- 未完结预售订单预估金额

## 注意事项

1. Access Token有效期为2小时，Refresh Token有效期更长，工具会自动刷新Token
2. 数据采集按分钟级拆分时间范围，支持多线程并发请求
3. 每个请求支持指数退避重试机制，最大重试10次
4. 多线程采集时注意API限流，默认线程数为40
5. 数据库写入使用append模式，请注意去重

## 数据库表结构

工具会自动创建表，字段名使用中文，字段类型为TEXT。建议预先创建表并设置合适的字段类型。

默认表名：
- 直播间画面表：ods_千川素材_直播画面_m
- 视频素材表：ods_千川素材_视频_M
- 其他创意表：ods_千川素材_其他
