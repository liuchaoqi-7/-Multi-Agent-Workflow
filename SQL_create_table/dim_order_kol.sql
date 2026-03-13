CREATE TABLE dim.`dim_达人维度` (
  `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
  `UID来源` VARCHAR(50) COMMENT 'UID来源, e.g., 千川, 抖店, 爬虫',
  `达人UID` VARCHAR(100) COMMENT '达人UID',
  `达人名称` VARCHAR(255) COMMENT '达人名称',
--   `达人标签` VARCHAR(50) COMMENT '达人标签',
  `最早成交时间` DATETIME COMMENT '最早成交时间',
  `最后成交时间` DATETIME COMMENT '最后成交时间',
  `总销量` INT COMMENT '达人总销量',
  `总GMV` DECIMAL(18, 2) COMMENT '达人总销售额',
  `总实付金额` DECIMAL(18, 2) COMMENT '达人总实付金额',
  `支付订单数` INT COMMENT '达人支付订单数',
  `总GSV` DECIMAL(18, 2) COMMENT '达人总GSV',
  `退后订单数` INT COMMENT '达人退后订单数',
  `退货率` DECIMAL(8, 2) COMMENT '达人退货率',
  `总佣金` DECIMAL(18, 2) COMMENT '达人总佣金',
  `最早投流时间` DATETIME COMMENT '最早投流时间',
  `最后投流时间` DATETIME COMMENT '最后投流时间',
  `是否投流` VARCHAR(10) COMMENT '是否投流: 是/否',
  `账号类型` VARCHAR(20) COMMENT '账号类型: 店播/达播/小店',
  `create_time` DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间',
  `update_time` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '记录更新时间',
  UNIQUE KEY `uk_daren_uid` (`达人UID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='达人维度表';

truncate table dim.dim_达人维度;
INSERT INTO dim.dim_达人维度 (
    `UID来源`,
    `达人UID`,
    `达人名称`,
    -- `达人标签`,
    `最早成交时间`,
    `最后成交时间`,
    `总销量`,
    `总GMV` ,
    `总实付金额`,
    `支付订单数`,
    `总GSV`,
    `退后订单数`,
    `退货率`,
    `总佣金`,
    `最早投流时间`,
    `最后投流时间`,
    `是否投流`,
    `账号类型`
)
SELECT
    base.`平台` AS `UID来源`,
    base.`达人UID` as `达人UID`,
    base.`最新达人名称` as `达人名称`,
    -- IF(DATEDIFF(NOW(), base.`最早加入时间`) > 30, '老客户', '新客户') AS `达人标签`,
    base.`最早加入时间` as `最早成交时间`,
    base.`最晚加入时间` as `最后成交时间`,
    base.`总销量` as `总销量`,
    base.`达人GMV` as `总GMV`,
    base.`总实付金额` as `总实付金额`,
    base.支付订单数 as `支付订单数`,
    base.`达人GSV` as `总GSV`,
    base.退后订单数 as `退后订单数`,
    IF(base.退货率 IS NULL, 0, base.退货率) as `退货率`,
    base.`总佣金` as `总佣金`,
    IFNULL(qc.最早投流时间, NULL) as `最早投流时间`,
    IFNULL(qc.最后投流时间, NULL) as `最后投流时间`,
    -- 基于正确聚合后的总消耗判断是否投流
    -- qc.总消耗,
    IF(IFNULL(qc.总消耗, 0) > 0, '是', '否') AS `是否投流`,
    IF(base.`达人UID` in ('95882467048','1748646316999804','sphUQcmVKamseY6','wx9e7fc0514292b805','7467699792727917625'), '店播', if(base.`最新达人名称` in('小店自卖'), '小店', '达播')) AS `账号类型`
FROM (
    -- 1. 电商宽表聚合：获取达人基础信息
    SELECT
        `平台`,
        `达人UID`,
        MAX(CASE WHEN rn1 = 1 THEN `达人名称` END) AS 最新达人名称,
        MIN(`提交时间`) AS 最早加入时间,
        MAX(`提交时间`) AS 最晚加入时间,
        SUM(`购买数量`) AS 总销量,
        SUM(`订单总额`) AS 达人GMV,
        SUM(`总实付金额`) AS 总实付金额,
        COUNT(支付时间) AS 支付订单数,
        SUM(`退后金额`) AS 达人GSV,
        COUNT(支付时间)-SUM(退货订单数) AS 退后订单数,
        CASE
            WHEN COUNT(支付时间) = 0 THEN 0  -- 如果支付订单数为0，则退货率为0
            ELSE ROUND(SUM(退货订单数) / COUNT(支付时间), 2) -- 否则正常计算
        END AS 退货率,
        SUM(`达人佣金`) AS 总佣金
    FROM (
        SELECT
            `平台`,
            `达人UID`,
            `达人名称`,
            `提交时间`,
            `购买数量`,
            `订单总额`,
            if(支付时间 IS NULL,0,`买家实付`) AS `总实付金额`,
            支付时间,
            if(支付时间 IS NULL,0,(`买家实付`-`退款金额`)) AS `退后金额`,
            CASE WHEN 退款金额>0 THEN 1 else 0 END AS 退货订单数,
            `达人佣金` AS `达人佣金`,
            ROW_NUMBER() OVER (PARTITION BY `平台`, `达人UID` ORDER BY `提交时间` DESC) AS rn1
        FROM dwd_电商数据_宽表
    ) t
    GROUP BY `平台`, `达人UID`
) base
-- 2. 关联聚合后的千川消耗数据
LEFT JOIN (
    -- 2.1 子查询：先为每个达人的所有千川记录打上分区和行号
    SELECT
        `【千抖H】平台` COLLATE utf8mb4_0900_ai_ci AS `平台`,
        `【千抖H】UID` COLLATE utf8mb4_0900_ai_ci AS `达人UID`,
        -- 聚合计算总消耗
        SUM(IFNULL(`【千抖H】整体消耗`, 0)) AS 总消耗,
        MAX(`【千抖H】Hour`) 最后投流时间,
        MIN(`【千抖H】Hour`) 最早投流时间,
        -- 保留最新的达人名称（通过窗口函数实现）
        MAX(CASE WHEN rn = 1 THEN `【千抖H】短ID` END) AS 千川达人名称
    FROM (
        SELECT
            `【千抖H】平台`,
            `【千抖H】UID`,
            `【千抖H】短ID`,
            【千抖H】Hour,
            `【千抖H】整体消耗`,
            -- 核心修正：按唯一的达人ID进行分区，以获取每个达人的最新记录
            ROW_NUMBER() OVER (
                PARTITION BY `【千抖H】UID`
                ORDER BY `【千抖H】Hour` DESC
            ) AS rn
        FROM spider_01.`千川抖音hour`
    ) t2
    -- 2.2 外层聚合：按达人ID分组，计算总消耗和最新名称
    GROUP BY `【千抖H】平台`, `【千抖H】UID`
) qc ON base.`达人UID` = qc.`达人UID`
-- 过滤无效数据
-- WHERE base.`达人UID` IS NOT NULL
ORDER BY 1 desc,2 

