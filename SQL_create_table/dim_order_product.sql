CREATE TABLE dim.`dim_商品纬度` (
  `索引` BIGINT AUTO_INCREMENT PRIMARY KEY,
  `商品ID来源` VARCHAR(50) NOT NULL COMMENT '商品ID来源, e.g., 抖店, 供应链',
  `商品ID` VARCHAR(100) NOT NULL COMMENT '商品ID',
  `商品名称` VARCHAR(512) COMMENT '商品名称',
  `总销量` INT COMMENT '商品销量',
  `总销售额` DECIMAL(18,2) COMMENT '商品销售额',
  `最早售出时间` DATETIME COMMENT '最早售出时间',
  `最后售出时间` DATETIME COMMENT '最后售出时间',
  `create_time` DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间',
  `update_time` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '记录更新时间',
  UNIQUE KEY `uk_product_id` (`索引`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='商品维度表';

truncate table dim.dim_商品维度;
INSERT INTO dim.`dim_商品维度` 
-- 步骤1：先按商品ID聚合，计算累计销量（核心：聚合所有数据）
WITH ProductSalesAgg AS (
    SELECT
        平台 AS 商品ID来源,
        `商品ID`,
        SUM(if(支付时间 IS NULL,0,购买数量)) AS 总销量,  -- 累计该商品ID的所有购买数量
        SUM(if(支付时间 IS NULL,0,订单总额)) AS 总销售额, -- 可选：累计销售额
        MIN(提交时间) AS 最早售出时间, -- 可选：商品最早售出时间
        MAX(提交时间) AS 最后售出时间  -- 可选：商品最后售出时间
    FROM
        dwd.dwd_电商数据_宽表
    GROUP BY
        平台, `商品ID`  -- 按商品ID维度聚合
),
-- 步骤2：获取每个商品ID的最新商品名称（解决商品名称可能变更的问题）
LatestProductName AS (
    SELECT
        `商品ID`,
        商品名称,
        -- 按商品ID分区，取最新提交时间的商品名称
        ROW_NUMBER() OVER(PARTITION BY `商品ID` ORDER BY 提交时间 DESC) AS rn
    FROM
        dwd.dwd_电商数据_宽表
    WHERE
        商品名称 IS NOT NULL AND 商品名称 != ''  -- 过滤空名称
)
-- 步骤3：关联聚合销量和最新商品名称
SELECT
    a.商品ID来源,
    a.`商品ID`,
    n.商品名称,  -- 每个商品ID的最新名称
    a.总销量,
    a.总销售额,
    -- CASE 
    --     WHEN a.总销量 = 0 THEN 0  -- 如果销量为0，则平均售价为0
    --     ELSE ROUND(a.总销售额 / a.总销量, 2) -- 否则正常计算
    -- END AS 平均售价,
    a.最早售出时间,
    a.最后售出时间,
    '2022-01-01 00:00:00' as update_time
FROM
    ProductSalesAgg a
LEFT JOIN
    LatestProductName n ON a.`商品ID` = n.`商品ID` AND n.rn = 1  -- 只关联最新名称
ORDER BY
    a.商品ID来源 DESC,
    a.`商品ID`;

