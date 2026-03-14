# 需要先补齐结算时间
ALTER TABLE ods.ods_微信_结算_全量 
ADD COLUMN 结算时间 DATETIME COMMENT '结算时间' AFTER 订单支付时间;

-- 核心更新语句：将编码映射为文本名称
UPDATE ods.ods_微信_结算_全量 s
LEFT JOIN ods.ods_微信_订单_全量 o 
  ON s.`订单ID` = o.`订单ID`
LEFT JOIN ods.ods_微信_售后_全量 a 
  ON s.`订单ID` = a.原订单号
SET s.结算时间 = COALESCE(
  IF(o.商家结算时间 IS NULL OR o.商家结算时间 = '', NULL, STR_TO_DATE(o.商家结算时间, '%Y-%m-%d %H:%i:%s')),
  IF(a.售后单更新时间 IS NULL OR a.售后单更新时间 = '', NULL, STR_TO_DATE(a.售后单更新时间, '%Y-%m-%d %H:%i:%s'))
)
WHERE s.结算时间 IS NULL; -- 仅更新结算时间为空的记录

CREATE TABLE IF NOT EXISTS dwd.dwd_微信_宽表(
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '唯一标识每行数据',
  店铺ID CHAR(64) COMMENT '店铺ID',
  主订单ID CHAR(64) COMMENT '主订单ID（抖店=根订单ID/微信/小红书=订单ID）',
  -- 子订单ID CHAR(64) COMMENT '子订单ID（无则为空）',
  售后单号 CHAR(255) COMMENT '售后单号',
  用户ID CHAR(255) COMMENT '用户唯一标识',
  提交时间 DATETIME COMMENT '订单提交时间',
  支付时间 DATETIME COMMENT '订单支付时间',
  完成时间 DATETIME COMMENT '订单完成时间',
  售后申请时间 DATETIME COMMENT '售后申请时间',
  结算时间 DATETIME COMMENT '结算时间',
  订单状态 CHAR(64) COMMENT '订单状态',
  售后状态 TEXT COMMENT '售后状态（无则为空）',
  结算状态 CHAR(64) COMMENT '结算状态（无则为空）',
  售后原因 TEXT COMMENT '售后原因',
  售后备注 TEXT COMMENT '售后备注',
  商品ID CHAR(64) COMMENT '商品ID',
  SKUID CHAR(64) COMMENT 'SKU ID',
  商品名称 CHAR(64) COMMENT '商品名称',
  下单来源 CHAR(64) COMMENT '下单来源（无则为空）',
  -- 视频ID CHAR(64) COMMENT '视频ID（无则为空）',
  直播ID CHAR(255) COMMENT '直播ID（无则为空）',
  支付方式 CHAR(64) COMMENT '支付方式',
  达人UID CHAR(64) COMMENT '达人ID',
  达人名称 CHAR(64) COMMENT '达人昵称',
  物流公司 CHAR(64) COMMENT '物流公司',
  物流单号 CHAR(64) COMMENT '物流单号',
  收件人姓名 CHAR(64) COMMENT '收件人姓名',
  收件人电话 CHAR(64) COMMENT '收件人电话',
  收件人省份 CHAR(64) COMMENT '收件人省份',
  收件人城市 CHAR(64) COMMENT '收件人城市',
  购买数量 DECIMAL(18,0) COMMENT '商品数量',
  商家实收 DECIMAL(18,2) COMMENT '商家实收金额(元)',
  买家实付 DECIMAL(18,2) COMMENT '买家实付金额(元)',
  订单总额 DECIMAL(18,2) COMMENT '订单总金额(元)',
  商家承担金额 DECIMAL(18,2) COMMENT '商家承担优惠金额(元)',
  平台承担金额 DECIMAL(18,2) COMMENT '平台承担优惠金额(元)',
  达人承担金额 DECIMAL(18,2) COMMENT '达人承担优惠金额(元)',
  退款金额 DECIMAL(18,2) COMMENT '退款金额(元)',
  结算金额 DECIMAL(18,2) COMMENT '结算金额(元)',
  店铺优惠 DECIMAL(18,2) COMMENT '店铺优惠券金额(元)',
  平台优惠 DECIMAL(18,2) COMMENT '平台优惠券金额(元)',
  达人优惠 DECIMAL(18,2) COMMENT '达人优惠券金额(元)',
  政府补贴 DECIMAL(18,2) COMMENT '政府补贴金额(元)',
  达人佣金 DECIMAL(18,2) COMMENT '达人佣金金额(元)',
  平台佣金 DECIMAL(18,2) COMMENT '平台佣金金额(元)',
  -- 服务商佣金 DECIMAL(18,2) COMMENT '服务商佣金金额(元)',
  团长佣金 DECIMAL(18,2) COMMENT '团长佣金金额(元)',
  平台 CHAR(16) COMMENT '数据来源：抖店/微信/小红书',
  -- 新增主键和索引（提升查询效率）
  PRIMARY KEY (id,主订单ID,平台) USING BTREE,
  -- INDEX idx_order_info (, 子订单ID, ) USING BTREE, -- 保留复合索引用于查询
  INDEX idx_店铺ID (店铺ID) USING BTREE,
  INDEX idx_提交时间 (提交时间) USING BTREE,
  INDEX idx_用户ID (用户ID) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '抖店合并数据全量表';

TRUNCATE TABLE dwd.dwd_微信_宽表;  -- 清空表（保留结构和索引）
-- 重新插入最新数据
INSERT INTO dwd.dwd_微信_宽表 (
  店铺ID,主订单ID,售后单号,用户ID,提交时间,支付时间,完成时间,售后申请时间,结算时间,
  订单状态,售后状态,结算状态,售后原因,售后备注,商品ID,SKUID,商品名称,下单来源,
  直播ID,支付方式,达人UID,达人名称,物流公司,物流单号,
  收件人姓名,收件人电话,收件人省份,收件人城市,购买数量,商家实收,买家实付,订单总额,
  商家承担金额,平台承担金额,达人承担金额,退款金额,结算金额,
  店铺优惠,平台优惠,达人优惠,政府补贴,达人佣金,平台佣金,团长佣金,
  平台
)
SELECT
  店铺ID,主订单ID,售后单号,用户ID,提交时间,支付时间,完成时间,售后申请时间,结算时间,
  订单状态,售后状态,结算状态,售后原因,售后备注,商品ID,SKUID,商品名称,下单来源,
  直播ID,支付方式,达人UID,达人名称,物流公司,物流单号,
  收件人姓名,收件人电话,收件人省份,收件人城市,购买数量,商家实收,买家实付,订单总额,
  商家承担金额,平台承担金额,达人承担金额,退款金额,结算金额,
  店铺优惠,平台优惠,达人优惠,政府补贴,达人佣金,平台佣金,团长佣金,
  平台
FROM (
  SELECT
    CAST(IFNULL(orders.`店铺ID`, '') AS CHAR(64)) AS 店铺ID,
    CAST(IFNULL(orders.`订单ID`, '') AS CHAR(64)) AS 主订单ID,
    -- CAST('' AS CHAR(64)) AS 子订单ID,
    CAST(IFNULL(afters.`售后单ID`, '') AS CHAR(255)) AS 售后单号,
    CAST(IFNULL(orders.`订单归属人OpenID`, '') AS CHAR(255)) AS 用户ID,
    IF(orders.创建时间 IS NULL OR orders.创建时间 = '', NULL, STR_TO_DATE(orders.创建时间, '%Y-%m-%d %H:%i:%s')) AS 提交时间,
    IF(orders.支付时间 IS NULL OR orders.支付时间 = '', NULL, STR_TO_DATE(orders.支付时间, '%Y-%m-%d %H:%i:%s')) AS 支付时间,
    IF(orders.商家结算时间 IS NULL OR orders.商家结算时间 = '', NULL, STR_TO_DATE(orders.商家结算时间, '%Y-%m-%d %H:%i:%s')) AS 完成时间,
    IF(afters.售后单创建时间 IS NULL OR afters.售后单创建时间 = '', NULL, STR_TO_DATE(afters.售后单创建时间, '%Y-%m-%d %H:%i:%s')) AS 售后申请时间,
    -- IF(settle.结算时间 IS NULL OR settle.结算时间 = '', NULL, STR_TO_DATE(settle.结算时间, '%Y-%m-%d %H:%i:%s')) AS 结算时间,
    settle.结算时间 结算时间,
    CAST(IFNULL(orders.订单状态描述, '') AS CHAR(64)) AS 订单状态,
    afters.售后单状态描述 售后状态,
    -- CAST(IFNULL(afters.售后单状态描述, '') AS CHAR(255)) AS 售后状态,
    CAST(IFNULL(settle.结算状态描述, '') AS CHAR(64)) AS 结算状态,
    afters.`退款原因解释` 售后原因,
    -- CAST(IFNULL(afters.`退款原因解释`, '') AS CHAR(255)) AS 售后原因,
    afters.`用户申请描述` 售后备注,
    -- CAST(IFNULL(afters.`用户申请描述`, '') AS CHAR(255)) AS 售后备注,
    CAST(IFNULL(orders.商品ID, '') AS CHAR(64)) AS 商品ID,
    CAST(IFNULL(orders.`SKU_ID`, '') AS CHAR(64)) AS SKUID,
    CAST(IFNULL(orders.商品标题, '') AS CHAR(64)) AS 商品名称,
    CAST(IFNULL(orders.下单场景描述, '') AS CHAR(64)) AS 下单来源,
    CAST(IFNULL(orders.直播ID, '') AS CHAR(255)) AS 直播ID,
    CAST(IFNULL(orders.支付方式描述, '') AS CHAR(64)) AS 支付方式,
    CAST(IFNULL(orders.`带货账户ID`, '') AS CHAR(64)) AS 达人UID,
    CAST(IFNULL(orders.`带货账户昵称`, '') AS CHAR(64)) AS 达人名称,
    CAST(IFNULL(orders.快递公司名称, '') AS CHAR(64)) AS 物流公司,
    CAST(IFNULL(orders.快递单号, '') AS CHAR(64)) AS 物流单号,
    CAST(IFNULL(orders.收货人姓名, '') AS CHAR(64)) AS 收件人姓名,
    CAST(IFNULL(orders.联系方式, '') AS CHAR(64)) AS 收件人电话,
    CAST(IFNULL(orders.省份, '') AS CHAR(64)) AS 收件人省份,
    CAST(IFNULL(orders.城市, '') AS CHAR(64)) AS 收件人城市,
    CAST(IFNULL(orders.SKU数量, 0) AS DECIMAL(18,0)) AS 购买数量,
    CAST(IFNULL(orders.`商家实收金额(元)`, 0.00) AS DECIMAL(18,2)) AS 商家实收,
    CAST(IFNULL(orders.`用户实付金额(元)`, 0.00) AS DECIMAL(18,2)) AS 买家实付,
    CAST(IFNULL(orders.`商品总价(元)`, 0.00) AS DECIMAL(18,2)) AS 订单总额,
    CAST(IFNULL(orders.`商家优惠金额(元)`, 0.00) AS DECIMAL(18,2)) AS 商家承担金额,
    CAST(IFNULL(orders.`平台券优惠金额(元)`, 0.00) AS DECIMAL(18,2)) AS 平台承担金额,
    CAST(IFNULL(orders.`达人优惠金额(元)`, 0.00) AS DECIMAL(18,2)) AS 达人承担金额, 
    CAST(IFNULL(afters.`退款金额(元)`, 0.00) AS DECIMAL(18,2)) AS 退款金额,
    -- 收入
    CAST(IFNULL(settle.`预计结算金额(元)`, 0.00) AS DECIMAL(18,2)) AS 结算金额,
    CAST(IFNULL(settle.`商户优惠金额(元)`, 0.00) AS DECIMAL(18,2)) AS 店铺优惠,
    CAST(IFNULL(settle.`平台优惠金额(元)`, 0.00) AS DECIMAL(18,2)) AS 平台优惠,
    CAST(IFNULL(settle.`达人优惠金额(元)`, 0.00) AS DECIMAL(18,2)) AS 达人优惠, -- 重复
    CAST(IFNULL(settle.`国家补贴金额(元)`, 0.00) AS DECIMAL(18,2)) AS 政府补贴,
    -- 支出
    -CAST(IFNULL(settle.`预计达人服务费(元)`, 0.00) AS DECIMAL(18,2)) AS 达人佣金,
    -CAST(IFNULL(settle.`原技术服务费(元)`, 0.00) AS DECIMAL(18,2)) AS 平台佣金,
    -CAST(IFNULL(settle.`预计机构服务费(元)`, 0.00) AS DECIMAL(18,2)) AS 团长佣金,
    '微信' AS 平台
  FROM ods.ods_微信_订单_全量 orders
  LEFT JOIN (
    SELECT
    原订单号 原订单号,
    GROUP_CONCAT(`售后单ID` SEPARATOR ';') AS `售后单ID`,
    max(售后单更新时间) 售后单创建时间,
    SUBSTRING_INDEX(GROUP_CONCAT(`售后单状态描述` ORDER BY `售后单更新时间` DESC SEPARATOR ','), ',', 1) AS `售后单状态描述`,
    SUBSTRING_INDEX(GROUP_CONCAT(`退款原因解释` ORDER BY `售后单更新时间` DESC SEPARATOR ','), ',', 1) AS `退款原因解释`,
    SUBSTRING_INDEX(GROUP_CONCAT(`用户申请描述` ORDER BY `售后单更新时间` DESC SEPARATOR ','), ',', 1) AS `用户申请描述`,
    -- GROUP_CONCAT(`售后单状态描述` SEPARATOR ';') AS `售后单状态描述`,
    -- GROUP_CONCAT(`退款原因解释` SEPARATOR ';') AS `退款原因解释`,
    -- GROUP_CONCAT(`用户申请描述` SEPARATOR ';') AS `用户申请描述`,
    SUM(IF(售后单状态描述='退款完成',`退款金额(元)`,0.00)) `退款金额(元)`
    FROM ods.ods_微信_售后_全量
    GROUP BY 1
   ) afters ON orders.`订单ID` = afters.原订单号
  LEFT JOIN ods.ods_微信_结算_全量 settle ON orders.`订单ID` = settle.`订单ID`
)t;
