CREATE TABLE IF NOT EXISTS dwd.dwd_红书_宽表(
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
  售后状态 CHAR(64) COMMENT '售后状态（无则为空）',
  结算状态 TEXT COMMENT '结算状态（无则为空）',
  售后原因 CHAR(64) COMMENT '售后原因',
  售后备注 CHAR(255) COMMENT '售后备注',
  商品ID CHAR(64) COMMENT '商品ID',
  SKUID CHAR(64) COMMENT 'SKU ID',
  商品名称 CHAR(64) COMMENT '商品名称',
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
  退款金额 DECIMAL(18,2) COMMENT '退款金额(元)',
  结算金额 DECIMAL(18,2) COMMENT '结算金额(元)',
  达人佣金 DECIMAL(18,2) COMMENT '达人佣金金额(元)',
  平台佣金 DECIMAL(18,2) COMMENT '平台佣金金额(元)',
  服务商佣金 DECIMAL(18,2) COMMENT '服务商佣金金额(元)',
  其他分成 DECIMAL(18,2) COMMENT '其他分成金额(元)',
  政府补贴 DECIMAL(18,2) COMMENT '政府补贴金额(元)',
  运费 DECIMAL(18,2) COMMENT '运费金额(元)',
  平台 CHAR(16) COMMENT '数据来源：抖店/微信/小红书',
  -- 新增主键和索引（提升查询效率）
  PRIMARY KEY (id,主订单ID,平台) USING BTREE,
  -- INDEX idx_order_info (, 子订单ID, ) USING BTREE, -- 保留复合索引用于查询
  INDEX idx_店铺ID (店铺ID) USING BTREE,
  INDEX idx_提交时间 (提交时间) USING BTREE,
  INDEX idx_用户ID (用户ID) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '红书合并数据全量表';

TRUNCATE TABLE dwd.dwd_红书_宽表;  -- 清空表（保留结构和索引）

INSERT INTO dwd.dwd_红书_宽表(
  店铺ID,主订单ID,售后单号,用户ID,提交时间,支付时间,完成时间,售后申请时间,结算时间,
  订单状态,售后状态,结算状态,售后原因,售后备注,商品ID,SKUID,商品名称,
  支付方式,达人UID,达人名称,物流公司,物流单号,
  收件人姓名,收件人电话,收件人省份,收件人城市,购买数量,商家实收,买家实付,订单总额,
  商家承担金额,平台承担金额,退款金额,结算金额,
  达人佣金,平台佣金,服务商佣金,
  其他分成,政府补贴,运费,
  平台
)
SELECT
  店铺ID,主订单ID,售后单号,用户ID,提交时间,支付时间,完成时间,售后申请时间,结算时间,
  订单状态,售后状态,结算状态,售后原因,售后备注,商品ID,SKUID,商品名称,
  支付方式,达人UID,达人名称,物流公司,物流单号,
  收件人姓名,收件人电话,收件人省份,收件人城市,购买数量,商家实收,买家实付,订单总额,
  商家承担金额,平台承担金额,退款金额,结算金额,
  达人佣金,平台佣金,服务商佣金,
  其他分成,政府补贴,运费,
  平台
FROM(
  SELECT
  CAST(IFNULL(orders.`店铺ID`, '') AS CHAR(64)) AS 店铺ID,
  CAST(IFNULL(orders.`订单ID`, '') AS CHAR(64)) AS 主订单ID,
  -- CAST('' AS CHAR(64)) AS 子订单ID,
  CAST(IFNULL(afters.`售后ID`, '') AS CHAR(255)) AS 售后单号,
  CAST(IFNULL(orders.`用户编号`, '') AS CHAR(255)) AS 用户ID,
  IF(orders.订单创建时间 IS NULL OR orders.订单创建时间 = '', NULL, STR_TO_DATE(orders.订单创建时间, '%Y-%m-%d %H:%i:%s')) AS 提交时间,
  IF(orders.订单支付时间 IS NULL OR orders.订单支付时间 = '', NULL, STR_TO_DATE(orders.订单支付时间, '%Y-%m-%d %H:%i:%s')) AS 支付时间,
  IF(orders.订单完成时间 IS NULL OR orders.订单完成时间 = '', NULL, STR_TO_DATE(orders.订单完成时间, '%Y-%m-%d %H:%i:%s')) AS 完成时间,
  IF(afters.创建时间 IS NULL OR afters.创建时间 = '', NULL, STR_TO_DATE(afters.创建时间, '%Y-%m-%d %H:%i:%s')) AS 售后申请时间,
  IF(settle.结算时间 IS NULL OR settle.结算时间 = '', NULL, STR_TO_DATE(settle.结算时间, '%Y-%m-%d %H:%i:%s')) AS 结算时间,
  CAST(IFNULL(orders.订单状态, '') AS CHAR(64)) AS 订单状态,
  CAST(IFNULL(afters.售后状态, '') AS CHAR(64)) AS 售后状态,
  CAST(IFNULL(settle.结算状态, '') AS CHAR(64)) AS 结算状态,
  CAST(IFNULL(afters.`售后原因说明`, '') AS CHAR(255)) AS 售后原因,
  CAST(IFNULL(afters.`用户描述`, '') AS CHAR(255)) AS 售后备注,
  CAST(IF(orders.商品ID!='',orders.商品ID, orders.`主SKU ID`) AS CHAR(64)) AS 商品ID,
  CAST(IFNULL(orders.`单品SKU ID`, '') AS CHAR(64)) AS SKUID,
  CAST(IFNULL(orders.商品名称, '') AS CHAR(64)) AS 商品名称,
  CAST(IFNULL(orders.支付方式, '') AS CHAR(64)) AS 支付方式,
  -- CAST(IFNULL(orders.`达人ID`, '') AS CHAR(64)) AS 达人UID,
  -- CAST(IFNULL(orders.`达人名称`, '') AS CHAR(64)) AS 达人名称,
  CAST(IFNULL(orders.达人ID_y, '') AS CHAR(64)) AS 达人UID,
  CAST(IFNULL(orders.达人昵称, '') AS CHAR(64)) AS 达人名称,
  CAST(IFNULL(orders.快递公司, '') AS CHAR(64)) AS 物流公司,
  CAST(IFNULL(orders.快递单号, '') AS CHAR(64)) AS 物流单号,
  CAST(IFNULL(orders.收件人姓名, '') AS CHAR(64)) AS 收件人姓名,
  CAST(IFNULL(orders.收件人电话, '') AS CHAR(64)) AS 收件人电话,
  CAST(IFNULL(orders.省, '') AS CHAR(64)) AS 收件人省份,
  CAST(IFNULL(orders.市, '') AS CHAR(64)) AS 收件人城市,
  CAST(IFNULL(orders.主SKU数量, 0) AS DECIMAL(18,0)) AS 购买数量,
  CAST(IFNULL(IF(订单状态='已取消',0,orders.`商家应收金额(元)`), 0.00) AS DECIMAL(18,2)) AS 商家实收,
  CAST(IFNULL(IF(订单状态='已取消',0,orders.`订单实付金额(包含运费和定金)`), 0.00) AS DECIMAL(18,2)) AS 买家实付,
  CAST(IFNULL(IF(`商品总价(元)`!=`商家应收金额(元)`,`商家应收金额(元)`,`商品总价(元)`), 0.00) AS DECIMAL(18,2)) AS 订单总额,  -- 商品总实付
  CAST(IFNULL(orders.`商家承担总优惠(元)`, 0.00) AS DECIMAL(18,2)) AS 商家承担金额,
  CAST(IFNULL(orders.`平台承担总优惠(元)`, 0.00) AS DECIMAL(18,2)) AS 平台承担金额,
  CAST(IFNULL((afters.`退款金额(元)`)*100, 0.00) AS DECIMAL(18,2)) AS 退款金额,
  CAST(IFNULL(settle.`动账金额`, 0.00) AS DECIMAL(18,2)) AS 结算金额,
  -- 支出
  CAST(IFNULL(settle.`分销佣金`, 0.00) AS DECIMAL(18,2)) AS 达人佣金,
  CAST(IFNULL(settle.`佣金`, 0.00) AS DECIMAL(18,2)) AS 平台佣金,
  CAST((IFNULL(settle.`代运营服务商佣金`, 0.00) + IFNULL(settle.`代开发服务商佣金`, 0.00)) AS DECIMAL(18,2)) AS 服务商佣金, 
  CAST((IFNULL(settle.`花呗分期手续费`, 0.00) + IFNULL(settle.`支付渠道费`, 0.00) + IFNULL(settle.`附加费`, 0.00)) AS DECIMAL(18,2)) AS 其他分成, 
  -- 收入
  CAST(IFNULL(settle.`国补订单毛保金额`, 0.00) AS DECIMAL(18,2)) AS 政府补贴, 
  CAST(IFNULL(settle.`平台优惠补贴`, 0.00) AS DECIMAL(18,2)) AS 平台优惠, 
  CAST(IFNULL(settle.`平台运费补贴`, 0.00) AS DECIMAL(18,2)) AS 运费, 
  '小红书' AS 平台
  FROM ods.ods_红书_订单_全量 orders
  LEFT JOIN (
    SELECT
    订单ID 订单ID,
    GROUP_CONCAT(`售后ID` SEPARATOR ',') AS `售后ID`,
    MAX(更新时间) 创建时间, 
    GROUP_CONCAT(`售后状态` SEPARATOR ',') AS `售后状态`,
    GROUP_CONCAT(`售后原因说明` SEPARATOR ',') AS `售后原因说明`,
    GROUP_CONCAT(`用户描述` SEPARATOR ',') AS `用户描述`,
    SUM(`退款金额(元)`) AS `退款金额(元)`
    FROM ods.ods_红书_售后_全量 
    GROUP BY 1
  ) afters ON orders.`订单ID` = afters.`订单ID`
  LEFT JOIN (
    SELECT
      订单号 订单号,
      MAX(结算时间) 结算时间, 
      GROUP_CONCAT(`结算状态` SEPARATOR ',') AS `结算状态`,
      SUM((`动账金额`)) AS `动账金额`,
      SUM(IFNULL(`分销佣金`, 0.00)) AS `分销佣金`,
      SUM(IFNULL(`佣金`, 0.00)) AS `佣金`,
      SUM(IFNULL(`代运营服务商佣金`, 0.00)) AS `代运营服务商佣金`,
      SUM(IFNULL(`代开发服务商佣金`, 0.00)) AS `代开发服务商佣金`,
      SUM(IFNULL(`花呗分期手续费`, 0.00)) AS `花呗分期手续费`,
      SUM(IFNULL(`支付渠道费`, 0.00)) AS `支付渠道费`,
      SUM(IFNULL(`附加费`, 0.00)) AS `附加费`,
      SUM(IFNULL(`国补订单毛保金额`, 0.00)) AS `国补订单毛保金额`,
      SUM(IFNULL(`平台优惠补贴`, 0.00)) AS `平台优惠补贴`,
      SUM(IFNULL(`平台运费补贴`, 0.00)) AS `平台运费补贴`
    FROM ods.ods_红书_结算_全量 
    GROUP BY 1
  ) settle ON orders.`订单ID` = settle.`订单号`
)t;


