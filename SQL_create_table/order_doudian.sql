CREATE TABLE IF NOT EXISTS dwd.dwd_抖店_宽表(
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '唯一标识每行数据',
  店铺ID CHAR(64) COMMENT '店铺ID', 
  主订单ID CHAR(64) COMMENT '主订单ID（抖店=根订单ID/微信/小红书=订单ID）',
  子订单ID CHAR(64) COMMENT '子订单ID（无则为空）',
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
  下单来源 CHAR(64) COMMENT '下单来源（无则为空）',
  视频ID CHAR(64) COMMENT '视频ID（无则为空）',
  直播ID CHAR(64) COMMENT '直播ID（无则为空）',
  支付方式 CHAR(64) COMMENT '支付方式',
  达人UID CHAR(64) COMMENT '达人ID',
  达人名称 CHAR(64) COMMENT '达人昵称',
  内容ID CHAR(64) COMMENT '内容ID（无则为空）',
  广告ID CHAR(64) COMMENT '广告ID（无则为空）',
  流量来源ID CHAR(64) COMMENT '流量来源ID（无则为空）',
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
  结算总收入 DECIMAL(18,2) COMMENT '结算总收入(元)',
  结算总支出 DECIMAL(18,2) COMMENT '结算总支出(元)',
  店铺优惠券 DECIMAL(18,2) COMMENT '店铺优惠券金额(元)',
  平台优惠券 DECIMAL(18,2) COMMENT '平台优惠券金额(元)',
  达人优惠券 DECIMAL(18,2) COMMENT '达人优惠券金额(元)',
  达人佣金 DECIMAL(18,2) COMMENT '达人佣金金额(元)',
  平台佣金 DECIMAL(18,2) COMMENT '平台佣金金额(元)',
  服务商佣金 DECIMAL(18,2) COMMENT '服务商佣金金额(元)',
  团长佣金 DECIMAL(18,2) COMMENT '团长佣金金额(元)',
  渠道分成 DECIMAL(18,2) COMMENT '渠道分成金额(元)',
  其他分成 DECIMAL(18,2) COMMENT '其他分成金额(元)',
  渠道推广费 DECIMAL(18,2) COMMENT '渠道推广费金额(元)',
  运费 DECIMAL(18,2) COMMENT '运费金额(元)',
  支付补贴 DECIMAL(18,2) COMMENT '支付补贴金额(元)',
  政府补贴 DECIMAL(18,2) COMMENT '政府补贴金额(元)',
  其它补贴 DECIMAL(18,2) COMMENT '其它补贴金额(元)', 
  免佣补贴 DECIMAL(18,2) COMMENT '免佣补贴金额(元)',
  主播ID CHAR(16) COMMENT '区分订单归属',
  主播名称 CHAR(16) COMMENT '区分订单归属',
  平台 CHAR(16) COMMENT '数据来源：抖店/微信/小红书',
  -- 新增主键和索引（提升查询效率）
  PRIMARY KEY (id,主订单ID,平台) USING BTREE,
  -- INDEX idx_order_info (, 子订单ID, ) USING BTREE, -- 保留复合索引用于查询
  INDEX idx_店铺ID (店铺ID) USING BTREE,
  INDEX idx_提交时间 (提交时间) USING BTREE,
  INDEX idx_用户ID (用户ID) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '抖店合并数据全量表';

TRUNCATE TABLE dwd.dwd_抖店_宽表;  -- 清空表（保留结构和索引）
-- 重新插入最新数据
INSERT INTO dwd.dwd_抖店_宽表 (
  店铺ID,主订单ID,子订单ID,售后单号,用户ID,提交时间,支付时间,完成时间,售后申请时间,结算时间,
  订单状态,售后状态,结算状态,售后原因,售后备注,商品ID,SKUID,商品名称,下单来源,
  视频ID,直播ID,支付方式,达人UID,达人名称,内容ID,广告ID,流量来源ID,物流公司,物流单号,
  收件人姓名,收件人电话,收件人省份,收件人城市,购买数量,商家实收,买家实付,订单总额,
  商家承担金额,平台承担金额,达人承担金额,退款金额,结算金额,结算总收入,结算总支出,
  店铺优惠券,平台优惠券,达人优惠券,达人佣金,平台佣金,服务商佣金,团长佣金,
  渠道分成,其他分成,渠道推广费,运费,支付补贴,政府补贴,其它补贴,免佣补贴,
  主播ID,主播名称,
  平台)
SELECT
  店铺ID,主订单ID,子订单ID,售后单号,用户ID,提交时间,支付时间,完成时间,售后申请时间,结算时间,
  订单状态,售后状态,结算状态,售后原因,售后备注,商品ID,SKUID,商品名称,下单来源,
  视频ID,直播ID,支付方式,达人UID,达人名称,内容ID,广告ID,流量来源ID,物流公司,物流单号,
  收件人姓名,收件人电话,收件人省份,收件人城市,购买数量,商家实收,买家实付,订单总额,
  商家承担金额,平台承担金额,达人承担金额,退款金额,结算金额,结算总收入,结算总支出,
  店铺优惠券,平台优惠券,达人优惠券,达人佣金,平台佣金,服务商佣金,团长佣金,
  渠道分成,其他分成,渠道推广费,运费,支付补贴,政府补贴,其它补贴,免佣补贴,
  主播ID,主播名称,
  平台
FROM (
  SELECT
    CAST(IFNULL(orders.`店铺ID`, '') AS CHAR(64)) AS 店铺ID,
    CAST(IFNULL(orders.`根订单ID`, '') AS CHAR(64)) AS 主订单ID,
    CAST(IFNULL(orders.`子订单ID`, '') AS CHAR(64)) AS 子订单ID,
    CAST(IFNULL(afters.`售后单号`, '') AS CHAR(255)) AS 售后单号,
    CAST(IFNULL(orders.`用户唯一ID`, '') AS CHAR(255)) AS 用户ID,
    IF(orders.提交时间 IS NULL OR orders.提交时间 = '', NULL, STR_TO_DATE(orders.提交时间, '%Y-%m-%d %H:%i:%s')) AS 提交时间,
    IF(orders.支付时间 IS NULL OR orders.支付时间 = '', NULL, STR_TO_DATE(orders.支付时间, '%Y-%m-%d %H:%i:%s')) AS 支付时间,
    IF(orders.订单完成时间 IS NULL OR orders.订单完成时间 = '', NULL, STR_TO_DATE(orders.订单完成时间, '%Y-%m-%d %H:%i:%s')) AS 完成时间,
    IF(afters.申请时间 IS NULL OR afters.申请时间 = '', NULL, STR_TO_DATE(afters.申请时间, '%Y-%m-%d %H:%i:%s')) AS 售后申请时间,
    IF(settle.结算时间 IS NULL OR settle.结算时间 = '', NULL, STR_TO_DATE(settle.结算时间, '%Y-%m-%d %H:%i:%s')) AS 结算时间,
    CAST(IFNULL(orders.订单状态, '') AS CHAR(64)) AS 订单状态,
    CAST(IFNULL(afters.售后状态,'') AS CHAR(64)) AS 售后状态,
    CAST(
      IFNULL(
        CASE settle.交易类型编码
          WHEN '0' THEN '已结算'
          WHEN '1' THEN '结算后退款-原路退回'
          WHEN '2' THEN '保证金退款-支出退回'
          WHEN '3' THEN '结算后退款-非原路退回'
          ELSE ''  
        END,''  ) AS CHAR(64)
    ) AS 结算状态,
    -- settle.`结算状态` `结算状态`,
    CAST(IFNULL(afters.`售后原因`, '') AS CHAR(255)) AS 售后原因,
    CAST(IFNULL(afters.`售后备注`, '') AS CHAR(255)) AS 售后备注,
    CAST(IFNULL(orders.商品ID, '') AS CHAR(64)) AS 商品ID,
    CAST(IFNULL(orders.`SKU ID`, '') AS CHAR(64)) AS SKUID,
    CAST(IFNULL(orders.商品名称, '') AS CHAR(64)) AS 商品名称,
    CAST(IFNULL(orders.下单端, '') AS CHAR(64)) AS 下单来源,
    CAST(IFNULL(orders.`视频ID`, '') AS CHAR(64)) AS 视频ID,
    CAST(IFNULL(orders.`直播房间ID`, '') AS CHAR(64)) AS 直播ID,
    CAST(IFNULL(CASE WHEN orders.支付方式 = '7' THEN '0元抽奖' ELSE orders.支付方式 END, '') AS CHAR(64)) AS 支付方式,    
    CAST(IFNULL(orders.`主播ID(达人)`, '') AS CHAR(64)) AS 达人UID,
    -- CAST(IFNULL(orders.`主播ID(达人)`,SUBSTRING_INDEX(orders.`流量来源ID`, '_', 1)) AS CHAR(64)) AS 达人UID,            
    CAST(IFNULL(orders.`主播名称(达人)`, '') AS CHAR(64)) AS 达人名称,
    CAST(IFNULL(orders.`内容ID`, '') AS CHAR(64)) AS 内容ID,
    CAST(IFNULL(orders.`广告ID`, '') AS CHAR(64)) AS 广告ID,
    CAST(IFNULL(orders.`流量来源ID`, '') AS CHAR(64)) AS 流量来源ID,
    CAST(IFNULL(orders.物流公司, '') AS CHAR(64)) AS 物流公司,
    CAST(IFNULL(orders.物流单号, '') AS CHAR(64)) AS 物流单号,
    CAST(IFNULL(orders.收件人姓名, '') AS CHAR(64)) AS 收件人姓名,
    CAST(IFNULL(orders.收件人电话, '') AS CHAR(64)) AS 收件人电话,
    CAST(IFNULL(orders.收件人省份, '') AS CHAR(64)) AS 收件人省份,
    CAST(IFNULL(orders.收件人城市, '') AS CHAR(64)) AS 收件人城市,
    CAST(IFNULL(orders.购买数量, 0) AS DECIMAL(18,0)) AS 购买数量,
    CAST(IFNULL(orders.`商家实际收入(元)`, 0.00) AS DECIMAL(18,2)) AS 商家实收,
    CAST(IFNULL(orders.`支付金额(元)`, 0.00) AS DECIMAL(18,2)) AS 买家实付,
    CAST(IFNULL(orders.`订单总金额(元)`, 0.00) AS DECIMAL(18,2)) AS 订单总额,
    CAST(IFNULL(orders.`商家承担金额(元)`, 0.00) AS DECIMAL(18,2)) AS 商家承担金额,
    CAST(IFNULL(orders.`平台承担金额(元)`, 0.00) AS DECIMAL(18,2)) AS 平台承担金额,
    CAST(IFNULL(orders.`达人承担金额(元)`, 0.00) AS DECIMAL(18,2)) AS 达人承担金额,
    CAST(IFNULL(afters.`退款总金额(元)`, 0.00) AS DECIMAL(18,2)) AS 退款金额,
    CAST(IFNULL(settle.`结算金额(元)`, 0.00) AS DECIMAL(18,2)) AS 结算金额,
    CAST(IFNULL(settle.`总收入(元)`, 0.00) AS DECIMAL(18,2)) AS 结算总收入,
    CAST(IFNULL(settle.`总支出(元)`, 0.00) AS DECIMAL(18,2)) AS 结算总支出,
    CAST(IFNULL(settle.`店铺优惠券金额(元)`, 0.00) AS DECIMAL(18,2)) AS 店铺优惠券,
    CAST(IFNULL(settle.`平台优惠券金额(元)`, 0.00) AS DECIMAL(18,2)) AS 平台优惠券,
    CAST(IFNULL(settle.`达人优惠券金额(元)`, 0.00) AS DECIMAL(18,2)) AS 达人优惠券,
    CAST(IFNULL(settle.`达人佣金(元)`, 0.00) AS DECIMAL(18,2)) AS 达人佣金,
    CAST(IFNULL(settle.`平台服务费(元)`, 0.00) AS DECIMAL(18,2)) AS 平台佣金,
    CAST(IFNULL(settle.`合作伙伴佣金(元)`, 0.00) AS DECIMAL(18,2)) AS 服务商佣金,
    CAST(IFNULL(settle.`上校服务费(元)`, 0.00) AS DECIMAL(18,2)) AS 团长佣金,
    CAST(IFNULL(settle.`上校渠道费(元)`, 0.00) AS DECIMAL(18,2)) AS 渠道分成,
    CAST(IFNULL(settle.`其他分摊金额(元)`, 0.00) AS DECIMAL(18,2)) AS 其他分成,
    CAST(IFNULL(settle.`渠道推广费(元)`, 0.00) AS DECIMAL(18,2)) AS 渠道推广费,
    CAST((IFNULL(settle.`打包费(元)`, 0.00) + IFNULL(settle.`运费金额(元)`, 0.00) - IFNULL(settle.`运费优惠金额(元)`, 0.00)) AS DECIMAL(18,2)) AS 运费, 
    CAST((IFNULL(settle.`ZT支付补贴(元)`, 0.00) + IFNULL(settle.`ZR支付补贴(元)`, 0.00) + IFNULL(settle.`银行补贴金额(元)`, 0.00)) AS DECIMAL(18,2)) AS 支付补贴, 
    CAST((IFNULL(settle.`政府补贴金额(元)`, 0.00) + IFNULL(settle.`政府补贴店铺减免(元)`, 0.00)) AS DECIMAL(18,2)) AS 政府补贴, 
    CAST((IFNULL(settle.`换新补贴金额(元)`, 0.00) + IFNULL(settle.`其他平台补贴(元)`, 0.00)) AS DECIMAL(18,2)) AS 其它补贴, 
    CAST(IFNULL(settle.`实际免佣金金额(元)`, 0.00) AS DECIMAL(18,2)) AS 免佣补贴,
    CAST(IFNULL(live.`主播ID`, '') AS CHAR(16)) AS 主播ID,
    CAST(IFNULL(live.`主播名称`, '') AS CHAR(16)) AS 主播名称,
    orders.平台 平台
  FROM ods.ods_抖店_订单_全量 orders
  LEFT JOIN (
    SELECT
      `店铺订单ID`,
      GROUP_CONCAT(售后单号 SEPARATOR ',') AS 售后单号,
      MAX(申请时间) AS 申请时间,
      -- MAX(售后状态) AS 售后状态,
      SUBSTRING_INDEX(GROUP_CONCAT(`售后状态` ORDER BY `售后完结时间` DESC SEPARATOR ','), ',', 1) AS `售后状态`,
      -- GROUP_CONCAT(DISTINCT 售后状态 SEPARATOR '; ') AS 售后状态,
      GROUP_CONCAT(DISTINCT 售后原因 SEPARATOR '; ') AS 售后原因,
      GROUP_CONCAT(DISTINCT 售后备注 SEPARATOR '; ') AS 售后备注,
      SUM(`退款总金额(元)`) AS '退款总金额(元)'
    FROM ods.ods_抖店_售后_全量
    GROUP BY  1
  )afters 
  ON orders.`根订单ID` = afters.`店铺订单ID`
  LEFT JOIN (
    SELECT
      `订单ID` 订单ID,
      MAX(`结算时间`) `结算时间`, 
      -- 交易类型编码,
      SUBSTRING_INDEX(GROUP_CONCAT(`交易类型编码` ORDER BY `结算时间` DESC SEPARATOR ','), ',', 1) AS `交易类型编码`,
      -- GROUP_CONCAT(`结算状态描述` SEPARATOR ',') AS `结算状态`,
      SUM(IFNULL(`结算金额(元)`, 0.00)) AS `结算金额(元)`,
      SUM(IFNULL(`总收入(元)`, 0.00)) AS `总收入(元)`,
      SUM(IFNULL(`总支出(元)`, 0.00)) AS `总支出(元)`,
      SUM(IFNULL(`达人佣金(元)`, 0.00)) AS `达人佣金(元)`,
      SUM(IFNULL(`平台服务费(元)`, 0.00)) AS `平台服务费(元)`,
      SUM(IFNULL(`合作伙伴佣金(元)`, 0.00)) AS `合作伙伴佣金(元)`,
      SUM(IFNULL(`上校服务费(元)`, 0.00)) AS `上校服务费(元)`,
      SUM(IFNULL(`上校渠道费(元)`, 0.00)) AS `上校渠道费(元)`,
      SUM(IFNULL(`其他分摊金额(元)`, 0.00)) AS `其他分摊金额(元)`,
      SUM(IFNULL(`渠道推广费(元)`, 0.00)) AS `渠道推广费(元)`,
      SUM(IFNULL(`打包费(元)`, 0.00)) AS `打包费(元)`,
      SUM(IFNULL(`运费金额(元)`, 0.00)) AS `运费金额(元)`,
      SUM(IFNULL(`运费优惠金额(元)`, 0.00)) AS `运费优惠金额(元)`,
      SUM(IFNULL(`ZT支付补贴(元)`, 0.00)) AS `ZT支付补贴(元)`,
      SUM(IFNULL(`ZR支付补贴(元)`, 0.00)) AS `ZR支付补贴(元)`,
      SUM(IFNULL(`银行补贴金额(元)`, 0.00)) AS `银行补贴金额(元)`,
      SUM(IFNULL(`政府补贴金额(元)`, 0.00)) AS `政府补贴金额(元)`,
      SUM(IFNULL(`政府补贴店铺减免(元)`, 0.00)) AS `政府补贴店铺减免(元)`,
      SUM(IFNULL(`换新补贴金额(元)`, 0.00)) AS `换新补贴金额(元)`,
      SUM(IFNULL(`其他平台补贴(元)`, 0.00)) AS `其他平台补贴(元)`,
      SUM(IFNULL(`实际免佣金金额(元)`, 0.00)) AS `实际免佣金金额(元)`,
      SUM(IFNULL(`平台优惠券金额(元)`, 0.00)) AS `平台优惠券金额(元)`,
      SUM(IFNULL(`店铺优惠券金额(元)`, 0.00)) AS `店铺优惠券金额(元)`,
      SUM(IFNULL(`达人优惠券金额(元)`, 0.00)) AS `达人优惠券金额(元)`
    FROM ods.ods_抖店_结算_全量 
    GROUP BY 1
  )settle 
  ON orders.`根订单ID` = settle.`订单ID`
  LEFT JOIN (
    SELECT
    【直播M】排期UID COLLATE utf8mb4_0900_ai_ci 排期UID
    ,【直播M】主播ID COLLATE utf8mb4_0900_ai_ci 主播ID
    ,`【直播M】主播名称` COLLATE utf8mb4_0900_ai_ci 主播名称
    ,`【直播M】minute`
    FROM spider_01.直播数据minute
  )live ON orders.`主播ID(达人)`  = live.排期UID COLLATE utf8mb4_0900_ai_ci
        AND DATE_ADD(DATE_FORMAT(orders.提交时间,"%Y-%m-%d %H:%i:00"),INTERVAL 1 MINUTE) = live.`【直播M】minute`
)t;

