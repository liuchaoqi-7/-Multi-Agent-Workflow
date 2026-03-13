-- 1. 创建合并订单表
CREATE TABLE IF NOT EXISTS dwd.dwd_电商数据_宽表(
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '唯一标识每行数据',
  更新时间 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
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
  结算状态 CHAR(64) COMMENT '结算状态（无则为空）',
  售后原因 CHAR(255) COMMENT '售后原因',
  售后备注 CHAR(255) COMMENT '售后备注',
  商品ID CHAR(64) COMMENT '商品ID',
  SKUID CHAR(64) COMMENT 'SKU ID',
  商品名称 CHAR(64) COMMENT '商品名称',
  下单来源 CHAR(64) COMMENT '下单来源（无则为空）',
  视频ID CHAR(64) COMMENT '视频ID（无则为空）',
  直播ID CHAR(255) COMMENT '直播ID（无则为空）',
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
  订单当前盈收 DECIMAL(18,2) COMMENT '订单当前盈收(元)',
  结算总收入 DECIMAL(18,2) COMMENT '结算总收入(元)',
  结算总支出 DECIMAL(18,2) COMMENT '结算总支出(元)',
  店铺优惠 DECIMAL(18,2) COMMENT '店铺优惠券金额(元)',
  平台优惠 DECIMAL(18,2) COMMENT '平台优惠券金额(元)',
  达人优惠 DECIMAL(18,2) COMMENT '达人优惠券金额(元)',
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
  免佣补贴 DECIMAL(18,2) COMMENT '免佣补贴(元)',  
  平台 CHAR(16) COMMENT '数据来源：抖店/微信/小红书',
  -- 主键和索引（提升查询效率）
  PRIMARY KEY (id) USING BTREE,
  INDEX idx_order_info (主订单ID, 平台) USING BTREE, -- 保留复合索引用于查询
  INDEX idx_shop_ID (店铺ID) USING BTREE,
  INDEX idx_create_time (提交时间) USING BTREE,
  INDEX idx_user_ID (用户ID) USING BTREE,
  INDEX idx_kol_ID (达人UID) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '抖店+微信+小红书合并数据全量表';


TRUNCATE TABLE dwd.dwd_电商数据_宽表;  
-- 更新数据
INSERT INTO dwd.dwd_电商数据_宽表 (
  店铺ID,主订单ID,子订单ID,售后单号,用户ID,提交时间,支付时间,完成时间,售后申请时间,结算时间,
  订单状态,售后状态,结算状态,售后原因,售后备注,商品ID,SKUID,商品名称,下单来源,
  视频ID,直播ID,支付方式,达人UID,达人名称,内容ID,广告ID,流量来源ID,物流公司,物流单号,
  收件人姓名,收件人电话,收件人省份,收件人城市,购买数量,商家实收,买家实付,订单总额,
  商家承担金额,平台承担金额,达人承担金额,退款金额,结算金额,订单当前盈收,结算总收入,结算总支出,
  店铺优惠,平台优惠,达人优惠,达人佣金,平台佣金,服务商佣金,团长佣金,
  渠道分成,其他分成,渠道推广费,运费,支付补贴,政府补贴,其它补贴,免佣补贴,
  平台
)
SELECT
  店铺ID,主订单ID,子订单ID,售后单号,用户ID,提交时间,支付时间,完成时间,售后申请时间,结算时间,
  订单状态,售后状态,结算状态,售后原因,售后备注,商品ID,SKUID,商品名称,下单来源,
  视频ID,直播ID,支付方式,达人UID,达人名称,内容ID,广告ID,流量来源ID,物流公司,物流单号,
  收件人姓名,收件人电话,收件人省份,收件人城市,购买数量,商家实收,买家实付,订单总额,
  商家承担金额,平台承担金额,达人承担金额,退款金额,结算金额,订单当前盈收,结算总收入,结算总支出,
  店铺优惠,平台优惠,达人优惠,达人佣金,平台佣金,服务商佣金,团长佣金,
  渠道分成,其他分成,渠道推广费,运费,支付补贴,政府补贴,其它补贴,免佣补贴,
  平台
FROM (
  -- 1. 抖店订单表
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
    CAST(
      IFNULL(
        CASE orders.订单状态
          WHEN '已使用' THEN '已完成'
          WHEN '待使用' THEN '待收货'
          ELSE orders.订单状态
        END, ''
      ) AS CHAR(64)
    ) AS 订单状态,
    -- CAST(
    --   IFNULL(
    --     IF(afters.`退款总金额(元)`< orders.`支付金额(元)`,'部分退款',IF(afters.售后状态 = '同意退款，退款成功', '全额退款',
    --       IF(afters.售后状态 = '补寄成功', '售后关闭', afters.售后状态)
    --     )), '无售后'
    --   ) AS CHAR(64)
    -- ) AS 售后状态,
    cast(IF(afters.部分售后类型 IS NOT NULL,IF(afters.部分售后类型='1','部分退款','全额退款'),'无售后') AS CHAR(64)) AS 售后状态,
    -- CAST(
    --   IFNULL(
    --     CASE settle.交易类型编码
    --       WHEN '0' THEN '已结算'
    --       WHEN '1' THEN '已结算'     -- '结算后退款-原路退回'
    --       WHEN '2' THEN '已结算'     --'保证金退款-支出退回'
    --       WHEN '3' THEN '已结算'     --'结算后退款-非原路退回'
    --       ELSE '无需结算'  
    --     END,'无需结算'  
    --   ) AS CHAR(64)
    -- ) AS 结算状态,
    CAST(
      IFNULL(
        CASE settle.交易类型编码
          WHEN '0' THEN CONCAT('已结算') 
          WHEN '1' THEN CONCAT('已结算')
          WHEN '2' THEN CONCAT('已结算')
          WHEN '3' THEN CONCAT('已结算')
          ELSE CONCAT('无需结算')        
        END,
        '无需结算' 
      ) AS CHAR(64)
    ) AS 结算状态    ,
    -- settle.`结算状态` 结算状态,
    CAST(IFNULL(afters.`售后原因`, '') AS CHAR(255)) AS 售后原因,
    CAST(IFNULL(afters.`售后备注`, '') AS CHAR(255)) AS 售后备注,
    CAST(IFNULL(orders.商品ID, '') AS CHAR(64)) AS 商品ID,
    CAST(IFNULL(orders.`SKU ID`, '') AS CHAR(64)) AS SKUID,
    CAST(IFNULL(orders.商品名称, '') AS CHAR(64)) AS 商品名称,
    CAST(IFNULL(orders.下单端, '') AS CHAR(64)) AS 下单来源,
    CAST(IFNULL(orders.`视频ID`, '') AS CHAR(255)) AS 视频ID,
    CAST(IFNULL(orders.`直播房间ID`, '') AS CHAR(64)) AS 直播ID,
    CAST(IFNULL(orders.支付方式, '') AS CHAR(64)) AS 支付方式,
    -- CAST(IF(CAST(orders.`主播ID(达人)` AS CHAR(64))='0' OR orders.`主播ID(达人)`=0 OR orders.`主播ID(达人)`='', IF(settle.`达人ID` !='', settle.达人ID, 'douyin0'), orders.`主播ID(达人)`) AS CHAR(64)) AS 达人UID,
    CAST(
      IF(
        TRIM(COALESCE(CAST(orders.`主播ID(达人)` AS CHAR(64)), '')) IN ('0', '', '00', '000'),
        IF(
          TRIM(COALESCE(settle.`达人ID`, '')) NOT IN ('0', '', '00', '000'),
          settle.`达人ID`,
          'douyin0'
        ),
        IF(
          TRIM(COALESCE(CAST(orders.`主播ID(达人)` AS CHAR(64)), '')) IN ('0', '', '00', '000'),
          'douyin0',
          orders.`主播ID(达人)`
        )
      ) AS CHAR(64)
    ) AS 达人UID,    
    CAST(IF(orders.`主播名称(达人)`='', IF(settle.`达人名称` !='', settle.达人名称, '小店自卖'),orders.`主播名称(达人)`) AS CHAR(64)) AS 达人名称,
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
    CAST(
      IFNULL(
        settle.`结算金额(元)`,(orders.`商家实际收入(元)`- afters.`退款总金额(元)`)
      ) AS DECIMAL(18,2)
    ) AS 订单当前盈收,
    CAST(IFNULL(settle.`总收入(元)`, 0.00) AS DECIMAL(18,2)) AS 结算总收入,
    CAST(IFNULL(settle.`总支出(元)`, 0.00) AS DECIMAL(18,2)) AS 结算总支出,
    CAST(IFNULL(settle.`店铺优惠券金额(元)`, 0.00) AS DECIMAL(18,2)) AS 店铺优惠,
    CAST(IFNULL(settle.`平台优惠券金额(元)`, 0.00) AS DECIMAL(18,2)) AS 平台优惠,
    CAST(IFNULL(settle.`达人优惠券金额(元)`, 0.00) AS DECIMAL(18,2)) AS 达人优惠,
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
    orders.平台 平台
  FROM ods.ods_抖店_订单_全量 orders
  LEFT JOIN (
    SELECT
      店铺订单ID,
      GROUP_CONCAT(售后单号 SEPARATOR ',') AS 售后单号,
      MAX(申请时间) AS 申请时间,
      -- SUBSTRING_INDEX(GROUP_CONCAT(结算状态描述 ORDER BY 结算状态描述 SEPARATOR ','), ',', 1) AS 结算状态,
      SUBSTRING_INDEX(GROUP_CONCAT(售后状态 ORDER BY 申请时间 DESC SEPARATOR ','), ',', 1) AS 售后状态,
      SUBSTRING_INDEX(GROUP_CONCAT(售后原因 ORDER BY 申请时间 SEPARATOR ','), ',', 1) AS 售后原因,
      SUBSTRING_INDEX(GROUP_CONCAT(售后备注 ORDER BY 申请时间 SEPARATOR ','), ',', 1) AS 售后备注,
      SUBSTRING_INDEX(GROUP_CONCAT(部分售后类型 ORDER BY 申请时间 SEPARATOR ','), ',', 1) AS 部分售后类型,
      -- GROUP_CONCAT(DISTINCT 售后状态 SEPARATOR '; ') AS 售后状态,
      -- GROUP_CONCAT(DISTINCT 售后原因 SEPARATOR '; ') AS 售后原因,
      -- GROUP_CONCAT(DISTINCT 售后备注 SEPARATOR '; ') AS 售后备注,
      max(`实付金额(元)`) AS `实付金额(元)`,
      SUM(`退款总金额(元)`) AS `退款总金额(元)`
      FROM ods.ods_抖店_售后_全量
      GROUP BY  店铺订单ID
    )afters 
  ON orders.`根订单ID` = afters.`店铺订单ID`
  LEFT JOIN (
    SELECT
      订单ID 订单ID,
      max(达人ID) 达人ID,
      MAX(达人名称) 达人名称,
      MAX(结算时间) 结算时间, 
      -- 交易类型编码,
      SUBSTRING_INDEX(GROUP_CONCAT(`交易类型编码` ORDER BY `交易类型编码` SEPARATOR ','), ',', 1) AS `交易类型编码`,
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
  )settle ON orders.`根订单ID` = settle.`订单ID`
  --
  UNION ALL
  -- 2. 微信订单表
  SELECT
    CAST(IFNULL(orders.`店铺ID`, '') AS CHAR(64)) AS 店铺ID,
    CAST(IFNULL(orders.`订单ID`, '') AS CHAR(64)) AS 主订单ID,
    CAST('' AS CHAR(64)) AS 子订单ID,
    CAST(IFNULL(afters.`售后单ID`, '') AS CHAR(255)) AS 售后单号,
    CAST(IFNULL(orders.`订单归属人OpenID`, '') AS CHAR(255)) AS 用户ID,
    IF(orders.创建时间 IS NULL OR orders.创建时间 = '', NULL, STR_TO_DATE(orders.创建时间, '%Y-%m-%d %H:%i:%s')) AS 提交时间,
    IF(orders.支付时间 IS NULL OR orders.支付时间 = '', NULL, STR_TO_DATE(orders.支付时间, '%Y-%m-%d %H:%i:%s')) AS 支付时间,
    IF(orders.商家结算时间 IS NULL OR orders.商家结算时间 = '', NULL, STR_TO_DATE(orders.商家结算时间, '%Y-%m-%d %H:%i:%s')) AS 完成时间,
    IF(afters.售后单创建时间 IS NULL OR afters.售后单创建时间 = '', NULL, STR_TO_DATE(afters.售后单创建时间, '%Y-%m-%d %H:%i:%s')) AS 售后申请时间,
    -- IF(settle.结算时间 IS NULL OR settle.结算时间 = '', NULL, STR_TO_DATE(settle.结算时间, '%Y-%m-%d %H:%i:%s')) AS 结算时间,
    settle.结算时间 结算时间,
    CAST(
      IFNULL(
        CASE orders.订单状态描述
          WHEN '全部商品售后之后，订单取消' THEN '已关闭'
          WHEN '完成' THEN '已完成'
          WHEN '未付款用户主动取消或超时未付款订单自动取消' THEN '已关闭'
          ELSE orders.订单状态描述
        END, ''
      ) AS CHAR(64)
    ) AS 订单状态,
    CAST(
      IFNULL(
        CASE 
          WHEN afters.`退款金额(元)` = orders.`用户实付金额(元)` THEN '全额退款'
          WHEN afters.`退款金额(元)` < orders.`用户实付金额(元)` THEN '部分退款'
          WHEN afters.售后单状态描述 = '用户取消申请' THEN '售后关闭'
          ELSE afters.售后单状态描述
        END,'无售后'
      ) AS CHAR(64)
    ) AS 售后状态,
    CAST(
      IFNULL(   
        CASE CAST(settle.结算状态描述 AS CHAR) 
          WHEN '结算完成' THEN '已结算'
          -- WHEN '无需结算' THEN '已结算'
          ELSE settle.结算状态描述
        END, '无需结算'
      ) AS CHAR(64)
    ) AS 结算状态,
    -- settle.结算状态描述 AS 结算状态,
    CAST(IFNULL(afters.`退款原因解释`, '') AS CHAR(255)) AS 售后原因,
    CAST(IFNULL(afters.`用户申请描述`, '') AS CHAR(255)) AS 售后备注,
    CAST(IFNULL(orders.商品ID, '') AS CHAR(64)) AS 商品ID,
    CAST(IFNULL(orders.`SKU_ID`, '') AS CHAR(64)) AS SKUID,
    CAST(IFNULL(orders.商品标题, '') AS CHAR(64)) AS 商品名称,
    CAST('' AS CHAR(64)) AS 下单来源,
    CAST('' AS CHAR(64)) AS 视频ID,
    CAST(IFNULL(orders.直播ID, '') AS CHAR(255)) AS 直播ID,
    CAST(IFNULL(orders.支付方式描述, '') AS CHAR(64)) AS 支付方式,
    -- CAST(IF(orders.`带货账户ID`='', IF(orders.视频号ID='', 'weixin0',orders.视频号ID),orders.`带货账户ID`) AS CHAR(64)) AS 达人UID,
    -- CAST(IF(orders.`带货账户昵称`='', IF(orders.视频号ID='', '小店自卖',orders.视频号ID),orders.`带货账户昵称`) AS CHAR(64)) AS 达人名称,
    CAST(
      IF(
        TRIM(COALESCE(orders.`带货账户ID`, '')) NOT IN ('', '0', 'weixin0'),
        CASE orders.带货账户ID
          WHEN 'sphnJ3JweyM6zFr' THEN 'wxde71967c7416c027'
          WHEN 'sphQxztpPrGBpan' THEN 'wx4b7580c0cbca1071'
          WHEN 'sph93OZOgUMJG8s' THEN 'wx64d619ff0068905c'
          WHEN 'sphI8Nk0amRWdz3' THEN 'wx629fd8a977d25def'
          WHEN 'sphUQcmVKamseY6' THEN 'wx9e7fc0514292b805'
          WHEN 'sphOIiQKHr2Ywsk' THEN 'wxa8a809c4269cd3b9'
          WHEN 'sphBk4rkpgEf6Y5' THEN 'wxe506f2b7684b2cd8'
          WHEN 'sphbLDpNmYlCSMN' THEN 'wxb13b513021a69c70'
          WHEN 'sph2IXbTvTRI1gO' THEN 'wx156daed3949ba47c'
          WHEN 'sphrWZFUSq19v3S' THEN 'wxd5dedb5d25b2332a'
          WHEN 'sph4knxmryHcJiH' THEN 'wxa5267dfd8d539eff'
          WHEN 'sphO7tQ8wGTshh8' THEN 'wx688b2a803d197831'
          WHEN 'sphYGGNgmRRRmDL' THEN 'wx3231d01040a64c04'
          WHEN 'sphmX4VxhNNVy9d' THEN 'wx6dcd44282f8d93cb'
          WHEN 'sphUDmi383juh68' THEN 'wxf2bce44ea78bbe84'
          WHEN 'sphI5lPps4FWZp4' THEN 'wx6acae2157c0ea908'
          when 'sph05HSFLkrnfdx' THEN 'wx522fd7a327011d44'
          WHEN 'sph5RQEtewTtyVs' THEN 'wxf1787af9dfabbd29'
          WHEN 'sphYuONnHM0fDgl' THEN 'wx68b34c0e2024c158'
          ELSE orders.带货账户ID  
        END  ,       
        IF(
          TRIM(COALESCE(orders.视频号ID, '')) NOT IN ('', '0'),
          CASE orders.视频号ID
            WHEN 'sphnJ3JweyM6zFr' THEN 'wxde71967c7416c027'
            WHEN 'sphQxztpPrGBpan' THEN 'wx4b7580c0cbca1071'
            WHEN 'sph93OZOgUMJG8s' THEN 'wx64d619ff0068905c'
            WHEN 'sphI8Nk0amRWdz3' THEN 'wx629fd8a977d25def'
            WHEN 'sphUQcmVKamseY6' THEN 'wx9e7fc0514292b805'
            WHEN 'sphOIiQKHr2Ywsk' THEN 'wxa8a809c4269cd3b9'
            WHEN 'sphBk4rkpgEf6Y5' THEN 'wxe506f2b7684b2cd8'
            WHEN 'sphbLDpNmYlCSMN' THEN 'wxb13b513021a69c70'
            WHEN 'sph2IXbTvTRI1gO' THEN 'wx156daed3949ba47c'
            WHEN 'sphrWZFUSq19v3S' THEN 'wxd5dedb5d25b2332a'
            WHEN 'sph4knxmryHcJiH' THEN 'wxa5267dfd8d539eff'
            WHEN 'sphO7tQ8wGTshh8' THEN 'wx688b2a803d197831'
            WHEN 'sphYGGNgmRRRmDL' THEN 'wx3231d01040a64c04'
            WHEN 'sphmX4VxhNNVy9d' THEN 'wx6dcd44282f8d93cb'
            WHEN 'sphUDmi383juh68' THEN 'wxf2bce44ea78bbe84'
            WHEN 'sphI5lPps4FWZp4' THEN 'wx6acae2157c0ea908'
            when 'sph05HSFLkrnfdx' THEN 'wx522fd7a327011d44'
            WHEN 'sph5RQEtewTtyVs' THEN 'wxf1787af9dfabbd29'
            WHEN 'sphYuONnHM0fDgl' THEN 'wx68b34c0e2024c158'
            ELSE 'weixin0'  
          END         
          ,
          'weixin0'  
        )
      ) AS CHAR(64)
    ) AS 达人UID,   
    CAST(
      IF(
        TRIM(COALESCE(orders.`带货账户昵称`, '')) != '',
        orders.`带货账户昵称`,
        IF(
          TRIM(COALESCE(orders.视频号ID, '')) != '',  
          CASE 
            WHEN orders.视频号ID = 'sphnJ3JweyM6zFr' THEN '我是兰心Vanessa'
            WHEN orders.视频号ID = 'sphUQcmVKamseY6' THEN '三五小星'
            WHEN orders.视频号ID = 'sphI8Nk0amRWdz3' THEN '柯妈带三娃'
            WHEN orders.视频号ID = 'sph05HSFLkrnfdx' THEN 'An安的小窝'
            WHEN orders.视频号ID = 'sphQxztpPrGBpan' THEN '二重奏宝妈'   
            WHEN orders.视频号ID = 'sphYGGNgmRRRmDL' THEN '哈佛常爸'
            WHEN orders.视频号ID = 'sphOIiQKHr2Ywsk' THEN '小板牙严选'  
            WHEN orders.视频号ID = 'sphbLDpNmYlCSMN' THEN '薪传好物' 
            WHEN orders.视频号ID = 'sphO7tQ8wGTshh8' THEN '丹妈读童书'  
            WHEN orders.视频号ID = 'sphrWZFUSq19v3S' THEN '粥悦悦'  
            WHEN orders.视频号ID = 'sph93OZOgUMJG8s' THEN '硅谷工程师憨爸'  
            WHEN orders.视频号ID = 'sphBk4rkpgEf6Y5' THEN '小满和他的博士老爸'  
            WHEN orders.视频号ID = 'sph5RQEtewTtyVs' THEN '修养生息181'
            WHEN orders.视频号ID = 'sphYuONnHM0fDgl' THEN '雯小雯-四年级'
            -- WHEN orders.视频号ID = 'sphYuONnHM0fDgl' THEN '三娃的小窝'
            ELSE '未知达人'      
          END,
          '小店自卖' 
        )
      ) AS CHAR(64)
    ) AS 达人名称,     
    CAST('' AS CHAR(64)) AS 内容ID,
    CAST('' AS CHAR(64)) AS 广告ID,
    CAST('' AS CHAR(64)) AS 流量来源ID,
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
    CAST(IFNULL(settle.`预计结算金额(元)`, 0.00) AS DECIMAL(18,2)) AS 结算金额,
    CAST(
      IFNULL(
        settle.`预计结算金额(元)`, (orders.`商家实收金额(元)`- afters.`退款金额(元)`)
      ) AS DECIMAL(18,2)
    ) AS 订单当前盈收,
    CAST(0 AS DECIMAL(18,2)) AS 结算总收入,
    CAST(0 AS DECIMAL(18,2)) AS 结算总支出,
    CAST(IFNULL(settle.`商户优惠金额(元)`, 0.00) AS DECIMAL(18,2)) AS 店铺优惠,
    CAST(IFNULL(settle.`平台优惠金额(元)`, 0.00) AS DECIMAL(18,2)) AS 平台优惠,
    CAST(0 AS DECIMAL(18,2)) AS 达人优惠, 
    -- CAST(IFNULL(settle.`达人优惠金额(元)`, 0.00) AS DECIMAL(18,2)) AS 达人优惠, -- 重复
    -CAST(IFNULL(settle.`预计达人服务费(元)`, 0.00) AS DECIMAL(18,2)) AS 达人佣金,
    -CAST(IFNULL(settle.`原技术服务费(元)`, 0.00) AS DECIMAL(18,2)) AS 平台佣金,
    CAST(0 AS DECIMAL(18,2)) AS 服务商佣金,
    -CAST(IFNULL(settle.`预计机构服务费(元)`, 0.00) AS DECIMAL(18,2)) AS 团长佣金,
    CAST(0 AS DECIMAL(18,2)) AS 渠道分成,
    CAST(0 AS DECIMAL(18,2)) AS 其他分成,
    CAST(0 AS DECIMAL(18,2)) AS 渠道推广费,
    CAST(0 AS DECIMAL(18,2)) AS 运费, 
    CAST(0 AS DECIMAL(18,2)) AS 支付补贴, 
    CAST(IFNULL(settle.`国家补贴金额(元)`, 0.00) AS DECIMAL(18,2)) AS 政府补贴,
    CAST(0 AS DECIMAL(18,2)) AS 其它补贴, 
    CAST(0 AS DECIMAL(18,2)) AS 免佣补贴,
    '微信' AS 平台
  FROM ods.ods_微信_订单_全量 orders
  LEFT JOIN (
    SELECT
      原订单号 原订单号,
      GROUP_CONCAT(`售后单ID` SEPARATOR ';') AS `售后单ID`,
      max(售后单更新时间) 售后单创建时间,
      SUBSTRING_INDEX(GROUP_CONCAT(`售后单状态描述` ORDER BY `售后单更新时间` DESC SEPARATOR ','), ',', 1) AS `售后单状态描述`,
      SUBSTRING_INDEX(GROUP_CONCAT(`退款原因解释` ORDER BY `退款原因解释` SEPARATOR ','), ',', 1) AS `退款原因解释`,
      SUBSTRING_INDEX(GROUP_CONCAT(`用户申请描述` ORDER BY `用户申请描述` SEPARATOR ','), ',', 1) AS `用户申请描述`,
      -- GROUP_CONCAT(`售后单状态描述` SEPARATOR ';') AS `售后单状态描述`,
      -- GROUP_CONCAT(`退款原因解释` SEPARATOR ';') AS `退款原因解释`,
      -- GROUP_CONCAT(`用户申请描述` SEPARATOR ';') AS `用户申请描述`,
      SUM(IF(售后单状态描述='退款完成',`退款金额(元)`,0.00)) `退款金额(元)`
      FROM ods.ods_微信_售后_全量
      GROUP BY 1
   ) afters ON orders.`订单ID` = afters.原订单号
  LEFT JOIN ods.ods_微信_结算_全量 settle ON orders.`订单ID` = settle.`订单ID`
  UNION ALL
  -- 3. 小红书
  SELECT
    CAST(IFNULL(orders.`店铺ID`, '') AS CHAR(64)) AS 店铺ID,
    CAST(IFNULL(orders.`订单ID`, '') AS CHAR(64)) AS 主订单ID,
    CAST('' AS CHAR(64)) AS 子订单ID,
    CAST(IFNULL(afters.`售后ID`, '') AS CHAR(255)) AS 售后单号,
    CAST(IFNULL(orders.`用户编号`, '') AS CHAR(255)) AS 用户ID,
    IF(orders.订单创建时间 IS NULL OR orders.订单创建时间 = '', NULL, STR_TO_DATE(orders.订单创建时间, '%Y-%m-%d %H:%i:%s')) AS 提交时间,
    IF(orders.订单支付时间 IS NULL OR orders.订单支付时间 = '', NULL, STR_TO_DATE(orders.订单支付时间, '%Y-%m-%d %H:%i:%s')) AS 支付时间,
    IF(orders.订单完成时间 IS NULL OR orders.订单完成时间 = '', NULL, STR_TO_DATE(orders.订单完成时间, '%Y-%m-%d %H:%i:%s')) AS 完成时间,
    IF(afters.创建时间 IS NULL OR afters.创建时间 = '', NULL, STR_TO_DATE(afters.创建时间, '%Y-%m-%d %H:%i:%s')) AS 售后申请时间,
    IF(settle.结算时间 IS NULL OR settle.结算时间 = '', NULL, STR_TO_DATE(settle.结算时间, '%Y-%m-%d %H:%i:%s')) AS 结算时间,
    CAST(
      IFNULL(
        CASE orders.订单状态
          WHEN '已取消' THEN '已关闭'
          ELSE orders.订单状态
        END , ''
      ) AS CHAR(64)
    ) AS 订单状态,
    CAST(
      IFNULL(
        CASE afters.售后状态
          WHEN '完成' THEN '全额退款'
          WHEN '取消' THEN '售后关闭'
          ELSE afters.售后状态
        END,'无售后'
      ) AS CHAR(64)
    ) AS 售后状态,
    CAST(IFNULL(settle.结算状态, '无需结算') AS CHAR(64)) AS 结算状态,
    -- settle.结算状态 AS 结算状态,
    CAST(IFNULL(afters.`售后原因说明`, '') AS CHAR(255)) AS 售后原因,
    CAST(IFNULL(afters.`用户描述`, '') AS CHAR(255)) AS 售后备注,
    CAST(IF(orders.商品ID!='',orders.商品ID, orders.`主SKU ID`) AS CHAR(64)) AS 商品ID,
    CAST(IFNULL(orders.`单品SKU ID`, '') AS CHAR(64)) AS SKUID,
    CAST(IFNULL(orders.商品名称, '') AS CHAR(64)) AS 商品名称,
    CAST('' AS CHAR(64)) AS 下单来源,
    CAST('' AS CHAR(64)) AS 视频ID,
    CAST('' AS CHAR(255)) AS 直播ID,
    CAST(IFNULL(orders.支付方式, '') AS CHAR(64)) AS 支付方式,
    -- CAST(IF(orders.`达人ID_x`='', 'hongshu0',orders.`达人ID_x`) AS CHAR(64)) AS 达人UID,
    -- CAST(IF(orders.`达人名称`='', '小店自卖',orders.`达人名称`) AS CHAR(64)) AS 达人名称,
    CAST(IF((orders.`达人ID_y`='hongshu0' AND 达人ID_x!=''), orders.`达人ID_x`,orders.`达人ID_y`) AS CHAR(64)) AS 达人UID,
    CAST(IF((orders.达人昵称='小店自卖' AND 达人名称!=''), orders.达人名称,orders.达人昵称) AS CHAR(64)) AS 达人名称,

    CAST('' AS CHAR(64)) AS 内容ID,
    CAST('' AS CHAR(64)) AS 广告ID,
    CAST('' AS CHAR(64)) AS 流量来源ID,
    CAST(IFNULL(orders.快递公司, '') AS CHAR(64)) AS 物流公司,
    CAST(IFNULL(orders.快递单号, '') AS CHAR(64)) AS 物流单号,
    CAST(IFNULL(orders.收件人姓名, '') AS CHAR(64)) AS 收件人姓名,
    CAST(IFNULL(orders.收件人电话, '') AS CHAR(64)) AS 收件人电话,
    CAST(IFNULL(orders.省, '') AS CHAR(64)) AS 收件人省份,
    CAST(IFNULL(orders.市, '') AS CHAR(64)) AS 收件人城市,
    CAST(IFNULL(orders.主SKU数量, 0) AS DECIMAL(18,0)) AS 购买数量,
    CAST(IFNULL(IF(订单支付时间='',0,`商家应收金额(元)`), 0.00) AS DECIMAL(18,2)) AS 买家实付,
    CAST(IFNULL(IF(订单支付时间='',0,`订单实付金额(包含运费和定金)`), 0.00) AS DECIMAL(18,2)) AS 商家实收,
    -- CAST(IFNULL(IF(订单状态='已取消',0,orders.`商家应收金额(元)`), 0.00) AS DECIMAL(18,2)) AS 商家实收,
    -- CAST(IFNULL(IF(订单状态='已取消',0,orders.`订单实付金额(包含运费和定金)`), 0.00) AS DECIMAL(18,2)) AS 买家实付,
    -- CAST(IFNULL(`单个SKU原价(元)`, 0.00) AS DECIMAL(18,2)) AS 订单总额,  -- 商品总实付
    CAST(IFNULL(IF(`商品总价(元)`!=`商家应收金额(元)`+`商家承担总优惠(元)`,`商家应收金额(元)`+`商家承担总优惠(元)`,`商品总价(元)`), 0.00) AS DECIMAL(18,2)) AS 订单总额,  -- 商品总实付
    -- CAST(IFNULL(IF(`商品总价(元)`!=`商家应收金额(元)`,`商家应收金额(元)`+`商家承担总优惠(元)`,`商品总价(元)`), 0.00) AS DECIMAL(18,2)) AS 订单总额,  -- 商品总实付
    -- CAST(IFNULL(IF(`商品总价(元)`!=`商家应收金额(元)`,`商家应收金额(元)`,`商品总价(元)`), 0.00) AS DECIMAL(18,2)) AS 订单总额,  -- 商品总实付
    -- CAST(IFNULL(orders.`商家应收金额(元)`, 0.00) AS DECIMAL(18,2)) AS 商家实收,
    -- CAST(IFNULL(orders.`订单实付金额(包含运费和定金)`, 0.00) AS DECIMAL(18,2)) AS 买家实付,
    -- CAST(IFNULL(orders.`商品总价(元)`, 0.00) AS DECIMAL(18,2)) AS 订单总额,
    CAST(IFNULL(orders.`商家承担总优惠(元)`, 0.00) AS DECIMAL(18,2)) AS 商家承担金额,
    CAST(IFNULL(orders.`平台承担总优惠(元)`, 0.00) AS DECIMAL(18,2)) AS 平台承担金额,
    CAST(0.00 AS DECIMAL(18,2)) AS 达人承担金额,
    CAST(IFNULL((afters.`退款金额(元)`), 0.00)*100 AS DECIMAL(18,2)) AS 退款金额,
    CAST(IFNULL(settle.`动账金额`, 0.00) AS DECIMAL(18,2)) AS 结算金额,
    -- CAST(
    --   IFNULL(
    --     settle.`动账金额`,((IF(订单支付时间='',0,`订单实付金额(包含运费和定金)`))- (IFNULL((afters.`退款金额(元)`), 0.00)*100))
    --     ) AS DECIMAL(18,2)
    -- ) AS 订单当前盈收,
    CAST((`订单实付金额(包含运费和定金)` - afters.`退款金额(元)`) AS DECIMAL(18,2)) AS 订单当前盈收,
    CAST(0 AS DECIMAL(18,2)) AS 结算总收入,
    CAST(0 AS DECIMAL(18,2)) AS 结算总支出,
    CAST(0 AS DECIMAL(18,2)) AS 店铺优惠,
    CAST(IFNULL(settle.`平台优惠补贴`, 0.00) AS DECIMAL(18,2)) AS 平台优惠, 
    CAST(0 AS DECIMAL(18,2)) AS 达人优惠,
    CAST(IFNULL(settle.`分销佣金`, 0.00) AS DECIMAL(18,2)) AS 达人佣金,
    CAST(IFNULL(settle.`佣金`, 0.00) AS DECIMAL(18,2)) AS 平台佣金,
    CAST((IFNULL(settle.`代运营服务商佣金`, 0.00) + IFNULL(settle.`代开发服务商佣金`, 0.00)) AS DECIMAL(18,2)) AS 服务商佣金, 
    CAST(0 AS DECIMAL(18,2)) AS 团长佣金,
    CAST(0 AS DECIMAL(18,2)) AS 渠道分成,
    CAST((IFNULL(settle.`花呗分期手续费`, 0.00) + IFNULL(settle.`支付渠道费`, 0.00) + IFNULL(settle.`附加费`, 0.00)) AS DECIMAL(18,2)) AS 其他分成, 
    CAST(0 AS DECIMAL(18,2)) AS 渠道推广费,
    CAST(IFNULL(settle.`平台运费补贴`, 0.00) AS DECIMAL(18,2)) AS 运费, 
    CAST(0 AS DECIMAL(18,2)) AS 支付补贴, 
    CAST(IFNULL(settle.`国补订单毛保金额`, 0.00) AS DECIMAL(18,2)) AS 政府补贴, 
    CAST(0 AS DECIMAL(18,2)) AS 其它补贴, 
    CAST(0 AS DECIMAL(18,2)) AS 免佣补贴,
    '小红书' AS 平台
  FROM ods.ods_红书_订单_全量 orders
  LEFT JOIN (
    SELECT
      订单ID 订单ID,
      GROUP_CONCAT(`售后ID` SEPARATOR ',') AS `售后ID`,
      MAX(创建时间) 创建时间,  -- 口径更改：售后完成时间=更新时间
      SUBSTRING_INDEX(GROUP_CONCAT(`售后状态` ORDER BY `更新时间` DESC SEPARATOR ','), ',', 1) AS `售后状态`,
      SUBSTRING_INDEX(GROUP_CONCAT(`售后原因说明` ORDER BY `售后原因说明` SEPARATOR ','), ',', 1) AS `售后原因说明`,
      SUBSTRING_INDEX(GROUP_CONCAT(`用户描述` ORDER BY `用户描述` SEPARATOR ','), ',', 1) AS `用户描述`,
      -- GROUP_CONCAT(`售后状态` SEPARATOR ',') AS `售后状态`,
      -- GROUP_CONCAT(`售后原因说明` SEPARATOR ',') AS `售后原因说明`,
      -- GROUP_CONCAT(`用户描述` SEPARATOR ',') AS `用户描述`,
      SUM(IF(售后状态='完成',`退款金额(元)`,0.00)) AS `退款金额(元)`
    FROM ods.ods_红书_售后_全量 
    GROUP BY 1
  ) afters ON orders.`订单ID` = afters.`订单ID`
  LEFT JOIN (
    SELECT
      订单号 订单号,
      MIN(结算时间) 结算时间, 
      SUBSTRING_INDEX(GROUP_CONCAT(`结算状态` ORDER BY `结算时间` DESC SEPARATOR ','), ',', 1) AS `结算状态`,
      -- GROUP_CONCAT(`结算状态` SEPARATOR ',') AS `结算状态`,
      SUM(`动账金额`) AS `动账金额`,
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
) t;

