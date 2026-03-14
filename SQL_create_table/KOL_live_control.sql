CREATE OR REPLACE VIEW ads.`周洲专场_分时段效果监控` AS
-- 1. 售后表预聚合：防止订单发散导致 GMV 虚高
WITH refund_agg AS (
    SELECT 
        `店铺订单ID`, 
        SUM(`退款总金额(元)`) AS 总退款金额
    FROM ods.ods_抖店_售后_全量2
    GROUP BY `店铺订单ID`
),
-- 2. 【核心池子】：获取今天在周洲下单的所有订单及用户
zhouzhou_today AS (
    SELECT 
        o.用户唯一ID AS 用户ID, 
        o.根订单ID AS 主订单ID,
        DATE_FORMAT(o.支付时间, '%H点') AS 时段, 
        o.`支付金额(元)` AS 买家实付, 
        IFNULL(a.总退款金额, 0) AS 退款金额
    FROM ods.ods_抖店_订单_全量2 o
    LEFT JOIN refund_agg a 
        ON o.根订单ID = a.`店铺订单ID`
    WHERE o.`主播名称(达人)` LIKE '%周洲%'
      -- 锁定今天的订单
      AND o.支付时间 BETWEEN '2026-03-11 00:00:00' AND '2026-03-11 23:59:59'
      AND o.支付时间 != '' 
      AND o.支付时间 IS NOT NULL
),
-- 3. 【查户口】：只查今天的用户，在24年到26年3月10日的历史购买记录
user_history AS (
    SELECT 
        用户ID, 
        1 AS is_shop_old, -- 只要能在历史表查到，就是全店历史老用户
        MAX(CASE WHEN `达人名称` LIKE '%周洲%' THEN 1 ELSE 0 END) AS is_zhouzhou_old -- 历史是否在周洲直播间买过
    FROM dwd.dwd_电商数据_宽表
    -- 历史订单时间范围：截至到昨天 
    WHERE 支付时间 < '2026-03-11 00:00:00'
      -- 核心性能优化：严格限制只扫描今天下单的人，速度极快
      AND 用户ID IN (SELECT DISTINCT 用户ID FROM zhouzhou_today)
    GROUP BY 用户ID
),
-- 4. 拼合与按时段聚合
zhouzhou_agg AS (
    SELECT 
        z.时段,
        COUNT(DISTINCT z.主订单ID) AS 订单量,
        SUM(z.买家实付) AS GMV,
        SUM(z.买家实付 - z.退款金额) AS GSV,
        -- 分母基数：今天出单的用户
        COUNT(DISTINCT z.用户ID) AS 购买总数,
        -- 【满足要求1】：无历史订单的=新用户，有历史订单的=老用户
        COUNT(DISTINCT CASE WHEN h.is_shop_old IS NULL THEN z.用户ID END) AS 新用户数,
        COUNT(DISTINCT CASE WHEN h.is_shop_old = 1 THEN z.用户ID END) AS 老用户数,
        -- 【满足要求2】：有多少个在周洲直播间购买过的用户
        COUNT(DISTINCT CASE WHEN h.is_zhouzhou_old = 1 THEN z.用户ID END) AS 周洲老用户数
    FROM zhouzhou_today z
    LEFT JOIN user_history h ON z.用户ID = h.用户ID
    GROUP BY z.时段
    
    UNION ALL
    -- 追加总计行
    SELECT 
        '总计' AS 时段,
        COUNT(DISTINCT z.主订单ID) AS 订单量,
        SUM(z.买家实付) AS GMV,
        SUM(z.买家实付 - z.退款金额) AS GSV,
        COUNT(DISTINCT z.用户ID) AS 购买总数,
        COUNT(DISTINCT CASE WHEN h.is_shop_old IS NULL THEN z.用户ID END) AS 新用户数,
        COUNT(DISTINCT CASE WHEN h.is_shop_old = 1 THEN z.用户ID END) AS 老用户数,
        COUNT(DISTINCT CASE WHEN h.is_zhouzhou_old = 1 THEN z.用户ID END) AS 周洲老用户数
    FROM zhouzhou_today z
    LEFT JOIN user_history h ON z.用户ID = h.用户ID
)
-- 5. 最终输出计算（所有比率的分母全部统一）
SELECT 
    时段,
    订单量,
    ROUND(IFNULL(GMV, 0), 2) AS GMV,
    ROUND(IFNULL(GSV, 0), 2) AS GSV,
    ROUND(IF(购买总数 = 0, 0, GSV / 购买总数), 2) AS 客单价,
    
    新用户数,
    老用户数,
    周洲老用户数,
    购买总数,
    -- 新老用户、周洲老用户的分母，统一全都是今天出单的“购买总数”
    CONCAT(ROUND(IF(购买总数 = 0, 0, 新用户数 / 购买总数 * 100), 2), '%') AS 新用户占比,
    CONCAT(ROUND(IF(购买总数 = 0, 0, 老用户数 / 购买总数 * 100), 2), '%') AS 老用户占比,
    CONCAT(ROUND(IF(购买总数 = 0, 0, 周洲老用户数 / 购买总数 * 100), 2), '%') AS 周洲用户占比
    
FROM zhouzhou_agg
ORDER BY 
    CASE WHEN 时段 = '总计' THEN 1 ELSE 0 END, 
    时段 ASC;




    
