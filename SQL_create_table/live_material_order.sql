CREATE TABLE IF NOT EXISTS ads.ads_直播素材_m分摊表 (
    -- 1. 直播基础信息
    `【直播M】更新时间` DATETIME COMMENT '直播M数据更新时间',
    `【直播M】File Paths` VARCHAR(500) COMMENT '直播M文件路径',
    `【直播M】场次ID` VARCHAR(64) COMMENT '直播M场次唯一标识',
    `【直播M】UID` VARCHAR(64) COMMENT '直播M关联的UID（清洗后）',
    `【直播M】Day` DATETIME COMMENT '直播M业务日期',
    `【直播M】Hour` DATETIME COMMENT '直播M小时维度时间',
    `【直播M】minute` DATETIME COMMENT '直播M分钟维度时间',
    `【直播M】ID` VARCHAR(64) COMMENT '直播M唯一ID',
    `【直播M】主播ID` VARCHAR(64) COMMENT '直播M主播ID（清洗后）',
    `【直播M】排期UID` VARCHAR(64) COMMENT '直播M关联的排期UID',
    `【直播M】主播名称` VARCHAR(100) COMMENT '直播M主播名称',
    `【直播M】主播归属` VARCHAR(100) COMMENT '直播M主播所属机构/团队',
    `【直播M】有效分钟` DECIMAL(10, 2) DEFAULT 0 COMMENT '直播M有效时长（分钟）',
    
    -- 2. 直播原始指标（DECIMAL保证精度，默认0避免NULL）
    `【直播M】曝光人次_(曝光-观看率反推)` DECIMAL(18, 2) DEFAULT 0 COMMENT '直播M曝光人次（观看率反推值）',
    `【直播M】进入直播间人数` DECIMAL(18, 2) DEFAULT 0 COMMENT '直播M进入直播间人数',
    `【直播M】直播间离开人数` DECIMAL(18, 2) DEFAULT 0 COMMENT '直播M直播间离开人数',
    `【直播M】实时在线人数` DECIMAL(18, 2) DEFAULT 0 COMMENT '直播M实时在线人数',
    `【直播M】人均观看时长(秒)` DECIMAL(18, 2) DEFAULT 0 COMMENT '直播M人均观看时长（秒）',
    `【直播M】商品曝光人数` DECIMAL(18, 2) DEFAULT 0 COMMENT '直播M商品曝光人数',
    `【直播M】商品点击人数` DECIMAL(18, 2) DEFAULT 0 COMMENT '直播M商品点击人数',
    `【直播M】成交人数` DECIMAL(18, 2) DEFAULT 0 COMMENT '直播M成交人数',
    `【直播M】成交订单数` DECIMAL(18, 2) DEFAULT 0 COMMENT '直播M成交订单数',
    `【直播M】成交金额` DECIMAL(18, 2) DEFAULT 0 COMMENT '直播M成交金额',
    `【直播M】新增粉丝数` DECIMAL(18, 2) DEFAULT 0 COMMENT '直播M新增粉丝数',
    `【直播M】点赞次数` DECIMAL(18, 2) DEFAULT 0 COMMENT '直播M点赞次数',
    `【直播M】评论次数` DECIMAL(18, 2) DEFAULT 0 COMMENT '直播M评论次数',
    `【直播M】新加直播团人数` DECIMAL(18, 2) DEFAULT 0 COMMENT '直播M新加直播团人数',
    `【直播M】互动人数_(互动率反推)` DECIMAL(18, 2) DEFAULT 0 COMMENT '直播M互动人数（互动率反推值）',
    `【直播M】负反馈人次_(反馈率反推)` DECIMAL(18, 2) DEFAULT 0 COMMENT '直播M负反馈人次（反馈率反推值）',
    
    -- 3. 千川分摊消耗
    `【千川M】消耗` DECIMAL(18, 2) DEFAULT 0 COMMENT '千川M分钟级分摊消耗（小时总额*分钟占比）',
    
    -- 4. 千川分钟级效果
    `【千川M】展现` DECIMAL(18, 0) DEFAULT 0 COMMENT '千川M展现量',
    `【千川M】点击` DECIMAL(18, 0) DEFAULT 0 COMMENT '千川M点击量',
    `【千川M】订单数` DECIMAL(18, 0) DEFAULT 0 COMMENT '千川M订单数',
    `【千川M】成交金额` DECIMAL(18, 2) DEFAULT 0 COMMENT '千川M成交金额',
    `【千川M】用户实际支付金额` DECIMAL(18, 2) DEFAULT 0 COMMENT '千川M用户实际支付金额',
    `【千川M】净成交订单数` DECIMAL(18, 0) DEFAULT 0 COMMENT '千川M净成交订单数',
    `【千川M】净成交金额` DECIMAL(18, 2) DEFAULT 0 COMMENT '千川M净成交金额',
    `【千川M】用户实际支付净成交金额` DECIMAL(18, 2) DEFAULT 0 COMMENT '千川M用户实际支付净成交金额',
    
    -- 5. 追投指标分摊
    `【千川M】追投消耗` DECIMAL(18, 2) DEFAULT 0 COMMENT '千川M分钟级追投分摊消耗',
    `【千川M】追投展现` DECIMAL(18, 0) DEFAULT 0 COMMENT '千川M追投展现量',
    `【千川M】追投点击` DECIMAL(18, 0) DEFAULT 0 COMMENT '千川M追投点击量',
    `【千川M】追投订单数` DECIMAL(18, 0) DEFAULT 0 COMMENT '千川M追投订单数',
    `【千川M】追投成交金额` DECIMAL(18, 2) DEFAULT 0 COMMENT '千川M追投成交金额',
    `【千川M】追投用户实际支付金额` DECIMAL(18, 2) DEFAULT 0 COMMENT '千川M追投用户实际支付金额',
    `【千川M】视频播放数` DECIMAL(18, 0) DEFAULT 0 COMMENT '千川M视频播放数',
    `【千川M】视频完播数` DECIMAL(18, 0) DEFAULT 0 COMMENT '千川M视频完播数',
    `【千川M】粉丝数` DECIMAL(18, 0) DEFAULT 0 COMMENT '千川M粉丝数',
    `【千川M】评论数` DECIMAL(18, 0) DEFAULT 0 COMMENT '千川M评论数',
    `【千川M】点赞数` DECIMAL(18, 0) DEFAULT 0 COMMENT '千川M点赞数',
    
    -- 6. 订单分摊指标
    `【订单】全店成交额` DECIMAL(18, 2) DEFAULT 0 COMMENT '订单维度全店成交额',
    `【订单】全店订单数` DECIMAL(18, 0) DEFAULT 0 COMMENT '订单维度全店订单数',
    `【订单·】支付成交额` DECIMAL(18, 2) DEFAULT 0 COMMENT '订单维度支付成交额（字段名保留原始命名）',
    `【订单】支付订单数` DECIMAL(18, 0) DEFAULT 0 COMMENT '订单维度支付订单数',
    `【订单】退后成交额` DECIMAL(18, 2) DEFAULT 0 COMMENT '订单维度退款后成交额（GSV）',
    `【订单】退后订单数` DECIMAL(18, 0) DEFAULT 0 COMMENT '订单维度退款后订单数',

    -- 主键与索引（核心维度组合主键，保证数据唯一性）
    PRIMARY KEY (`【直播M】ID`, `【直播M】minute`, `【直播M】UID`),
    -- 常用查询维度索引（提升筛选/聚合效率）
    INDEX idx_live_time (`【直播M】Day`, `【直播M】Hour`, `【直播M】minute`),
    INDEX idx_live_uid (`【直播M】UID`),
    INDEX idx_live_anchor (`【直播M】主播ID`, `【直播M】主播归属`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='直播素材M分钟级分摊表（整合直播、千川、订单多维度指标，按分钟分摊消耗）';

TRUNCATE TABLE ads.ads_直播素材_m分摊表;  -- 清空表（保留结构和索引）

INSERT INTO ads.ads_直播素材_M分摊表 
SELECT
    -- 1. 直播基础信息 (类型转换确保精度)
    CAST(IFNULL(live_min.【直播M】更新时间, '') AS DATETIME) AS 【直播M】更新时间,
    live_min.`【直播M】File Paths`,
    live_min.【直播M】场次ID,
    live_min.【直播M】UID_clean AS 【直播M】UID,
    CAST(IFNULL(live_min.日期, '') AS DATETIME) AS 【直播M】Day,   
    CAST(IFNULL(live_min.小时时间, '') AS DATETIME) AS 【直播M】Hour,     
    CAST(IFNULL(live_min.分钟时间, '') AS DATETIME) AS 【直播M】minute,
    live_min.`【直播M】ID` AS 【直播M】ID, 
    live_min.`【直播M】主播ID_clean` AS 【直播M】主播ID,
    live_min.`【直播M】排期UID` AS 【直播M】排期UID,
    live_min.`【直播M】主播名称` AS 【直播M】主播名称,
    live_min.`【直播M】主播归属` AS 【直播M】主播归属,
    live_min.【直播M】有效分钟,
    -- 2. 直播原始指标
    CAST(IFNULL(live_min.`【直播M】曝光人次_(曝光-观看率反推)`, 0) AS DECIMAL(18, 2)) AS `【直播M】曝光人次_(曝光-观看率反推)`,
    CAST(IFNULL(live_min.`【直播M】进入直播间人数`, 0) AS DECIMAL(18, 2)) AS `【直播M】进入直播间人数`,
    CAST(IFNULL(live_min.`【直播M】直播间离开人数`, 0) AS DECIMAL(18, 2)) AS `【直播M】直播间离开人数`,
    CAST(IFNULL(live_min.`【直播M】实时在线人数`, 0) AS DECIMAL(18, 2)) AS `【直播M】实时在线人数`,
    CAST(IFNULL(live_min.`【直播M】人均观看时长(秒)`, 0) AS DECIMAL(18, 2)) AS `【直播M】人均观看时长(秒)`,
    CAST(IFNULL(live_min.`【直播M】商品曝光人数`, 0) AS DECIMAL(18, 2)) AS `【直播M】商品曝光人数`,
    CAST(IFNULL(live_min.`【直播M】商品点击人数`, 0) AS DECIMAL(18, 2)) AS `【直播M】商品点击人数`,
    CAST(IFNULL(live_min.`【直播M】成交人数`, 0) AS DECIMAL(18, 2)) AS `【直播M】成交人数`,
    CAST(IFNULL(live_min.`【直播M】成交订单数`, 0) AS DECIMAL(18, 2)) AS `【直播M】成交订单数`,
    CAST(IFNULL(live_min.`【直播M】成交金额`, 0) AS DECIMAL(18, 2)) AS `【直播M】成交金额`,
    CAST(IFNULL(live_min.`【直播M】新增粉丝数`, 0) AS DECIMAL(18, 2)) AS `【直播M】新增粉丝数`,
    CAST(IFNULL(live_min.`【直播M】点赞次数`, 0) AS DECIMAL(18, 2)) AS `【直播M】点赞次数`,
    CAST(IFNULL(live_min.`【直播M】评论次数`, 0) AS DECIMAL(18, 2)) AS `【直播M】评论次数`,
    CAST(IFNULL(live_min.`【直播M】新加直播团人数`, 0) AS DECIMAL(18, 2)) AS `【直播M】新加直播团人数`,
    CAST(IFNULL(live_min.`【直播M】互动人数_(互动率反推)`, 0) AS DECIMAL(18, 2)) AS `【直播M】互动人数_(互动率反推)`,
    CAST(IFNULL(live_min.`【直播M】负反馈人次_(反馈率反推)`, 0) AS DECIMAL(18, 2)) AS `【直播M】负反馈人次_(反馈率反推)`,
    -- 3. 千川分摊消耗 (使用小时总额 * 分钟占比)
    CAST(IFNULL(qc_h.小时总消耗 * live_min.分钟消耗占比, 0) AS DECIMAL(18,2)) AS 【千川M】消耗,
    -- 4. 千川分钟级效果 (展现、点击、成交等)
    CAST(qc.展现 AS DECIMAL(18,0)) AS 【千川M】展现,
    CAST(qc.点击 AS DECIMAL(18,0)) AS 【千川M】点击,
    CAST(qc.订单数 AS DECIMAL(18,0)) AS 【千川M】订单数,
    CAST(qc.成交金额 AS DECIMAL(18,2)) AS 【千川M】成交金额,
    CAST(qc.用户实际支付金额 AS DECIMAL(18,2)) AS 【千川M】用户实际支付金额,
    CAST(qc.净成交订单数 AS DECIMAL(18,0)) AS 【千川M】净成交订单数,
    CAST(qc.净成交金额 AS DECIMAL(18,2)) AS 【千川M】净成交金额,
    CAST(qc.用户实际支付净成交金额 AS DECIMAL(18,2)) AS 【千川M】用户实际支付净成交金额,
    -- 5. 追投指标分摊
    CAST(IFNULL(qc_h.小时追投消耗 * live_min.分钟消耗占比, 0) AS DECIMAL(18,2)) AS 【千川M】追投消耗,
    CAST(qc.追投展现 AS DECIMAL(18,0)) AS 【千川M】追投展现,
    CAST(qc.追投点击 AS DECIMAL(18,0)) AS 【千川M】追投点击,
    CAST(qc.追投订单数 AS DECIMAL(18,0)) AS 【千川M】追投订单数,
    CAST(qc.追投成交金额 AS DECIMAL(18,2)) AS 【千川M】追投成交金额,
    CAST(qc.追投用户实际支付金额 AS DECIMAL(18,2)) AS 【千川M】追投用户实际支付金额,
    CAST(qc.视频播放数 AS DECIMAL(18,0)) AS 【千川M】视频播放数,
    CAST(qc.视频完播数 AS DECIMAL(18,0)) AS 【千川M】视频完播数,
    CAST(qc.粉丝数 AS DECIMAL(18,0)) AS 【千川M】粉丝数,
    CAST(qc.评论数 AS DECIMAL(18,0)) AS 【千川M】评论数,
    CAST(qc.点赞数 AS DECIMAL(18,0)) AS 【千川M】点赞数,
    -- 6. 订单分摊指标 (来自强聚合后的宽表)
    CAST(ROUND(IFNULL(live_min.`小时全局GMV` , 0), 2) AS DECIMAL(18, 2)) AS `【订单】全店成交额`,
    CAST(ROUND(IFNULL(live_min.`小时全局订单数` , 0), 0) AS DECIMAL(18, 0)) AS `【订单】全店订单数`,
    CAST(ROUND(IFNULL(live_min.`支付成交额` , 0), 2) AS DECIMAL(18, 2)) AS `【订单·】支付成交额`,
    CAST(ROUND(IFNULL(live_min.`支付订单数` , 0), 0) AS DECIMAL(18, 0)) AS `【订单】支付订单数`,
    CAST(ROUND(IFNULL(live_min.`小时GSV` , 0), 2) AS DECIMAL(18, 2)) AS `【订单】退后成交额`,
    CAST(ROUND(IFNULL(live_min.`退后订单数` , 0), 0) AS DECIMAL(18, 0)) AS `【订单】退后订单数`

FROM (
    -- 第一层：直播、排期与订单宽表的聚合
    SELECT
        lm.*,
        wb_hour.小时全局GMV,
        wb_hour.小时全局订单数,
        wb_hour.支付成交额,
        wb_hour.支付订单数,
        wb_hour.小时GSV,
        wb_hour.退后订单数,
        -- 计算分摊权重：当前分钟在所属账号小时内的占比
        CAST(IFNULL(1 / NULLIF(COUNT(lm.`【直播M】minute`) OVER (PARTITION BY lm.小时时间, lm.【直播M】UID_clean), 0), 0) AS DECIMAL(18, 6)) AS 分钟消耗占比
    FROM (
        -- 核心：直播表关联排期表，解决 251->240 误差
        SELECT
            t1.`【直播M】更新时间`, t1.`【直播M】File Paths`, t1.`【直播M】场次ID`, t1.`【直播M】有效分钟`, t1.`【直播M】ID`,
            DATE_FORMAT(t1.`【直播M】minute`, '%Y-%m-%d %H:00:00') AS 小时时间,
            DATE_FORMAT(t1.`【直播M】minute`, '%Y-%m-%d %H:%i:00') AS 分钟时间,
            t1.`【直播M】minute`,
            t2.`日期` AS 日期, -- 使用排期表的业务日期
            CONVERT(t1.`【直播M】UID` USING utf8mb4) AS 【直播M】UID_clean,
            t2.`账号` AS 【直播M】排期UID,
            t2.`主播ID`->>'$[0].number' AS 【直播M】主播ID_clean,
            t2.`主播` AS 【直播M】主播名称,
            t2.`主播归属` AS 【直播M】主播归属,
            t1.`【直播M】曝光人次_(曝光-观看率反推)`, t1.`【直播M】进入直播间人数`, t1.`【直播M】直播间离开人数`, t1.`【直播M】实时在线人数`,
            t1.`【直播M】人均观看时长(秒)`, t1.`【直播M】商品曝光人数`, t1.`【直播M】商品点击人数`, t1.`【直播M】成交人数`, t1.`【直播M】成交订单数`,
            t1.`【直播M】成交金额`, t1.`【直播M】新增粉丝数`, t1.`【直播M】点赞次数`, t1.`【直播M】评论次数`, t1.`【直播M】新加直播团人数`,
            t1.`【直播M】互动人数_(互动率反推)`, t1.`【直播M】负反馈人次_(反馈率反推)`
        FROM spider_01.直播数据minute t1
        INNER JOIN ods.ods_主播排期表 t2
            ON CONVERT(t1.`【直播M】UID` USING utf8mb4) = CONVERT(t2.`账号` USING utf8mb4)
            -- 修正：左闭右开，彻底解决交班重叠导致的重复行
            AND t1.`【直播M】minute` >= t2.开始时间 
            AND t1.`【直播M】minute` <= t2.结束时间 
        WHERE t2.开始时间 IS NOT NULL AND t2.结束时间 IS NOT NULL
        -- WHERE t1.`【直播M】minute` BETWEEN '2025-12-01 00:00:00' AND '2026-01-11 00:00:00'
    ) lm
    -- 修正：订单宽表强制分钟级聚合，解决 449条重复中的宽表膨胀问题
    LEFT JOIN (
        SELECT
            DATE_FORMAT(o.`支付时间`, '%Y-%m-%d %H:%i:00') AS 时间,
            CONVERT(o.达人UID USING utf8mb4) AS 达人UID_clean,
            CAST(IFNULL(SUM(o.`订单总额`), 0) AS DECIMAL(18, 2)) AS 小时全局GMV,
            CAST(IFNULL(COUNT(o.`提交时间`), 0) AS DECIMAL(18, 2)) AS 小时全局订单数,
            SUM(o.`买家实付`) AS 支付成交额,
            COUNT(CASE WHEN o.`买家实付` > 0 THEN 1 END) AS 支付订单数,
            SUM(o.`买家实付` - IFNULL(o.`退款金额`, 0)) AS 小时GSV,
            SUM(CASE WHEN (IFNULL(o.`退款金额`, 0)) > 0 THEN 0 ELSE 1 END) AS 退后订单数    
        FROM dwd.dwd_电商数据_宽表 o
        WHERE  平台 = '抖店'
        GROUP BY 1, 2
    ) wb_hour 
        ON lm.分钟时间 = wb_hour.时间
        AND lm.【直播M】UID_clean = wb_hour.达人UID_clean
) live_min
-- 关联 A：小时级消耗汇总（用于公平分摊）
LEFT JOIN (
    SELECT
        CONVERT(抖音UID USING utf8mb4) AS UID_clean,
        DATE_FORMAT(时间, '%Y-%m-%d %H:00:00') AS 小时时间,
        SUM(消耗) AS 小时总消耗,
        SUM(追投消耗) AS 小时追投消耗
    FROM dwd.千川_素材_汇总
    GROUP BY 1, 2
) qc_h
    ON live_min.小时时间 = qc_h.小时时间
    AND live_min.【直播M】UID_clean = qc_h.UID_clean
-- 关联 B：分钟级素材效果（确保每分钟唯一）
LEFT JOIN (
    SELECT
        CONVERT(抖音UID USING utf8mb4) AS UID_clean,
        时间, 
        SUM(展现) AS 展现, SUM(点击) AS 点击, SUM(订单数) AS 订单数, 
        SUM(成交金额) AS 成交金额, SUM(用户实际支付金额) AS 用户实际支付金额,
        SUM(净成交订单数) AS 净成交订单数, SUM(净成交金额) AS 净成交金额, 
        SUM(用户实际支付净成交金额) AS 用户实际支付净成交金额, 
        SUM(追投展现) AS 追投展现, SUM(追投点击) AS 追投点击, 
        SUM(追投订单数) AS 追投订单数, SUM(追投成交金额) AS 追投成交金额, 
        SUM(追投用户实际支付金额) AS 追投用户实际支付金额, 
        SUM(视频播放数) AS 视频播放数, SUM(视频完播数) AS 视频完播数, 
        SUM(粉丝数) AS 粉丝数, SUM(评论数) AS 评论数, SUM(点赞数) AS 点赞数
    FROM dwd.千川_素材_汇总
    GROUP BY 1, 2
) qc
    ON live_min.分钟时间 = qc.时间
    AND live_min.【直播M】UID_clean = qc.UID_clean;

