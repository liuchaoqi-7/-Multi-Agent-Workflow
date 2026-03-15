CREATE TABLE IF NOT EXISTS dim.`dim_达人榜单` (
    `日期` DATETIME COMMENT '达人最新数据的日期',
    `行业类目` VARCHAR(50) COMMENT '标准化后的行业类目（如电子教育、图书教育等）',
    `榜单` VARCHAR(30) COMMENT '达人所属榜单类型（直播带货榜/短视频带货榜）',
    `账号类型` VARCHAR(20) COMMENT '达人账号类型',
    `头像URL` VARCHAR(500) COMMENT '达人头像链接',
    `达人ID` VARCHAR(64) NOT NULL COMMENT '达人唯一标识（主键）',
    `达人昵称` VARCHAR(100) COMMENT '达人昵称',
    `抖音号ID` VARCHAR(64) COMMENT '抖音号唯一ID',
    `品牌名称` VARCHAR(100) COMMENT '所属品牌名称',
    `品牌ID` VARCHAR(64) COMMENT '所属品牌ID',
    `机构名称` VARCHAR(100) COMMENT '所属MCN机构名称',
    `机构ID` VARCHAR(64) COMMENT '所属MCN机构ID',
    `粉丝数` BIGINT COMMENT '达人最新粉丝数',
    `直播今日排名` INT COMMENT '直播带货榜最新排名',
    `视频今日排名` INT COMMENT '短视频带货榜最新排名',
    `直播成交额下限` DECIMAL(20,2) DEFAULT 0 COMMENT '直播带货榜最新成交额下限',
    `直播成交额上限` DECIMAL(20,2) DEFAULT 0 COMMENT '直播带货榜最新成交额上限',
    `直播成交数下限` BIGINT DEFAULT 0 COMMENT '直播带货榜最新成交数下限',
    `直播成交数上限` BIGINT DEFAULT 0 COMMENT '直播带货榜最新成交数上限',
    `直播累计成交额下限` DECIMAL(20,2) DEFAULT 0 COMMENT '直播带货榜累计成交额下限',
    `直播累计成交额上限` DECIMAL(20,2) DEFAULT 0 COMMENT '直播带货榜累计成交额上限',
    `直播累计成交数下限` BIGINT DEFAULT 0 COMMENT '直播带货榜累计成交数下限',
    `直播累计成交数上限` BIGINT DEFAULT 0 COMMENT '直播带货榜累计成交数上限',
    `视频成交额下限` DECIMAL(20,2) DEFAULT 0 COMMENT '短视频带货榜最新成交额下限',
    `视频成交额上限` DECIMAL(20,2) DEFAULT 0 COMMENT '短视频带货榜最新成交额上限',
    `视频成交数下限` BIGINT DEFAULT 0 COMMENT '短视频带货榜最新成交数下限',
    `视频成交数上限` BIGINT DEFAULT 0 COMMENT '短视频带货榜最新成交数上限',
    `视频累计成交额下限` DECIMAL(20,2) DEFAULT 0 COMMENT '短视频带货榜累计成交额下限',
    `视频累计成交额上限` DECIMAL(20,2) DEFAULT 0 COMMENT '短视频带货榜累计成交额上限',
    `视频累计成交数下限` BIGINT DEFAULT 0 COMMENT '短视频带货榜累计成交数下限',
    `视频累计成交数上限` BIGINT DEFAULT 0 COMMENT '短视频带货榜累计成交数上限',
    `update_time` DATETIME COMMENT '数据更新时间',
    -- 主键约束：达人ID唯一，保证数据唯一性
    PRIMARY KEY (`达人ID`),
    -- 索引优化：提升常用查询效率
    INDEX idx_dim_daren_date (`日期`),
    INDEX idx_dim_daren_category (`行业类目`),
    INDEX idx_dim_daren_rank (`直播今日排名`, `视频今日排名`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='达人榜单维度表（整合直播/短视频榜单的最新及累计指标）';

TRUNCATE TABLE dim.`dim_达人榜单`

INSERT INTO dim.`dim_达人榜单`
WITH base_stats AS ( 
    SELECT 
        `达人ID`,
        `榜单`,
        `账号类型`,
        `日期`,
        当前排名 AS `排名`,
        `达人昵称`, `抖音号ID`, `头像`, `行业类目`, `粉丝数`,
        `品牌名称`, `品牌ID`, `机构名称`, `机构ID`,
        `成交额下限`, `成交额上限`, `成交数下限`, `成交数上限`,
        -- 1. rn 用于锁定达人最新的基础信息（属性）
        ROW_NUMBER() OVER(PARTITION BY `达人ID` ORDER BY `日期` DESC, 成交额下限 DESC) as rn,
        -- 2. 分榜单计算最新一天的排名和成交指标（解决今日指标被rn=1误杀问题）
        ROW_NUMBER() OVER(PARTITION BY `达人ID`, `榜单` ORDER BY `日期` DESC) as rn_per_list,
        -- 3. 计算累计值
        SUM(成交额下限) OVER(PARTITION BY `达人ID`, `榜单`) AS 累计额下限,
        SUM(成交额上限) OVER(PARTITION BY `达人ID`, `榜单`) AS 累计额上限,
        SUM(成交数下限) OVER(PARTITION BY `达人ID`, `榜单`) AS 累计数下限,
        SUM(成交数上限) OVER(PARTITION BY `达人ID`, `榜单`) AS 累计数上限
    FROM ods.ods_达人榜单_day
    WHERE `日期` >= '2026-01-01'
)
SELECT 
    -- 属性部分：严格取 rn=1 (全表最新)
    MAX(CASE WHEN rn = 1 THEN CAST(DATE_FORMAT(`日期`, '%Y-%m-%d 00:00:00') AS DATETIME) END) AS `日期`,
    CASE MAX(CASE WHEN rn = 1 THEN `行业类目` END) 
        WHEN '3C数码家电-电子教育' THEN '电子教育'
        WHEN '图书教育-书籍/杂志/报纸' THEN '图书教育'
        WHEN '图书教育-学习用品/办公用品' THEN '学习用品'
        WHEN '母婴宠物-儿童床品/家纺' THEN '儿童家纺'
        WHEN '母婴宠物-奶粉/辅食/营养品/零食' THEN '母婴奶辅'
        WHEN '母婴宠物-婴童用品' THEN '婴童用品'
        WHEN '母婴宠物-童装/婴儿装/亲子装' THEN '母婴童装'
        WHEN '母婴宠物-童鞋/婴儿鞋/亲子鞋' THEN '母婴童鞋'
        WHEN '玩具乐器-玩具/童车/益智/积木/模型' THEN '玩具乐器'
        WHEN '母婴宠物-婴童尿裤' THEN '婴童尿裤'
        ELSE '未知类目'
    END AS `行业类目`,
    MAX(CASE WHEN rn = 1 THEN `榜单` END) AS `榜单`, 
    MAX(CASE WHEN rn = 1 THEN `账号类型` END) AS `账号类型`,
    MAX(CASE WHEN rn = 1 THEN `头像` END) AS `头像URL`,
    `达人ID`,
    MAX(CASE WHEN rn = 1 THEN `达人昵称` END) AS `达人昵称`,
    MAX(CASE WHEN rn = 1 THEN `抖音号ID` END) AS `抖音号ID`,
    MAX(CASE WHEN rn = 1 THEN `品牌名称` END) AS `品牌名称`,
    MAX(CASE WHEN rn = 1 THEN `品牌ID` END) AS `品牌ID`,
    MAX(CASE WHEN rn = 1 THEN `机构名称` END) AS `机构名称`,
    MAX(CASE WHEN rn = 1 THEN `机构ID` END) AS `机构ID`,
    MAX(CASE WHEN rn = 1 THEN `粉丝数` END) AS `粉丝数`,
    -- 指标部分：改为针对每个榜单取各自最新的记录 (rn_per_list=1)
    -- ================== 【排名】 ==================
    MAX(CASE WHEN `榜单` = '直播带货榜' AND rn_per_list = 1 THEN `排名` END) AS `直播今日排名`,
    MAX(CASE WHEN `榜单` = '短视频带货榜' AND rn_per_list = 1 THEN `排名` END) AS `视频今日排名`,
    -- ================== 【直播榜指标】 ==================
    MAX(CASE WHEN `榜单` = '直播带货榜' AND rn_per_list = 1 THEN `成交额下限` ELSE 0 END) AS `直播成交额下限`,
    MAX(CASE WHEN `榜单` = '直播带货榜' AND rn_per_list = 1 THEN `成交额上限` ELSE 0 END) AS `直播成交额上限`,
    MAX(CASE WHEN `榜单` = '直播带货榜' AND rn_per_list = 1 THEN `成交数下限` ELSE 0 END) AS `直播成交数下限`,
    MAX(CASE WHEN `榜单` = '直播带货榜' AND rn_per_list = 1 THEN `成交数上限` ELSE 0 END) AS `直播成交数上限`,
    -- 累计（取该榜单历史最大值）
    MAX(CASE WHEN `榜单` = '直播带货榜' THEN `累计额下限` ELSE 0 END) AS `直播累计成交额下限`,
    MAX(CASE WHEN `榜单` = '直播带货榜' THEN `累计额上限` ELSE 0 END) AS `直播累计成交额上限`,
    MAX(CASE WHEN `榜单` = '直播带货榜' THEN `累计数下限` ELSE 0 END) AS `直播累计成交数下限`,
    MAX(CASE WHEN `榜单` = '直播带货榜' THEN `累计数上限` ELSE 0 END) AS `直播累计成交数上限`,
    -- ================== 【视频榜指标】 ==================
    MAX(CASE WHEN `榜单` = '短视频带货榜' AND rn_per_list = 1 THEN `成交额下限` ELSE 0 END) AS `视频成交额下限`,
    MAX(CASE WHEN `榜单` = '短视频带货榜' AND rn_per_list = 1 THEN `成交额上限` ELSE 0 END) AS `视频成交额上限`,
    MAX(CASE WHEN `榜单` = '短视频带货榜' AND rn_per_list = 1 THEN `成交数下限` ELSE 0 END) AS `视频成交数下限`,
    MAX(CASE WHEN `榜单` = '短视频带货榜' AND rn_per_list = 1 THEN `成交数上限` ELSE 0 END) AS `视频成交数上限`,
    -- 累计
    MAX(CASE WHEN `榜单` = '短视频带货榜' THEN `累计额下限` ELSE 0 END) AS `视频累计成交额下限`,
    MAX(CASE WHEN `榜单` = '短视频带货榜' THEN `累计额上限` ELSE 0 END) AS `视频累计成交额上限`,
    MAX(CASE WHEN `榜单` = '短视频带货榜' THEN `累计数下限` ELSE 0 END) AS `视频累计成交数下限`,
    MAX(CASE WHEN `榜单` = '短视频带货榜' THEN `累计数上限` ELSE 0 END) AS `视频累计成交数上限`,

    NOW() AS update_time
FROM base_stats
GROUP BY `达人ID`
-- 保持过滤逻辑
HAVING MAX(CASE WHEN rn = 1 THEN `抖音号ID` END) IS NOT NULL 
   AND MAX(CASE WHEN rn = 1 THEN `抖音号ID` END) != ''
ORDER BY `日期` DESC, `直播今日排名` ASC, `视频今日排名` ASC;
