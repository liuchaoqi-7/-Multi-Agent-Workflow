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
