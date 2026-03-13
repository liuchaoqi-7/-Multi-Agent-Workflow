TRUNCATE TABLE dwd.千川_素材_汇总;

insert into dwd.千川_素材_汇总
SELECT
--  千川UID,
    抖音UID,
    时间,
    SUM(消耗) 消耗,
    sum(展现) 展现,
    sum(点击) 点击,
    sum(订单数) 订单数,
    sum(成交金额) 成交金额,
    sum(用户实际支付金额) 用户实际支付金额,
    sum(净成交订单数) 净成交订单数,
    sum(净成交金额) 净成交金额,
    sum(用户实际支付净成交金额) 用户实际支付净成交金额,
    sum(追投消耗) 追投消耗,
    sum(追投展现) 追投展现,
    sum(追投点击) 追投点击,
    sum(追投订单数) 追投订单数,
    sum(追投成交金额) 追投成交金额,
    sum(追投用户实际支付金额) 追投用户实际支付金额,
    sum(视频播放数) 视频播放数,
    cast(SUM(视频播放数*CAST(REPLACE(2秒播放率, '%', '') AS DECIMAL(18, 2)))/100 AS DECIMAL(18, 0)) AS 2秒播放数,
    cast(SUM(视频播放数*CAST(REPLACE(3秒播放率, '%', '') AS DECIMAL(18, 2)))/100 AS DECIMAL(18, 0)) AS 3秒播放数,
    cast(SUM(视频播放数*CAST(REPLACE(5秒播放率, '%', '') AS DECIMAL(18, 2)))/100 AS DECIMAL(18, 0)) AS 5秒播放数,
    cast(SUM(视频播放数*CAST(REPLACE(10秒播放率, '%', '') AS DECIMAL(18, 2)))/100 AS DECIMAL(18, 0)) AS 10秒播放数,
    sum(视频完播数) 视频完播数,
    sum(粉丝数) 粉丝数,
    sum(评论数) 评论数,
    sum(点赞数) 点赞数
FROM dwd.dwd_千川_素材m
GROUP BY 1,2

