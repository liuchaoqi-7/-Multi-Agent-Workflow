truncate table dwd.dwd_抖音_千川_素材H;

INSERT INTO dwd.dwd_抖音_千川_素材H
SELECT
    t1.来源,
    t1.千川UID,
    t1.抖音UID,
    t1.全域素材视频类型,
    t1.全域素材视频名称,
    t1.素材ID,
    t1.日期,
    t1.创建时间,
    t1.投放时间,
    t1.时间,
    t1.消耗,
    t1.展现,
    t1.点击,
    t1.订单数,
    t1.成交金额,
    t1.用户实际支付金额,
    t1.净成交订单数,
    t1.净成交金额,
    t1.用户实际支付净成交金额,
    t1.追投消耗,
    t1.追投展现,
    t1.追投点击,
    t1.追投订单数,
    t1.追投成交金额,
    t1.追投用户实际支付金额,
    t1.视频播放数,
    t1.2秒播放数,
    t1.3秒播放数,
    t1.5秒播放数,
    t1.10秒播放数,
    t1.视频完播数,
    t1.粉丝数,
    t1.评论数,
    t1.点赞数,
    t2.总消耗/(COUNT(千川UID) OVER(PARTITION BY `千川UID`, 日期)) AS 【千财】总消耗,
    t2.非赠款消耗/(COUNT(千川UID) OVER(PARTITION BY `千川UID`, 日期)) AS 【千财】非赠消耗,
    t2.赠款消耗/(COUNT(千川UID) OVER(PARTITION BY `千川UID`, 日期)) AS 【千财】赠款消耗,
    t2.共享余额消耗/(COUNT(千川UID) OVER(PARTITION BY `千川UID`, 日期)) AS 【千财】共享赠款消耗,
    t2.消返红包消耗/(COUNT(千川UID) OVER(PARTITION BY `千川UID`, 日期)) AS 【千财】消返红包,
    t2.优惠券消耗/(COUNT(千川UID) OVER(PARTITION BY `千川UID`, 日期)) AS 【千财】立减红包
FROM dwd.dwd_千川素材h_中间表 t1
LEFT JOIN ods.ods_千川_财务 t2
ON t1.`千川UID` = CONVERT(t2.`账户ID` USING utf8mb4) 
AND t1.日期 = t2.统计日期
-- where 消耗>0 or 展现>0 or 追投展现>0 or 追投消耗>0 or 视频播放数>0 订单数>0 or 净成交订单数>0

