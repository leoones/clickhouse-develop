背景：
  Stream 实时取数,类似flink, spark, storm 实时计算框架
/**
  5w2h
 why - 为什么要这么做？
 what - 是什么？ 目的是什么
 where - 何处？
 when  -- 何时
 how -- 怎么做？
 how much -- 多少？ 做到什么程度? 数量如何? 质量水平如何？ 费用产出如何？
 */
create table t_event_log(
    user_id  String comment  '用户ID', -- who
    event_time DateTime comment '事件发生时间', -- when
    event_name  String comment  '事件名', --what
    ip  IPv4 comment '发生地址IP', --where
    properties String comment '2h 具体事件对应的信息'
)
engine = MergeTree
partition by toYYYYMM(event_time)
order by (user_id, event_name) ;

--一个client session 开启实时 stream
set allow_experimental_live_view = 1;
drop table lv_latest_time_user;
CREATE LIVE VIEW lv_latest_time_user AS
SELECT
    user_id,
    event_time
FROM t_event_log
WHERE event_time >= today()
order by event_time desc limit 5;

-- 向事件表插入数据
insert into t_event_log(user_id, event_time, event_name, ip, properties) values ('user_23',  now(), 'visit', '168.30.16.104', '{"url": "http://www.baidu.com"}');

 --再开启一个client session
 watch lv_latest_time_user

