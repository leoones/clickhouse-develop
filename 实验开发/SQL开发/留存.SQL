Create table t_liucun(
useid UInt8 comment '用户id',
loginday Date comment '登录日期',
event String comment '事件'
) engine = MergeTree
partition by toYYYYMM(loginday)
order by useid;



insert into t_liucun(useid, loginday, event)
values(1, '2019-02-10', 'register'),
      (1, '2019-02-12', 'visit'),
      (1, '2019-02-13', 'order'),
      (1, '2019-02-15', 'pay'),
       
      (2, '2019-02-11', 'register'),
      (2, '2019-02-12', 'visit'),
      
      (3, '2019-02-10', 'register'),
      (3, '2019-02-12', 'visit'),
      (3, '2019-02-13', 'order');    



insert into t_liucun(useid, loginday, event)
values
       
      (22, '2019-02-11', 'register'),
      (22, '2019-02-12', 'visit'); 
      
      
      
-------------------------------------------------------------------方案1-------------------------------------------------------------------
-- 1 , 2, 3, 6 , 15, 30留存
select sum(retentions[1]) as registers,
       sum(retentions[2]) as first,
       sum(retentions[3]) as second,
       sum(retentions[4]) as third,
       sum(retentions[5]) as seven,
       sum(retentions[6]) as fifth,
       sum(retentions[7]) as thirty
from
(select tlc.useid,
       retention(loginday='2019-02-10' and event='register',
                 loginday=addDays(toDate('2019-02-10'), 1),
                 loginday=addDays(toDate('2019-02-10'), 2),
                 loginday=addDays(toDate('2019-02-10'), 3),
                 loginday=addDays(toDate('2019-02-10'), 7),
                 loginday=addDays(toDate('2019-02-10'), 15),
                  loginday=addDays(toDate('2019-02-10'), 30)
                 ) AS retentions
  from t_liucun tlc
where tlc.loginday >= '2019-02-10'
group by  tlc.useid
    );
    
    
    
-------------------------------------------------------------------方案2-------------------------------------------------------------------    
with (
    select bitmapBuild(arrayDistinct(groupArray(toUInt64(fssgvd.goodsid)))) as s1
    from dw_hr.fct_sale_shop_good_vender_day fssgvd
    where fssgvd.hsday = toDate('2020-05-04')
) as current_day,
    (
        select bitmapBuild(arrayDistinct(groupArray(toUInt64(fssgvd.goodsid)))) as s2
        from dw_hr.fct_sale_shop_good_vender_day fssgvd
        where fssgvd.hsday =  subtractDays( toDate('2020-05-04'), 1)
    ) as day1,
   (
       select bitmapBuild(arrayDistinct(groupArray(toUInt64(fssgvd.goodsid)))) as s2
        from dw_hr.fct_sale_shop_good_vender_day fssgvd
        where fssgvd.hsday =  subtractDays( toDate('2020-05-04'), 2)
    ) as day2,
   (
       select bitmapBuild(arrayDistinct(groupArray(toUInt64(fssgvd.goodsid)))) as s2
        from dw_hr.fct_sale_shop_good_vender_day fssgvd
        where fssgvd.hsday =  subtractDays( toDate('2020-05-04'), 3)
    ) as day3,
   (
     select bitmapBuild(arrayDistinct(groupArray(toUInt64(fssgvd.goodsid)))) as s2
        from dw_hr.fct_sale_shop_good_vender_day fssgvd
        where fssgvd.hsday =  subtractDays( toDate('2020-05-04'), 6)
    ) as day6,
  (
       select bitmapBuild(arrayDistinct(groupArray(toUInt64(fssgvd.goodsid)))) as s2
        from dw_hr.fct_sale_shop_good_vender_day fssgvd
        where fssgvd.hsday =  subtractDays( toDate('2020-05-04'), 15)
    ) as day15,
  (
     select bitmapBuild(arrayDistinct(groupArray(toUInt64(fssgvd.goodsid)))) as s2
        from dw_hr.fct_sale_shop_good_vender_day fssgvd
        where fssgvd.hsday =  subtractDays( toDate('2020-05-04'), 30)
    ) as day30
select bitmapCardinality(current_day)             as `当日`,
       bitmapAndCardinality(current_day, day1)    as `1日留存`,
       bitmapAndnotCardinality(current_day, day1) as `1日流失`,
       `1日留存` / `当日` as `1日留存率`,

       bitmapAndCardinality(current_day, day2)    as `2日留存`,
       bitmapAndnotCardinality(current_day, day2) as `2日流失`,
       `2日留存` / `当日` as `2日留存率`,

       bitmapAndCardinality(current_day, day3)    as `3日留存`,
       bitmapAndnotCardinality(current_day, day3) as `3日流失`,
       `3日留存` / `当日` as `3日留存率`,

       bitmapAndCardinality(current_day, day6)    as `6日留存`,
       bitmapAndnotCardinality(current_day, day6) as `6日流失`,
       `6日留存` / `当日` as `6日留存率`,

       bitmapAndCardinality(current_day, day15)    as `15日留存`,
       bitmapAndnotCardinality(current_day, day15) as `15日流失`,
       `15日留存` / `当日` as `15日留存率`,

       bitmapAndCardinality(current_day, day30)    as `30日留存`,
       bitmapAndnotCardinality(current_day, day30) as `30日流失`,
       `30日留存` / `当日` as `30日留存率`
