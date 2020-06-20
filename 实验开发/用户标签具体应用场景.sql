---活动预热 选取特定用户群
  --.性别为'女'  且 ( 最近60天有登录 或者 渠道 好友推荐过来)
WITH
   (select groupBitmapMergeState(ctud.users)
         from tag_dt.ch_tag_user_int ctud
        where ctud.label_id = 'A2011'
        and ctud.label_value > 1000
        ) AS user0,
    (select groupBitmapMergeState(ctud.users)
         from tag_dt.ch_tag_user_str ctud
        where ctud.label_id = 'A1005'
        and ctud.label_value = 'FRIENDS'
        ) AS user1
select bitmapOrCardinality(user0, user1)
;





 --- 活动期间效果分析
select bitmapAnd()
  from
 (select 1 as join_id, groupBitmapMergeState(ctud.users) as user_0
   from tag_dt.ch_tag_user_date ctud
where ctud.label_id = 'A1009'
  and ctud.label_value >= toDate('2019-06-01')
  and ctud.label_value <  toDate('2019-07-01')
     ) tmp1
inner join
  (select 1 as join_id, groupBitmapMergeState(ctud.users) as user_1
   from tag_dt.ch_tag_user_date ctud
where ctud.label_id = 'A1005'
  and ctud.label_value >= toDate('2019-06-01')
  and ctud.label_value <  toDate('2019-07-01')
     ) tmp2 on tmp1.join_id = tmp2.join_id
