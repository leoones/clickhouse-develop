
  --3.1 测算人群 人数
    select bitmapAndCardinality(user0, user1)
      from
        (select 1 as jion_id,
               groupBitmapMergeState(users) as user0
          from ch_tag_user
          where label_id = 'sex'
              ) tmp1
    inner join
       (select 1 as jion_id,
               groupBitmapMergeState(users) as user1
          from ch_tag_user
          where label_id = 'pay_qty'
              ) tmp2 on tmp1.jion_id = tmp2.jion_id;

  --3.2 特定人群 对应相关标签
    select label_id,label_value, bitmapAndCardinality(main_userlist, detail_userlist) as user_number
      from
          (select  1 as join_id, label_id, label_value, groupBitmapMergeState(users) as main_userlist
              from  ch_tag_user group by label_id, label_value) main
    inner join
        (select jion_id, bitmapAnd( user0, user1) as detail_userlist
          from
            (select 1 as jion_id,
                   groupBitmapMergeState(users) as user0
              from ch_tag_user
              where label_id = 'sex'
                  ) tmp1
        inner join
           (select 1 as jion_id,
                   groupBitmapMergeState(users) as user1
              from ch_tag_user
              where label_id = 'pay_qty'
                  ) tmp2 on tmp1.jion_id = tmp2.jion_id
              ) detail
    on main.join_id = detail.jion_id;

  --3.3 查看特定用户的全标签
    select *
        from
    (select  label_id, label_value,groupBitmapMergeState(users) as main_userlist
       from  ch_tag_user group by label_id, label_value
        ) xx
    where bitmapAndCardinality(bitmapBuild([toUInt64(900)]), main_userlist) >=1;

  --3.4 用户清单ID
 select  arrayJoin(bitmapToArray(bitmapAnd( user0, user1))) as users
          from
            (select 1 as jion_id,
                   groupBitmapMergeState(users) as user0
              from ch_tag_user
              where label_id = 'sex'
                  ) tmp1
        inner join
           (select 1 as jion_id,
                   groupBitmapMergeState(users) as user1
              from ch_tag_user
              where label_id = 'pay_qty'
                  ) tmp2 on tmp1.jion_id = tmp2.jion_id
;


---------------------------------------------------------------- 结论 ----------------------------------------------------------------
1. 标签可以并行导入，加快用户画像就绪工作

2. 可以做到实时增加标签
