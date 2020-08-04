--A1001 注册日期
        alter table tag_dt.ch_user_tag_info drop partition 'A1001';
        insert into tag_dt.ch_user_tag_info(userid,
                                                  tag_name,
                                                  tag_value)
            SELECT useid,
                'A1001',
                   addDays(toDate('2015-01-01') , modulo(rand64(), 1988))
            FROM generateRandom('useid UInt32, register_time DateTime', 1, 10, 2)
            LIMIT 100000000;

        alter table  tag_dt.ch_tag_user_date drop partition 'A1001';
        insert into tag_dt.ch_tag_user_date(label_id, label_value, users)
           select  tag_name,
                   tag_value,
                   groupBitmapState(userid)
             from tag_dt.ch_user_tag_info
            where tag_name = 'A1001'
          group by tag_name, tag_value;

-- 注册渠道
        alter table tag_dt.ch_user_tag_info drop partition 'A1002';
        insert into tag_dt.ch_user_tag_info(userid,
                                                  tag_name,
                                                  tag_value)
            select  userid,
                   'A1002',
                   arrayElement(['QQ','WEIXIN', 'FRIENDS','ERWEIMA'],modulo(rand64(), length(['QQ','WEIXIN', 'FRIENDS','ERWEIMA'])))
              from tag_dt.ch_user_tag_info
             where tag_name = 'A1001';

        alter table tag_dt.ch_tag_user_str drop partition 'A1002';
        insert into tag_dt.ch_tag_user_str(label_id,
                                               label_value,
                                               users)
               select  tag_name,
                        tag_value,
                       groupBitmapState(userid)
                 from tag_dt.ch_user_tag_info
                where tag_name = 'A1002'
              group by tag_name, tag_value;

--A1003 出生地省份
        alter table tag_dt.ch_user_tag_info drop partition 'A1003';
        insert into tag_dt.ch_user_tag_info(userid,
                                                  tag_name,
                                                  tag_value)

            select  userid,
                   'A1003',
                   arrayElement(['河北省','山西省','辽宁省','吉林省','黑龙江省','江苏省','浙江省',
                            '安徽省','福建省','江西省','山东省','河南省','湖北省','湖南省','广东省','海南省','四川省','贵州省','云南省','陕西省',
                            '甘肃省','青海省','台湾省','内蒙古自治区','广西壮族自治区','西藏自治区','宁夏回族自治区','新疆维吾尔自治区',
                            '北京市','天津市','上海市','重庆市'],modulo(rand64(),
                                length(['河北省','山西省','辽宁省','吉林省','黑龙江省','江苏省','浙江省',
                            '安徽省','福建省','江西省','山东省','河南省','湖北省','湖南省','广东省','海南省','四川省','贵州省','云南省','陕西省',
                            '甘肃省','青海省','台湾省','内蒙古自治区','广西壮族自治区','西藏自治区','宁夏回族自治区','新疆维吾尔自治区',
                            '北京市','天津市','上海市','重庆市'])))
              from tag_dt.ch_user_tag_info
             where tag_name = 'A1001'
             order by  rand()  limit 38876492;
        alter table tag_dt.ch_tag_user_str drop partition 'A1003';
        insert into tag_dt.ch_tag_user_str(label_id,
                                               label_value,
                                               users)
               select  tag_name,
                      tag_value,
                       groupBitmapState(userid)
                 from tag_dt.ch_user_tag_info
                where tag_name = 'A1003'
              group by tag_name, tag_value;

--A1004 注册手机号码
    alter table tag_dt.ch_user_tag_info drop partition 'A1004';
    insert into tag_dt.ch_user_tag_info(userid,
                                              tag_name,
                                              tag_value)
        select  userid,
               'A1004',
               arrayElement([133,149,153,173,177,180,181,189,199, 130,131,132,145,155,156,166,171,175,176,185,186,166,
               135,136,137,138,139,147,150,151,152,157,158,159,172,178,182,183,184,187,188,198],modulo(rand64(),
                   length([133,149,153,173,177,180,181,189,199, 130,131,132,145,155,156,166,171,175,176,185,186,166,
               135,136,137,138,139,147,150,151,152,157,158,159,172,178,182,183,184,187,188,198])))
          from tag_dt.ch_user_tag_info
         where tag_name = 'A1001';
    alter table  tag_dt.ch_tag_user_str drop partition 'A1004';
    insert into tag_dt.ch_tag_user_str(label_id,
                                           label_value,
                                           users)
           select  tag_name, tag_value,
                   groupBitmapState(userid)
             from tag_dt.ch_user_tag_info
            where tag_name = 'A1004'
          group by tag_name, tag_value;

--A1005 出生日期
    alter table tag_dt.ch_user_tag_info drop partition 'A1005';
    insert into tag_dt.ch_user_tag_info(userid,
                                              tag_name,
                                              tag_value)
        select  userid,
               'A1002',
               addDays(toDate('1970-01-01'),modulo(rand(),dateDiff('day',toDate('1970-01-01'), toDate('2020-06-11'))))
              from tag_dt.ch_user_tag_info ctum
            where tag_name =  'A1003';
    alter table tag_dt.ch_tag_user_date drop partition 'A1005';
    insert into tag_dt.ch_tag_user_date(label_id,
                                           label_value,
                                           users)
           select  tag_name, tag_value,
                   groupBitmapState(userid)
             from tag_dt.ch_user_tag_info
            where tag_name = 'A1005'
          group by tag_name, tag_value;

--A1006 问卷调查 职业
    alter table tag_dt.ch_user_tag_info drop partition 'A1006';
    insert into tag_dt.ch_user_tag_info(userid,
                                              tag_name,
                                              tag_value)
        select  userid,
               'A1006',
               arrayElement(['作业员','技术员','工程师','设计师','管理员','总务人员','厨师','服务员','营销人员','保安','司机','导游','售票员',
        '调酒师','营业员','促销','保姆','医生','护士','药剂师','营养师','后勤','健身教练','按摩技师',
        '演员','导演','制片','经纪','编剧','场务','音乐人'
        ] ,modulo(rand64(),
            length(['作业员','技术员','工程师','设计师','管理员','总务人员','厨师','服务员','营销人员','保安','司机','导游','售票员',
        '调酒师','营业员','促销','保姆','医生','护士','药剂师','营养师','后勤','健身教练','按摩技师',
        '演员','导演','制片','经纪','编剧','场务','音乐人'
        ] )))
          from tag_dt.ch_user_tag_info
         where tag_name = 'A1001'
        order by  rand() limit 859764;
    alter table  tag_dt.ch_tag_user_str drop partition 'A1006';
    insert into tag_dt.ch_tag_user_str(label_id,
                                             label_value,
                                              users)
           select  tag_name,
                  tag_value,
                   groupBitmapState(userid)
             from tag_dt.ch_user_tag_info
            where tag_name = 'A1006'
          group by tag_name, tag_value;

--A1007 问卷调查 学历
    alter table tag_dt.ch_user_tag_info drop partition 'A1007';
    insert into tag_dt.ch_user_tag_info(userid,
                                              tag_name,
                                              tag_value)

        select  userid,
               'A1007',
               arrayElement(['初中','高中', '中专','大专', '本科', '研究生', '博士生'],modulo(rand64(),
                   length(['初中','高中', '中专','大专', '本科', '研究生', '博士生'])))
          from tag_dt.ch_user_tag_info
         where tag_name = 'A1006'
       order by  rand() limit 789807;
    alter table tag_dt.ch_tag_user_str drop partition 'A1007';
    insert into tag_dt.ch_tag_user_str(label_id,
                                       label_value,
                                       users)
       select  tag_name, tag_value,
               groupBitmapState(userid)
         from tag_dt.ch_user_tag_info
        where tag_name = 'A1007'
      group by tag_name, tag_value;

--A1008 问卷调查 婚姻
    alter table tag_dt.ch_user_tag_info drop partition 'A1008';
    insert into tag_dt.ch_user_tag_info(userid,
                                            tag_name,
                                            tag_value)
        select  userid,
               'A1008',
               arrayElement(['是', '否'] ,modulo(rand64(), length(['是', '否'] )))
          from tag_dt.ch_user_tag_info
         where tag_name = 'A1007'
       order by  rand() limit 449805;
    alter table tag_dt.ch_tag_user_str drop partition 'A1008';
    insert into tag_dt.ch_tag_user_str(label_id,
                                           label_value,
                                           users)
           select  tag_name, tag_value,
                   groupBitmapState(userid)
             from tag_dt.ch_user_tag_info
            where tag_name = 'A1008'
          group by tag_name, tag_value;

--A1009 男女
    alter table tag_dt.ch_user_tag_info drop partition 'A1009';
    insert into tag_dt.ch_user_tag_info(userid,
                                            tag_name,
                                            tag_value)
        select  userid,
               'A1008',
               arrayElement(['男', '女'] ,modulo(rand64(), length(['男', '女'] )))
          from tag_dt.ch_user_tag_info
          where tag_name = 'A1003';
    alter table tag_dt.ch_tag_user_str drop partition 'A1009';
    insert into tag_dt.ch_tag_user_str(label_id,
                                           label_value,
                                           users)
           select  tag_name, tag_value,
                   groupBitmapState(userid)
             from tag_dt.ch_user_tag_info
            where tag_name = 'A1009'
          group by tag_name, tag_value;

--A1010 手机号码对应省份
        alter table tag_dt.ch_user_tag_info drop partition 'A1010';
        insert into tag_dt.ch_user_tag_info(userid,
                                                  tag_name,
                                                  tag_value)

            select  userid,
                   'A1010',
                   arrayElement(['河北省','山西省','辽宁省','吉林省','黑龙江省','江苏省','浙江省',
                            '安徽省','福建省','江西省','山东省','河南省','湖北省','湖南省','广东省','海南省','四川省','贵州省','云南省','陕西省',
                            '甘肃省','青海省','台湾省','内蒙古自治区','广西壮族自治区','西藏自治区','宁夏回族自治区','新疆维吾尔自治区',
                            '北京市','天津市','上海市','重庆市'],modulo(rand64(),
                                length(['河北省','山西省','辽宁省','吉林省','黑龙江省','江苏省','浙江省',
                            '安徽省','福建省','江西省','山东省','河南省','湖北省','湖南省','广东省','海南省','四川省','贵州省','云南省','陕西省',
                            '甘肃省','青海省','台湾省','内蒙古自治区','广西壮族自治区','西藏自治区','宁夏回族自治区','新疆维吾尔自治区',
                            '北京市','天津市','上海市','重庆市'])))
              from tag_dt.ch_user_tag_info
             where tag_name = 'A1004';
        alter table tag_dt.ch_tag_user_str drop partition 'A1010';
        insert into tag_dt.ch_tag_user_str(label_id,
                                               label_value,
                                               users)
               select  tag_name,
                      tag_value,
                       groupBitmapState(userid)
                 from tag_dt.ch_user_tag_info
                where tag_name = 'A1010'
              group by tag_name, tag_value;

---A2001 下单日期
        alter table tag_dt.ch_user_tag_info drop partition 'A2001';
        insert into tag_dt.ch_user_tag_info(userid,
                                                  tag_name,
                                                  tag_value)
            select tmp1.userid,
                   'A2001',
                           addDays(toDate(tmp1.tag_value) , modulo(rand(), 365))
                  from
                      (
                        select userid,
                             tag_value
                          from tag_dt.ch_user_tag_info
                         where tag_name = 'A1001'
                         ) tmp1
                 SEMI LEFT JOIN
                (
                select userid
                  from tag_dt.ch_user_tag_info
                 where tag_name = 'A1003'
                ) tmp2 on tmp1.userid = tmp2.userid
                    order by rand() limit  33738974;
        alter table tag_dt.ch_tag_user_date drop partition 'A2001';
        insert into tag_dt.ch_tag_user_date(label_id,
                                               label_value,
                                               users)
               select  tag_name, tag_value,
                       groupBitmapState(userid)
                 from tag_dt.ch_user_tag_info
                where tag_name = 'A2001'
              group by tag_name, tag_value;

---A2002 首单金额
        alter table tag_dt.ch_user_tag_info drop partition 'A2002';
        insert into tag_dt.ch_user_tag_info(userid,
                                                  tag_name,
                                                  tag_value)
            select ctum.userid,
                   'A2002',
                   modulo(rand(), 1000)
              from tag_dt.ch_user_tag_info ctum
            where ctum.tag_name = 'A2001';
        alter table tag_dt.ch_tag_user_int drop partition 'A2002';
        insert into tag_dt.ch_tag_user_int(label_id,
                                               label_value,
                                               users)
               select  tag_name, tag_value,
                       groupBitmapState(userid)
                 from tag_dt.ch_user_tag_info
                where tag_name = 'A2002'
              group by tag_name, tag_value;

--A2003  下单总笔数
        alter table tag_dt.ch_user_tag_info drop partition 'A2003';
        insert into tag_dt.ch_user_tag_info(userid,
                                                  tag_name,
                                                  tag_value)
            select ctum.userid,
                   'A2003',
                   modulo(rand(), 200)
              from tag_dt.ch_user_tag_info ctum
            where ctum.tag_name = 'A2001';
        alter table tag_dt.ch_tag_user_int drop partition 'A2003';
        insert into tag_dt.ch_tag_user_int(label_id,
                                               label_value,
                                               users)
               select  tag_name, tag_value,
                       groupBitmapState(userid)
                 from tag_dt.ch_user_tag_info
                where tag_name = 'A2003'
              group by tag_name, tag_value;

--A2004  下单总数量
    alter table tag_dt.ch_user_tag_info drop partition 'A2004';
    insert into tag_dt.ch_user_tag_info(userid,
                                              tag_name,
                                              tag_value)
        select ctum.userid,
               'A2004',
               toUInt64(ctum.tag_value) * modulo(rand(), 10)
          from tag_dt.ch_user_tag_info ctum
        where ctum.tag_name = 'A2003';
    alter table tag_dt.ch_tag_user_int drop partition 'A2004';
    insert into tag_dt.ch_tag_user_int(label_id,
                                           label_value,
                                           users)
           select  tag_name, tag_value,
                   groupBitmapState(userid)
             from tag_dt.ch_user_tag_info
            where tag_name = 'A2004'
          group by tag_name, tag_value;

--A2005  下单总金额
    alter table tag_dt.ch_user_tag_info drop partition 'A2005';
    insert into tag_dt.ch_user_tag_info(userid,
                                              tag_name,
                                              tag_value)
        select ctum.userid,
               'A2005',
               toUInt64(ctum.tag_value) * modulo(rand64(), 500)
          from tag_dt.ch_user_tag_info ctum
        where ctum.tag_name = 'A2004';
    alter table tag_dt.ch_tag_user_int drop partition 'A2005';
    insert into tag_dt.ch_tag_user_int(label_id,
                                       label_value,
                                       users)
       select  tag_name, tag_value,
               groupBitmapState(userid)
         from tag_dt.ch_user_tag_info
        where tag_name = 'A2005'
      group by tag_name, tag_value;

--A2006 最近下单时间
    alter table tag_dt.ch_user_tag_info drop partition 'A2006';
    insert into tag_dt.ch_user_tag_info(userid,
                                              tag_name,
                                              tag_value)
        select ctum.userid,
               'A2006',
               toDate(ctum.tag_value) + modulo(rand(), 712)
          from tag_dt.ch_user_tag_info ctum
        where ctum.tag_name = 'A2001';

    alter table tag_dt.ch_tag_user_date drop partition 'A2006';
    insert into tag_dt.ch_tag_user_date(label_id,
                                           label_value,
                                           users)
           select  tag_name, tag_value,
                   groupBitmapState(userid)
             from tag_dt.ch_user_tag_info
            where tag_name = 'A2006'
          group by tag_name, tag_value;


--A2007 最近下单金额
    alter table tag_dt.ch_user_tag_info drop partition 'A2007';
    insert into tag_dt.ch_user_tag_info(userid,
                                              tag_name,
                                              tag_value)
        select ctum.userid,
               'A2007',
               modulo(rand(), 500)
          from tag_dt.ch_user_tag_info ctum
        where ctum.tag_name = 'A2001';
    alter table tag_dt.ch_tag_user_int drop partition 'A2007';
    insert into tag_dt.ch_tag_user_int(label_id,
                                           label_value,
                                           users)
           select  tag_name,
                   tag_value,
                   groupBitmapState(userid)
             from tag_dt.ch_user_tag_info
            where tag_name = 'A2007'
          group by tag_name, tag_value;

--A2008 首单支付时间
    alter table tag_dt.ch_user_tag_info drop partition 'A2008';
    insert into tag_dt.ch_user_tag_info(userid,
                                          tag_name,
                                          tag_value)
        select ctum.userid,
               'A2008',
               addDays(toDate(ctum.tag_value), modulo(rand64(), 10))
          from tag_dt.ch_user_tag_info ctum
        where ctum.tag_name = 'A2001'
        order by rand64() limit 32798278;

    alter table tag_dt.ch_tag_user_date drop partition 'A2008';
    insert into tag_dt.ch_tag_user_date(label_id,
                                           label_value,
                                           users)
           select  tag_name, tag_value,
                   groupBitmapState(userid)
             from tag_dt.ch_user_tag_info
            where tag_name = 'A2008'
          group by tag_name, tag_value;


--A2009 首单支付金额
    alter table tag_dt.ch_user_tag_info drop partition 'A2009';
    insert into tag_dt.ch_user_tag_info(userid,
                                          tag_name,
                                          tag_value)
        select tmp1.userid,
              'A2009',
              tmp2.tag_value
       from
        (select userid
            from tag_dt.ch_user_tag_info
        where tag_name = 'A2008') tmp1
    semi left join     (select userid, tag_value
            from tag_dt.ch_user_tag_info
        where tag_name = 'A2002') tmp2  on tmp1.userid = tmp2.userid;

      alter table tag_dt.ch_tag_user_int drop partition 'A2009';
        insert into tag_dt.ch_tag_user_int(label_id,
                                               label_value,
                                               users)
               select  tag_name, tag_value,
                       groupBitmapState(userid)
                 from tag_dt.ch_user_tag_info
                where tag_name = 'A2009'
              group by tag_name, tag_value;

--A2010 支付总笔数
    alter table tag_dt.ch_user_tag_info drop partition 'A2010';
    insert into tag_dt.ch_user_tag_info(userid,
                                          tag_name,
                                          tag_value)
    select tmp1.userid,
'A2010',
          tmp2.tag_value
       from
        (select userid
            from tag_dt.ch_user_tag_info
        where tag_name = 'A2009') tmp1
    semi left join   (select userid, tag_value
            from tag_dt.ch_user_tag_info
        where tag_name = 'A2003') tmp2  on tmp1.userid = tmp2.userid;

     alter table tag_dt.ch_tag_user_int drop partition 'A2010';
    insert into tag_dt.ch_tag_user_int(label_id,
                                       label_value,
                                       users)
       select  tag_name, tag_value,
               groupBitmapState(userid)
         from tag_dt.ch_user_tag_info
        where tag_name = 'A2010'
      group by tag_name, tag_value;


--A2011 支付总金额
    alter table tag_dt.ch_user_tag_info drop partition 'A2010';
    insert into tag_dt.ch_user_tag_info(userid,
                                          tag_name,
                                          tag_value)
    select tmp1.userid,
'A2011',
          tmp2.tag_value
       from
        (select userid
            from tag_dt.ch_user_tag_info
        where tag_name = 'A2009') tmp1
    semi left join   (select userid, tag_value
            from tag_dt.ch_user_tag_info
        where tag_name = 'A2005') tmp2  on tmp1.userid = tmp2.userid;

     alter table tag_dt.ch_tag_user_int drop partition 'A2011';
    insert into tag_dt.ch_tag_user_int(label_id,
                                       label_value,
                                       users)
       select  tag_name, tag_value,
               groupBitmapState(userid)
         from tag_dt.ch_user_tag_info
        where tag_name = 'A2011'
      group by tag_name, tag_value;

--A2012 支付总数量
    alter table tag_dt.ch_user_tag_info drop partition 'A2012';
    insert into tag_dt.ch_user_tag_info(userid,
                                          tag_name,
                                          tag_value)
    select tmp1.userid,
'A2012',
          tmp2.tag_value
       from
        (select userid
            from tag_dt.ch_user_tag_info
        where tag_name = 'A2009') tmp1
    semi left join   (select userid, tag_value
            from tag_dt.ch_user_tag_info
        where tag_name = 'A2005') tmp2  on tmp1.userid = tmp2.userid;

     alter table tag_dt.ch_tag_user_int drop partition 'A2012';
    insert into tag_dt.ch_tag_user_int(label_id,
                                       label_value,
                                       users)
       select  tag_name, tag_value,
               groupBitmapState(userid)
         from tag_dt.ch_user_tag_info
        where tag_name = 'A2012'
      group by tag_name, tag_value;

--A2013 支付总数量
    alter table tag_dt.ch_user_tag_info drop partition 'A2013';
    insert into tag_dt.ch_user_tag_info(userid,
                                          tag_name,
                                          tag_value)
    select tmp1.userid,
'A2013',
          toDate(tmp2.tag_value) + modulo(rand64(), 5)
       from
        (select userid
            from tag_dt.ch_user_tag_info
        where tag_name = 'A2009') tmp1
    semi left join   (select userid, tag_value
            from tag_dt.ch_user_tag_info
        where tag_name = 'A2006') tmp2  on tmp1.userid = tmp2.userid;

     alter table tag_dt.ch_tag_user_int drop partition 'A2013';
    insert into tag_dt.ch_tag_user_date(label_id,
                                       label_value,
                                       users)
       select  tag_name, tag_value,
               groupBitmapState(userid)
         from tag_dt.ch_user_tag_info
        where tag_name = 'A2013'
      group by tag_name, tag_value;


--A2014 最近支付金额
    alter table tag_dt.ch_user_tag_info drop partition 'A2014';
    insert into tag_dt.ch_user_tag_info(userid,
                                          tag_name,
                                          tag_value)
    select tmp1.userid,
'A2014',
            tmp2.tag_value
       from
        (select userid
            from tag_dt.ch_user_tag_info
        where tag_name = 'A2009') tmp1
    semi left join   (select userid, tag_value
            from tag_dt.ch_user_tag_info
        where tag_name = 'A2007') tmp2  on tmp1.userid = tmp2.userid;

     alter table tag_dt.ch_tag_user_int drop partition 'A2014';
    insert into tag_dt.ch_tag_user_int(label_id,
                                       label_value,
                                       users)
       select  tag_name, tag_value,
               groupBitmapState(userid)
         from tag_dt.ch_user_tag_info
        where tag_name = 'A2014'
      group by tag_name, tag_value;

--A3001 是否关注公众号
    alter table tag_dt.ch_user_tag_info drop partition 'A3001';
    insert into tag_dt.ch_user_tag_info(userid,
                                            tag_name,
                                            tag_value)
        select  userid,
               'A3001',
                 1
          from tag_dt.ch_user_tag_info
         where tag_name = 'A1001'
       order by  rand() limit 949305;

    alter table tag_dt.ch_tag_user_int drop partition 'A3001';
    insert into tag_dt.ch_tag_user_int(label_id,
                                           label_value,
                                           users)
           select  tag_name, tag_value,
                   groupBitmapState(userid)
             from tag_dt.ch_user_tag_info
            where tag_name = 'A3001'
          group by tag_name, tag_value;

--A3002 是否下载APP
    alter table tag_dt.ch_user_tag_info drop partition 'A3002';
    insert into tag_dt.ch_user_tag_info(userid,
                                            tag_name,
                                            tag_value)
        select  userid,
               'A3002',
               1
          from tag_dt.ch_user_tag_info
         where tag_name = 'A1001'
       order by  rand() limit 5949305;

    alter table tag_dt.ch_tag_user_int drop partition 'A3002';
    insert into tag_dt.ch_tag_user_int(label_id,
                                           label_value,
                                           users)
           select  tag_name, tag_value,
                   groupBitmapState(userid)
             from tag_dt.ch_user_tag_info
            where tag_name = 'A3002'
          group by tag_name, tag_value;

--A3003 首次签到日期
    alter table tag_dt.ch_user_tag_info drop partition 'A3003';
    insert into tag_dt.ch_user_tag_info(userid,
                                            tag_name,
                                            tag_value)
        select  userid,
               'A3003',
               toDate(tag_value)+ modulo(rand64(), 365)
          from tag_dt.ch_user_tag_info
         where tag_name = 'A1001'
       order by  rand() limit 6949305;

    alter table tag_dt.ch_tag_user_date drop partition 'A3003';
    insert into tag_dt.ch_tag_user_date(label_id,
                                           label_value,
                                           users)
           select  tag_name, tag_value,
                   groupBitmapState(userid)
             from tag_dt.ch_user_tag_info
            where tag_name = 'A3003'
          group by tag_name, tag_value;

--A3004 最后签到日期
    alter table tag_dt.ch_user_tag_info drop partition 'A3004';
    insert into tag_dt.ch_user_tag_info(userid,
                                            tag_name,
                                            tag_value)
        select  userid,
               'A3004',
               toDate(tag_value)+ modulo(rand64(), 365)
          from tag_dt.ch_user_tag_info
         where tag_name = 'A3003';

    alter table tag_dt.ch_tag_user_int drop partition 'A3004';
    insert into tag_dt.ch_tag_user_int(label_id,
                                           label_value,
                                           users)
           select  tag_name, tag_value,
                   groupBitmapState(userid)
             from tag_dt.ch_user_tag_info
            where tag_name = 'A3004'
          group by tag_name, tag_value;

--A3005 最后登录日期
    alter table tag_dt.ch_user_tag_info drop partition 'A3005';
    insert into tag_dt.ch_user_tag_info(userid,
                                            tag_name,
                                            tag_value)
        select  userid,
               'A3005',
               toDate(tag_value)+ modulo(rand64(), 365)+ modulo(rand64(), 365)
          from tag_dt.ch_user_tag_info
         where tag_name = 'A1001';

    alter table tag_dt.ch_tag_user_date drop partition 'A3005';
    insert into tag_dt.ch_tag_user_date(label_id,
                                           label_value,
                                           users)
           select  tag_name, tag_value,
                   groupBitmapState(userid)
             from tag_dt.ch_user_tag_info
            where tag_name = 'A3005'
          group by tag_name, tag_value;
