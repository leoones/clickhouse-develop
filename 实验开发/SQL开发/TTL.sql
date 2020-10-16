背景: 在数仓系统中, 每天都有大量的数据加载.但是数据价值随着时间逐步衰减. 因此有历史数据删除策略

### 1. 表 TTL 
表可以设置过期记录删除或者转移到其他磁盘/卷。
 1.1 
      set allow_suspicious_low_cardinality_types = 1;

      create table if not exists dw_hr.ttl_20201016_table
          (hsday Date,
        shopid LowCardinality(String),
        placeid String,
        brandid LowCardinality(String),
        familyid String,
        goodsid LowCardinality(String),
        shelfid String,
        venderid LowCardinality(String),
        payflag String,
        paytypeid LowCardinality(String),
        costtaxrate Decimal(18,4),
        saletaxrate Decimal(18,4),
        logistics LowCardinality(UInt8),
        disctype LowCardinality(String),
        pickflag String,
        saleqty Decimal(18,4),
        salevalue Decimal(18,4),
        discvalue Decimal(18,4),
        saletax Decimal(18,4),
        salecostvalue Decimal(18,4),
        costdisc Decimal(18,4),
        costtax Decimal(18,4),
        discticketvalue Decimal(18,4),
        source String,
        buid UInt8,
        good_key UInt64,
        venderid_key UInt64,
        stat_year UInt16,
        stat_month UInt32
      )
      engine = MergeTree PARTITION BY toYYYYMM(hsday)
      ORDER BY (hsday, shopid, goodsid)
      TTL hsday +  INTERVAL  30 DAY
      SETTINGS index_granularity = 8192
      ;
      
  1.2 
      insert into ttl_20201016_table select * from dw_hr.fct_sale_shop_good_vender_day fssgvdt
      where fssgvdt.hsday >= toDate('2020-09-01')；

  1.3
    select hsday,
           count() as logs
      from ttl_20201016_table
    group by hsday;

    optimize table ttl_20201016_table final ;

    select hsday,
           count() as logs
      from ttl_20201016_table
    group by hsday;
    发现 2020-09-17之前的数据 都已经清除了.

  1.4 
      alter table ttl_20201016_table modify TTL hsday +  INTERVAL  7 DAY;
      执行更改过期时间时， 系统会立马删除过期数据. 不受SYSTEM START/STOP TTL MERGES开关影响 
      

### 列 TTL
drop table if exists ttl_20201016_delete;
create table if not exists ttl_20201016_delete (a Int, b Int, x Int, y Int, d DateTime) engine = MergeTree order by (a, b) ttl d + interval 1 second delete where x % 10 == 0 and y > 5;
insert into ttl_20201016_delete values (1, 1, 0, 4, now() + 10);
insert into ttl_20201016_delete values (1, 1, 10, 6, now());
insert into ttl_20201016_delete values (1, 2, 3, 7, now());
insert into ttl_20201016_delete values (1, 3, 0, 5, now());
insert into ttl_20201016_delete values (2, 1, 20, 1, now());
insert into ttl_20201016_delete values (2, 1, 0, 1, now());
insert into ttl_20201016_delete values (3, 1, 0, 8, now());
select * from ttl_20201016_delete;

drop table if exists  ttl_20201016_group;
create table ttl_20201016_group
(a Int, b Int, x Array(Int32), y Double, d DateTime) engine = MergeTree order by (a, b) ttl d + interval 1 second group by a, b set x = minForEach(x), y = sum(y), d = max(d);
insert into ttl_20201016_group values (1, 1, array(0, 2, 3), 4, now() + 10);
insert into ttl_20201016_group values (1, 1, array(5, 4, 3), 6, now());
insert into ttl_20201016_group values (1, 1, array(5, 5, 1), 7, now());
insert into ttl_20201016_group values (1, 3, array(3, 0, 4), 5, now());
insert into ttl_20201016_group values (1, 3, array(1, 1, 2, 1), 9, now());
insert into ttl_20201016_group values (1, 3, array(3, 2, 1, 0), 3, now());
insert into ttl_20201016_group values (2, 1, array(3, 3, 3), 7, now());
insert into ttl_20201016_group values (2, 1, array(11, 1, 0, 3), 1, now());
insert into ttl_20201016_group values (3, 1, array(2, 4, 5), 8, now());
select * from ttl_20201016_group;
optimize table ttl_20201016_group;
select * from ttl_20201016_group;

drop table if exists  ttl_20201016_group;
create table ttl_20201016_group (a Int, b Int, x Int64, y Int, d DateTime) engine = MergeTree order by (a, b) ttl d + interval 1 second group by a set x = argMax(x, d), y = argMax(y, d), d = max(d);
insert into ttl_20201016_group values (1, 1, 0, 4, now() + 10);
insert into ttl_20201016_group values (1, 1, 10, 6, now() + 1);
insert into ttl_20201016_group values (1, 2, 3, 7, now());
insert into ttl_20201016_group values (1, 3, 0, 5, now());
insert into ttl_20201016_group values (2, 1, 20, 1, now());
insert into ttl_20201016_group values (2, 1, 0, 3, now() + 1);
insert into ttl_20201016_group values (3, 1, 0, 3, now());
insert into ttl_20201016_group values (3, 2, 8, 2, now() + 1);
insert into ttl_20201016_group values (3, 5, 5, 8, now());
select * from ttl_20201016_group;
optimize table ttl_20201016_group;
select * from ttl_20201016_group;
