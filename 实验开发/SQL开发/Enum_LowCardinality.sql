SET allow_suspicious_low_cardinality_types = 1;

drop table if exists dt_test.fct_rt_dc_shop_sku_vender_day;

create table if not exists  dt_test.fct_rt_dc_shop_sku_vender_day
(
	stat_year UInt16,
	stat_month UInt32,
	stat_day Date,
	sheetid String,
	out_buid LowCardinality(UInt8),
	out_tsbuid LowCardinality(UInt8),
	out_shop_id LowCardinality(String),
	out_shop_dctype Enum8('干货'= 1, '生鲜'=2),
	in_shop_id LowCardinality(String),
	in_shopformid LowCardinality(UInt8),
	in_shop_ytid Enum8('大卖场'=1, '小业态'=2, '配送中心'=5 ),
	datasource Enum8('crv'=1, 'sg'=2 , 'ole'= 3, 'jv'=4, 'xg' = 5),
	venderid LowCardinality(String),
	key_venderid UInt64,
	goodsid LowCardinality(String),
	key_goodsid UInt64,
	low_categoryid String,
	logistics_id Enum8('直通' = 1,'存储' = 2),
	bun_type_id Enum8('正常配送' = 1, '调拨' = 2, '批发' = 3, '提货' = 4),
	box_flag Enum8('正常' = 0, '香烟' = 1, '物料' = 2),
	package_num Decimal(18,4),
	rt_qty Decimal(18,4),
	rt_boxes Decimal(18,4),
	rt_cost Decimal(18,4),
	rt_taxcost Decimal(18,4),
	rt_shops LowCardinality(Nullable(String)),

	rt_gh_qty Decimal(18,4) default multiIf(out_shop_dctype = '干货', rt_qty, 0),
	rt_gh_boxes Decimal(18,4) default multiIf(out_shop_dctype = '干货', rt_boxes, 0),
	rt_gh_cost Decimal(18,4) default multiIf(out_shop_dctype = '干货', rt_cost, 0),
	rt_gh_taxcost Decimal(18,4) default multiIf(out_shop_dctype = '干货', rt_taxcost, 0),
	rt_gh_shops LowCardinality(Nullable(String)) default CAST(multiIf(out_shop_dctype = '干货', rt_shops, NULL), 'LowCardinality(Nullable(String))'),

	rt_sx_qty Decimal(18,4) default multiIf(out_shop_dctype  IN ('生鲜'), rt_qty, 0),
	rt_sx_cost Decimal(18,4) default multiIf(out_shop_dctype  IN ('生鲜'), rt_cost, 0),
	rt_sx_taxcost Decimal(18,4) default multiIf(out_shop_dctype  IN ('生鲜'), rt_taxcost, 0),
	rt_sx_shops LowCardinality(Nullable(String)) default CAST(multiIf(out_shop_dctype  IN ('生鲜'), rt_shops, NULL), 'LowCardinality(Nullable(String))'),

	rt_dmc_qty Decimal(18,4) default multiIf(in_shop_ytid  IN ('大卖场'), rt_qty, 0),
	rt_dmc_boxes Decimal(18,4) default multiIf(in_shop_ytid  IN ('大卖场'), rt_boxes, 0),
	rt_dmc_cost Decimal(18,4) default multiIf(in_shop_ytid  IN ('大卖场'), rt_cost, 0),
	rt_dmc_taxcost Decimal(18,4) default multiIf(in_shop_ytid  IN ('大卖场'), rt_taxcost, 0),
	rt_dmc_shops LowCardinality(Nullable(String)) default CAST(multiIf(in_shop_ytid  IN ('大卖场'), rt_shops, NULL), 'LowCardinality(Nullable(String))'),

	rt_xyt_qty Decimal(18,4) default multiIf(in_shop_ytid IN ('小业态'), rt_qty, 0),
	rt_xyt_boxes Decimal(18,4) default multiIf(in_shop_ytid IN ('小业态'), rt_boxes, 0),
	rt_xyt_cost Decimal(18,4) default multiIf(in_shop_ytid IN ('小业态'), rt_cost, 0),
	rt_xyt_taxcost Decimal(18,4) default multiIf(in_shop_ytid IN ('小业态'), rt_taxcost, 0),
	rt_xyt_shops LowCardinality(Nullable(String)) default CAST(multiIf(in_shop_ytid IN ('小业态'), rt_shops, NULL), 'LowCardinality(Nullable(String))'),

	rt_cj_qty Decimal(18,4) default multiIf(in_shop_ytid IN ('配送中心'), rt_qty, 0),
	rt_cj_boxes Decimal(18,4) default multiIf(in_shop_ytid IN ('配送中心'), rt_boxes, 0),
	rt_cj_cost Decimal(18,4) default multiIf(in_shop_ytid IN ('配送中心'), rt_cost, 0),
	rt_cj_taxcost Decimal(18,4) default multiIf(in_shop_ytid IN ('配送中心'), rt_taxcost, 0),
	rt_cj_shops LowCardinality(Nullable(String)) default CAST(multiIf(in_shop_ytid IN ('配送中心'), rt_shops, NULL), 'LowCardinality(Nullable(String))'),

	rt_gh_dmc_qty Decimal(18,4) default multiIf((in_shop_ytid  IN ('大卖场')) AND (out_shop_dctype = '干货'), rt_qty, 0),
	rt_gh_dmc_boxes Decimal(18,4) default multiIf((in_shop_ytid  IN ('大卖场')) AND (out_shop_dctype = '干货'), rt_boxes, 0),
	rt_gh_dmc_cost Decimal(18,4) default multiIf((in_shop_ytid  IN ('大卖场')) AND (out_shop_dctype = '干货'), rt_cost, 0),
	rt_gh_dmc_taxcost Decimal(18,4) default multiIf((in_shop_ytid  IN ('大卖场')) AND (out_shop_dctype = '干货'), rt_taxcost, 0),
	rt_gh_dmc_shops LowCardinality(Nullable(String)) default CAST(multiIf((in_shop_ytid  IN ('大卖场')) AND (out_shop_dctype = '干货'), rt_shops, NULL), 'LowCardinality(Nullable(String))'),

	rt_gh_xyt_qty Decimal(18,4) default multiIf((in_shop_ytid IN ('小业态')) AND (out_shop_dctype = '干货'), rt_qty, 0),
	rt_gh_xyt_boxes Decimal(18,4) default multiIf((in_shop_ytid IN ('小业态')) AND (out_shop_dctype = '干货'), rt_boxes, 0),
	rt_gh_xyt_cost Decimal(18,4) default multiIf((in_shop_ytid IN ('小业态')) AND (out_shop_dctype = '干货'), rt_cost, 0),
	rt_gh_xyt_taxcost Decimal(18,4) default multiIf((in_shop_ytid IN ('小业态')) AND (out_shop_dctype = '干货'), rt_taxcost, 0),
	rt_gh_xyt_shops LowCardinality(Nullable(String)) default CAST(multiIf((in_shop_ytid IN ('小业态')) AND (out_shop_dctype = '干货'), rt_shops, NULL), 'LowCardinality(Nullable(String))'),

	rt_gh_cj_qty Decimal(18,4) default multiIf((in_shop_ytid IN ('配送中心')) AND (out_shop_dctype = '干货'), rt_qty, 0),
	rt_gh_cj_boxes Decimal(18,4) default multiIf((in_shop_ytid IN ('配送中心')) AND (out_shop_dctype = '干货'), rt_boxes, 0),
	rt_gh_cj_cost Decimal(18,4) default multiIf((in_shop_ytid IN ('配送中心')) AND (out_shop_dctype = '干货'), rt_cost, 0),
	rt_gh_cj_taxcost Decimal(18,4) default multiIf((in_shop_ytid IN ('配送中心')) AND (out_shop_dctype = '干货'), rt_taxcost, 0),
	rt_gh_cj_shops LowCardinality(Nullable(String)) default CAST(multiIf((in_shop_ytid IN ('配送中心')) AND (out_shop_dctype = '干货'), rt_shops, NULL), 'LowCardinality(Nullable(String))'),

	rt_sx_dmc_qty Decimal(18,4) default multiIf((in_shop_ytid  IN ('大卖场')) AND (out_shop_dctype  IN ('生鲜')), rt_qty, 0),
	rt_sx_dmc_cost Decimal(18,4) default multiIf((in_shop_ytid  IN ('大卖场')) AND (out_shop_dctype  IN ('生鲜')), rt_cost, 0),
	rt_sx_dmc_taxcost Decimal(18,4) default multiIf((in_shop_ytid  IN ('大卖场')) AND (out_shop_dctype  IN ('生鲜')), rt_taxcost, 0),
	rt_sx_dmc_shops LowCardinality(Nullable(String)) default CAST(multiIf((in_shop_ytid  IN ('大卖场')) AND (out_shop_dctype  IN ('生鲜')), rt_shops, NULL), 'LowCardinality(Nullable(String))'),

	rt_sx_xyt_qty Decimal(18,4) default multiIf((in_shop_ytid IN ('小业态')) AND (out_shop_dctype  IN ('生鲜')), rt_qty, 0),
	rt_sx_xyt_cost Decimal(18,4) default multiIf((in_shop_ytid IN ('小业态')) AND (out_shop_dctype  IN ('生鲜')), rt_cost, 0),
	rt_sx_xyt_taxcost Decimal(18,4) default multiIf((in_shop_ytid IN ('小业态')) AND (out_shop_dctype  IN ('生鲜')), rt_taxcost, 0),
	rt_sx_xyt_shops LowCardinality(Nullable(String)) default CAST(multiIf((in_shop_ytid IN ('小业态')) AND (out_shop_dctype  IN ('生鲜')), rt_shops, NULL), 'LowCardinality(Nullable(String))'),

	rt_sx_cj_qty Decimal(18,4) default multiIf((in_shop_ytid IN ('配送中心')) AND (out_shop_dctype  IN ('生鲜')), rt_qty, 0),
	rt_sx_cj_cost Decimal(18,4) default multiIf((in_shop_ytid IN ('配送中心')) AND (out_shop_dctype  IN ('生鲜')), rt_cost, 0),
	rt_sx_cj_taxcost Decimal(18,4) default multiIf((in_shop_ytid IN ('配送中心')) AND (out_shop_dctype  IN ('生鲜')), rt_taxcost, 0),
	rt_sx_cj_shops LowCardinality(Nullable(String)) default CAST(multiIf((in_shop_ytid IN ('配送中心')) AND (out_shop_dctype  IN ('生鲜')), rt_shops, NULL), 'LowCardinality(Nullable(String))'),

	rt_zt_qty Decimal(18,4) default multiIf(logistics_id IN ('直通'), rt_qty, 0),
	rt_zt_boxes Decimal(18,4) default multiIf(logistics_id IN ('直通'), rt_boxes, 0),
	rt_zt_cost Decimal(18,4) default multiIf(logistics_id IN ('直通'), rt_cost, 0),
	rt_zt_taxcost Decimal(18,4) default multiIf(logistics_id IN ('直通'), rt_taxcost, 0),
	rt_zt_shops LowCardinality(Nullable(String)) default CAST(multiIf(logistics_id IN ('直通'), rt_shops, NULL), 'LowCardinality(Nullable(String))'),

	rt_cc_qty Decimal(18,4) default multiIf(logistics_id  IN ('存储'), rt_qty, 0),
	rt_cc_boxes Decimal(18,4) default multiIf(logistics_id  IN ('存储'), rt_boxes, 0),
	rt_cc_cost Decimal(18,4) default multiIf(logistics_id  IN ('存储'), rt_cost, 0),
	rt_cc_taxcost Decimal(18,4) default multiIf(logistics_id  IN ('存储'), rt_taxcost, 0),
	rt_cc_shops LowCardinality(Nullable(String)) default CAST(multiIf(logistics_id  IN ('存储'), rt_shops, NULL), 'LowCardinality(Nullable(String))')
)
engine = MergeTree PARTITION BY toYYYYMM(stat_day)
ORDER BY (stat_day, out_shop_id, in_shop_id)
SETTINGS index_granularity = 8192;


--set optimize_trivial_insert_select = 1;
SET min_insert_block_size_rows = 3000000;
--SET min_insert_block_size_bytes = 0;
set max_insert_threads = 4;

insert into dt_test.fct_rt_dc_shop_sku_vender_day(
    stat_year,
    stat_month,
    stat_day,
    sheetid,
    out_buid,
    out_tsbuid,
    out_shop_id,
    out_shop_dctype,
    in_shop_id,
    in_shopformid,
    in_shop_ytid,
    datasource,
    venderid,
    key_venderid,
    goodsid,
    key_goodsid,
    low_categoryid,
    logistics_id,
    bun_type_id,
    box_flag,
    package_num,
    rt_qty,
    rt_boxes,
    rt_cost,
    rt_taxcost,
    rt_shops
    )
select stat_year,
    stat_month,
    stat_day,
    '' as sheetid,
    out_buid,
    out_tsbuid,
    out_shop_id,
   out_shop_dctype,
    in_shop_id,
    in_shopformid,
    case when in_shop_ytid = 2 then '小业态'
         when  in_shop_ytid = 5 then '配送中心'
          else '大卖场' end ,
    datasource,
    venderid,
    key_venderid,
    goodsid,
    key_goodsid,
    low_categoryid,
    case when logistics_id in (1, 3) then '存储'
        else '直通' end ,
    case when bun_type_id = 1 then '正常配送'
          when bun_type_id = 2 then '调拨'
          when bun_type_id = 3 then '批发'
          when bun_type_id = 4 then '提货'
          else '正常配送'
          end ,
    case  when box_flag = 0 then '正常'
          when box_flag = 1 then '香烟'
          when  box_flag = 2 then '物料'
          else '正常'
        end ,
    package_num,
    rt_qty,
    rt_boxes,
    rt_cost,
    rt_taxcost,
    rt_shops
from dw_hr.fct_rt_dc_shop_sku_vender_day_t frdssvdt
where frdssvdt.stat_day >= toDate('2018-11-01')
  and frdssvdt.stat_day <  addMonths(toDate('2018-11-01'), 1);
;
