---------------------------------------------------------------1 精确去重计数 ---------------------------------------------------------------
  --1.1 uniqExact 支持任何数据类型
        select frdssvd.out_shop_id,
               uniqExact(frdssvd.goodsid)
          from dw_hr.fct_rt_dc_shop_sku_vender_day frdssvd
        where frdssvd.stat_day >= toDate('2019-01-01')
          and frdssvd.stat_day  < toDate('2020-01-01')
        group by frdssvd.out_shop_id;

 --1.2 count(distinct ) 支持任何数据类型
        select frdssvd.out_shop_id,
               count(distinct  frdssvd.goodsid)
          from dw_hr.fct_rt_dc_shop_sku_vender_day frdssvd
        where frdssvd.stat_day >= toDate('2019-01-01')
          and frdssvd.stat_day  < toDate('2020-01-01')
        group by frdssvd.out_shop_id;

 --1.3 groupBitmap 仅支持整形
        select frdssvd.out_shop_id,
               groupBitmap(toUInt64OrZero(frdssvd.goodsid))
          from dw_hr.fct_rt_dc_shop_sku_vender_day frdssvd
        where frdssvd.stat_day >= toDate('2019-01-01')
          and frdssvd.stat_day  < toDate('2020-01-01')
        group by frdssvd.out_shop_id;


---------------------------------------------------------------2 非精确去重计数 ---------------------------------------------------------------
  --2.1 uniq
      select frdssvd.out_shop_id,
               uniq(frdssvd.goodsid)
          from dw_hr.fct_rt_dc_shop_sku_vender_day frdssvd
        where frdssvd.stat_day >= toDate('2019-01-01')
          and frdssvd.stat_day  < toDate('2020-01-01')
        group by frdssvd.out_shop_id;

  --2.2 uniqHLL12
      select frdssvd.out_shop_id,
               uniqHLL12(frdssvd.goodsid)
          from dw_hr.fct_rt_dc_shop_sku_vender_day frdssvd
        where frdssvd.stat_day >= toDate('2019-01-01')
          and frdssvd.stat_day  < toDate('2020-01-01')
        group by frdssvd.out_shop_id;

   --2.3 uniqCombined
      select frdssvd.out_shop_id,
               uniqCombined(frdssvd.goodsid)
          from dw_hr.fct_rt_dc_shop_sku_vender_day frdssvd
        where frdssvd.stat_day >= toDate('2019-01-01')
          and frdssvd.stat_day  < toDate('2020-01-01')
        group by frdssvd.out_shop_id;

   --结论：非精确去重 各个方法存在一定的误差率，其中uniq误差率低一些
