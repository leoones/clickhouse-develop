----------------------------------- 精确计算 -----------------------------------
select fssgvd.source,
       count(distinct fssgvd.goodsid)
  from dw_hr.fct_sale_shop_good_vender_day fssgvd
group by fssgvd.source;

select fssgvd.source,
       groupBitmap(toUInt32(fssgvd.goodsid))
  from dw_hr.fct_sale_shop_good_vender_day fssgvd
group by fssgvd.source;

select fssgvd.source,
       uniqExact(fssgvd.goodsid)
  from dw_hr.fct_sale_shop_good_vender_day fssgvd
group by fssgvd.source;

--------------------------  估算值 --------------------------
select fssgvd.source,
       uniqCombined64(fssgvd.goodsid)
  from dw_hr.fct_sale_shop_good_vender_day fssgvd
group by fssgvd.source;

select fssgvd.source,
       uniqHLL12(fssgvd.goodsid)
  from dw_hr.fct_sale_shop_good_vender_day fssgvd
group by fssgvd.source;


select fssgvd.source,
       uniq(fssgvd.goodsid)
  from dw_hr.fct_sale_shop_good_vender_day fssgvd
group by fssgvd.source;




