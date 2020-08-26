select dc_id,
       goodsid,
       f.1.2 as package_qty,
       f.1.1 as start_day,
       f.2.1 - 1 as  end_day
  from
( select dc_id,
        goodsid,
         arrayJoin(arrayMap(i -> (arr_result[i], arr_result[i + 1]), arrayEnumerate(arr_result))) f
   from
       (
           select dc_id,
              goodsid,
              arraySort(j-> j.1, groupArray((stat_day, mx_package)))                                  as arr_tmp,
              arrayEnumerate(arr_tmp)                                                                 as len_arr,
              arrayFilter(x -> arrayElement(arr_tmp, x).2 <> arrayElement(arr_tmp, x - 1).2, len_arr) as filter_index,
              arrayPushBack(arrayMap(y-> arrayElement(arr_tmp, y), filter_index), (toDate('2038-01-02'), 0))   as arr_result
       from (select frdsgvd.dc_id,
                    frdsgvd.goodsid,
                    frdsgvd.stat_day,
                    max(frdsgvd.packing_qty) mx_package
             from ods_hr.bm_article_packing_t frdsgvd
             group by frdsgvd.dc_id,
                      frdsgvd.goodsid,
                      frdsgvd.stat_day
                )
       group by dc_id, goodsid
           )
       )
where start_day<> toDate('2038-01-02')
  and end_day is not null

;
