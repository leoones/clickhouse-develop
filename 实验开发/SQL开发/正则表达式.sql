with (select upperUTF8('苹果12kg*5') ) as goodsname

select case when match(goodsname, '[1-9]*\\*[1-9]*[KG|千克|公斤|克|升|毫升|ML|CL|升|L]{1,2}')
              then splitByChar('*',extract(goodsname, '[1-9]*\\*[1-9]*[KG|千克|公斤|克|公斤|升|毫升|ML|CL|升|L]{1,2}'))

             when match(goodsname, '[1-9]*[KG|千克|公斤|克|公斤|升|毫升|ML|CL|升|L]{1,2}\\*[1-9]*')
              then splitByChar('*',extract(goodsname, '[1-9]*[KG|千克|公斤|克|公斤|升|毫升|ML|CL|升|L]{1,2}\\*[1-9]*'))

             when match(goodsname, '[1-9]*[KG|千克|公斤|克|公斤|升|毫升|ML|CL|升|L]{1,2}')
              then splitByChar('*',extract(goodsname, '[1-9]*[KG|千克|公斤|克|公斤|升|毫升|ML|CL|升|L]{1,2}'))
       else [] end cc,

       arrayFilter(i->multiMatchAny(i, ['KG','公斤', '千克', 'L', '升', '斤','CL','克','毫升', 'ML']),cc),

       arrayMap(i-> case when multiMatchAny(i,['KG','公斤', '千克', 'L', '升']) then (toUInt64(extract(i, '[1-9]*')) as num_weight)/1
                         when  multiMatchAny(i,['斤'])  then num_weight / 2
                         when  multiMatchAny(i,['CL'])  then num_weight / 100
                         when multiMatchAny(i,['克','毫升', 'ML']) then num_weight / 1000
                        else toUInt64OrZero(i) / 1 end

               ,cc);





