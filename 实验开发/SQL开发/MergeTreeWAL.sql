 背景: 多个客户端小批量频繁写入.就会引发DB: Exception: Too parts错误. Write-Ahead Log 会先把数据写到内存，等“攒”到一定程度后再回写到磁盘.
      默认已开启.
 
 相关参数: min_rows_for_compact_part: 一次写入的数据行数大于此值，就会按照传统方式直接向磁盘flush形成Compact part（或者Wide part），
                                     不保存在内存中，也不会写WAL。反之，则会将数据保留成In-Memory part，并同时写入WAL，在下一次发生merge时再进行flush。
                                     默认: 0
                                     
          min_rows_for_compact_part: 一次写入的数据大小大于此值，就会按照传统方式直接向磁盘flush形成Compact part（或者Wide part），
                                     不保存在内存中，也不会写WAL。反之，则会将数据保留成In-Memory part，并同时写入WAL，在下一次发生merge时再进行flush。
                                     默认: 0
                                     
          min_bytes_for_wide_part: part数据以Wide形式存储的大小阈值. 若大于此值，数据以Wide(每列数据分开存储)形式. 否则数据以Polymorphic()形式.
                              
          
          min_rows_for_wide_part: part数据以Wide形式存储的大小阈值. 若大于此值，数据以Wide(每列数据分开存储)形式. 否则数据以Polymorphic()形式.
          
          write_ahead_log_max_bytes: 限制wal.bin的大小，默认值为1G
          
