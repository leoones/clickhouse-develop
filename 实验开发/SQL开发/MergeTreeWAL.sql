背景: 多个客户端小批量频繁写入.就会引发DB: Exception: Too parts错误. Write-Ahead Log 会先把数据写到内存，等“攒”到一定程度后再回写到磁盘.
      默认已开启.
 
相关参数: min_rows_for_compact_part: 一次写入的数据行数大于此值，就会按照传统方式直接向磁盘flush形成Compact part（或者Wide part），
                                     不保存在内存中，也不会写WAL。反之，则会将数据保留成In-Memory part，并同时写入WAL，在下一次发生merge时再进行flush。
                                     默认: 0
                                     
          min_bytes_for_compact_part: 一次写入的数据大小大于此值，就会按照传统方式直接向磁盘flush形成Compact part（或者Wide part），
                                     不保存在内存中，也不会写WAL。反之，则会将数据保留成In-Memory part，并同时写入WAL，在下一次发生merge时再进行flush。
                                     默认: 0
                                     
          min_bytes_for_wide_part: part数据以Wide形式存储的大小阈值. 若大于此值，数据以Wide(每列数据分开存储)形式. 否则数据以Polymorphic()形式.
                              
          min_rows_for_wide_part: part数据以Wide形式存储的大小阈值. 若大于此值，数据以Wide(每列数据分开存储)形式. 否则数据以Polymorphic()形式.
          
          write_ahead_log_max_bytes: 限制wal.bin的大小，默认值为1G
                 
数据整个写入流程:
  In-Memory -> Compact -> Wide
  
实验:
 1.1 创建表 
  create table test_write_ahead_log(
     a Int32,
     b Int32,
     c Int32
   )
   engine  = MergeTree
   partition by a
   order by  b;
   
 1.2 分两批插入:
  insert into test_write_ahead_log(a, b, c) values (1, 7, 3),(1, 7, 5);
  insert into test_write_ahead_log(a, b, c) values (1, 4, 3),(1, 4, 6), (1, 4, 7), (1, 4, 7), (1, 4, 7);
   此时有 2 个目录

 1.3  
  alter table test_write_ahead_log modify SETTING  min_rows_for_compact_part = 3;
  insert into test_write_ahead_log(a, b, c) values (2, 4, 3);
  insert into test_write_ahead_log(a, b, c) values (2, 5, 3);
  
  此时目录只有个 wal.bin文件. 当数据行累计大于min_rows_for_compact_part 或者 数据大小大于min_bytes_for_compact_part.会自动刷新创建一个新目录
  
 1.4 
  alter table test_write_ahead_log modify SETTING  min_rows_for_wide_part = 5;
  
 若单个目录下的行数小于该参数值,则只生产一个data.bin数据文件; 若大于该参数值,则生成相应的列数据文件. 字段名.bin
