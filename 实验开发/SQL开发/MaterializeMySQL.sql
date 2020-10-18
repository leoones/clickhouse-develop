背景: 实时同步mysql数据到clickhouse

步骤
   1:   







注意点:
  1. 若mysql数据库 存在表 不含有unique key 或者 primary key 或者 auto increment 则会导致clickhouse crash
