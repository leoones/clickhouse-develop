背景: 实时同步mysql数据到clickhouse

原理: 基于mysql binlo GTID模式进行同步. 每次消费完一批binlog event ，记录even的微店到.metadata文件.
      每当clickhouse重启时, clickhouse会将(binlog文件, 位置)通过协议告知mysql server， mysql从这个位点发送数据.
      


步骤
   1:  开启开关
    set allow_experimental_database_materialize_mysql = 1
    
   2:  创建通道
    create database clickhouse_database_name ENGINE = MeterializeMySQL('mysql_host:mysql_port', 'mysql_database_name', 'user', 'passwd')







注意点:
  1. 若mysql数据库 存在表 不含有unique key 或者 primary key 或者 auto increment 则会导致clickhouse crash
