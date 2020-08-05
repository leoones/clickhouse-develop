开启rbac两步骤
  1, config.xml 中添加 access_control_path 配置项
    <access_control_path>/home/data/clickhouse/access/</access_control_path>
  2, user.xml 中为默认用户 default 添加 access_management 配置项
     <access_management>1</access_management>
  
