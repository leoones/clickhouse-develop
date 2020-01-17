-- DDL 创建字段
CREATE DICTIONARY [IF NOT EXISTS] [db.]dictionary_name
(
    key1 type1  [DEFAULT|EXPRESSION expr1] [HIERARCHICAL|INJECTIVE|IS_OBJECT_ID],
    key2 type2  [DEFAULT|EXPRESSION expr2] [HIERARCHICAL|INJECTIVE|IS_OBJECT_ID],
    attr1 type2 [DEFAULT|EXPRESSION expr3],
    attr2 type2 [DEFAULT|EXPRESSION expr4]
)
PRIMARY KEY key1, key2
SOURCE(SOURCE_NAME([param1 value1 ... paramN valueN]))
LAYOUT(LAYOUT_NAME([param_name param_value]))
LIFETIME([MIN val1] MAX val2)
       
SOURCE_NAME(数据源) 可为 FILE, EXECUTABLE, HTTP, ODBC, MYSQL, CLICKHOUSE, MONGO, REDIS
LAYOUT_NAME(主键类型) 可为 FLAT(), HASHED(), SPARSE_HASHED(), COMPLEX_KEY_HASHED(), RANGE_HASHED()

---案例:
DROP DICTIONARY if exists database_for_dict.mapping_dc_bu_info;
CREATE DICTIONARY database_for_dict.mapping_dc_bu_info
(
dc_id  String,
dc_name String,
bu_area_name  String default '未知'
)
PRIMARY KEY dc_id
SOURCE(MYSQL(HOST '10.239.33.43' PORT 3306 USER 'root' TABLE 'mapping_dc_bu_info' PASSWORD '1q2w3e' DB 'dw_hr'))
LAYOUT(COMPLEX_KEY_HASHED())
LIFETIME(MIN 30 MAX 600);
       
-- 重载字典
SYSTEM RELOAD DICTIONARIES
