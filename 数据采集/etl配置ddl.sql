create table if not exists etl_source_db_config
(
	db_id bigint auto_increment comment '数据库ID'
		primary key,
	jndi_name varchar(50) null comment 'JNDI名',
	jndi_desc varchar(100) null comment '数据库连接说明',
	db_type varchar(25) null comment '数据库类型',
	property longtext null comment '数据库连接',
	system_id bigint null comment '系统ID',
	status bigint default 1 null comment '有效性',
	create_time datetime default CURRENT_TIMESTAMP null comment '创建时间',
	modify_time timestamp default CURRENT_TIMESTAMP not null comment '更新时间'
);

create table if not exists etl_system_config
(
	system_id bigint auto_increment comment '系统ID'
		primary key,
	system_enname varchar(100) null comment '系统英文名',
	system_zhname varchar(200) null comment '系统中文名',
	status bigint null comment '有效性',
	create_time datetime default CURRENT_TIMESTAMP null comment '创建时间',
	modify_time timestamp default CURRENT_TIMESTAMP not null comment '更新时间'
);

create table if not exists etl_topic_group_config
(
	topic_group_id bigint auto_increment comment '主题ID'
		primary key,
	topic_group_enname varchar(50) null comment '主题名-英文',
	topic_group_chname varchar(1000) null comment '主题名-中文',
	model_type varchar(15) null comment '模块 etl: 同步 mail: 数据推送',
	status bigint null comment '有效性 ',
	target_table_fields longtext null comment '字段名',
	create_time datetime default CURRENT_TIMESTAMP null comment '创建时间',
	modify_time timestamp default CURRENT_TIMESTAMP not null comment '更新时间'
);

create table if not exists etl_topic_task_config
(
	task_id bigint auto_increment comment '任务ID'
		primary key,
	task_name varchar(1000) null comment '任务名-中文',
	topic_group_id bigint null comment '主题ID',
	system_id bigint null comment '系统ID',
	sql_text longtext null comment '脚本',
	status bigint null comment '有效性 ',
    property longtext null comment '设置',
	create_time datetime default CURRENT_TIMESTAMP null comment '创建时间',
	modify_time timestamp default CURRENT_TIMESTAMP not null comment '更新时间'
);