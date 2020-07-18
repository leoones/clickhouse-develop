INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (1, 1, 'RATION', 'RATION记录数', 'select ''{DAY}'' as stat_day_s,
       ''{TOPIC_ENNAME}'' as metric_topic,
       ''{SYSTEM_ENNAME}'' as metric_source,
       metric_date,
       metric_key,
       ''day'' as agg_type,
       ''{"logs":'' || logs || ''}'' as metric_value
  from
    (select
           metric_date,
           metric_key,
            ''day'' as agg_type,
            sum(1) as logs
      from
        (select
            to_char(t.outexedate,''%Y%m%d'') as metric_date,
            t.outshopid as metric_key
          FROM ration t
         WHERE t.outexedate >= today - 90
           and t.outexedate < today
           AND t.outshopid NOT IN (''K0LF'', ''A0LT'', ''995045'', ''Y1LG'', ''T9LF'')
           AND t.inshopid NOT IN (''K0LF'', ''A0LT'', ''995045'', ''Y1LG'', ''T9LF'')
           )
     GROUP BY  metric_date, metric_key
     )', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (2, 1, 'RATIONACC', 'RATIONACC记录数', 'select ''{DAY}'' as stat_day_s,
       ''{TOPIC_ENNAME}'' as metric_topic,
       ''{SYSTEM_ENNAME}'' as metric_source,
       metric_date,
       metric_key,
       ''day'' as agg_type,
       ''{"logs":'' || logs || ''}'' as metric_value
  from
    (select
           metric_date,
           metric_key,
            ''day'' as agg_type,
            sum(1) as logs
      from
        ( select  t1.outshopid as metric_key,
                  to_char(t1.outexedate,''%Y%m%d'')  as metric_date
          FROM rationacc t, ration t1
         WHERE t.sheetid = t1.sheetid
           AND t1.outexedate >= today - 90
           AND t1.outexedate  < today
           and t1.outshopid NOT IN (''K0LF'', ''A0LT'', ''995045'', ''Y1LG'', ''T9LF'')
           AND t1.inshopid NOT IN (''K0LF'', ''A0LT'', ''995045'', ''Y1LG'', ''T9LF'')
           )
     GROUP BY  metric_date, metric_key
     )', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (3, 1, 'RATIONITEM', 'RATIONITEM记录数', 'select ''{DAY}'' as stat_day_s,
       ''{TOPIC_ENNAME}'' as metric_topic,
       ''{SYSTEM_ENNAME}'' as metric_source,
       metric_date,
       metric_key,
       ''day'' as agg_type,
       ''{"logs":'' || logs || ''}'' as metric_value
  from
    (select
           metric_date,
           metric_key,
            ''day'' as agg_type,
            sum(1) as logs
      from
        ( select  t1.outshopid as metric_key,
                 to_char(t1.outexedate,''%Y%m%d'')  as metric_date
              FROM rationitem t, ration t1
             WHERE t.sheetid = t1.sheetid
               AND t1.outexedate >= today - 90
               AND t1.outexedate  < today
               and t1.outshopid NOT IN (''K0LF'', ''A0LT'', ''995045'', ''Y1LG'', ''T9LF'')
               AND t1.inshopid NOT IN (''K0LF'', ''A0LT'', ''995045'', ''Y1LG'', ''T9LF'')
           )
     GROUP BY  metric_date, metric_key )', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (4, 1, 'RECEIPT', 'RECEIPT记录数', 'select ''{DAY}'' as stat_day_s,
       ''{TOPIC_ENNAME}'' as metric_topic,
       ''{SYSTEM_ENNAME}'' as metric_source,
       metric_date,
       metric_key,
       ''day'' as agg_type,
       ''{"logs":'' || logs || ''}'' as metric_value
  from
    (select
           metric_date,
           metric_key,
            ''day'' as agg_type,
            sum(1) as logs
      from
        ( select t.shopid as metric_key,
                 to_char(t.checkdate,''%Y%m%d'') as metric_date
          FROM receipt t
         WHERE t.checkdate >= today - 90
           and t.checkdate  < today
           and t.shopid NOT IN (''K0LF'', ''A0LT'', ''995045'', ''Y1LG'', ''T9LF'')
           )
     GROUP BY  metric_date, metric_key )', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (5, 1, 'RECEIPTITEM', 'RECEIPTITEM记录数', 'select ''{DAY}'' as stat_day_s,
       ''{TOPIC_ENNAME}'' as metric_topic,
       ''{SYSTEM_ENNAME}'' as metric_source,
       metric_date,
       metric_key,
       ''day'' as agg_type,
       ''{"logs":'' || logs || ''}'' as metric_value
  from
    (select
           metric_date,
           metric_key,
            ''day'' as agg_type,
            sum(1) as logs
      from
        (  select t1.shopid as metric_key,
                  to_char(t1.checkdate,''%Y%m%d'')  as metric_date
          FROM receiptitem t,
                receipt t1
         WHERE t.sheetid = t1.sheetid
           AND t1.checkdate >= today - 90
           and t1.checkdate  < today
           )
     GROUP BY  metric_date, metric_key )', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (6, 1, 'SALESHIP', 'SALESHIP记录数', 'select ''{DAY}'' as stat_day_s,
       ''{TOPIC_ENNAME}'' as metric_topic,
       ''{SYSTEM_ENNAME}'' as metric_source,
       metric_date,
       metric_key,
       ''day'' as agg_type,
       ''{"logs":'' || logs || ''}'' as metric_value
  from
    (select
           metric_date,
           metric_key,
            ''day'' as agg_type,
            sum(1) as logs
      from
        (  select t.outshopid as metric_key,
                  to_char(t.executedate,''%Y%m%d'')  as metric_date
              from saleship t
             where t.executedate > today - 90
               and t.executedate  < today
           )
     GROUP BY  metric_date, metric_key )', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (7, 1, 'SALESHIPACC', 'SALESHIPACC记录数', 'select ''{DAY}'' as stat_day_s,
       ''{TOPIC_ENNAME}'' as metric_topic,
       ''{SYSTEM_ENNAME}'' as metric_source,
       metric_date,
       metric_key,
       ''day'' as agg_type,
       ''{"logs":'' || logs || ''}'' as metric_value
  from
    (select
           metric_date,
           metric_key,
            ''day'' as agg_type,
            sum(1) as logs
      from
        (  select t.outshopid as metric_key,
                  to_char(t.executedate,''%Y%m%d'')  as metric_date
          from saleship t, saleshipacc t1
         where t1.sheetid = t.sheetid
           and t.executedate > today - 90
           and t.executedate  < today
           )
     GROUP BY  metric_date, metric_key )



', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (8, 1, 'SALESHIPITEM', 'SALESHIPACC记录数', 'select ''{DAY}'' as stat_day_s,
       ''{TOPIC_ENNAME}'' as metric_topic,
       ''{SYSTEM_ENNAME}'' as metric_source,
       metric_date,
       metric_key,
       ''day'' as agg_type,
       ''{"logs":'' || logs || ''}'' as metric_value
  from
    (select
           metric_date,
           metric_key,
            ''day'' as agg_type,
            sum(1) as logs
      from
        (  select t.outshopid as metric_key,
                 to_char(t.executedate,''%Y%m%d'') as metric_date
          from saleship t, saleshipitem t1
         where t1.sheetid = t.sheetid
           and t.executedate > today - 90
           and t.executedate  < today
           )
     GROUP BY  metric_date, metric_key )', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (9, 1, 'TRAN', 'TRAN记录数', 'select ''{DAY}'' as stat_day_s,
       ''{TOPIC_ENNAME}'' as metric_topic,
       ''{SYSTEM_ENNAME}'' as metric_source,
       metric_date,
       metric_key,
       ''day'' as agg_type,
       ''{"logs":'' || logs || ''}'' as metric_value
  from
    (select
           metric_date,
           metric_key,
            ''day'' as agg_type,
            sum(1) as logs
      from
        (  select  t.outshopid as metric_key,
                   to_char(t.outexedate,''%Y%m%d'') as metric_date
              from tran t
             where t.outexedate >= today - 90
               and t.outexedate  < today
           )
     GROUP BY  metric_date, metric_key )', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (10, 1, 'TRANACC', 'TRANACC记录数', 'select ''{DAY}'' as stat_day_s,
       ''{TOPIC_ENNAME}'' as metric_topic,
       ''{SYSTEM_ENNAME}'' as metric_source,
       metric_date,
       metric_key,
       ''day'' as agg_type,
       ''{"logs":'' || logs || ''}'' as metric_value
  from
    (select
           metric_date,
           metric_key,
            ''day'' as agg_type,
            sum(1) as logs
      from
        (  select   t.outshopid as metric_key,
                    to_char(t.outexedate,''%Y%m%d'')  as metric_date
              FROM tran t, tranacc t1
             WHERE t.sheetid = t1.sheetid
               AND t.outexedate >= today - 90
               and t.outexedate  < today
           )
     GROUP BY  metric_date, metric_key )', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (11, 1, 'TRANITEM', 'TRANITEM记录数', 'select ''{DAY}'' as stat_day_s,
       ''{TOPIC_ENNAME}'' as metric_topic,
       ''{SYSTEM_ENNAME}'' as metric_source,
       metric_date,
       metric_key,
       ''day'' as agg_type,
       ''{"logs":'' || logs || ''}'' as metric_value
  from
    (select
           metric_date,
           metric_key,
            ''day'' as agg_type,
            sum(1) as logs
      from
        (  select t.outshopid as metric_key,
                  to_char(t.outexedate,''%Y%m%d'')  as metric_date
              FROM tran t,
                   tranitem t1
             WHERE t.sheetid = t1.sheetid
               AND t.outexedate >= today - 90
               AND t.outexedate  < today
           )
     GROUP BY  metric_date, metric_key )', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (12, 1, 'WHOLESALE', 'WHOLESALE记录数', 'select ''{DAY}'' as stat_day_s,
       ''{TOPIC_ENNAME}'' as metric_topic,
       ''{SYSTEM_ENNAME}'' as metric_source,
       metric_date,
       metric_key,
       ''day'' as agg_type,
       ''{"logs":'' || logs || ''}'' as metric_value
  from
    (select
           metric_date,
           metric_key,
            ''day'' as agg_type,
            sum(1) as logs
      from
        (   select  t.shopid as metric_key,
                   to_char(t.outexedate,''%Y%m%d'')  as metric_date
              from wholesale t
             where t.outexedate >= today - 90
               and t.outexedate  < today
           )
     GROUP BY  metric_date, metric_key )', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (13, 1, 'WHOLESALEACC', 'WHOLESALEACC记录数', 'select ''{DAY}'' as stat_day_s,
       ''{TOPIC_ENNAME}'' as metric_topic,
       ''{SYSTEM_ENNAME}'' as metric_source,
       metric_date,
       metric_key,
       ''day'' as agg_type,
       ''{"logs":'' || logs || ''}'' as metric_value
  from
    (select
           metric_date,
           metric_key,
            ''day'' as agg_type,
            sum(1) as logs
      from
        (    select  t.shopid as metric_key,
                     to_char(t.outexedate,''%Y%m%d'') as metric_date
              from Wholesale t, WHOLESALEACC T1
             where T.SHEETID = T1.SHEETID
               AND t.outexedate >= today - 90
               AND t.outexedate  < today
           )
     GROUP BY  metric_date, metric_key )', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (14, 1, 'WHOLESALEITEM', 'WHOLESALEITEM记录数', 'select ''{DAY}'' as stat_day_s,
       ''{TOPIC_ENNAME}'' as metric_topic,
       ''{SYSTEM_ENNAME}'' as metric_source,
       metric_date,
       metric_key,
       ''day'' as agg_type,
       ''{"logs":'' || logs || ''}'' as metric_value
  from
    (select
           metric_date,
           metric_key,
            ''day'' as agg_type,
            sum(1) as logs
      from
        (    select  t.shopid as metric_key ,
                     to_char(t.outexedate,''%Y%m%d'')  as  metric_date
              from Wholesale t, WHOLESALEITEM T1
             where T.SHEETID = T1.SHEETID
               AND t.outexedate >= today - 90
               and t.outexedate  < today
           )
     GROUP BY  metric_date, metric_key )', 'monitor', 0, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (15, 3, 'ACC_JV_TRANSFER', 'ACC_JV_TRANSFER记录数', 'SELECT ''{DAY}'' as stat_day_s,
        ''{TOPIC_ENNAME}'' as metric_topic,
        ''{SYSTEM_ENNAME}'' as metric_source,
         to_char(t.sdate, ''yyyymmdd'') as metric_date,
          t.OUTSHOPID as metric_key,
        ''day'' as agg_type,
        ''{"logs":'' || count(1) || ''}'' as metric_value
  FROM JVD2.ACC_JV_TRANSFER T
 WHERE T.sdate >= TRUNC(SYSDATE) - 90
   AND T.sdate < TRUNC(SYSDATE)
 GROUP BY to_char(t.sdate, ''yyyymmdd''), T.OUTSHOPID', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (16, 3, 'ACC_JV_TRANSFERITEM', 'ACC_JV_TRANSFERITEM记录数', 'SELECT ''{DAY}'' as stat_day_s,
        ''{TOPIC_ENNAME}'' as metric_topic,
        ''{SYSTEM_ENNAME}'' as metric_source,
         to_char(t.sdate, ''yyyymmdd'') as metric_date,
          t.OUTSHOPID as metric_key,
        ''day'' as agg_type,
        ''{"logs":'' || count(1) || ''}'' as metric_value
  FROM JVD2.ACC_JV_TRANSFER T, JVD2.ACC_JV_TRANSFERITEM T1
 WHERE T.SHEETID = T1.SHEETID
   AND T.sdate >= TRUNC(SYSDATE) - 90
   AND T.sdate  < TRUNC(SYSDATE)
 GROUP BY to_char(t.sdate, ''yyyymmdd''), T.OUTSHOPID', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (17, 3, 'ACC_JV_RECEIPT', 'ACC_JV_RECEIPT记录数', 'SELECT ''{DAY}'' as stat_day_s,
        ''{TOPIC_ENNAME}'' as metric_topic,
        ''{SYSTEM_ENNAME}'' as metric_source,
         to_char(t.sdate, ''yyyymmdd'') as metric_date,
          t.shopid as metric_key,
        ''day'' as agg_type,
        ''{"logs":'' || count(1) || ''}'' as metric_value
from jvd2.acc_jv_receipt t
 where t.sdate >= trunc(sysdate) - 90
   and t.sdate  < trunc(sysdate)
 group by to_char(t.sdate, ''yyyymmdd''), t.shopid', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (18, 3, 'ACC_JV_RECEIPTITEM', 'ACC_JV_RECEIPTITEM记录数', 'SELECT ''{DAY}'' as stat_day_s,
        ''{TOPIC_ENNAME}'' as metric_topic,
        ''{SYSTEM_ENNAME}'' as metric_source,
         to_char(t.sdate, ''yyyymmdd'') as metric_date,
          t.shopid as metric_key,
        ''day'' as agg_type,
        ''{"logs":'' || count(1) || ''}'' as metric_value
 FROM JVD2.ACC_JV_RECEIPT T, JVD2.ACC_JV_RECEIPTITEM T1
 WHERE T.SHEETID = T1.SHEETID
   AND T.JV_UPDATE_TIME >= TRUNC(SYSDATE) - 90
   AND T.JV_UPDATE_TIME  < TRUNC(SYSDATE)
 GROUP BY to_char(t.sdate, ''yyyymmdd''), T.SHOPID
', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (19, 4, 'ORDER_SHEET', 'wms预约清单明细', 'SELECT DISTINCT t4.article_name,
                      t4.article_no,
                      t5.l_group_no,
                      t5.l_group_name,
                      t6.a_length,
                      t6.a_width,
                      t6.a_height,
                      t4.barcode,
                      t.order_serial,
                      t6.packing_unit,
                      t3.po_qty,
                      t1.request_date,
                      t7.supplier_name,
                      t7.supplier_no,
                      t.po_no,
                      t6.packing_weight,
                       ''{DC_ID}'' as dc_id,
                      t8.owner_customer,
                      t8.customer_name,
                      t1.start_time || ''~'' || t1.end_time TIME,
                      t9.cost * t6.packing_qty * t3.po_qty as COST
        FROM CRCWMS.im_order_sheet     t,
             CRCWMS.im_order_status    t1,
             CRCWMS.im_import_m        t2,
             CRCWMS.im_import_allot    t3,
             CRCWMS.bm_defarticle      t4,
             CRCWMS.bm_l_group         t5,
             CRCWMS.bm_article_packing t6,
             CRCWMS.bm_defsupplier     t7,
             CRCWMS.bm_defcustomer     t8,
             CRCWMS.tmp_article_cost   t9
       WHERE  t.order_serial = t1.order_serial
         AND t1.s_import_no = t2.s_import_no
         AND t.po_no = t2.po_no
         AND t3.article_no = t9.article_no
         AND t2.po_type = ''ID''
         AND t.po_type = ''ID''
         AND t2.po_no = t3.po_no
         AND t3.article_no = t4.article_no
         AND t2.l_group_no = t5.l_group_no
         AND t3.article_no = t6.article_no
         AND t3.article_unit = t6.packing_unit
         AND t2.supplier_no = t7.supplier_no
         AND t3.customer_no = t8.customer_no
         AND t.cancel_status = ''N''
         AND t1.request_date >= date''{DAY}''
         AND t1.request_date < date''{DAY}'' + 8', 'etl', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (20, 5, 'ORDER_SHEET', 'nwms预约清单明细', 'SELECT DISTINCT t4.article_name,
                      t4.article_no,
                      t5.l_group_no,
                      t5.l_group_name,
                      t6.a_length,
                      t6.a_width,
                      t6.a_height,
                      t11.barcode,
                      t.order_serial,
                      t6.packing_unit,
                      t9.article_qty / t9.packing_qty as PO_QTY,
                      t1.request_date,
                      t7.supplier_name,
                      t7.supplier_no,
                      t.po_no,
                      t6.packing_weight,
                      t10.batch_serial_no as DC_ID,
                      t8.owner_cust_no as OWNER_CUSTOMER,
                      t8.cust_name as CUSTOMER_NAME,
                      t1.start_time || ''~'' || t1.end_time TIME,
                      t12.cost * t9.article_qty as COST
        FROM NWMS_WMS.im_order_sheet    t,
             NWMS_WMS.im_order_status   t1,
             NWMS_WMS.im_import_m       t2,
             NWMS_WMS.om_exp_m          t3,
             NWMS_WMS.om_exp_d          t9,
             NWMS_WMS.bm_defarticle     t4,
             NWMS_WMS.bm_article_group  t5,
             NWMS_WMS.bm_article_packing t6,
             NWMS_WMS.bm_defsupplier    t7,
             NWMS_WMS.bm_defcust        t8,
             NWMS_WMS.bm_defdcshop      t10,
             NWMS_WMS.bm_article_barcode t11,
             NWMS_WMS.tmp_article_cost  t12
       WHERE t.locno = t1.locno
         AND t.order_serial = t1.order_serial
         AND t.po_no = t2.po_no
         AND t.locno = t2.locno
         AND t2.po_type = ''ID''
         AND t.po_type = ''ID''
         AND t2.locno = t3.locno
         AND t2.import_no = t3.import_no
         AND t3.locno = t9.locno
         AND t3.exp_no = t9.exp_no
         AND t9.article_no = t4.article_no
         AND t9.owner_no = t4.owner_no
         AND t4.group_no = t5.group_no
         AND t4.owner_no = t5.owner_no
         AND t9.article_no = t6.article_no
         AND t9.article_qty = t6.packing_qty
         AND t2.supplier_no = t7.supplier_no
         AND t2.owner_no = t7.owner_no
         AND t3.cust_no = t8.cust_no
         AND t3.owner_no = t8.owner_no
         AND t.locno = t10.locno
         AND t9.owner_no = t11.owner_no
         AND t9.article_no = t11.article_no
         AND t11.primary_flag = ''0''
         AND t11.packing_qty = ''1''
         AND t.cancel_serial = ''N''
          AND t12.article_no = t9.article_no
         AND t1.request_date >= date''{DAY}''
         and t1.request_date  < date''{DAY}'' + 8', 'etl', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (21, 8, 'RATION_METRIC', '正常配送-散数，金额', 'select  ''{DAY}'' as stat_day_s,
        ''{TOPIC_ENNAME}'' as metric_topic,
       ''{SYSTEM_ENNAME}'' as metric_source,
        to_char(t1.sdate, ''yyyymmdd'') as metric_date,
        t1.OUTSHOPID  as  metric_key  ,
        ''day'' as agg_type,
       ''{"cost":'' || sum(COSTVALUE + COSTTAXVALUE) || '','' || ''"qty":'' || sum(t2.QTY)|| ''}''  as metric_value
 from dw.acc_jv_transfer t1,
      dw.acc_jv_transferitem t2,
      dw.jv_shop  js
where t1.sheetid = t2.sheetid
  and t1.OUTSHOPID = js.SHOPID
  and js.SHOPFORMID = 9
  and t1.sheetid  NOT LIKE ''CT%''
  and t1.sdate >= trunc(sysdate -91, ''MONTH'')
  and t1.sdate <= trunc(sysdate) -1
  and t1.transfertype in (''I'')
  group by t1.OUTSHOPID, to_char(t1.sdate, ''yyyymmdd'')
union all
select  ''{DAY}'' as stat_day_s,
        ''{TOPIC_ENNAME}'' as metric_topic,
        ''{SYSTEM_ENNAME}'' as metric_source,
         to_char(t.outexedate, ''yyyymmdd'') as metric_date,
         t.outshopid as  metric_key  ,
        ''day'' as agg_type,
       ''{"cost":'' || sum(t2.cost * t2.outqty) || '','' || ''"qty":'' || sum(t2.outqty)|| ''}''  as metric_value
    FROM dw.ration     t,
         dw.rationitem t1,
         dw.rationacc  t2
 WHERE t.sheetid = t1.sheetid
   AND t.sheetid = t2.sheetid
   AND t1.goodsid = t2.goodsid
   AND t.outexedate >= trunc(sysdate -91, ''MONTH'')
   AND t.outexedate <= trunc(sysdate) -1
   AND t.outshopid != ''995045''
   AND t.rationtype in ( ''I'')
  group by to_char(t.outexedate, ''yyyymmdd''),  t.outshopid
union all
select ''{DAY}'' as stat_day_s,
       ''{TOPIC_ENNAME}'' as metric_topic,
       ''{SYSTEM_ENNAME}'' as metric_source,
        xrt.deliver_date as metric_date,
        xrt.out_shop_id  as  metric_key  ,
        ''day'' as agg_type,
       ''{"cost":'' || sum(xrt.cost * xrt.deliver_scatter_num) || '','' || ''"qty":'' || sum(xrt.deliver_scatter_num)|| ''}''  as metric_value
  from dw.xg_deliver_t xrt
where xrt.deliver_date >= to_char(trunc(sysdate -91, ''MONTH''), ''yyyymmdd'')
  and xrt.deliver_date <= to_char(trunc(sysdate) -1, ''yyyymmdd'')
  and out_shop_id in (''001001'', ''006901'')
  and IN_SHOP_ID NOT IN (''001002'', ''001003'')
  and logistics in (''G'', ''F'', ''T'')
group by  deliver_date,  xrt.out_shop_id
union all
select ''{DAY}'' as stat_day_s,
        ''{TOPIC_ENNAME}'' as metric_topic,
        ''{SYSTEM_ENNAME}'' as metric_source,
        xrt.deliver_date as metric_date,
        xrt.out_shop_id  as  metric_key  ,
        ''day'' as agg_type,
       ''{"cost":'' || sum(xrt.deliver_taxamount) || '','' || ''"qty":'' || sum(xrt.DELIVER_SCATTER_NUM)|| ''}''  as metric_value
  from dw.sg_deliver_t xrt
where xrt.deliver_date >= to_char(trunc(sysdate -91, ''MONTH''), ''yyyymmdd'')
  and xrt.deliver_date <= to_char(trunc(sysdate) -1, ''yyyymmdd'')
  AND xrt.goods_code NOT IN  (''888886'', ''888887'', ''888894'', ''999258258'')
  and xrt.logistics in  (''3调整'', ''0存储'', ''1直流'')
group by deliver_date,  xrt.out_shop_id', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (22, 9, 'RATION_METRIC', '正常配送-散数，金额', 'select stat_day_s,
       metric_topic,
       ''{SYSTEM_ENNAME}'' as metric_source,
       metric_date,
       metric_key,
       agg_type,
       concat(''{"cost":'',toString(cost),'','', ''"qty":'',toString(qty),'','',''"box":'',toString(boxes), ''}'') as metric_value
 from  (select ''{DAY}'' as stat_day_s,
                 ''{TOPIC_ENNAME}'' as metric_topic,
               toYYYYMMDD(frdssvd.stat_day) as metric_date,
               frdssvd.out_shop_id as metric_key,
               ''day'' as agg_type,
               sum(frdssvd.rt_cost) as cost,
               sum(frdssvd.rt_qty) as qty,
               sum(frdssvd.rt_boxes) boxes
          from dw_hr.fct_rt_dc_shop_sku_vender_day frdssvd
        where frdssvd.stat_day  >= toStartOfMonth(yesterday() -90)
          and frdssvd.stat_day  <= yesterday()
          and bun_type_id = 1
        group by metric_date,
                 metric_key,
                 metric_topic
            )', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (23, 10, 'RATION_METRIC', '正常配送-散数，金额', 'select stat_day_s,
       metric_topic,
       ''{SYSTEM_ENNAME}'' as metric_source,
       metric_date,
       metric_key,
       agg_type,
       concat(''{"cost":'',toString(cost),'','', ''"qty":'',toString(qty),'','',''"box":'',toString(boxes), ''}'') as metric_value
 from  (select ''{DAY}'' as stat_day_s,
               ''{TOPIC_ENNAME}'' as metric_topic,
               frdssvd.stat_month as metric_date,
               frdssvd.out_shop_id as metric_key,
               ''month'' as agg_type,
               sum(frdssvd.rt_cost) as cost,
               sum(frdssvd.rt_qty) as qty,
               sum(frdssvd.rt_boxes) boxes
          from dw_hr.fct_rt_dc_shop_sku_vender_mon frdssvd
        where frdssvd.stat_month  >= toYYYYMM(toStartOfMonth(yesterday() -90))
          and frdssvd.stat_month  <= toYYYYMM(yesterday())
          and frdssvd.bun_type_id = 1
        group by metric_date,
                 metric_key,
                 metric_topic
            )', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (24, 8, 'RATION', 'RATION记录数', 'SELECT ''{DAY}'' as stat_day_s,
        ''{TOPIC_ENNAME}'' as metric_topic,
        ''{SYSTEM_ENNAME}'' as metric_source,
        to_char(t.outexedate,''yyyymmdd'') as metric_date,
        t.outshopid as metric_key,
        ''day'' as agg_type,
        ''{"logs":'' || count(1) || ''}'' as metric_value
  FROM dw.ration t
 WHERE t.outexedate >= trunc(sysdate) - 90
   and t.outexedate <  trunc(sysdate)
   AND t.outshopid NOT IN (''K0LF'', ''A0LT'', ''995045'', ''Y1LG'', ''T9LF'')
   AND t.inshopid NOT IN (''K0LF'', ''A0LT'', ''995045'', ''Y1LG'', ''T9LF'')
 GROUP BY to_char(t.outexedate,''yyyymmdd''), t.outshopid', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (25, 8, 'RATIONACC', 'RATIONACC记录数', 'SELECT ''{DAY}'' as stat_day_s,
        ''{TOPIC_ENNAME}'' as metric_topic,
        ''{SYSTEM_ENNAME}'' as metric_source,
        to_char(t1.outexedate,''yyyymmdd'') as metric_date,
        t1.outshopid as metric_key,
        ''day'' as agg_type,
        ''{"logs":'' || count(1) || ''}'' as metric_value
 FROM dw.rationacc t,
       dw.ration t1
 WHERE t.sheetid = t1.sheetid
   AND t1.outexedate >= trunc(sysdate) - 90
   AND t1.outexedate  < trunc(sysdate)
   and t1.outshopid NOT IN (''K0LF'', ''A0LT'', ''995045'', ''Y1LG'', ''T9LF'')
   AND t1.inshopid NOT IN (''K0LF'', ''A0LT'', ''995045'', ''Y1LG'', ''T9LF'')
 GROUP BY to_char(t1.outexedate, ''yyyymmdd''), t1.outshopid', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (26, 8, 'RATIONITEM', 'RATIONITEM记录数', 'SELECT ''{DAY}'' as stat_day_s,
        ''{TOPIC_ENNAME}'' as metric_topic,
        ''{SYSTEM_ENNAME}'' as metric_source,
        to_char(t1.outexedate,''yyyymmdd'') as metric_date,
        t1.outshopid as metric_key,
        ''day'' as agg_type,
        ''{"logs":'' || count(1) || ''}'' as metric_value
 FROM dw.rationitem t,
       dw.ration t1
 WHERE t.sheetid = t1.sheetid
   AND t1.outexedate >= trunc(sysdate) - 90
   AND t1.outexedate  < trunc(sysdate)
   and t1.outshopid NOT IN (''K0LF'', ''A0LT'', ''995045'', ''Y1LG'', ''T9LF'')
   AND t1.inshopid NOT IN (''K0LF'', ''A0LT'', ''995045'', ''Y1LG'', ''T9LF'')
 GROUP BY to_char(t1.outexedate, ''yyyymmdd''),  t1.outshopid', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (27, 8, 'RECEIPT', 'RECEIPT记录数', 'SELECT ''{DAY}'' as stat_day_s,
        ''{TOPIC_ENNAME}'' as metric_topic,
        ''{SYSTEM_ENNAME}'' as metric_source,
        to_char(t.checkdate, ''yyyymmdd'') as metric_date,
         t.shopid as metric_key,
        ''day'' as agg_type,
        ''{"logs":'' || count(1) || ''}'' as metric_value
  FROM receipt t
 WHERE t.checkdate >= trunc(sysdate) - 90
   and t.checkdate  < trunc(sysdate)
   and t.shopid NOT IN (''K0LF'', ''A0LT'', ''995045'', ''Y1LG'', ''T9LF'')
 GROUP BY to_char(t.checkdate, ''yyyymmdd''),  t.shopid', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (28, 8, 'RECEIPTITEM', 'RECEIPTITEM记录数', 'SELECT ''{DAY}'' as stat_day_s,
        ''{TOPIC_ENNAME}'' as metric_topic,
        ''{SYSTEM_ENNAME}'' as metric_source,
         to_char(t1.checkdate, ''yyyymmdd'') as metric_date,
         t1.shopid as metric_key,
        ''day'' as agg_type,
        ''{"logs":'' || count(1) || ''}'' as metric_value
  FROM  dw.receiptitem t,
        dw.receipt t1
 WHERE t.sheetid = t1.sheetid
   AND t1.checkdate >= trunc(sysdate) - 90
   and t1.checkdate  < trunc(sysdate)
 GROUP BY to_char(t1.checkdate, ''yyyymmdd''),  t1.shopid', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (29, 8, 'SALESHIP', 'SALESHIP记录数', 'SELECT ''{DAY}'' as stat_day_s,
        ''{TOPIC_ENNAME}'' as metric_topic,
        ''{SYSTEM_ENNAME}'' as metric_source,
         to_char(t.executedate, ''yyyymmdd'') as metric_date,
         t.outshopid as metric_key,
        ''day'' as agg_type,
        ''{"logs":'' || count(1) || ''}'' as metric_value
   from dw.saleship t
 where t.executedate >= trunc(sysdate) - 90
   and t.executedate  < trunc(sysdate)
 group by t.outshopid, to_char(t.executedate, ''yyyymmdd'')', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (30, 8, 'SALESHIPACC', 'SALESHIPACC记录数', 'SELECT ''{DAY}'' as stat_day_s,
        ''{TOPIC_ENNAME}'' as metric_topic,
        ''{SYSTEM_ENNAME}'' as metric_source,
         to_char(t.executedate, ''yyyymmdd'') as metric_date,
         t.outshopid as metric_key,
        ''day'' as agg_type,
        ''{"logs":'' || count(1) || ''}'' as metric_value
     from dw.saleship t,
           dw.saleshipacc t1
     where t1.sheetid = t.sheetid
       and t.executedate >  trunc(sysdate) - 90
       and t.executedate  < trunc(sysdate)
     group by t.outshopid, to_char(t.executedate, ''yyyymmdd'')', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (31, 8, 'SALESHIPITEM', 'SALESHIPACC记录数', 'SELECT ''{DAY}'' as stat_day_s,
        ''{TOPIC_ENNAME}'' as metric_topic,
        ''{SYSTEM_ENNAME}'' as metric_source,
         to_char(t.executedate, ''yyyymmdd'') as metric_date,
         t.outshopid as metric_key,
        ''day'' as agg_type,
        ''{"logs":'' || count(1) || ''}'' as metric_value
     from saleship t, saleshipitem t1
 where t1.sheetid = t.sheetid
   and t.executedate >= trunc(sysdate) - 90
   and t.executedate  < trunc(sysdate)
 group by t.outshopid, to_char(t.executedate,''yyyymmdd'')', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (32, 8, 'TRAN', 'TRAN记录数', 'SELECT ''{DAY}'' as stat_day_s,
        ''{TOPIC_ENNAME}'' as metric_topic,
        ''{SYSTEM_ENNAME}'' as metric_source,
         to_char(t.outexedate,''yyyymmdd'') as metric_date,
         t.outshopid as metric_key,
        ''day'' as agg_type,
        ''{"logs":'' || count(1) || ''}'' as metric_value
   from tran t
 where t.outexedate >= trunc(sysdate) - 90
   and t.outexedate  < trunc(sysdate)
 group by to_char(t.outexedate,''yyyymmdd''),   t.outshopid', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (33, 8, 'TRANACC', 'TRANACC记录数', 'SELECT ''{DAY}'' as stat_day_s,
        ''{TOPIC_ENNAME}'' as metric_topic,
        ''{SYSTEM_ENNAME}'' as metric_source,
         to_char(t.outexedate,''yyyymmdd'') as metric_date,
         t.outshopid as metric_key,
        ''day'' as agg_type,
        ''{"logs":'' || count(1) || ''}'' as metric_value
FROM dw.tran t, dw.tranacc t1
 WHERE t.sheetid = t1.sheetid
   AND t.outexedate >= trunc(sysdate) - 90
   and t.outexedate  < trunc(sysdate)
 GROUP BY to_char(t.outexedate,''yyyymmdd''), t.outshopid', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (34, 8, 'TRANITEM', 'TRANITEM记录数', 'SELECT ''{DAY}'' as stat_day_s,
        ''{TOPIC_ENNAME}'' as metric_topic,
        ''{SYSTEM_ENNAME}'' as metric_source,
         to_char(t.outexedate,''yyyymmdd'') as metric_date,
         t.outshopid as metric_key,
        ''day'' as agg_type,
        ''{"logs":'' || count(1) || ''}'' as metric_value
  FROM dw.tran t,   dw.tranitem t1
 WHERE t.sheetid = t1.sheetid
   AND t.outexedate >= trunc(sysdate) - 90
   AND t.outexedate  < trunc(sysdate)
 GROUP BY to_char(t.outexedate,''yyyymmdd''), t.outshopid', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (35, 8, 'WHOLESALE', 'WHOLESALE记录数', 'SELECT ''{DAY}'' as stat_day_s,
        ''{TOPIC_ENNAME}'' as metric_topic,
        ''{SYSTEM_ENNAME}'' as metric_source,
         to_char(t.outexedate,''yyyymmdd'') as metric_date,
          t.shopid as metric_key,
        ''day'' as agg_type,
        ''{"logs":'' || count(1) || ''}'' as metric_value
 from wholesale t
 where t.outexedate >= trunc(sysdate) - 90
   and t.outexedate  < trunc(sysdate)
 group by to_char(t.outexedate,''yyyymmdd''),  t.shopid', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (36, 8, 'WHOLESALEACC', 'WHOLESALEACC记录数', 'SELECT ''{DAY}'' as stat_day_s,
        ''{TOPIC_ENNAME}'' as metric_topic,
        ''{SYSTEM_ENNAME}'' as metric_source,
         to_char(t.outexedate,''yyyymmdd'') as metric_date,
          t.shopid as metric_key,
        ''day'' as agg_type,
        ''{"logs":'' || count(1) || ''}'' as metric_value
  from dw.wholesale t,  dw.wholesaleacc T1
 where T.SHEETID = T1.SHEETID
   AND t.outexedate >= trunc(sysdate) - 90
   AND t.outexedate  < trunc(sysdate)
 group by to_char(t.outexedate,''yyyymmdd''), t.shopid', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (37, 8, 'WHOLESALEITEM', 'WHOLESALEITEM记录数', 'SELECT ''{DAY}'' as stat_day_s,
        ''{TOPIC_ENNAME}'' as metric_topic,
        ''{SYSTEM_ENNAME}'' as metric_source,
         to_char(t.outexedate,''yyyymmdd'') as metric_date,
          t.shopid as metric_key,
        ''day'' as agg_type,
        ''{"logs":'' || count(1) || ''}'' as metric_value
 from dw.Wholesale t,   dw.WHOLESALEITEM T1
 where T.SHEETID = T1.SHEETID
   AND t.outexedate >= trunc(sysdate) - 90
   and t.outexedate  < trunc(sysdate)
 group by to_char(t.outexedate,''yyyymmdd''),  t.shopid', 'display', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (38, 4, 'BH', '补货', 'SELECT date''{DAY}'' as days,
     ''{DC_ID}'' as dc ,
      ''{SYSTEM_ENNAME}'' as SYSTEM_NAME,
      ''{TOPIC_ENNAME}'' as METRIC_TOPIC,
     SUM(ooi.real_qty) as dc_bh_xs,
     COUNT(DISTINCT ooi.article_no) as dc_bh_skus,
     SUM(ooi.real_qty * bap.packing_qty * tac.cost) as dc_bh_je,
     COUNT(DISTINCT ooi.rgst_name) as dc_bh_rs
FROM CRCWMS.om_outstock        oo,
     CRCWMS.om_outstock_item   ooi,
     CRCWMS.tmp_article_cost   tac,
     CRCWMS.bm_article_packing bap
WHERE oo.outstock_no = ooi.outstock_no
 AND oo.operate_date >=date''{DAY}''
 AND oo.operate_date < date''{DAY}'' + 1
 AND ooi.article_no = bap.article_no
 AND ooi.article_unit = bap.packing_unit
 AND ooi.article_no = tac.article_no
 AND oo.outstock_type = 1
 AND oo.operate_status = ''13''', 'display', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (39, 4, 'JH', '检货', 'SELECT date''{DAY}'' as days,
      ''{DC_ID}'' as dc,
       ''{SYSTEM_ENNAME}'' as SYSTEM_NAME,
      ''{TOPIC_ENNAME}'' as METRIC_TOPIC,
 count(distinct case when oo.operate_type in (''P'',''M'') and ooi.source_no = ''N'' then ooi.outstock_container_no else null end) as dc_jh_bx_bqs_pt,
 sum(case when oo.operate_type in (''P'',''M'') and ooi.source_no = ''N'' then ooi.real_qty else 0 end) as dc_jh_bx_xs_pt,
 count(distinct case when oo.operate_type in (''P'',''M'') and ooi.source_no = ''N'' then ooi.article_no else null end) as dc_jh_bx_skus_pt,
 sum(case when oo.operate_type in (''P'',''M'') and ooi.source_no = ''N'' then ooi.real_qty*bap.packing_qty*tac.cost else 0 end) as dc_jh_bx_je_pt,
 count(distinct case when oo.operate_type in (''P'',''M'') and ooi.source_no = ''N'' then ooi.rgst_name else null end) as dc_jh_bx_rs_pt,
 sum(case when oo.operate_type in (''C'') and ooi.source_no = ''N'' then ooi.real_qty else 0 end) as dc_jh_xx_xs_pt,
 count(distinct case when oo.operate_type in (''C'') and ooi.source_no = ''N'' then ooi.article_no else null end) as dc_jh_xx_skus_pt,
 sum(case when oo.operate_type in (''C'') and ooi.source_no = ''N'' then ooi.real_qty*bap.packing_qty*tac.cost else 0 end) as dc_jh_xx_je_pt,
 count(distinct case when oo.operate_type in (''C'') and ooi.source_no = ''N'' then ooi.rgst_name else null end) as dc_jh_xx_rs_pt,
 count(distinct case when oo.operate_type in (''B'',''D'') and ooi.source_no = ''N'' then ooi.article_no || ooi.customer_no else null end) as dc_jh_chl_bs_pt,
 sum(case when oo.operate_type in (''B'',''D'') and ooi.source_no = ''N'' then ooi.real_qty*bap.packing_qty else 0 end) as dc_jh_chl_js_pt,
 count(distinct case when oo.operate_type in (''B'',''D'') and ooi.source_no = ''N'' then ooi.container_no else null end) as dc_jh_chl_bqs_pt,
 count(distinct case when oo.operate_type in (''B'',''D'') and ooi.source_no = ''N'' then ooi.article_no else null end) as dc_jh_chl_skus_pt,
 sum(case when oo.operate_type in (''B'',''D'') and ooi.source_no = ''N'' then ooi.real_qty*bap.packing_qty*tac.cost else 0 end) as dc_jh_chl_je_pt,
 count(distinct case when oo.operate_type in (''B'',''D'') and ooi.source_no = ''N'' then ooi.updt_name else null end) as dc_jh_chl_rs_pt,
 count(distinct case when oo.operate_type in (''P'',''M'') and ooi.source_no != ''N'' then ooi.outstock_container_no else null end) as dc_jh_bx_bqs_jj,
 sum(case when oo.operate_type in (''P'',''M'') and ooi.source_no != ''N'' then ooi.real_qty else 0 end) as dc_jh_bx_xs_jj,
 count(distinct case when oo.operate_type in (''P'',''M'') and ooi.source_no != ''N'' then ooi.article_no else null end) as dc_jh_bx_skus_jj,
 sum(case when oo.operate_type in (''P'',''M'') and ooi.source_no != ''N'' then ooi.real_qty*bap.packing_qty*tac.cost else 0 end) as dc_jh_bx_je_jj,
 count(distinct case when oo.operate_type in (''P'',''M'') and ooi.source_no != ''N'' then ooi.rgst_name else null end) as dc_jh_bx_rs_jj,
 sum(case when oo.operate_type in (''C'') and ooi.source_no != ''N'' then ooi.real_qty else 0 end) as dc_jh_xx_xs_jj,
 count(distinct case when oo.operate_type in (''C'') and ooi.source_no != ''N'' then ooi.article_no else null end) as dc_jh_xx_skus_jj,
 sum(case when oo.operate_type in (''C'') and ooi.source_no != ''N'' then ooi.real_qty*bap.packing_qty*tac.cost else 0 end) as  dc_jh_xx_je_jj,
 count(distinct case when oo.operate_type in (''C'') and ooi.source_no != ''N'' then ooi.rgst_name else null end) as dc_jh_xx_rs_jj,
 count(distinct case when oo.operate_type in (''B'',''D'') and ooi.source_no != ''N'' then ooi.article_no || ooi.customer_no else null end) as dc_jh_chl_bs_jj,
 sum(case when oo.operate_type in (''B'',''D'') and ooi.source_no != ''N'' then ooi.real_qty*bap.packing_qty else 0 end) as dc_jh_chl_js_jj,
 count(distinct case when oo.operate_type in (''B'',''D'') and ooi.source_no != ''N'' then ooi.source_no else null end) as dc_jh_chl_bqs_jj,
 count(distinct case when oo.operate_type in (''B'',''D'') and ooi.source_no != ''N'' then ooi.article_no else null end) as dc_jh_chl_skus_jj,
 sum(case when oo.operate_type in (''B'',''D'') and ooi.source_no != ''N'' then ooi.real_qty*bap.packing_qty*tac.cost else 0 end) as dc_jh_chl_je_jj,
 count(distinct case when oo.operate_type in (''B'',''D'') and ooi.source_no != ''N'' then ooi.updt_name else null end) as dc_jh_chl_rs_jj,
 count(distinct  ooi.article_no) as dc_jh_skus_zs,
 count(distinct  ooi.updt_name) as dc_jh_rs_zs
FROM CRCWMS.om_outstock     oo,
 CRCWMS.om_outstock_item ooi,
 CRCWMS.tmp_article_cost tac,
 CRCWMS.bm_article_packing bap
WHERE oo.outstock_no = ooi.outstock_no
AND oo.operate_date >=date''{DAY}''
AND oo.operate_date < date''{DAY}'' + 1
and ooi.article_no = bap.article_no
and ooi.article_unit = bap.packing_unit
and ooi.article_no = tac.article_no
AND oo.outstock_type = 0
and ooi.real_qty > 0
and oo.OPERATE_STATUS = ''13''', 'display', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (40, 4, 'SJ', '上架', 'SELECT date''{DAY}'' as days,
''{DC_ID}'' as dc ,
  ''{SYSTEM_ENNAME}'' as SYSTEM_NAME,
      ''{TOPIC_ENNAME}'' as METRIC_TOPIC,
COUNT(DISTINCT iid.container_no) as dc_sj_bs,
COUNT(DISTINCT iid.article_no) as dc_sj_skus,
SUM(iid.instock_qty) as dc_sj_xs,
count(DISTINCT iim.rgst_name) as dc_sj_rs,
SUM(iid.instock_qty * bap.packing_qty * tac.cost) as dc_sj_je
FROM CRCWMS.im_instock_m        iim,
CRCWMS.im_instock_d       iid,
CRCWMS.tmp_article_cost   tac,
CRCWMS.bm_article_packing  bap
WHERE iim.instock_no = iid.instock_no
AND iim.status = ''13''
AND iid.article_no = bap.article_no
AND iid.article_unit = bap.packing_unit
AND iid.article_no = tac.article_no
AND iim.source_type = ''I''
AND iim.instock_date >= date''{DAY}''
AND iim.instock_date < date''{DAY}'' + 1', 'display', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (41, 4, 'ZQ', '转区', 'SELECT date''{DAY}'' as days,
       ''{DC_ID}'' as dc ,
        ''{SYSTEM_ENNAME}'' as SYSTEM_NAME,
      ''{TOPIC_ENNAME}'' as METRIC_TOPIC,
     COUNT(DISTINCT t.cell_no) as dc_zq_bs_zs,
     COUNT(DISTINCT t.article_no) as dc_zq_skus_zs,
     COUNT(DISTINCT t.rgst_name) as dc_zq_rs,
     SUM(CASE WHEN t.operate_type IN (''P'', ''C'') THEN  t.qty  ELSE  0  END) as dc_zq_yx_xs,
     SUM(CASE WHEN t.operate_type IN (''P'', ''C'') THEN  t.cost ELSE 0 END) as dc_zq_yx_je,
     COUNT(DISTINCT CASE  WHEN t.operate_type = ''B'' THEN  b_qty  ELSE NULL END) as dc_zq_wlx_xs,
     SUM(CASE WHEN t.operate_type = ''B'' THEN t.cost ELSE 0 END) as dc_zq_wlx_je,
     COUNT(DISTINCT CASE WHEN t.operate_type = ''P1'' THEN  b_qty  ELSE  NULL   END) as dc_zq_zbys_xs,
     SUM(CASE  WHEN t.operate_type = ''P1'' THEN  t.cost  ELSE  0  END) as dc_zq_zbys_je,
     COUNT(DISTINCT CASE WHEN t.operate_type = ''P1'' THEN  t.cell_no  ELSE NULL  END) as dc_zq_zbys_bs,
     COUNT(DISTINCT CASE  WHEN t.operate_type = ''P2'' THEN  b_qty  ELSE NULL  END) as dc_zq_zbxj_xs,
     SUM(CASE WHEN t.operate_type = ''P2'' THEN  t.cost ELSE 0 END) as dc_zq_zbxj_je,
     COUNT(DISTINCT CASE  WHEN t.operate_type = ''P2'' THEN  t.cell_no ELSE  NULL END) as dc_zq_zbxj_bs
FROM (SELECT ipd.pack_qty qty,
                     ipd.article_no,
                     ccml1.rgst_name,
                     ccml.cell_no,
                     NULL b_qty,
                     ''P'' operate_type,
                     ipd.pack_qty * bap.packing_qty * tac.cost cost
                FROM container_case_move_log ccml1,
                     container_case_move_log ccml,
                     im_printing_d           ipd,
                     bm_article_packing      bap,
                     tmp_article_cost        tac
               WHERE ccml1.rgst_date >= date''{DAY}''
                 AND ccml1.rgst_date < date''{DAY}'' + 1
                 AND ccml1.status = ''G''
                 AND ccml.cell_no = ccml1.container_no
                 AND ccml.status = ''A''
                 AND ccml.container_no = ipd.container_no
                 AND ipd.article_no = bap.article_no
                 AND ipd.article_unit = bap.packing_unit
                 AND ipd.article_no = tac.article_no
                 AND ccml1.row_id =
                     (SELECT MAX(ccml2.row_id)
                        FROM container_case_move_log ccml2
                       WHERE ccml1.container_no = ccml2.container_no
                         and ccml1.container_type = ccml2.container_type
                         and ccml1.container_date = ccml2.container_date
                         AND ccml2.status = ''G'')
              UNION ALL
              SELECT icca.real_qty / icca.packing_qty qty,
                     icca.article_no,
                     ccml1.rgst_name,
                     ccml.cell_no,
                     NULL b_qty,
                     ''P'' operate_type,
                     icca.real_qty * tac.cost cost
                FROM container_case_move_log ccml1,
                     container_case_move_log ccml,
                     im_container_case_allot icca,
                     container_case_relation car,
                     tmp_article_cost        tac
               WHERE ccml1.rgst_date >= date''{DAY}''
                 AND ccml1.rgst_date < date''{DAY}'' + 1
                 AND ccml1.status = ''G''
                 AND ccml.cell_no = ccml1.container_no
                 AND ccml.status = ''A''
                 AND icca.container_no = car.oldcontainer_no
                 AND ccml.container_no = car.newcontainer_no
                 AND ccml.container_type != ''C''
                 AND icca.article_no = tac.article_no
                 AND ccml1.row_id =
                      (SELECT MAX(ccml2.row_id)
                        FROM container_case_move_log ccml2
                       WHERE ccml1.container_no = ccml2.container_no
                         and ccml1.container_type = ccml2.container_type
                         and ccml1.container_date = ccml2.container_date
                         AND ccml2.status = ''G'')
              UNION ALL
              SELECT 1 qty,
                     oc.article_no,
                     ccml1.rgst_name,
                     ccml.cell_no,
                     NULL b_qty,
                     ''C'' operate_type,
                     bap.packing_qty * tac.cost cost
                FROM container_case_move_log ccml1,
                     container_case_move_log ccml,
                     om_case                 oc,
                     bm_article_packing      bap,
                     tmp_article_cost        tac
               WHERE ccml1.rgst_date >= date''{DAY}''
                 AND ccml1.rgst_date < date''{DAY}'' + 1
                 AND ccml1.status = ''G''
                 AND ccml.cell_no = ccml1.container_no
                 AND ccml.status = ''A''
                 AND ccml.container_no = oc.case_no
                 AND oc.article_no = bap.article_no
                 AND oc.article_unit = bap.packing_unit
                 AND oc.article_no = tac.article_no
                 AND ccml1.row_id =
                      (SELECT MAX(ccml2.row_id)
                        FROM container_case_move_log ccml2
                       WHERE ccml1.container_no = ccml2.container_no
                         and ccml1.container_type = ccml2.container_type
                         and ccml1.container_date = ccml2.container_date
                         AND ccml2.status = ''G'')
              UNION ALL
              SELECT CASE
                       WHEN oo.operate_type IN (''P'', ''M'', ''C'') THEN
                        ooi.real_qty
                       ELSE
                        0
                     END qty,
                     ooi.article_no,
                     ccml1.rgst_name,
                     ccml.cell_no,
                     CASE
                       WHEN oo.operate_type IN (''B'', ''D'') THEN
                        ooi.container_no
                       ELSE
                        NULL
                     END b_qty,
                     CASE
                       WHEN oo.operate_type IN (''P'', ''M'', ''C'') THEN
                        ''P''
                       ELSE
                        ''B''
                     END operate_type,
                     ooi.real_qty * bap.packing_qty * tac.cost cost
                FROM container_case_move_log ccml1,
                     container_case_move_log ccml,
                     om_outstock             oo,
                     om_outstock_item        ooi,
                     bm_article_packing      bap,
                     tmp_article_cost        tac
               WHERE ccml1.rgst_date >= date''{DAY}''
                 AND ccml1.rgst_date < date''{DAY}'' + 1
                 AND ccml1.status = ''G''
                 AND ccml.status = ''A''
                 AND ccml.cell_no = ccml1.container_no
                 AND ooi.source_no = ''N''
                 AND oo.outstock_no = ooi.outstock_no
                 AND substr(ccml.container_no, 1, 10) = ooi.container_no
                 AND ooi.article_no = bap.article_no
                 AND ooi.article_unit = bap.packing_unit
                 AND ooi.article_no = tac.article_no
                 AND ccml1.row_id =
                     (SELECT MAX(ccml2.row_id)
                        FROM container_case_move_log ccml2
                       WHERE ccml1.container_no = ccml2.container_no
                         and ccml1.container_type = ccml2.container_type
                         and ccml1.container_date = ccml2.container_date
                         AND ccml2.status = ''G'')
              UNION ALL
              SELECT ipd.pack_qty qty,
                     ipd.article_no,
                     ccml1.rgst_name,
                     ccml1.cell_no,
                     NULL b_qty,
                     ''P1'' operate_type,
                     ipd.pack_qty * bap.packing_qty * tac.cost cost
                FROM container_case_move_log ccml1,
                     im_printing_d           ipd,
                     bm_article_packing      bap,
                     tmp_article_cost        tac
               WHERE ccml1.rgst_date >= date''{DAY}''
                 AND ccml1.rgst_date < date''{DAY}'' + 1
                 AND ccml1.status = ''G''
                 AND ccml1.container_no = ipd.container_no
                 AND ipd.article_no = bap.article_no
                 AND ipd.article_unit = bap.packing_unit
                 AND ipd.article_no = tac.article_no
                 AND NOT EXISTS
               (SELECT 1
                        FROM container_case_move_log ccml
                       WHERE ccml1.container_no = ccml.container_no
                         and ccml1.container_type = ccml.container_type
                         and ccml1.container_date = ccml.container_date
                         AND ccml.status = ''A'')
                 AND ccml1.row_id =
                     (SELECT MAX(ccml2.row_id)
                        FROM container_case_move_log ccml2
                       WHERE ccml1.container_no = ccml2.container_no
                         and ccml1.container_type = ccml2.container_type
                         and ccml1.container_date = ccml2.container_date
                         AND ccml2.status = ''G'')
              UNION ALL
              SELECT ooi.real_qty qty,
                     ooi.article_no,
                     ccml1.rgst_name,
                     ccml1.cell_no,
                     NULL b_qty,
                     ''P2'' operate_type,
                     ooi.real_qty * bap.packing_qty * tac.cost cost
                FROM container_case_move_log ccml1,
                     om_outstock             oo,
                     om_outstock_item        ooi,
                     bm_article_packing      bap,
                     tmp_article_cost        tac
               WHERE ccml1.rgst_date >= date''{DAY}''
                 AND ccml1.rgst_date < date''{DAY}'' + 1
                 AND ccml1.status = ''G''
                 AND ooi.source_no = ''N''
                 AND oo.outstock_no = ooi.outstock_no
                 AND substr(ccml1.container_no, 1, 10) = ooi.container_no
                 AND ooi.article_no = bap.article_no
                 AND ooi.article_unit = bap.packing_unit
                 AND ooi.article_no = tac.article_no
                 AND NOT EXISTS
               (SELECT 1
                        FROM container_case_move_log ccml
                       WHERE ccml1.container_no = ccml.container_no
                         and ccml1.container_type = ccml.container_type
                         and ccml1.container_date = ccml.container_date
                         AND ccml.status = ''A'')
                 AND ccml1.row_id =
                      (SELECT MAX(ccml2.row_id)
                        FROM container_case_move_log ccml2
                       WHERE ccml1.container_no = ccml2.container_no
                         and ccml1.container_type = ccml2.container_type
                         and ccml1.container_date = ccml2.container_date
                         AND ccml2.status = ''G'')) t', 'display', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (42, 4, 'ZB', '组板', 'SELECT date''{DAY}'' as days,
      ''{DC_ID}'' as dc ,
       ''{SYSTEM_ENNAME}'' as SYSTEM_NAME,
      ''{TOPIC_ENNAME}'' as METRIC_TOPIC,
     COUNT(DISTINCT t.cell_no) as dc_zb_bs_zs,
     COUNT(DISTINCT t.article_no) as dc_zb_skus_zs,
     COUNT(DISTINCT t.rgst_name) as dc_zb_rs,
     SUM(CASE WHEN t.operate_type = ''P'' THEN  t.qty  ELSE 0 END) as dc_zb_yb_xs,
     SUM(CASE WHEN t.operate_type = ''P'' THEN  t.cost ELSE  0 END) as dc_zb_yb_je,
     SUM(CASE WHEN t.operate_type = ''C'' THEN t.qty ELSE 0 END) as dc_zb_pb_xs,
     SUM(CASE WHEN t.operate_type = ''C'' THEN t.cost ELSE 0 END) as dc_zb_pb_je,
     COUNT(DISTINCT CASE WHEN t.operate_type = ''B'' THEN  b_qty ELSE  NULL END) as dc_zb_wlx_xs,
     SUM(CASE WHEN t.operate_type = ''B'' THEN t.cost  ELSE 0 END) as dc_zb_wlx_je
FROM (SELECT ipd.pack_qty qty,
         ipd.article_no,
         ccml.rgst_name,
         ccml.cell_no,
         NULL b_qty,
         ''P'' operate_type,
         ipd.pack_qty * bap.packing_qty * tac.cost cost
    FROM CRCWMS.container_case_move_log ccml,
         CRCWMS.im_printing_d           ipd,
         CRCWMS.bm_article_packing      bap,
         CRCWMS.tmp_article_cost        tac
   WHERE ccml.rgst_date >= date''{DAY}''
     AND ccml.rgst_date < date''{DAY}'' + 1
     AND ccml.status = ''A''
     AND ccml.container_no = ipd.container_no
     AND ipd.article_no = bap.article_no
     AND ipd.article_unit = bap.packing_unit
     AND ipd.article_no = tac.article_no
  UNION ALL
  SELECT icca.real_qty / icca.packing_qty qty,
         icca.article_no,
         ccml.rgst_name,
         ccml.cell_no,
         NULL b_qty,
         ''P'' operate_type,
         icca.real_qty * tac.cost cost
    FROM CRCWMS.container_case_move_log ccml,
         CRCWMS.im_container_case_allot icca,
         CRCWMS.container_case_relation car,
         CRCWMS.tmp_article_cost        tac
   WHERE ccml.rgst_date >= date''{DAY}''
     AND ccml.rgst_date < date''{DAY}'' + 1
     AND ccml.status = ''A''
     AND icca.container_no = car.oldcontainer_no
     AND ccml.container_no = car.newcontainer_no
     AND ccml.container_type != ''C''
     AND icca.article_no = tac.article_no
  UNION ALL
  SELECT 1 qty,
         oc.article_no,
         ccml.rgst_name,
         ccml.cell_no,
         NULL b_qty,
         ''C'' operate_type,
         bap.packing_qty * tac.cost cost
    FROM CRCWMS.container_case_move_log ccml,
         CRCWMS.om_case                 oc,
         CRCWMS.bm_article_packing      bap,
         CRCWMS.tmp_article_cost        tac
   WHERE ccml.rgst_date >= date''{DAY}''
     AND ccml.rgst_date < date''{DAY}'' + 1
     AND ccml.status = ''A''
     AND ccml.container_no = oc.case_no
     AND oc.article_no = bap.article_no
     AND oc.article_unit = bap.packing_unit
     AND oc.article_no = tac.article_no
  UNION ALL
  SELECT CASE
           WHEN oo.operate_type IN (''P'', ''M'', ''C'') THEN
            ooi.real_qty
           ELSE
            0
         END qty,
         ooi.article_no,
         ccml.rgst_name,
         ccml.cell_no,
         CASE
           WHEN oo.operate_type IN (''B'', ''D'') THEN
            ooi.container_no
           ELSE
            NULL
         END b_qty,
         CASE
           WHEN oo.operate_type IN (''P'', ''M'', ''C'') THEN
            ''P''
           ELSE
            ''B''
         END operate_type,
         ooi.real_qty * bap.packing_qty * tac.cost cost
    FROM CRCWMS.container_case_move_log ccml,
         CRCWMS.om_outstock             oo,
         CRCWMS.om_outstock_item        ooi,
         CRCWMS.bm_article_packing      bap,
         CRCWMS.tmp_article_cost        tac
   WHERE ccml.rgst_date >= date''{DAY}''
     AND ccml.rgst_date < date''{DAY}'' + 1
     AND ccml.status = ''A''
     AND ooi.source_no = ''N''
     AND oo.outstock_no = ooi.outstock_no
     AND substr(ccml.container_no, 1, 10) = ooi.container_no
     AND ooi.article_no = bap.article_no
     AND ooi.article_unit = bap.packing_unit
     AND ooi.article_no = tac.article_no) t', 'display', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (43, 4, 'YS_1', '验收1', 'SELECT date''{DAY}'' as days,
         ''{DC_ID}'' as dc ,
          ''{SYSTEM_ENNAME}'' as SYSTEM_NAME,
      ''{TOPIC_ENNAME}'' as METRIC_TOPIC,
         SUM(CASE WHEN t.po_type = ''ID''  AND ipd.label_type = ''22'' THEN ipd.pack_qty * bap.packing_qty * tac.cost ELSE 0 END) as dc_ys_zt_zh_je,
         SUM(CASE WHEN t.po_type = ''ID'' AND ipd.label_type = ''22'' THEN ipd.pack_qty ELSE 0 END) as dc_ys_zt_zh_xs,
         COUNT(DISTINCT CASE WHEN t.po_type = ''ID'' AND ipd.label_type = ''22'' THEN  ipd.article_no ELSE NULL END) as dc_ys_zt_zh_skus,
         COUNT(CASE WHEN t.po_type = ''ID''  AND ipd.label_type = ''22'' THEN ipd.container_no  ELSE NULL END) as dc_ys_zt_zh_bqs,
         SUM(CASE WHEN t.po_type = ''ID''  AND ipd.label_type = ''03'' THEN ipd.pack_qty * bap.packing_qty * tac.cost  ELSE 0  END) as dc_ys_zt_xx_je,
         SUM(CASE WHEN t.po_type = ''ID'' AND ipd.label_type = ''03'' THEN  ipd.pack_qty  ELSE  0  END) as dc_ys_zt_xx_xs,
         COUNT(DISTINCT CASE  WHEN t.po_type = ''ID''  AND ipd.label_type = ''03'' THEN ipd.article_no  ELSE NULL END) as dc_ys_zt_xx_skus,
         COUNT(CASE  WHEN t.po_type = ''ID'' AND ipd.label_type = ''03'' THEN ipd.container_no ELSE NULL  END) as dc_ys_zt_xx_bqs,
         SUM(CASE WHEN t.po_type = ''ID'' AND ipd.label_type = ''21'' THEN ipd.pack_qty * bap.packing_qty * tac.cost  ELSE 0  END) as dc_ys_zt_bx_je,
         SUM(CASE WHEN t.po_type = ''ID''  AND ipd.label_type = ''21'' THEN ipd.pack_qty ELSE  0  END) as dc_ys_zt_bx_xs,
         COUNT(DISTINCT CASE WHEN t.po_type = ''ID''  AND ipd.label_type = ''21'' THEN ipd.article_no ELSE  NULL  END) as dc_ys_zt_bx_skus ,
         COUNT(CASE WHEN t.po_type = ''ID''  AND ipd.label_type = ''21'' THEN ipd.container_no ELSE  NULL  END) as dc_ys_zt_bx_bqs,
         SUM(CASE  WHEN t.po_type = ''IS'' THEN ipd.pack_qty * bap.packing_qty * tac.cost ELSE 0 END) as dc_ys_cc_je,
         SUM(CASE WHEN t.po_type = ''IS'' THEN  ipd.pack_qty ELSE  0  END) as dc_ys_cc_xs,
         COUNT(DISTINCT CASE  WHEN t.po_type = ''IS'' THEN ipd.article_no  ELSE NULL END) as dc_ys_cc_skus,
         COUNT(CASE  WHEN t.po_type = ''IS'' THEN ipd.container_no ELSE  NULL END) as dc_ys_cc_bqs
    FROM (SELECT DISTINCT iim.po_type,
                          icm.s_check_no
            FROM CRCWMS.im_import_m  iim,
                 CRCWMS.im_check_m   icm
           WHERE iim.import_no = icm.import_no
             AND icm.check_start_date >= date''{DAY}''
             AND icm.check_start_date < date''{DAY}'' + 1
             AND icm.status = 13) t,
         CRCWMS.im_printing_d  ipd,
         CRCWMS.bm_article_packing  bap,
         CRCWMS.tmp_article_cost  tac
   WHERE t.s_check_no = ipd.source_no
     AND ipd.status = 13
     AND t.po_type NOT IN (''ZT'', ''PT'')
     AND ipd.article_no = bap.article_no
     AND ipd.article_unit = bap.packing_unit
     AND ipd.article_no = tac.article_no
     AND NOT EXISTS (SELECT 1
            FROM CRCWMS.container_case_status  ccs
           WHERE ccs.status IN (''D'')
             AND ipd.container_no = ccs.container_no
             AND ipd.container_date = ccs.container_date)
     AND NOT EXISTS
   (SELECT 1
            FROM CRCWMS.container_case_status  ccs,
                 CRCWMS.om_case                oc
           WHERE ccs.status IN (''D'')
             AND oc.case_no = ccs.container_no
             AND oc.source_no = ipd.source_no
             AND oc.outstock_container_no = ipd.check_no
             AND substr(oc.case_no, 1, 10) = ipd.container_no)', 'display', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (44, 4, 'YS_2', '验收2', 'SELECT date''{DAY}'' as days,
         ''{DC_ID}'' as dc ,
          ''{SYSTEM_ENNAME}'' as SYSTEM_NAME,
          ''{TOPIC_ENNAME}'' as METRIC_TOPIC,
         COUNT(DISTINCT icm.supplier_no) as dc_ys_gyss,
         COUNT(DISTINCT icd.article_no) as dc_ys_skus,
         COUNT(DISTINCT icm.rgst_name) as dc_ys_rs
    FROM CRCWMS.im_check_m icm,
         CRCWMS.im_check_d icd
   WHERE icm.check_no = icd.check_no
     AND icm.check_start_date >=  date''{DAY}''
     AND icm.check_start_date <  date''{DAY}'' + 1', 'display', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (45, 4, 'ZCH', '装车', 'SELECT date''{DAY}'' as days,
       ''{DC_ID}'' as dc,
        ''{SYSTEM_ENNAME}'' as SYSTEM_NAME,
      ''{TOPIC_ENNAME}'' as METRIC_TOPIC,
       COUNT(DISTINCT t.cell_no) as dc_zch_bs_zs,
       COUNT(DISTINCT t.article_no) as dc_zch_skus_zs,
       COUNT(DISTINCT t.rgst_name) as dc_zch_rs,

       SUM(CASE WHEN t.operate_type IN (''P'', ''C'') THEN t.qty  ELSE  0  END) as dc_zch_yx_xs,
       SUM(CASE WHEN t.operate_type IN (''P'', ''C'') THEN   t.cost  ELSE 0  END) as dc_zch_yx_je,

       COUNT(DISTINCT CASE WHEN t.operate_type = ''B'' THEN b_qty  ELSE  NULL END) as dc_zch_wlx_xs,
       SUM(CASE  WHEN t.operate_type = ''B'' THEN  t.cost ELSE   0  END) as dc_zch_wlx_je,

       COUNT(DISTINCT CASE WHEN t.operate_type = ''P1'' THEN  b_qty ELSE  NULL END) as dc_zch_zbys_xs,
       SUM(CASE WHEN t.operate_type = ''P1'' THEN  t.cost  ELSE   0  END) as dc_zch_zbys_je,
       COUNT(DISTINCT CASE WHEN t.operate_type = ''P1'' THEN t.cell_no  ELSE  NULL END) as dc_zch_zbys_bs,

       COUNT(DISTINCT CASE WHEN t.operate_type = ''P2'' THEN b_qty  ELSE NULL END) as dc_zch_zbxj_xs,
       SUM(CASE WHEN t.operate_type = ''P2'' THEN  t.cost ELSE  0 END) as dc_zch_zbxj_je,
       COUNT(DISTINCT CASE  WHEN t.operate_type = ''P2'' THEN  t.cell_no  ELSE NULL  END) as dc_zch_zbxj_bs
FROM (SELECT ipd.pack_qty qty,
     ipd.article_no,
     ccml1.rgst_name,
     ccml.cell_no,
     NULL b_qty,
     ''P'' operate_type,
     ipd.pack_qty * bap.packing_qty * tac.cost cost
FROM container_case_move_log ccml1,
     container_case_move_log ccml,
     im_printing_d           ipd,
     bm_article_packing      bap,
     tmp_article_cost        tac
WHERE ccml1.rgst_date >= date''{DAY}''
 AND ccml1.rgst_date < date''{DAY}'' + 1
 AND ccml1.status = ''L''
 AND ccml.cell_no = ccml1.container_no
 AND ccml.status = ''A''
 AND ccml.container_no = ipd.container_no
 AND ipd.article_no = bap.article_no
 AND ipd.article_unit = bap.packing_unit
 AND ipd.article_no = tac.article_no
 AND ccml1.row_id =
     (SELECT MAX(ccml2.row_id)
        FROM container_case_move_log ccml2
       WHERE ccml1.container_no = ccml2.container_no
         and ccml1.container_type = ccml2.container_type
         and ccml1.container_date = ccml2.container_date
         AND ccml2.status = ''L'')
UNION ALL
SELECT icca.real_qty / icca.packing_qty qty,
     icca.article_no,
     ccml1.rgst_name,
     ccml.cell_no,
     NULL b_qty,
     ''P'' operate_type,
     icca.real_qty * tac.cost cost
FROM container_case_move_log ccml1,
     container_case_move_log ccml,
     im_container_case_allot icca,
     container_case_relation car,
     tmp_article_cost        tac
WHERE ccml1.rgst_date >= date''{DAY}''
 AND ccml1.rgst_date < date''{DAY}'' + 1
 AND ccml1.status = ''L''
 AND ccml.cell_no = ccml1.container_no
 AND ccml.status = ''A''
 AND icca.container_no = car.oldcontainer_no
 AND ccml.container_no = car.newcontainer_no
 AND ccml.container_type != ''C''
 AND icca.article_no = tac.article_no
 AND ccml1.row_id =
      (SELECT MAX(ccml2.row_id)
        FROM container_case_move_log ccml2
       WHERE ccml1.container_no = ccml2.container_no
         and ccml1.container_type = ccml2.container_type
         and ccml1.container_date = ccml2.container_date
         AND ccml2.status = ''L'')
UNION ALL
SELECT 1 qty,
     oc.article_no,
     ccml1.rgst_name,
     ccml.cell_no,
     NULL b_qty,
     ''C'' operate_type,
     bap.packing_qty * tac.cost cost
FROM container_case_move_log ccml1,
     container_case_move_log ccml,
     om_case                 oc,
     bm_article_packing      bap,
     tmp_article_cost        tac
WHERE ccml1.rgst_date >= date''{DAY}''
 AND ccml1.rgst_date < date''{DAY}'' + 1
 AND ccml1.status = ''L''
 AND ccml.cell_no = ccml1.container_no
 AND ccml.status = ''A''
 AND ccml.container_no = oc.case_no
 AND oc.article_no = bap.article_no
 AND oc.article_unit = bap.packing_unit
 AND oc.article_no = tac.article_no
 AND ccml1.row_id =
      (SELECT MAX(ccml2.row_id)
        FROM container_case_move_log ccml2
       WHERE ccml1.container_no = ccml2.container_no
         and ccml1.container_type = ccml2.container_type
         and ccml1.container_date = ccml2.container_date
         AND ccml2.status = ''L'')
UNION ALL
SELECT CASE
       WHEN oo.operate_type IN (''P'', ''M'', ''C'') THEN
        ooi.real_qty
       ELSE
        0
     END qty,
     ooi.article_no,
     ccml1.rgst_name,
     ccml.cell_no,
     CASE
       WHEN oo.operate_type IN (''B'', ''D'') THEN
        ooi.container_no
       ELSE
        NULL
     END b_qty,
     CASE
       WHEN oo.operate_type IN (''P'', ''M'', ''C'') THEN
        ''P''
       ELSE
        ''B''
     END operate_type,
     ooi.real_qty * bap.packing_qty * tac.cost cost
FROM container_case_move_log ccml1,
     container_case_move_log ccml,
     om_outstock             oo,
     om_outstock_item        ooi,
     bm_article_packing      bap,
     tmp_article_cost        tac
WHERE ccml1.rgst_date >= date''{DAY}''
 AND ccml1.rgst_date < date''{DAY}'' + 1
 AND ccml1.status = ''L''
 AND ccml.status = ''A''
 AND ccml.cell_no = ccml1.container_no
 AND ooi.source_no = ''N''
 AND oo.outstock_no = ooi.outstock_no
 AND substr(ccml.container_no, 1, 10) = ooi.container_no
 AND ooi.article_no = bap.article_no
 AND ooi.article_unit = bap.packing_unit
 AND ooi.article_no = tac.article_no
 AND ccml1.row_id =
     (SELECT MAX(ccml2.row_id)
        FROM container_case_move_log ccml2
       WHERE ccml1.container_no = ccml2.container_no
         and ccml1.container_type = ccml2.container_type
         and ccml1.container_date = ccml2.container_date
         AND ccml2.status = ''L'')
UNION ALL
SELECT ipd.pack_qty qty,
     ipd.article_no,
     ccml1.rgst_name,
     ccml1.cell_no,
     NULL b_qty,
     ''P1'' operate_type,
     ipd.pack_qty * bap.packing_qty * tac.cost cost
FROM container_case_move_log ccml1,
     im_printing_d           ipd,
     bm_article_packing      bap,
     tmp_article_cost        tac
WHERE ccml1.rgst_date >= date''{DAY}''
 AND ccml1.rgst_date < date''{DAY}'' + 1
 AND ccml1.status = ''L''
 AND ccml1.container_no = ipd.container_no
 AND ipd.article_no = bap.article_no
 AND ipd.article_unit = bap.packing_unit
 AND ipd.article_no = tac.article_no
 AND NOT EXISTS
(SELECT 1
        FROM container_case_move_log ccml
       WHERE ccml1.container_no = ccml.container_no
         and ccml1.container_type = ccml.container_type
         and ccml1.container_date = ccml.container_date
         AND ccml.status = ''A'')
 AND ccml1.row_id =
     (SELECT MAX(ccml2.row_id)
        FROM container_case_move_log ccml2
       WHERE ccml1.container_no = ccml2.container_no
         and ccml1.container_type = ccml2.container_type
         and ccml1.container_date = ccml2.container_date
         AND ccml2.status = ''L'')
UNION ALL
SELECT ooi.real_qty qty,
     ooi.article_no,
     ccml1.rgst_name,
     ccml1.cell_no,
     NULL b_qty,
     ''P2'' operate_type,
     ooi.real_qty * bap.packing_qty * tac.cost cost
FROM container_case_move_log ccml1,
     om_outstock             oo,
     om_outstock_item        ooi,
     bm_article_packing      bap,
     tmp_article_cost        tac
WHERE ccml1.rgst_date >= date''{DAY}''
 AND ccml1.rgst_date < date''{DAY}'' + 1
 AND ccml1.status = ''L''
 AND ooi.source_no = ''N''
 AND oo.outstock_no = ooi.outstock_no
 AND substr(ccml1.container_no, 1, 10) = ooi.container_no
 AND ooi.article_no = bap.article_no
 AND ooi.article_unit = bap.packing_unit
 AND ooi.article_no = tac.article_no
 AND NOT EXISTS
(SELECT 1
        FROM container_case_move_log ccml
       WHERE ccml1.container_no = ccml.container_no
         and ccml1.container_type = ccml.container_type
         and ccml1.container_date = ccml.container_date
         AND ccml.status = ''A'')
 AND ccml1.row_id =
      (SELECT MAX(ccml2.row_id)
        FROM container_case_move_log ccml2
       WHERE ccml1.container_no = ccml2.container_no
         and ccml1.container_type = ccml2.container_type
         and ccml1.container_date = ccml2.container_date
         AND ccml2.status = ''L'')) t
', 'display', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (46, 4, 'DC_OPERATION_METRIC', '运作指标', 'select OPERATE_DATE,
       ''{DC_ID}'' as DC_ID,
       ''{TOPIC_ENNAME}'' as METRIC_TOPIC,
       ''{SYSTEM_ENNAME}'' as SYSTEM_NAME,
      INVENTORY_BOX,
      INVENTORY_COST,
      INVENTORY_RECEDE_BOX,
      INVENTORY_RECEDE_COST,
      INVENTORY_UNUSUAL_BOX,
      INVENTORY_UNUSUAL_COST,
      ORDER_COUNT,
      BOOKING_ORDER_COUNT,
      BOOKING_NEXT_ORDER_COUNT,
      BOOKING_ORDER_BOX,
      BOOKING_NEXT_ORDER_BOX,
      CHECK_ORDER_COUNT,
      CHECK_ORDER_ID_BOX,
      CHECK_ORDER_BOX,
      CHECK_ORDER_COST,
      DELIVER_IS_BOX,
      DELIVER_BOX,
      DELIVER_IS_COST,
      DELIVER_COST,
      UNLOADCONTAINER_COUNT,
      UNTREAD_UNCHECK_COUNT,
      UNTREAD_CHECK_COST,
      RET_COST,
      CHECK_IS_PAL_COUNT,
      INSTOCK_PAL_COUNT,
      UNINSTOCK_PAL_COUNT,
      OUTSTOCK_DIRECT_C_BOX,
      OUTSTOCK_DIRECT_B_COUNT,
      OUTSTOCK_C_BOX,
      OUTSTOCK_QTY,
      OUTSTOCK_BOX,
      OUTSTOCK_B_COUNT,
      OUTSTOCK_PO_QTY,
      OUTSTOCK_UN_C_BOX,
      OUTSTOCK_UN_B_COUNT,
      CB_PARALLELBOARD_CASE,
      CHECK_PARALLELBOARD_BOX,
      OUTSTOCK_PARALLELBOARD_BOX,
      UN_PARALLELBOARD_BOX,
      ONTIME_DELIVER_BOX,
      UNCHECK_DELIVER_COUNT,
      UNBOOKING_ORDER_BOX
  from CRCWMS.DC_OPERATION_DATA
 where OPERATE_DATE >= to_date(''{DAY}'', ''yyyy-mm-dd'')
   and OPERATE_DATE  < to_date(''{DAY}'', ''yyyy-mm-dd'') + 1', 'display', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (47, 5, 'DC_OPERATION_METRIC', '运作指标', 'select OPERATE_DATE,
      DC_ID,
          ''{TOPIC_ENNAME}'' as METRIC_TOPIC,
       ''{SYSTEM_ENNAME}'' as SYSTEM_NAME,
      INVENTORY_BOX,
      INVENTORY_COST,
      INVENTORY_RECEDE_BOX,
      INVENTORY_RECEDE_COST,
      INVENTORY_UNUSUAL_BOX,
      INVENTORY_UNUSUAL_COST,
      ORDER_COUNT,
      BOOKING_ORDER_COUNT,
      BOOKING_NEXT_ORDER_COUNT,
      BOOKING_ORDER_BOX,
      BOOKING_NEXT_ORDER_BOX,
      CHECK_ORDER_COUNT,
      CHECK_ORDER_ID_BOX,
      CHECK_ORDER_BOX,
      CHECK_ORDER_COST,
      DELIVER_IS_BOX,
      DELIVER_BOX,
      DELIVER_IS_COST,
      DELIVER_COST,
      UNLOADCONTAINER_COUNT,
      UNTREAD_UNCHECK_COUNT,
      UNTREAD_CHECK_COST,
      RET_COST,
      CHECK_IS_PAL_COUNT,
      INSTOCK_PAL_COUNT,
      UNINSTOCK_PAL_COUNT,
      OUTSTOCK_DIRECT_C_BOX,
      OUTSTOCK_DIRECT_B_COUNT,
      OUTSTOCK_C_BOX,
      OUTSTOCK_QTY,
      OUTSTOCK_BOX,
      OUTSTOCK_B_COUNT,
      OUTSTOCK_PO_QTY,
      OUTSTOCK_UN_C_BOX,
      OUTSTOCK_UN_B_COUNT,
      CB_PARALLELBOARD_CASE,
      CHECK_PARALLELBOARD_BOX,
      OUTSTOCK_PARALLELBOARD_BOX,
      UN_PARALLELBOARD_BOX,
      ONTIME_DELIVER_BOX,
      UNCHECK_DELIVER_COUNT,
      UNBOOKING_ORDER_BOX
  from  NWMS_WMS.DC_OPERATION_DATA
 where OPERATE_DATE >= to_date(''{DAY}'', ''yyyy-mm-dd'')
   and OPERATE_DATE  < to_date(''{DAY}'', ''yyyy-mm-dd'') + 1', 'display', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (48, 8, 'SALESHIP_METRIC', '提货配送指标-散数，金额', 'SELECT  ''{DAY}''as stat_day_s,
        ''{TOPIC_ENNAME}'' as metric_topic,
       ''{SYSTEM_ENNAME}'' as metric_source,
        to_char(t.executedate, ''yyyymmdd'') as metric_date,
        t.outshopid as  metric_key  ,
        ''day'' as agg_type,
       ''{"cost":'' || sum(t1.cost * t1.qty) || '','' || ''"qty":'' || sum(t1.qty)|| ''}''  as metric_value
FROM dw.saleship    t,
     dw.saleshipacc t1,
     dw.jv_shop     t3
WHERE t.sheetid = t1.sheetid
 AND t.outshopid = t3.shopid
 AND t.executedate >= trunc(sysdate -91, ''MONTH'')
 AND t.executedate <= trunc(sysdate) -1
 AND t3.shopformid = 9
 group by to_char(t.executedate, ''yyyymmdd''),  t.outshopid
', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (49, 9, 'SALESHIP_METRIC', '提货配送指标-散数，金额', 'select stat_day_s,
       metric_topic,
       ''{SYSTEM_ENNAME}'' as metric_source,
       metric_date,
       metric_key,
       agg_type,
       concat(''{"cost":'',toString(cost),'','', ''"qty":'',toString(qty),'','',''"box":'',toString(boxes), ''}'') as metric_value
 from  (select ''{DAY}'' as stat_day_s,
                 ''{TOPIC_ENNAME}'' as metric_topic,
               toYYYYMMDD(frdssvd.stat_day) as metric_date,
               frdssvd.out_shop_id as metric_key,
               ''day'' as agg_type,
               sum(frdssvd.rt_cost) as cost,
               sum(frdssvd.rt_qty) as qty,
               sum(frdssvd.rt_boxes) boxes
          from dw_hr.fct_rt_dc_shop_sku_vender_day frdssvd
        where frdssvd.stat_day  >= toStartOfMonth(yesterday() -90)
          and frdssvd.stat_day  <= yesterday()
          and bun_type_id = 4
        group by metric_date,
                 metric_key,
                 metric_topic
            )', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (50, 10, 'SALESHIP_METRIC', '提货配送指标-散数，金额', 'select stat_day_s,
       metric_topic,
       ''{SYSTEM_ENNAME}'' as metric_source,
       metric_date,
       metric_key,
       agg_type,
       concat(''{"cost":'',toString(cost),'','', ''"qty":'',toString(qty),'','',''"box":'',toString(boxes), ''}'') as metric_value
 from  (select ''{DAY}'' as stat_day_s,
               ''{TOPIC_ENNAME}'' as metric_topic,
               frdssvd.stat_month as metric_date,
               frdssvd.out_shop_id as metric_key,
               ''month'' as agg_type,
               sum(frdssvd.rt_cost) as cost,
               sum(frdssvd.rt_qty) as qty,
               sum(frdssvd.rt_boxes) boxes
          from dw_hr.fct_rt_dc_shop_sku_vender_mon frdssvd
        where frdssvd.stat_month  >= toYYYYMM(toStartOfMonth(yesterday() -90))
          and frdssvd.stat_month  <= toYYYYMM(yesterday())
          and frdssvd.bun_type_id = 4
        group by metric_date,
                 metric_key,
                 metric_topic
            )', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (51, 8, 'TRAN_METRIC', '调拨配送指标-散数，金额', 'select  ''{DAY}'' as stat_day_s,
         ''{TOPIC_ENNAME}'' as metric_topic,
       ''{SYSTEM_ENNAME}'' as metric_source,
        to_char(t.outexedate, ''yyyymmdd'') as metric_date,
        t.outshopid as  metric_key  ,
        ''day'' as agg_type,
       ''{"cost":'' || sum(t2.cost * t2.qty) || '','' || ''"qty":'' || sum(t2.qty)|| ''}''  as metric_value
  FROM dw.tran     t,
       dw.tranacc  t2,
       dw.jv_shop  t3
 WHERE t.sheetid = t2.sheetid
   AND t.outshopid = t3.shopid
   AND t.outexedate >= trunc(sysdate -91, ''MONTH'')
   AND t.outexedate  <= trunc(sysdate) -1
   AND t3.shopformid = 9
   group by   to_char(t.outexedate, ''yyyymmdd''),  t.outshopid', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (52, 9, 'TRAN_METRIC', '调拨配送指标-散数，金额', 'select stat_day_s,
       metric_topic,
       ''{SYSTEM_ENNAME}'' as metric_source,
       metric_date,
       metric_key,
       agg_type,
       concat(''{"cost":'',toString(cost),'','', ''"qty":'',toString(qty),'','',''"box":'',toString(boxes), ''}'') as metric_value
 from  (select ''{DAY}'' as stat_day_s,
                 ''{TOPIC_ENNAME}'' as metric_topic,
               toYYYYMMDD(frdssvd.stat_day) as metric_date,
               frdssvd.out_shop_id as metric_key,
               ''day'' as agg_type,
               sum(frdssvd.rt_cost) as cost,
               sum(frdssvd.rt_qty) as qty,
               sum(frdssvd.rt_boxes) boxes
          from dw_hr.fct_rt_dc_shop_sku_vender_day frdssvd
        where frdssvd.stat_day  >= toStartOfMonth(yesterday() -90)
          and frdssvd.stat_day  <= yesterday()
          and bun_type_id = 2
        group by metric_date,
                 metric_key,
                 metric_topic
            )', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (53, 10, 'TRAN_METRIC', '调拨配送指标-散数，金额', 'select stat_day_s,
       metric_topic,
       ''{SYSTEM_ENNAME}'' as metric_source,
       metric_date,
       metric_key,
       agg_type,
       concat(''{"cost":'',toString(cost),'','', ''"qty":'',toString(qty),'','',''"box":'',toString(boxes), ''}'') as metric_value
 from  (select ''{DAY}'' as stat_day_s,
               ''{TOPIC_ENNAME}'' as metric_topic,
               frdssvd.stat_month as metric_date,
               frdssvd.out_shop_id as metric_key,
               ''month'' as agg_type,
               sum(frdssvd.rt_cost) as cost,
               sum(frdssvd.rt_qty) as qty,
               sum(frdssvd.rt_boxes) boxes
          from dw_hr.fct_rt_dc_shop_sku_vender_mon frdssvd
        where frdssvd.stat_month  >= toYYYYMM(toStartOfMonth(yesterday() -90))
          and frdssvd.stat_month  <= toYYYYMM(yesterday())
          and frdssvd.bun_type_id = 2
        group by metric_date,
                 metric_key,
                 metric_topic
            )', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (54, 8, 'WHOLESALE_METRIC', '批发配送指标-散数，金额', 'SELECT ''{DAY}'' as stat_day_s,
        ''{TOPIC_ENNAME}'' as metric_topic,
       ''{SYSTEM_ENNAME}'' as metric_source,
        to_char(t.outexedate, ''yyyymmdd'') as metric_date,
        t.shopid  as  metric_key  ,
        ''day'' as agg_type,
       ''{"cost":'' || sum(t2.cost * t2.qty) || '','' || ''"qty":'' || sum(t2.qty)|| ''}''  as metric_value
    FROM dw.wholesale      t,
         dw.jv_shop        t1,
         dw.wholesaleacc   t2,
         dw.wholeguesttran t3
   WHERE t.sheetid = t2.sheetid
     AND t.shopid = t3.outshopid
     AND t.customerid = t3.oleguestid
     AND t.outexedate >= trunc(sysdate -91, ''MONTH'')
     AND t.outexedate  < trunc(sysdate) -1
     AND t.shopid = t1.shopid
     AND t1.shopformid = 9
  group by to_char(t.outexedate, ''yyyymmdd''),  t.shopid', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (55, 9, 'WHOLESALE_METRIC', '批发配送指标-散数，金额', 'select stat_day_s,
       metric_topic,
       ''{SYSTEM_ENNAME}'' as metric_source,
       metric_date,
       metric_key,
       agg_type,
       concat(''{"cost":'',toString(cost),'','', ''"qty":'',toString(qty),'','',''"box":'',toString(boxes), ''}'') as metric_value
 from  (select ''{DAY}'' as stat_day_s,
                 ''{TOPIC_ENNAME}'' as metric_topic,
               toYYYYMMDD(frdssvd.stat_day) as metric_date,
               frdssvd.out_shop_id as metric_key,
               ''day'' as agg_type,
               sum(frdssvd.rt_cost) as cost,
               sum(frdssvd.rt_qty) as qty,
               sum(frdssvd.rt_boxes) boxes
          from dw_hr.fct_rt_dc_shop_sku_vender_day frdssvd
        where frdssvd.stat_day  >= toStartOfMonth(yesterday() -90)
          and frdssvd.stat_day  <= yesterday()
          and bun_type_id = 3
        group by metric_date,
                 metric_key,
                 metric_topic
            )', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (56, 10, 'WHOLESALE_METRIC', '批发配送指标-散数，金额', 'select stat_day_s,
       metric_topic,
       ''{SYSTEM_ENNAME}'' as metric_source,
       metric_date,
       metric_key,
       agg_type,
       concat(''{"cost":'',toString(cost),'','', ''"qty":'',toString(qty),'','',''"box":'',toString(boxes), ''}'') as metric_value
 from  (select ''{DAY}'' as stat_day_s,
               ''{TOPIC_ENNAME}'' as metric_topic,
               frdssvd.stat_month as metric_date,
               frdssvd.out_shop_id as metric_key,
               ''month'' as agg_type,
               sum(frdssvd.rt_cost) as cost,
               sum(frdssvd.rt_qty) as qty,
               sum(frdssvd.rt_boxes) boxes
          from dw_hr.fct_rt_dc_shop_sku_vender_mon frdssvd
        where frdssvd.stat_month  >= toYYYYMM(toStartOfMonth(yesterday() -90))
          and frdssvd.stat_month  <= toYYYYMM(yesterday())
          and frdssvd.bun_type_id = 3
        group by metric_date,
                 metric_key,
                 metric_topic
            )', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (57, 8, 'RPT_METRIC', '验收指标-散数，金额', 'select ''{DAY}'' as stat_day_s,
          ''{SYSTEM_ENNAME}'' as metric_source,
          ''{TOPIC_ENNAME}'' as metric_topic,
         to_char(t1.checkdate, ''yyyymmdd'') as metric_date,
         t1.shopid as  metric_key  ,
        ''day'' as agg_type,
       ''{"cost":'' || sum(t2.cost * t2.qty) || '','' || ''"qty":'' || sum(t2.qty)|| ''}''  as metric_value
    from dw.receipt t1,
       dw.receiptitem t2
    where t1.sheetid = t2.sheetid
    and  t1.checkdate >= trunc(sysdate -91, ''MONTH'')
    AND t1.checkdate  <= trunc(sysdate) -1
    and t1.shopid != ''995045''
    group by to_char(t1.checkdate, ''yyyymmdd''), t1.shopid
UNION ALL
     select
           ''{DAY}'' as stat_day_s,
          ''{SYSTEM_ENNAME}'' as metric_source,
          ''{TOPIC_ENNAME}'' as metric_topic,
         to_char(a.sdate, ''yyyymmdd'') as metric_date,
         a.shopid as  metric_key  ,
        ''day'' as agg_type,
       ''{"cost":'' || sum(b.costvalue + b.costtaxvalue) || '','' || ''"qty":'' || sum(b.qty)|| ''}''  as metric_value
     FROM dw.acc_jv_receipt     a,
          dw.acc_jv_receiptitem b
    WHERE a.sheetid = b.sheetid
      AND a.sheetid NOT LIKE ''CT%''
      AND a.sdate >= trunc(sysdate -91, ''MONTH'')
      AND a.sdate <= trunc(sysdate) -1
    group by to_char(a.sdate, ''yyyymmdd''), a.shopid

UNION ALL
     select  ''{DAY}'' as stat_day_s,
               ''{SYSTEM_ENNAME}'' as metric_source,
              ''{TOPIC_ENNAME}'' as metric_topic,
             tt.check_date as metric_date,
             tt.shop_id as  metric_key  ,
            ''day'' as agg_type,
           ''{"cost":'' || sum(tt.check_taxamount) || '','' || ''"qty":'' || sum(tt.check_taxamount)|| ''}''  as metric_value
      from dw.sg_check_t tt
    where  tt.logistics not in (''2退货'')
     and tt.check_date >= to_char(trunc(sysdate -91, ''MONTH''), ''yyyymmdd'')
      and tt.check_date <= to_char(trunc(sysdate) -1, ''yyyymmdd'')
    group by tt.check_date, tt.shop_id
union all
     select  ''{DAY}'' as stat_day_s,
             ''{SYSTEM_ENNAME}'' as metric_source,
              ''{TOPIC_ENNAME}'' as metric_topic,
             tt.check_date as metric_date,
             tt.shop_id as  metric_key  ,
            ''day'' as agg_type,
           ''{"cost":'' || sum(tt.cost * tt.check_scatter_num ) || '','' || ''"qty":'' || sum(tt.check_scatter_num)|| ''}''  as metric_value
      from dw.xg_check_t tt
    where  tt.logistics in (''G'', ''F'', ''T'')
     and tt.check_date >= to_char(trunc(sysdate -91, ''MONTH''), ''yyyymmdd'')
     and tt.check_date <= to_char(trunc(sysdate) -1, ''yyyymmdd'')
    group by tt.check_date, tt.shop_id', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (58, 9, 'RPT_METRIC', '验收指标-散数，金额', 'select stat_day_s,
       metric_topic,
        ''{SYSTEM_ENNAME}'' as metric_source,
       metric_date,
       metric_key,
       agg_type,
       concat(''{"cost":'',toString(cost),'','', ''"qty":'',toString(qty),'','',''"box":'',toString(boxes), ''}'') as metric_value
 from  (select ''{DAY}'' as stat_day_s,
                ''{TOPIC_ENNAME}'' as metric_topic,
               toYYYYMMDD(frdssvd.stat_day) as metric_date,
               frdssvd.dc_shop_id as metric_key,
               ''day'' as agg_type,
               sum(frdssvd.rpt_cost) as cost,
               sum(frdssvd.rpt_qty) as qty,
               sum(frdssvd.rpt_boxes) boxes
          from dw_hr.fct_rpt_dc_shop_good_vender_day frdssvd
        where frdssvd.stat_day  >= toStartOfMonth(yesterday() -90)
          and frdssvd.stat_day  <= yesterday()
        group by metric_date,
                 metric_key,
                 metric_topic
            )', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (59, 10, 'RPT_METRIC', '验收指标-散数，金额', 'select stat_day_s,
       metric_topic,
     ''{SYSTEM_ENNAME}'' as metric_source,
       metric_date,
       metric_key,
       agg_type,
       concat(''{"cost":'',toString(cost),'','', ''"qty":'',toString(qty),'','',''"box":'',toString(boxes), ''}'') as metric_value
 from  (select ''{DAY}'' as stat_day_s,
                ''{TOPIC_ENNAME}'' as metric_topic,
               stat_month as metric_date,
               frdssvd.dc_shop_id as metric_key,
               ''month'' as agg_type,
               sum(frdssvd.rpt_cost) as cost,
               sum(frdssvd.rpt_qty) as qty,
               sum(frdssvd.rpt_boxes) boxes
          from dw_hr.fct_rpt_dc_shop_good_vender_mon frdssvd
        where frdssvd.stat_month  >= toYYYYMM(toStartOfMonth(yesterday() -90))
          and frdssvd.stat_month  <= toYYYYMM(yesterday())
        group by metric_date,
                 metric_key,
                 metric_topic
            )', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (60, 2, 'JV_RATION', 'JV_RATION记录数', 'SELECT ''{DAY}'' as stat_day_s,
        ''{TOPIC_ENNAME}'' as metric_topic,
        ''{SYSTEM_ENNAME}'' as metric_source,
        to_char(t.outexedate,''yyyymmdd'') as metric_date,
        t.outshopid as metric_key,
        ''day'' as agg_type,
        ''{"logs":'' || count(1) || ''}'' as metric_value
  FROM DBUSRJVDMS.ration t
 WHERE t.outexedate >= trunc(sysdate) - 90
   and t.outexedate <  trunc(sysdate)
   and t.outshopid in (''995063'', ''995045'', ''995062'', ''995061'', ''995088'');
group by to_char(t.outexedate,''yyyymmdd''),  t.outshopid', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (61, 2, 'JV_RATIONITEM', 'JV_RATIONITEM记录数', 'SELECT ''{DAY}'' as stat_day_s,
        ''{TOPIC_ENNAME}'' as metric_topic,
        ''{SYSTEM_ENNAME}'' as metric_source,
        to_char(t1.outexedate,''yyyymmdd'') as metric_date,
        t1.outshopid as metric_key,
        ''day'' as agg_type,
        ''{"logs":'' || count(1) || ''}'' as metric_value
 FROM DBUSRJVDMS.rationitem t,
       DBUSRJVDMS.ration t1
 WHERE t.sheetid = t1.sheetid
  and t1.outshopid in (''995063'', ''995045'', ''995062'', ''995061'', ''995088'')
   AND t1.outexedate >= trunc(sysdate) - 90
   AND t1.outexedate  < trunc(sysdate)
 GROUP BY to_char(t1.outexedate, ''yyyymmdd''), t1.outshopid', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (62, 8, 'JV_RATION', 'JV_RATION记录数', 'SELECT ''{DAY}'' as stat_day_s,
        ''{TOPIC_ENNAME}'' as metric_topic,
        ''{SYSTEM_ENNAME}'' as metric_source,
        to_char(t.outexedate,''yyyymmdd'') as metric_date,
        t.outshopid as metric_key,
        ''day'' as agg_type,
        ''{"logs":'' || count(1) || ''}'' as metric_value
  FROM dw.jv_ration t
 WHERE t.outexedate >= trunc(sysdate) - 90
   and t.outexedate <  trunc(sysdate)
   and t.OUTSHOPID in (''995063'', ''995045'', ''995062'', ''995061'', ''995088'')
group by to_char(t.outexedate,''yyyymmdd''),  t.outshopid', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (63, 8, 'JV_RATIONITEM', 'JV_RATIONITEM记录数', 'SELECT ''{DAY}'' as stat_day_s,
        ''{TOPIC_ENNAME}'' as metric_topic,
        ''{SYSTEM_ENNAME}'' as metric_source,
        to_char(t1.outexedate,''yyyymmdd'') as metric_date,
        t1.outshopid as metric_key,
        ''day'' as agg_type,
        ''{"logs":'' || count(1) || ''}'' as metric_value
 FROM dw.jv_rationitem t,
       dw.jv_ration t1
 WHERE t.sheetid = t1.sheetid
   AND t1.outexedate >= trunc(sysdate) - 90
   AND t1.outexedate  < trunc(sysdate)
  and t1.OUTSHOPID in (''995063'', ''995045'', ''995062'', ''995061'', ''995088'')
 GROUP BY to_char(t1.outexedate, ''yyyymmdd''), t1.outshopid', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (64, 5, 'OM_LOCATE_M', '定位主表', 'select LOCNO,
        LOCATE_NO,
        EXP_TYPE,
        STATUS,
        LOCATE_NAME,
        LOCATE_DATE,
        FAST_FLAG,
        DIVIDE_FLAG,
        SPECIFY_CELL,
        EXP_DATE,
        HM_MANUAL_FLAG,
        TASK_BATCH,
        SOURCE_TYPE,
        TMS_SEQID,
        ''{DC_ID}'' as SOURCE
  from NWMS_WMS.om_locate_m

union all

  select LOCNO,
        LOCATE_NO,
        EXP_TYPE,
        STATUS,
        LOCATE_NAME,
        LOCATE_DATE,
        FAST_FLAG,
        DIVIDE_FLAG,
        SPECIFY_CELL,
        EXP_DATE,
        HM_MANUAL_FLAG,
        TASK_BATCH,
        SOURCE_TYPE,
        TMS_SEQID,
         ''{DC_ID}'' as SOURCE
  from NWMS_WMS.om_locate_mhty
', 'etl', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (65, 5, 'OM_LOCATE_D', '定位明细表', 'select LOCNO,
      OWNER_NO,
      LOCATE_NO,
      ROW_ID,
      CUST_NO,
      SUB_CUST_NO,
      EXP_NO,
      ARTICLE_NO,
      PLAN_QTY,
      LOCATED_QTY,
      STATUS,
      LINE_NO,
      BATCH_NO,
      CONDITION,
      SPECIAL_BATCH,
      B_OUT_FLAG,
      PRIORITY,
      ADD_EXP_NO,
      ITEM_TYPE,
      EXP_DATE,
      PLAN_EXPORT_QTY,
      EXPORT_QTY,
      IMPORT_NO,
      STOCK_TYPE,
      QUALITY,
       ''{DC_ID}'' as source
 from NWMS_WMS.OM_LOCATE_D
union all
select LOCNO,
      OWNER_NO,
      LOCATE_NO,
      ROW_ID,
      CUST_NO,
      SUB_CUST_NO,
      EXP_NO,
      ARTICLE_NO,
      PLAN_QTY,
      LOCATED_QTY,
      STATUS,
      LINE_NO,
      BATCH_NO,
      CONDITION,
      SPECIAL_BATCH,
      B_OUT_FLAG,
      PRIORITY,
      ADD_EXP_NO,
      ITEM_TYPE,
      EXP_DATE,
      PLAN_EXPORT_QTY,
      EXPORT_QTY,
      IMPORT_NO,
      STOCK_TYPE,
      QUALITY,
       ''{DC_ID}'' as source
 from NWMS_WMS.OM_LOCATE_DHTY', 'etl', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (66, 4, 'OM_CASE', '原装箱明细', 'select OPERATE_DATE,
BATCH_NO,
SOURCE_NO,
OUTSTOCK_NO,
CASE_NO,
OWNER_NO,
CUSTOMER_NO,
ARTICLE_NO,
COLOR_CODE,
SIZE_CODE,
ARTICLE_UNIT,
QUALITY,
PRODUCE_DATE,
IMPORT_BATCH_NO,
SORT_NO,
CASE_STATUS,
RGST_NAME,
RGST_DATE,
UPDT_NAME,
UPDT_DATE,
OUTSTOCK_CONTAINER_DATE,
OUTSTOCK_CONTAINER_NO,
A_OUTSTOCK_CONTAINER_DATE,
A_OUTSTOCK_CONTAINER_NO,
SEND_FLAG,
LOCATE_NO,
'''' as ARS_TRANS_STATUS,
''{DC_ID}'' as DC_ID 
from om_case
where OPERATE_DATE >= to_date(''{DAY}'', ''yyyy-mm-dd'')-2
   and OPERATE_DATE  < to_date(''{DAY}'', ''yyyy-mm-dd'') + 1', 'etl', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (67, 4, 'CONTAINER_CASE_MOVE_LOG', '标签跟踪日志', 'select ROW_ID,
CONTAINER_TYPE,
CONTAINER_NO,
CONTAINER_DATE,
CELL_NO,
STATUS,
RGST_NAME,
RGST_DATE,
'''' as ARS_TRANS_STATUS,
''{DC_ID}'' as DC_ID
 from CONTAINER_CASE_MOVE_LOG
  where RGST_DATE >= to_date(''{DAY}'', ''yyyy-mm-dd'')
   and RGST_DATE  < to_date(''{DAY}'', ''yyyy-mm-dd'') + 1', 'etl', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (68, 9, 'KPI_RT_COST', '管报-经仓配送', 'select  toYYYYMM(toDate(''{DAY}'')) as STAT_MONTH,
        dc_id as DC_ID,

       case when  dc_id in (''A0LT'', ''995045'') then ''00''
             when dc_id in (''995050'') then ''12''
            else dictGet(''database_for_dict.mapping_provinces_regions_info'', ''provinces_regions_id'',
                 tuple(dictGet(''database_for_dict.dim_dc_shop_info'', ''provinces_regions'', tuple(dc_id)))
              ) end PROVINCES_REGIONS_ID,

        dictGet(''database_for_dict.mapping_dc_bu_info'', ''costcentercode'', tuple(dc_id)) as COST_CENTER_ID,
        dictGet(''database_for_dict.mapping_dc_bu_info'', ''bu_area_id'', tuple(dc_id)) as BU_AREA_ID,
        dictGet(''database_for_dict.mapping_dc_bu_info'', ''bu_area_name'', tuple(dc_id)) as BU_AREA_NAME,
       ''A01'' as REPORT_ID,
       ''货仓损益表'' as REPORT_NAME,

       item as ITEM_ID,

       mon_cost as MONTH_VALUE,

       year_cost as YEAR_ACT_VALUE

  from
      (select
       dictGet(''database_for_dict.mapping_dc_bu_info'',''kpi_dc_id'' ,tuple(frdssvd.out_shop_id)) as dc_id,
       array(''DCPL062'', ''DCPL063'', ''DCPL064'', ''DCPL065'', ''DCPL066'', ''DCPL067'', ''DCPL068'', ''DCPL077'',  ''DCPL079'') as item_list,
       array(
             sum(case when frdssvd.stat_day >= toStartOfMonth(toDate(''{DAY}'')) and frdssvd.stat_day < toStartOfMonth(addMonths(toDate(''{DAY}''), 1)) then frdssvd.rt_taxcost else 0 end),

             sum(case when frdssvd.stat_day >= toStartOfMonth(toDate(''{DAY}'')) and frdssvd.stat_day < toStartOfMonth(addMonths(toDate(''{DAY}''), 1)) and
                          frdssvd.out_shop_dctype = ''干货''
                         then frdssvd.rt_taxcost
                     else 0 end),

             sum(case when frdssvd.stat_day >= toStartOfMonth(toDate(''{DAY}'')) and frdssvd.stat_day < toStartOfMonth(addMonths(toDate(''{DAY}''), 1)) and
                          frdssvd.out_shop_dctype = ''干货'' and logistics_id in (0, 2)
                         then frdssvd.rt_taxcost
                     else 0 end),

             sum(case  when frdssvd.stat_day >= toStartOfMonth(toDate(''{DAY}'')) and frdssvd.stat_day < toStartOfMonth(addMonths(toDate(''{DAY}''), 1)) and
                          frdssvd.out_shop_dctype = ''干货'' and logistics_id not in (0, 2)
                         then frdssvd.rt_taxcost
                     else 0 end),

             sum(case when frdssvd.stat_day >= toStartOfMonth(toDate(''{DAY}'')) and frdssvd.stat_day < toStartOfMonth(addMonths(toDate(''{DAY}''), 1)) and
                          frdssvd.out_shop_dctype != ''干货''
                         then frdssvd.rt_taxcost
                     else 0 end),

             sum(case when frdssvd.stat_day >= toStartOfMonth(toDate(''{DAY}'')) and frdssvd.stat_day < toStartOfMonth(addMonths(toDate(''{DAY}''), 1)) and
                          frdssvd.out_shop_dctype != ''干货'' and logistics_id in (0, 2)
                         then frdssvd.rt_taxcost
                     else 0 end),

             sum(case when frdssvd.stat_day >= toStartOfMonth(toDate(''{DAY}'')) and frdssvd.stat_day < toStartOfMonth(addMonths(toDate(''{DAY}''), 1)) and
                          frdssvd.out_shop_dctype != ''干货'' and logistics_id not in (0, 2)
                         then frdssvd.rt_taxcost
                     else 0 end),

             sum(case when frdssvd.stat_day >= toStartOfMonth(toDate(''{DAY}'')) and frdssvd.stat_day < toStartOfMonth(addMonths(toDate(''{DAY}''), 1)) and
                          frdssvd.out_shop_dctype = ''干货'' and
                          (dictGet(''database_for_dict.dim_dc_shop_info'', ''shopformid'',
                                   tuple(in_shop_id)) as shop_flag) <> 9
                         then frdssvd.rt_taxcost
                     else 0 end),

             sum(case when frdssvd.stat_day >= toStartOfMonth(toDate(''{DAY}'')) and frdssvd.stat_day < toStartOfMonth(addMonths(toDate(''{DAY}''), 1)) and
                          frdssvd.out_shop_dctype != ''干货'' and shop_flag <> 9
                         then frdssvd.rt_taxcost
                     else 0 end)
           ) as mon_cost_list,
       array(
             sum(frdssvd.rt_taxcost),

             sum(case  when frdssvd.out_shop_dctype = ''干货''
                         then frdssvd.rt_taxcost
                     else 0 end),

             sum(case  when frdssvd.out_shop_dctype = ''干货'' and logistics_id in (0, 2)
                         then frdssvd.rt_taxcost
                     else 0 end),

             sum(case  when frdssvd.out_shop_dctype = ''干货'' and logistics_id not in (0, 2)
                         then frdssvd.rt_taxcost
                     else 0 end),

             sum(case when frdssvd.out_shop_dctype != ''干货''
                         then frdssvd.rt_taxcost
                     else 0 end),

             sum(case when frdssvd.out_shop_dctype != ''干货'' and logistics_id in (0, 2)
                         then frdssvd.rt_taxcost
                     else 0 end),

             sum(case  when frdssvd.out_shop_dctype != ''干货'' and logistics_id not in (0, 2)
                         then frdssvd.rt_taxcost
                     else 0 end),

             sum(case when frdssvd.out_shop_dctype = ''干货'' and
                          (dictGet(''database_for_dict.dim_dc_shop_info'', ''shopformid'',
                                   tuple(in_shop_id)) as shop_flag) <> 9
                         then frdssvd.rt_taxcost
                     else 0 end),

             sum(case when frdssvd.out_shop_dctype != ''干货'' and shop_flag <> 9
                         then frdssvd.rt_taxcost
                     else 0 end)

           ) as year_cost_list
 from dw_hr.fct_rt_dc_shop_sku_vender_day frdssvd
where frdssvd.stat_day >= toStartOfYear(toDate(''{DAY}''))
  and frdssvd.stat_day < toStartOfMonth(addMonths(toDate(''{DAY}''), 1))
group by  dc_id
      )
array join item_list as item, mon_cost_list as mon_cost, year_cost_list as year_cost
', 'display', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (69, 9, 'KPI_TUANTU', '管报-吞吐量', 'select toYYYYMM(toDate(''{DAY}'')) as STAT_MONTH,
        dc_id as DC_ID,

       case when  dc_id in (''A0LT'', ''995045'') then ''00''  
             when dc_id in (''995050'') then ''12''
            else dictGet(''database_for_dict.mapping_provinces_regions_info'', ''provinces_regions_id'',
                 tuple(dictGet(''database_for_dict.dim_dc_shop_info'', ''provinces_regions'', tuple(dc_id)))
              ) end PROVINCES_REGIONS_ID,

       dictGet(''database_for_dict.mapping_dc_bu_info'', ''costcentercode'', tuple(dc_id)) as COST_CENTER_ID,
        
       dictGet(''database_for_dict.mapping_dc_bu_info'', ''bu_area_id'', tuple(dc_id)) as BU_AREA_ID,
        
       dictGet(''database_for_dict.mapping_dc_bu_info'', ''bu_area_name'', tuple(dc_id)) as BU_AREA_NAME,
       
       ''A01'' as REPORT_ID,
       
       ''货仓损益表'' as REPORT_NAME,

       item as ITEM_ID,

       mon_cost   as MONTH_VALUE,

       year_cost  as YEAR_ACT_VALUE

    from
(
  select  dictGet(''database_for_dict.mapping_dc_bu_info'',''kpi_dc_id'' ,tuple(dc_shop_id)) as dc_id,
          array(''DCPL088'', ''DCPL089'') as item_list,
          array(sum(m_rt_boxes)+sum(m_rpt_boxes),sum(m_gh_rpt_boxes) +sum(m_gh_rt_boxes)) as mon_cost_list,
          array(sum(y_rt_boxes)+sum(y_rpt_boxes), sum(y_gh_rpt_boxes)+sum(y_gh_rt_boxes)) as year_cost_list
    from
  (  select  frdssvd.out_shop_id as dc_shop_id,

                 sum(case when frdssvd.stat_day >= toStartOfMonth(toDate(''{DAY}'')) and frdssvd.stat_day < toStartOfMonth(addMonths(toDate(''{DAY}''), 1)) then frdssvd.rt_boxes else 0 end) as m_rt_boxes,
                 sum( frdssvd.rt_boxes) as y_rt_boxes,

                 sum(case when frdssvd.stat_day >= toStartOfMonth(toDate(''{DAY}'')) and frdssvd.stat_day < toStartOfMonth(addMonths(toDate(''{DAY}''), 1)) and out_shop_dctype = ''干货'' then frdssvd.rt_boxes else 0 end) as m_gh_rt_boxes,

                 sum(case when out_shop_dctype = ''干货'' then frdssvd.rt_boxes else 0 end) as y_gh_rt_boxes,

                 0 as m_rpt_boxes,
                 0 as y_rpt_boxes,

                 0 as m_gh_rpt_boxes,
                 0 as y_gh_rpt_boxes
          from dw_hr.fct_rt_dc_shop_sku_vender_day frdssvd
        where frdssvd.stat_day >= toStartOfYear(toDate(''{DAY}''))
          and frdssvd.stat_day < toStartOfMonth(addMonths(toDate(''{DAY}''), 1))
        group by   frdssvd.out_shop_id
    union all
        select
               frdsgvd.dc_shop_id,
               0 as m_rt_boxes,
               0 as y_rt_boxes,

               0 as m_gh_rt_boxes,
               0 as y_gh_rt_boxes,

               sum(case when frdsgvd.stat_day >= toStartOfMonth(toDate(''{DAY}'')) and frdsgvd.stat_day < toStartOfMonth(addMonths(toDate(''{DAY}''), 1)) then frdsgvd.rpt_boxes else 0 end) as m_rpt_boxes,
               sum( frdsgvd.rpt_boxes) as y_rpt_boxes,

               sum(case when frdsgvd.dc_shop_dctype=''干货'' and frdsgvd.stat_day >= toStartOfMonth(toDate(''{DAY}'')) and frdsgvd.stat_day < toStartOfMonth(addMonths(toDate(''{DAY}''), 1)) then frdsgvd.rpt_boxes else 0 end) as m_gh_rpt_boxes,
               sum(case when frdsgvd.dc_shop_dctype=''干货'' then frdsgvd.rpt_boxes else 0 end) as y_gh_rpt_boxes
           from dw_hr.fct_rpt_dc_shop_good_vender_day frdsgvd
        where frdsgvd.stat_day >= toStartOfYear(toDate(''{DAY}''))
          and frdsgvd.stat_day  < toStartOfMonth(addMonths(toDate(''{DAY}''), 1))
          group by frdsgvd.dc_shop_id
    )
group by dc_id
    )
array join item_list as item, mon_cost_list as mon_cost, year_cost_list as year_cost', 'display', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (70, 9, 'KPI_KUCUN', '管报-日均库存', 'select  toYYYYMM(toDate(''{DAY}'')) as STAT_MONTH,
        dc_id as DC_ID,

       case when  dc_id in (''A0LT'', ''995045'') then ''00''
              when dc_id in (''995050'') then ''12''
            else dictGet(''database_for_dict.mapping_provinces_regions_info'', ''provinces_regions_id'',
                 tuple(dictGet(''database_for_dict.dim_dc_shop_info'', ''provinces_regions'', tuple(dc_id)))
              ) end PROVINCES_REGIONS_ID,

        dictGet(''database_for_dict.mapping_dc_bu_info'', ''costcentercode'', tuple(dc_id)) as COST_CENTER_ID,
        dictGet(''database_for_dict.mapping_dc_bu_info'', ''bu_area_id'', tuple(dc_id)) as BU_AREA_ID,
       dictGet(''database_for_dict.mapping_dc_bu_info'', ''bu_area_name'', tuple(dc_id)) as BU_AREA_NAME,
       ''A01'' as REPORT_ID,
       ''货仓损益表'' as REPORT_NAME,

       item as ITEM_ID,

       mon_cost as MONTH_VALUE,

       year_cost as YEAR_ACT_VALUE
 from
(select dc_id,
       array(''DCPL101'' ,''DCPL102'', ''DCPL103'', ''DCPL104'') as item_list,
       array(m_percent_stk_cost, m_percent_cc_stk_cost, m_percent_gh_stk_cost,m_percent_sx_stk_cost ) as mon_cost_list,
       array(y_percent_stk_cost, y_percent_cc_stk_cost, y_percent_gh_stk_cost,y_percent_sx_stk_cost) as year_cost_list
 from
    (select  dc_id,
            sum(case when stat_day >= toStartOfMonth(toDate(''{DAY}'')) and stat_day < toStartOfMonth(addMonths(toDate(''{DAY}''), 1)) then d_stk_cost else 0 end) as m_stk_cost,
            count(distinct case when stat_day >= toStartOfMonth(toDate(''{DAY}'')) and stat_day < toStartOfMonth(addMonths(toDate(''{DAY}''), 1)) then stat_day else null end) as m_stk_days,
            m_stk_cost / m_stk_days as m_percent_stk_cost, --月日均库存金额

            sum(d_stk_cost) as y_stk_cost,
            count(distinct case when stat_day >= toStartOfYear(toDate(''{DAY}'')) and stat_day < toStartOfMonth(addMonths(toDate(''{DAY}''), 1)) then stat_day else null end) as y_stk_days,
            y_stk_cost / y_stk_days as y_percent_stk_cost, --年日均库存金额

            sum(case when stat_day >= toStartOfMonth(toDate(''{DAY}'')) and stat_day < toStartOfMonth(addMonths(toDate(''{DAY}''), 1)) then d_cc_stk_cost else 0 end) as m_cc_stk_cost,
            m_cc_stk_cost / m_stk_days  as m_percent_cc_stk_cost, --月日均存储库存金额

            sum(d_cc_stk_cost) as y_cc_stk_cost,
            y_cc_stk_cost / y_stk_days as y_percent_cc_stk_cost, --年日均存储库存金额

            sum(case when stat_day >= toStartOfMonth(toDate(''{DAY}'')) and stat_day < toStartOfMonth(addMonths(toDate(''{DAY}''), 1)) and dctype = ''干货'' then d_cc_stk_cost else 0 end) as m_gh_stk_cost,
            m_gh_stk_cost / m_stk_days as m_percent_gh_stk_cost, --月日均干货存储库存金额

            sum(case when  dctype = ''干货'' then d_cc_stk_cost else 0 end) as y_gh_stk_cost,
            y_gh_stk_cost / y_stk_days as y_percent_gh_stk_cost, --年日均干货存储库存金额

            sum(case when stat_day >= toStartOfMonth(toDate(''{DAY}'')) and stat_day < toStartOfMonth(addMonths(toDate(''{DAY}''), 1)) and dctype not in( ''干货'') then d_cc_stk_cost else 0 end) as m_sx_stk_cost,
            m_sx_stk_cost / m_stk_days  as m_percent_sx_stk_cost,  --月日均生鲜存储库存金额

            sum(case when dctype not in( ''干货'') then d_cc_stk_cost else 0 end) as y_sx_stk_cost,
            y_sx_stk_cost / y_stk_days as y_percent_sx_stk_cost  --年日均生鲜存储库存金额
      from
        (select   stat_day ,
                 dc_id ,
                case when datasource in (''sg'', ''jv'') then high_category_id
                     else dictGet(''database_for_dict.dim_goods_info'', ''high_categoryid'', good_key) end `商品大类ID`,
                
                 case when (dictGet(''database_for_dict.mapping_dc_bu_info'', ''special_fresh_flag'',  tuple(dc_id)) as dc_special) = 1
                            and dictGet(''database_for_dict.mapping_bu_fresh_highcate_info'', ''valid'',  tuple( case when datasource in (''nwms'', ''wms'', ''oms'') then ''crv''  else datasource end, `商品大类ID`) ) =1
                       then ''生鲜''
                       else dictGet(''database_for_dict.mapping_dc_bu_info'', ''dctype'', tuple(dc_id)) end dctype,
     
                 case  when datasource in (''sg'', ''jv'') then ware_area_name
                  when datasource in (''oms'')
                   then dictGet(''database_for_dict.dim_oms_warearea_info'', ''mapping_area_name'', tuple(dc_id, fswngd.ware_area_no))
                   else dictGet(''database_for_dict.dim_wms_nwms_warearea_info'', ''mapping_area_name'', tuple(dc_id, fswngd.ware_area_no))
                 end `储区名称`,

                case when datasource in (''wms'', ''nwms'') and fswngd.ware_area_no not in (''1SS'') then stk_cost
                     when  datasource in (''oms'', ''sg'', ''jv'') and `储区名称` not in (''储区'') then stk_cost
                    else 0 end d_stk_cost,

                case when `储区名称` = ''存储区'' then  d_stk_cost else 0 end d_cc_stk_cost

         from dw_hr.fct_stk_wms_nwms_good_day  fswngd
        where stat_day  >= toStartOfYear(toDate(''{DAY}''))
          and stat_day  < toStartOfMonth(addMonths(toDate(''{DAY}''), 1))
            )
    group by dc_id
        )
     )
array join item_list as item, mon_cost_list as mon_cost, year_cost_list as year_cost', 'display', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (71, 5, 'IM_IMPORT_M_NWMS', 'IM_IMPORT_M_NWMS记录数', 'select ''{DAY}'' as stat_day_s,
       ''{TOPIC_ENNAME}'' as metric_topic,
       ''{SYSTEM_ENNAME}'' as metric_source,
       metric_date,
       metric_key,
       ''day'' as agg_type,
       ''{"logs":'' || logs || ''}'' as metric_value
  from (select metric_date, metric_key, ''day'' as agg_type, sum(1) as logs
          from (select t2.BATCH_SERIAL_NO as metric_key,
                       to_char(t1.rgst_date, ''yyyymmdd'') as metric_date
                  from NWMS_WMS.IM_IMPORT_M t1,
                       NWMS_WMS.BM_DEFDCSHOP t2
                 where t1.LOCNO=t2.LOCNO
                   and t1.rgst_date >= trunc(sysdate) - 90
                   and t1.rgst_date < trunc(sysdate))
         GROUP BY metric_date, metric_key)', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (72, 5, 'IM_IMPORT_D_NWMS', 'IM_IMPORT_D_NWMS记录数', 'select ''{DAY}'' as stat_day_s,
       ''{TOPIC_ENNAME}'' as metric_topic,
       ''{SYSTEM_ENNAME}'' as metric_source,
       metric_date,
       metric_key,
       ''day'' as agg_type,
       ''{"logs":'' || logs || ''}'' as metric_value
  from (select metric_date, metric_key, ''day'' as agg_type, sum(1) as logs
          from (select t3.BATCH_SERIAL_NO as metric_key,
                       to_char(t1.rgst_date, ''yyyymmdd'') as metric_date
                  from NWMS_WMS.IM_IMPORT_M t1, NWMS_WMS.IM_IMPORT_D T2,NWMS_WMS.BM_DEFDCSHOP T3
                 where t1.locno = t2.locno
                   and t1.owner_no = t2.owner_no
                   and t1.import_no = t2.import_no
                   and t1.LOCNO=t3.LOCNO
                   and t1.rgst_date >= trunc(sysdate) - 90
                   and t1.rgst_date < trunc(sysdate))
         GROUP BY metric_date, metric_key)', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (73, 5, 'IM_CHECK_M_NWMS', 'IM_CHECK_M_NWMS记录数', 'select ''{DAY}'' as stat_day_s,
       ''{TOPIC_ENNAME}'' as metric_topic,
       ''{SYSTEM_ENNAME}'' as metric_source,
       metric_date,
       metric_key,
       ''day'' as agg_type,
       ''{"logs":'' || logs || ''}'' as metric_value
  from (select metric_date, metric_key, ''day'' as agg_type, sum(1) as logs
          from (select t2.BATCH_SERIAL_NO as metric_key,
                       to_char(t1.rgst_date, ''yyyymmdd'') as metric_date
                  from NWMS_WMS.IM_CHECK_M t1,
                       NWMS_WMS.BM_DEFDCSHOP t2
                 where t1.LOCNO=t2.LOCNO
                   and t1.rgst_date >= trunc(sysdate) - 90
                   and t1.rgst_date < trunc(sysdate))
         GROUP BY metric_date, metric_key)', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (74, 5, 'IM_CHECK_D_NWMS', 'IM_CHECK_D_NWMS记录数', 'select ''{DAY}'' as stat_day_s,
       ''{TOPIC_ENNAME}'' as metric_topic,
       ''{SYSTEM_ENNAME}'' as metric_source,
       metric_date,
       metric_key,
       ''day'' as agg_type,
       ''{"logs":'' || logs || ''}'' as metric_value
  from (select metric_date, metric_key, ''day'' as agg_type, sum(1) as logs
          from (select t3.BATCH_SERIAL_NO as metric_key,
                       to_char(t1.rgst_date, ''yyyymmdd'') as metric_date
                  from NWMS_WMS.IM_CHECK_M t1, NWMS_WMS.IM_CHECK_D T2,NWMS_WMS.BM_DEFDCSHOP T3
                 where t1.locno = t2.locno
                   and t1.owner_no = t2.owner_no
                   and t1.CHECK_NO =t2.CHECK_NO
                   and t1.LOCNO=t3.LOCNO
                   and t1.rgst_date >= trunc(sysdate) - 90
                   and t1.rgst_date < trunc(sysdate))
         GROUP BY metric_date, metric_key)', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (75, 5, 'OM_DELIVER_M_NWMS', 'OM_DELIVER_M_NWMS记录数', 'select ''{DAY}'' as stat_day_s,
       ''{TOPIC_ENNAME}'' as metric_topic,
       ''{SYSTEM_ENNAME}'' as metric_source,
       metric_date,
       metric_key,
       ''day'' as agg_type,
       ''{"logs":'' || logs || ''}'' as metric_value
  from (select metric_date, metric_key, ''day'' as agg_type, sum(1) as logs
          from (select t2.BATCH_SERIAL_NO as metric_key,
                       to_char(t1.rgst_date, ''yyyymmdd'') as metric_date
                  from NWMS_WMS.OM_DELIVER_M t1,
                       NWMS_WMS.BM_DEFDCSHOP t2
                 where t1.LOCNO=t2.LOCNO
                   and t1.rgst_date >= trunc(sysdate) - 90
                   and t1.rgst_date < trunc(sysdate))
         GROUP BY metric_date, metric_key)', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (76, 5, 'OM_DELIVER_D_NWMS', 'OM_DELIVER_D_NWMS记录数', 'select ''{DAY}'' as stat_day_s,
       ''{TOPIC_ENNAME}'' as metric_topic,
       ''{SYSTEM_ENNAME}'' as metric_source,
       metric_date,
       metric_key,
       ''day'' as agg_type,
       ''{"logs":'' || logs || ''}'' as metric_value
  from (select metric_date, metric_key, ''day'' as agg_type, sum(1) as logs
          from (select t3.BATCH_SERIAL_NO as metric_key,
                       to_char(t1.rgst_date, ''yyyymmdd'') as metric_date
                  from NWMS_WMS.OM_DELIVER_M t1, NWMS_WMS.OM_DELIVER_D T2,NWMS_WMS.BM_DEFDCSHOP T3
                 where t1.locno = t2.locno
                   and t1.owner_no = t2.owner_no
                   and t1.DELIVER_NO =t2.DELIVER_NO
                   and t1.LOCNO=t3.LOCNO
                   and t1.rgst_date >= trunc(sysdate) - 90
                   and t1.rgst_date < trunc(sysdate))
         GROUP BY metric_date, metric_key)', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (77, 4, 'IM_IMPORT_M', 'IM_IMPORT_M记录数', 'select ''{DAY}'' as stat_day_s,
       ''{TOPIC_ENNAME}'' as metric_topic,
       ''{SYSTEM_ENNAME}'' as metric_source,
       metric_date,
       metric_key,
       ''day'' as agg_type,
       ''{"logs":'' || logs || ''}'' as metric_value
  from (select metric_date, metric_key, ''day'' as agg_type, sum(1) as logs
          from (select ''{DC_ID}'' as metric_key,
                       to_char(t1.rgst_date, ''yyyymmdd'') as metric_date
                  from CRCWMS.IM_IMPORT_M t1
                 where t1.rgst_date >= trunc(sysdate) - 90
                   and t1.rgst_date < trunc(sysdate))
         GROUP BY metric_date, metric_key)', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (78, 4, 'IM_IMPORT_D', 'IM_IMPORT_D记录数', 'select ''{DAY}'' as stat_day_s,
       ''{TOPIC_ENNAME}'' as metric_topic,
       ''{SYSTEM_ENNAME}'' as metric_source,
       metric_date,
       metric_key,
       ''day'' as agg_type,
       ''{"logs":'' || logs || ''}'' as metric_value
  from (select metric_date, metric_key, ''day'' as agg_type, sum(1) as logs
          from (select ''{DC_ID}'' as metric_key,
                       to_char(t1.rgst_date, ''yyyymmdd'') as metric_date
                  from CRCWMS.IM_IMPORT_M t1, CRCWMS.IM_IMPORT_D T2
                 where t1.import_no = t2.import_no
                   and t1.rgst_date >= trunc(sysdate) - 90
                   and t1.rgst_date < trunc(sysdate))
         GROUP BY metric_date, metric_key)', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (79, 4, 'IM_CHECK_M', 'IM_CHECK_M记录数', 'select ''{DAY}'' as stat_day_s,
       ''{TOPIC_ENNAME}'' as metric_topic,
       ''{SYSTEM_ENNAME}'' as metric_source,
       metric_date,
       metric_key,
       ''day'' as agg_type,
       ''{"logs":'' || logs || ''}'' as metric_value
  from (select metric_date, metric_key, ''day'' as agg_type, sum(1) as logs
          from (select ''{DC_ID}'' as metric_key,
                       to_char(t1.rgst_date, ''yyyymmdd'') as metric_date
                  from CRCWMS.IM_CHECK_M t1
                 where t1.rgst_date >= trunc(sysdate) - 90
                   and t1.rgst_date < trunc(sysdate))
         GROUP BY metric_date, metric_key)', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (80, 4, 'IM_CHECK_D', 'IM_CHECK_D记录数', 'select ''{DAY}'' as stat_day_s,
       ''{TOPIC_ENNAME}'' as metric_topic,
       ''{SYSTEM_ENNAME}'' as metric_source,
       metric_date,
       metric_key,
       ''day'' as agg_type,
       ''{"logs":'' || logs || ''}'' as metric_value
  from (select metric_date, metric_key, ''day'' as agg_type, sum(1) as logs
          from (select ''{DC_ID}'' as metric_key,
                       to_char(t1.rgst_date, ''yyyymmdd'') as metric_date
                  from CRCWMS.IM_CHECK_M t1, CRCWMS.IM_CHECK_D T2
                 where t1.CHECK_NO = t2.CHECK_NO
                   and t1.rgst_date >= trunc(sysdate) - 90
                   and t1.rgst_date < trunc(sysdate))
         GROUP BY metric_date, metric_key)', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (81, 4, 'OM_DELIVER_M', 'OM_DELIVER_M记录数', 'select ''{DAY}'' as stat_day_s,
       ''{TOPIC_ENNAME}'' as metric_topic,
       ''{SYSTEM_ENNAME}'' as metric_source,
       metric_date,
       metric_key,
       ''day'' as agg_type,
       ''{"logs":'' || logs || ''}'' as metric_value
  from (select metric_date, metric_key, ''day'' as agg_type, sum(1) as logs
          from (select ''{DC_ID}'' as metric_key,
                       to_char(t1.rgst_date, ''yyyymmdd'') as metric_date
                  from CRCWMS.OM_DELIVER_M t1
                 where t1.rgst_date >= trunc(sysdate) - 90
                   and t1.rgst_date < trunc(sysdate))
         GROUP BY metric_date, metric_key)', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (82, 4, 'OM_DELIVER_D', 'OM_DELIVER_D记录数', 'select ''{DAY}'' as stat_day_s,
       ''{TOPIC_ENNAME}'' as metric_topic,
       ''{SYSTEM_ENNAME}'' as metric_source,
       metric_date,
       metric_key,
       ''day'' as agg_type,
       ''{"logs":'' || logs || ''}'' as metric_value
  from (select metric_date, metric_key, ''day'' as agg_type, sum(1) as logs
          from (select ''{DC_ID}'' as metric_key,
                       to_char(t1.rgst_date, ''yyyymmdd'') as metric_date
                  from CRCWMS.OM_DELIVER_M t1, CRCWMS.OM_DELIVER_D T2
                 where t1.DELIVER_NO = t2.DELIVER_NO
                   and t1.rgst_date >= trunc(sysdate) - 90
                   and t1.rgst_date < trunc(sysdate))
         GROUP BY metric_date, metric_key)', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (83, 8, 'CK_RATION_RET_L', 'CRV日返配清单', 'select to_char(t.outexedate,''yyyy-mm-dd'') as deliver_date,
         trim(t.sheetid) as sheetid,
         case when t.rationtype = ''I'' then t.outshopid else t.inshopid end outshopid,
         case when t.rationtype = ''I'' then t.inshopid else t.outshopid end inshopid,
         replace(replace(t.editor, ''\\'', ''''), chr(13),'''') as editor,
         replace(replace(t.operator, ''\\'', ''''), chr(13),'''') as operator,
         t2.paytypeid||'''' as paytypeid,
         t2.goodsid||'''' as goodsid,
         to_char(t2.cost,''FM999999990.0000'') as cost,
         t2.logistics||'''' as logistics,
         t2.venderid,
         to_char(t2.costtaxrate,''FM999999990.0000'') as costtaxrate,
         to_char(t2.saletaxrate,''FM999999990.0000'') as saletaxrate,
         ''6''  as bun_type,
         to_char(t2.outqty,''FM999999990.0000'') as  rt_qty,
         to_char(t2.cost * t2.outqty,''FM999999990.0000'') as rt_costvalue,
         to_char(t2.cost * t2.outqty / (1 + t2.costtaxrate / 100), ''FM999999990.0000'') rt_cost,
         to_char(t1.pknum,''FM999999990.0000'') as  pknum,
         t2.CATEGORYID||'''' as CATEGORYID,
         retflag,
         badflag,
         nvl(isdiff, 0) as isdiff
  FROM dw.ration     t,
       dw.rationitem t1,
       dw.rationacc  t2
 WHERE t.sheetid = t1.sheetid
   AND t.sheetid = t2.sheetid
   AND t1.goodsid = t2.goodsid
   AND t.outexedate >= to_date(''{DAY}'', ''yyyy-mm-dd'') - {PRE_DAYS}
   AND t.outexedate  < to_date(''{DAY}'', ''yyyy-mm-dd'') + 1
   AND t.outshopid != ''995045''
   AND t.rationtype in (''O'')', 'etl', 1, null, null, 'deliver_date,sheetid,outshopid,inshopid,editor,operator,paytypeid,goodsid,cost,logistics,venderid,costtaxrate,saletaxrate,bun_type,rt_qty,rt_costvalue,rt_cost,pknum,low_categoryid,badflag,retflag,isdiff');
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (84, 8, 'CK_JV_RATION_RET_L', 'JV日返配清单', 'select  to_char(a.sdate,''yyyy-mm-dd'') as deliver_date,
        a.sheetid,
        case when a.transfertype = ''I'' then a.outshopid else a.inshopid end outshopid,
        case when a.transfertype = ''I'' then a.inshopid else a.outshopid  end inshopid,
        replace(a.editor, ''\\'', '''') as editor,
        replace(a.checker, ''\\'', '''') as operator,
        ''0'' as paytypeid,
        b.goodsid||'''' as goodsid,
        ''0.0000'' as cost,
        ''2'' as logistics,
        b.venderid as venderid,
        to_char(b.costtaxrate, ''FM999999990.0000'') as costtaxrate,
        to_char(b.SALETAXRATE, ''FM999999990.0000'') as saletaxrate,
         ''6''  as  bun_type,
        to_char(b.qty, ''FM999999990.0000'') as rt_qty ,
        to_char(b.costvalue, ''FM999999990.0000'') as  rt_cost,
        to_char(b.costvalue + b.costtaxvalue, ''FM999999990.0000'') as rt_costvalue,
        ''0.0000'' as  pknum,
        b.categoryid||'''' as categoryid,
        a.transfertype,
        '''' as  retflag,
        '''' as  badflag,
         '''' as isdiff
  FROM dw.acc_jv_transfer     a,
       dw.acc_jv_transferitem b
 WHERE a.sheetid = b.sheetid
   AND a.sdate >= to_date(''{DAY}'', ''yyyy-mm-dd'') - {PRE_DAYS}
   AND a.sdate  < to_date(''{DAY}'', ''yyyy-mm-dd'') +1
   and transfertype = ''O''
', 'etl', 1, null, null, 'deliver_date,sheetid,outshopid,inshopid,editor,operator,paytypeid,goodsid,cost,logistics,venderid,costtaxrate,saletaxrate,bun_type,rt_qty,rt_cost,rt_costvalue,pknum,category_id,transfertype,badflag,retflag,isdiff');
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (85, 8, 'CK_SG_RATION_RET_L', '苏果日返配清单', 'select to_char(to_date(tt.deliver_date, ''yyyymmdd''), ''yyyy-mm-dd'') as deliver_date,
    tt.deliver_id as sheetid,
    tt.out_shop_id as outshopid,
    tt.in_shop_id as inshopid,
    '''' as editor,
    '''' as operator,
    '''' as paytypeid,
    dw.replace_ts(tt.goods_code) as goodsid,
    dw.replace_ts(tt.goods_name) as goods_name,
    to_char(tt.cost,''FM999999990.0000'')  as cost,
    case when tt.logistics in (''3调整'', ''0存储'') then ''3''
         when tt.logistics in (''1直流'') then ''2''
         when tt.logistics in (''2退货'')  then ''3''
          else ''2'' end logistics,
    tt.venderid,
    dw.replace_ts(tt.vendername) as vendername,
    ''0.0000'' as costtaxrate,
    ''0.0000'' as saletaxrate,
    ''6'' as bun_type,
    to_char(tt.deliver_scatter_num,''FM999999990.0000'') as rt_qty,
    to_char(tt.deliver_cost,''FM999999990.0000'') as rt_cost,
    to_char(deliver_taxamount,''FM999999990.0000'') as rt_costvalue,
    to_char(tt.deliver_box_num,''FM999999990.0000'') as rt_boxes,
    to_char(tt.pknum,''FM999999990.0000'') as pknum,
    tt.BARCODE,
    in_shop_name,
    shop_from_id,
    shop_from_name,
    tt.category_id as category_high_id,
    category_name as category_high_name,
    category_low_name,
    category_low_id,
    category_middle_name,
    category_middle_id,
    provinces,
    city,
    '''' as badflag ,
    '''' as retflag ,
    '''' as  isdiff
 from dw.sg_deliver_t tt
where tt.deliver_date >= to_char(to_date(''{DAY}'', ''yyyy-mm-dd'') - {PRE_DAYS}, ''yyyymmdd'')
  and tt.deliver_date <  to_char(to_date(''{DAY}'', ''yyyy-mm-dd'')+1, ''yyyymmdd'')
  and tt.logistics in (''2退货'')', 'etl', 1, null, null, 'deliver_date,sheetid,outshopid,inshopid,editor,operator,paytypeid,goodsid,goodsname,cost,logistics,venderid,vendername,costtaxrate,saletaxrate,bun_type,rt_qty,rt_cost,rt_costvalue,rt_boxes,pknum,barcode,in_shop_name,shop_from_id,shop_from_name,category_high_id,category_high_name,category_low_name,category_low_id,category_middle_name,category_middle_id,provinces,city,badflag,retflag,isdiff');
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (86, 8, 'CK_XG_RATION_RET_L', '香港日返配清单', 'select to_char(to_date(tt.deliver_date, ''yyyymmdd''), ''yyyy-mm-dd'') as deliver_date,
    tt.deliver_id as sheetid,
    tt.out_shop_id as  outshopid,
    tt.in_shop_id as inshopid,
    '''' as editor,
    '''' as operator,
    '''' as paytypeid,
    tt.goods_code as goodsid,
    tt.goods_name,
    to_char(tt.cost,''FM999999990.0000''),
    case when tt.logistics in (''G'') then ''3''
            when  tt.logistics in (''F'', ''T'') then ''2''
              else ''2'' end logistics,
    '''' as venderid,
    '''' as vendername,
    ''0.0000'' as costtaxrate,
    ''0.0000'' as saletaxrate,
    ''6'' as bun_type,
    to_char(tt.deliver_scatter_num,''FM999999990.0000'') as rt_qty,
    to_char(tt.cost * tt.deliver_scatter_num,''FM999999990.0000'') as rt_cost,
    to_char(tt.cost * tt.deliver_scatter_num,''FM999999990.0000'') as rt_costvalue,
    to_char(tt.deliver_box_num,''FM999999990.0000'') as rt_boxes,
    tt.pknum,
    tt.barcode,
    tt.category_id,
    '''' as badflag ,
    '''' as retflag ,
    '''' as isdiff   
 from dw.xg_deliver_t tt
where  tt.deliver_date >= to_char(to_date(''{DAY}'', ''yyyy-mm-dd'') - {PRE_DAYS}, ''yyyymmdd'')
  and tt.deliver_date <  to_char(to_date(''{DAY}'', ''yyyy-mm-dd'')+1, ''yyyymmdd'')
   and tt.logistics in (''G'', ''F'', ''T'')
   and tt.in_shop_id in (''001002'', ''001003'')', 'etl', 1, null, null, 'deliver_date,sheetid,outshopid,inshopid,editor,operator,paytypeid,goodsid,goodsname,cost,logistics,venderid,vendername,costtaxrate,saletaxrate,bun_type,rt_qty,rt_cost,rt_costvalue,rt_boxes,pknum,barcode,category_id,badflag,retflag,isdiff');
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (87, 8, 'CK_TMS_RATION_L', 'TMS运单', 'SELECT to_char(t.leave_time,''yyyy-mm-dd'') as leave_day,
     to_char(t.leave_time,''yyyy-mm-dd hh:mi:ss'') as leave_time,
     t.WMS_CODE AS wms_pe_id,
     t.code as pe_id,
     tto.CODE as plat_dc_id,
     R1.CODE AS dc_id,
     R.CODE AS in_shop_id,
     t.ship_status,
     T.shipment_method,
     T.loading_method
FROM vtms.TMS_SHIPMENT@TMS T
left join vtms.TMS_ORGANIZATION@TMS tto on t.PLAT_FORM_ID = tto.id
LEFT JOIN vtms.TMS_LEG@TMS D ON T.ID = D.SHIPMENT_ID
LEFT JOIN vtms.TMS_RECEIVER@TMS R ON D.TO_RECEIVER_ID = r.ID
LEFT JOIN vtms.TMS_RECEIVER@TMS R1 ON D.FROM_RECEIVER_ID = R1.ID', 'etl', 1, null, null, 'leave_day,leave_time,wms_pe_id,pe_id,plat_dc_id,dc_id,in_shop_id,ship_status,shipment_method,loading_method');
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (88, 8, 'CK_RATION_L', 'CRV配送清单', 'SELECT to_char(t.outexedate,''yyyy-mm-dd'') as deliver_date,
        t.sheetid,
        case when t.rationtype = ''I'' then t.outshopid else t.inshopid end outshopid,
        case when t.rationtype = ''I'' then t.inshopid else t.outshopid end inshopid,
        replace(replace(t.editor, ''\\'', ''''), chr(13),'''') as editor,
        replace(replace(t.operator, ''\\'', ''''), chr(13),'''') as operator,
        t2.paytypeid||'''' as paytypeid,
        t2.goodsid||'''' as goodsid ,
        to_char(t2.cost,''FM999999990.0000'') as cost,
        t2.logistics||'''' as logistics,
        t2.venderid,
        to_char(t2.costtaxrate,''FM999999990.0000'') costtaxrate,
        to_char(t2.saletaxrate,''FM999999990.0000'') saletaxrate,
        ''1'' as  bun_type,
        to_char(t2.outqty,''FM999999990.0000'') rt_qty,
         to_char(t2.cost * t2.outqty,''FM999999990.0000'') as rt_costvalue,
         to_char(t2.cost * t2.outqty / (1 + t2.costtaxrate / 100),''FM999999990.0000'') rt_cost,
         to_char(t1.pknum,''FM999999990.0000'') as pknum,
         t2.CATEGORYID||'''' as CATEGORYID
  FROM dw.ration     t,
       dw.rationitem t1,
       dw.rationacc  t2
 WHERE t.sheetid = t1.sheetid
   AND t.sheetid = t2.sheetid
   AND t1.goodsid = t2.goodsid
   AND t.outexedate >= to_date(''{DAY}'', ''yyyy-mm-dd'') - {PRE_DAYS}
   AND t.outexedate  < to_date(''{DAY}'', ''yyyy-mm-dd'') + 1
   AND t.rationtype in ( ''I'')
union all
  SELECT to_char(t.outexedate,''yyyy-mm-dd'') as deliver_date,
         t.sheetid,
         t.shopid outshopid,
         t3.guestid inshopid,
         replace(replace(t.editor, ''\\'', ''''), ''.'','''') as editor,
         replace(replace(t.operator, ''\\'', ''''), ''.'', '''') as operator,
         t2.paytypeid||'''' as paytypeid,
         t2.goodsid||'''' as goodsid,
         to_char(t2.cost,''FM999999990.0000'') as cost,
         t2.logistics||'''' as logistics,
         t2.venderid,
         to_char(t2.costtaxrate,''FM999999990.0000'') as costtaxrate,
         to_char(t2.saletaxrate,''FM999999990.0000'') as saletaxrate,
         ''3'' as bun_type,
        to_char(t2.qty,''FM999999990.0000'') as rt_qty,
        to_char(t2.cost * t2.qty,''FM999999990.0000'') as rt_costvalue,
        to_char(t2.cost * t2.qty / (1 + t2.costtaxrate / 100),''FM999999990.0000'') as rt_cost,
         ''1.0000'' as pknum,
         t2.CATEGORYID||'''' as CATEGORYID
    FROM dw.wholesale      t,
         dw.jv_shop        t1,
         dw.wholesaleacc   t2,
         dw.wholeguesttran t3
   WHERE t.sheetid = t2.sheetid
     AND t.shopid = t3.outshopid
     AND t.customerid = t3.oleguestid
     AND t.outexedate >=  to_date(''{DAY}'', ''yyyy-mm-dd'')  - {PRE_DAYS}
     AND t.outexedate  <  to_date(''{DAY}'', ''yyyy-mm-dd'') + 1
     AND t.shopid = t1.shopid
     AND t1.shopformid = 9
union all
SELECT to_char(t.outexedate,''yyyy-mm-dd'') as deliver_date,
       t.sheetid,
       t.outshopid,
       t.inshopid,
       replace(t.editor, ''\\'', '''') as editor,
       replace(t.operator, ''\\'', '''') as operator,
       t2.paytypeid||'''' as paytypeid,
       t2.goodsid||'''' as goodsid,
       to_char(t2.cost,''FM999999990.0000'') as cost,
       ''3'' as logistics,
       t2.venderid,
       to_char(t2.costtaxrate,''FM999999990.0000'') as costtaxrate,
       to_char(t2.saletaxrate,''FM999999990.0000'') as saletaxrate,
       ''2'' as bun_type,
       to_char(t2.qty,''FM999999990.0000'')  as rt_qty,
       to_char(t2.cost * t2.qty,''FM999999990.0000'') as  rt_costvalue,
       to_char(t2.cost * t2.qty / (1 + t2.costtaxrate / 100),''FM999999990.0000'') as  rt_cost,
       ''1.0000'' as pknum,
       t2.CATEGORYID||'''' as CATEGORYID
  FROM dw.tran     t,
       dw.tranacc  t2,
       dw.jv_shop  t3
 WHERE t.sheetid = t2.sheetid
   AND t.outshopid = t3.shopid
   AND t.outexedate >= to_date(''{DAY}'', ''yyyy-mm-dd'') - {PRE_DAYS}
   AND t.outexedate  < to_date(''{DAY}'', ''yyyy-mm-dd'') + 1
   AND t3.shopformid = 9
union all
SELECT to_char(t.executedate,''yyyy-mm-dd'') as deliver_date   ,
       t.sheetid,
       t.outshopid as outshopid,
       t.saleshopid as inshopid,
       replace(t.editor, ''\\'', '''') as editor,
       replace(t.operator, ''\\'', '''') as operator,
       t1.paytypeid||'''' as paytypeid,
       t1.goodsid||'''' as goodsid,
       to_char(t1.cost,''FM999999990.0000'') as cost,
       t1.logistics||'''' as logistics,
       t1.venderid,
       to_char(t1.costtaxrate,''FM999999990.0000'') as costtaxrate,
       to_char(t1.saletaxrate,''FM999999990.0000'') as saletaxrate,
       ''4'' as bun_type,
       to_char(t1.qty,''FM999999990.0000'') as rt_qty,
       to_char(t1.cost * t1.qty,''FM999999990.0000'') as rt_costvalue,
       to_char(t1.cost * t1.qty / (1 + t1.costtaxrate / 100),''FM999999990.0000'')  as rt_cost,
       ''1.0000'' as pknum,
       t1.CATEGORYID||'''' as CATEGORYID
FROM dw.saleship    t,
     dw.saleshipacc t1,
     dw.jv_shop     t3
WHERE t.sheetid = t1.sheetid
 AND t.outshopid = t3.shopid
 AND t.executedate >= to_date(''{DAY}'', ''yyyy-mm-dd'') - {PRE_DAYS}
 AND t.executedate  < to_date(''{DAY}'', ''yyyy-mm-dd'') + 1
 AND t3.shopformid = 9', 'etl', 1, null, null, 'deliver_date,sheetid,outshopid,inshopid,editor,operator,paytypeid,goodsid,cost,logistics,venderid,costtaxrate,saletaxrate,bun_type,rt_qty,rt_costvalue,rt_cost,pknum,low_categoryid');
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (89, 8, 'CK_JV_RATION_L', 'ACCJV配送清单', 'select  to_char(a.sdate,''yyyy-mm-dd'') as deliver_date,
        a.sheetid,
        case when a.transfertype in ( ''I'',''T'') then a.outshopid else a.inshopid end outshopid,
        case when a.transfertype in ( ''I'',''T'') then a.inshopid else a.outshopid  end inshopid,
        replace(a.editor, ''\\'', '''') as editor,
       replace(a.checker, ''\\'', '''') as operator,
        ''0'' as paytypeid,
        b.goodsid as goodsid,
        ''0.0000'' as cost,
        ''2'' as logistics,
        b.venderid as venderid,
        to_char(b.costtaxrate,''FM999999990.0000'')  as costtaxrate,
        to_char(b.SALETAXRATE,''FM999999990.0000'') as saletaxrate, 
        ''1'' as  bun_type,
        to_char( b.qty,''FM999999990.0000'') as rt_qty ,
        to_char( b.costvalue,''FM999999990.0000'') as  rt_cost,
        to_char( b.costvalue + b.costtaxvalue,''FM999999990.0000'') as rt_costvalue,
        ''0.0000'' as  pknum,
        b.categoryid||'''' as categoryid,
        a.transfertype
FROM dw.acc_jv_transfer     a,
     dw.acc_jv_transferitem b
WHERE a.sheetid = b.sheetid
 AND a.sheetid NOT LIKE ''CT%''
 and a.sdate >= to_date(''{DAY}'', ''yyyy-mm-dd'') - {PRE_DAYS}
 and a.sdate < to_date(''{DAY}'', ''yyyy-mm-dd'') + 1
 and a.transfertype in ( ''I'' ,''T'')', 'etl', 1, null, null, 'deliver_date,sheetid,outshopid,inshopid,editor,operator,paytypeid,goodsid,cost,logistics,venderid,costtaxrate,saletaxrate,bun_type,rt_qty,rt_cost,rt_costvalue,pknum,category_id,transfertype');
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (90, 8, 'CK_SG_RATION_L', 'SG配送清单', 'select to_char(to_date(tt.deliver_date, ''yyyymmdd''), ''yyyy-mm-dd'') as deliver_date,
    tt.deliver_id as sheetid,
    tt.out_shop_id as  outshopid,
    tt.in_shop_id as inshopid,
    '''' as editor,
    '''' as operator,
    '''' as paytypeid,
    replace_ts(tt.goods_code) as goodsid,
    replace_ts(tt.goods_name) as goods_name,
    to_char( tt.cost  ,''FM99999999.0000'') as cost,
    case when tt.logistics in (''3调整'', ''0存储'') then ''3''
            when  tt.logistics in (''1直流'') then ''2''
            when tt.logistics in (''2退货'')  then ''3''
              else ''2'' end logistics,
    tt.venderid,
    replace_ts(tt.vendername) as vendername,
    ''0.0000'' as costtaxrate,
    ''0.0000'' as saletaxrate,
   case when tt.logistics in (''3调整'', ''0存储'') then ''1''
        when  tt.logistics in (''1直流'') then ''1''
         when tt.logistics in (''2退货'')  then ''6''
              else ''1'' end as bun_type,
    to_char(tt.deliver_scatter_num,''FM999999990.0000'') as rt_qty,
    to_char(tt.deliver_cost ,''FM999999990.0000'') as rt_cost,
     to_char(deliver_taxamount  ,''FM999999990.0000'') as rt_costvalue,
    to_char(tt.deliver_box_num  ,''FM999999990.0000'')  as rt_boxes,
    to_char( tt.pknum ,''FM999999990.0000'') as pknum,
    tt.BARCODE,
    in_shop_name,
    shop_from_id,
    shop_from_name,
    tt.CATEGORY_ID as category_high_id,
    category_name as category_high_name,
    category_low_name,
    category_low_id,
    category_middle_name,
    category_middle_id,
    provinces,
    city
 from dw.sg_deliver_t tt
 where tt.deliver_date >= to_char(to_date(''{DAY}'', ''yyyy-mm-dd'') - {PRE_DAYS}, ''yyyymmdd'')
   and tt.deliver_date  < to_char(to_date(''{DAY}'', ''yyyy-mm-dd'') + 1, ''yyyymmdd'')
   and tt.logistics not in (''2退货'') ', 'etl', 1, null, null, 'deliver_date,sheetid,outshopid,inshopid,editor,operator,paytypeid,goodsid,goodsname,cost,logistics,venderid,vendername,costtaxrate,saletaxrate,bun_type,rt_qty,rt_cost,rt_costvalue,rt_boxes,pknum,barcode,in_shop_name,shop_from_id,shop_from_name,category_high_id,category_high_name,category_low_name,category_low_id,category_middle_name,category_middle_id,provinces,city');
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (91, 8, 'CK_XG_RATION_L', 'XG配送清单', 'select to_char(to_date(tt.deliver_date, ''yyyymmdd''), ''yyyy-mm-dd'') as deliver_date,
        tt.deliver_id as sheetid,
        tt.out_shop_id as  outshopid,
        tt.in_shop_id as inshopid,
        '''' as editor,
        '''' as operator,
        '''' as paytypeid,
        tt.goods_code as goodsid,
        tt.goods_name,
        to_char( tt.cost,''FM99999999.0000'') as cost,
        case when tt.logistics in (''G'') then ''3''
                when  tt.logistics in (''F'', ''T'') then ''2''
                  else ''2'' end logistics,
        '''' as venderid,
        '''' as vendername,
        ''0.0000'' as costtaxrate,
        ''0.0000'' as saletaxrate,
        case when tt.in_shop_id IN (''001002'', ''001003'') then ''6'' else ''1'' end bun_type,
        to_char( tt.deliver_scatter_num,''FM999999990.0000'')  as rt_qty,
        to_char( tt.cost * tt.deliver_scatter_num,''FM999999990.0000'') as rt_cost,
        to_char( tt.cost * tt.deliver_scatter_num,''FM999999990.0000'')  as rt_costvalue,
        to_char( tt.deliver_box_num,''FM999999990.0000'')  as rt_boxes,
        to_char( tt.pknum ,''FM999999990.0000'') as pknum,
        tt.BARCODE,
        tt.CATEGORY_ID
 from dw.xg_deliver_t tt
 where tt.logistics in (''G'', ''F'', ''T'')
   and tt.deliver_date >= to_char(to_date(''{DAY}'', ''yyyy-mm-dd'') - {PRE_DAYS}, ''yyyymmdd'')
   and  tt.deliver_date < to_char(to_date(''{DAY}'', ''yyyy-mm-dd'') + 1, ''yyyymmdd'')
   and in_shop_id not IN (''001002'', ''001003'')', 'etl', 1, null, null, 'deliver_date,sheetid,outshopid,inshopid,editor,operator,paytypeid,goodsid,goodsname,cost,logistics,venderid,vendername,costtaxrate,saletaxrate,bun_type,rt_qty,rt_cost,rt_costvalue,rt_boxes,pknum,barcode,category_id');
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (92, 8, 'CK_RECEIPT_L', 'CRV验收清单', 'select to_char(t1.checkdate, ''yyyy-mm-dd'') as checkdate,
       t1.sheetid,
        replace(t1.OPERATOR,''\\'','''')  as OPERATOR,
       replace(t1.EDITOR, ''\\'', '''')  as EDITOR,
       t1.shopid as dc_id,
       t1.venderid,
       t1.paytypeid||'''' as paytypeid,
       t1.logistics||'''' as logistics,
       t2.goodsid||'''' as goodsid,
       t2.categoryid||'''' as categoryid,
       t2.shopid as order_shopid,
       to_char(t2.pknum,''FM999999990'') as pknum,
       to_char(t2.costtaxrate,''FM999999990.0000'') as costtaxrate,
       to_char(t2.saletaxrate,''FM999999990.0000'') as saletaxrate,
       ''crv'' as datasource,
        to_char(t2.qty,''FM999999990.0000'') as rpt_qty,
        to_char(t2.qty * t2.cost,''FM999999990.0000'') as rpt_costvalue ,
        to_char((t2.qty * t2.cost)  / (1+ t2.costtaxrate/100),''FM999999990.0000'') as costtaxvalue
  from dw.receipt t1,
       dw.receiptitem t2
  where t1.sheetid = t2.sheetid
    and  t1.checkdate >= to_date(''{DAY}'', ''yyyy-mm-dd'') - {PRE_DAYS}
    AND t1.checkdate   < to_date(''{DAY}'', ''yyyy-mm-dd'') + 1
    and t1.shopid != ''995045''
', 'etl', 1, null, null, 'checkdate,sheetid,operator,editor,dc_id,venderid,paytypeid,logistics,goodsid,categoryid,order_shopid,pknum,costtaxrate,saletaxrate,datasource,rpt_qty,rpt_costvalue,rpt_costtaxvalue');
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (93, 8, 'CK_JV_RECEIPT_L', 'JV验收清单', 'select  to_char(a.sdate, ''yyyy-mm-dd'') as checkdate,
         a.sheetid as sheetid,
         '''' as OPERATOR,
         replace(a.editor, ''\\'', '''')  as EDITOR,
         a.shopid as dc_id,
         a.venderid as venderid,
         a.paytypeid||'''' as paytypeid,
         a.logistics||'''' as logistics,
         b.goodsid as goodsid,
         b.categoryid||'''' as categoryid,
         '''' as order_shopid,
         ''0.0000'' as pknum,
         to_char(b.costtaxrate,''FM999999990.0000'') as costtaxrate,
         to_char(b.saletaxrate,''FM999999990.0000'') as saletaxrate,
         ''jv'' as datasource,
         to_char(b.qty,''FM999999990.0000'') as rpt_qty,
         to_char(b.costvalue + b.costtaxvalue,''FM999999990.0000'') as rpt_costvalue ,
         to_char(b.costvalue,''FM999999990.0000'') as costtaxvalue
 FROM dw.acc_jv_receipt     a,
      dw.acc_jv_receiptitem b
WHERE a.sheetid = b.sheetid
  AND a.sheetid NOT LIKE ''CT%''
  AND a.sdate >= to_date(''{DAY}'', ''yyyy-mm-dd'') - {PRE_DAYS}
  AND a.sdate  < to_date(''{DAY}'', ''yyyy-mm-dd'') + 1', 'etl', 1, null, null, 'checkdate,sheetid,operator,editor,dc_id,venderid,paytypeid,logistics,goodsid,categoryid,order_shopid,pknum,costtaxrate,saletaxrate,datasource,rpt_qty,rpt_costvalue,rpt_costtaxvalue');
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (94, 8, 'CK_SG_RECEIPT_L', '苏果验收清单', 'select to_char(to_date(tt.check_date, ''yyyymmdd''), ''yyyy-mm-dd'') as checkdate,
        tt.CHECK_ID as sheetid,
        '''' as operator,
        '''' as editor,
        tt.shop_id as dc_id,
        tt.venderid,
        replace_ts(tt.vendername) AS vendername,
        case when tt.logistics in (''3调整'', ''0存储'') then ''3''
            when  tt.logistics in (''1直流'') then ''2''
              else ''2'' end logistics,
        tt.goods_code as goodsid,
        replace_ts(tt.goods_name) as goodsname,
        tt.category_id as categoryid,
         tt.category_name as categoryname,
        tt.category_low_id as low_categoryid,
        tt.category_low_name as low_categoryname,
        tt.category_middle_id as middle_categoryid,
        tt.category_middle_name as middle_categoryname,
        '''' as order_shopid,
        to_char(pknum,''FM999999990.0000'') as pknum,
        ''0.0000'' as costtaxrate,
        ''0.000'' as saletaxrate,
        ''sg'' as datasource,
        to_char(tt.check_scatter_num,''FM999999990.0000'') as rpt_qty,
        to_char(tt.check_taxamount,''FM999999990.0000'')  as rpt_costvalue,
        to_char(tt.check_cost,''FM999999990.0000'') as rpt_costtaxvalue,
        to_char(tt.check_box_num,''FM999999990.0000'') as rpt_boxes
  from dw.sg_check_t tt
where  tt.logistics not in (''2退货'')
 and tt.check_date >= to_char( to_date(''{DAY}'', ''yyyy-mm-dd'') - {PRE_DAYS}, ''yyyymmdd'')
 and tt.check_date < to_char( to_date(''{DAY}'', ''yyyy-mm-dd'') + 1, ''yyyymmdd'')', 'etl', 1, null, null, 'checkdate,sheetid,operator,editor,dc_id,venderid,vendername,logistics,goodsid,goodsname,categoryid,categoryname,low_categoryid,low_categoryname,middle_categoryid,middle_categoryname,order_shopid,pknum,costtaxrate,saletaxrate,datasource,rpt_qty,rpt_costvalue,rpt_costtaxvalue,rpt_boxes');
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (95, 8, 'CK_XG_RECEIPT_L', '香港验收清单', 'select to_char(to_date(tt.check_date, ''yyyymmdd''), ''yyyy-mm-dd'') as checkdate,
        tt.CHECK_ID as sheetid,
        '''' as operator,
        '''' as editor,
        tt.shop_id as dc_id,
        '''' as venderid,
        '''' as vendername,
        case when tt.logistics in (''G'') then ''3''
            when  tt.logistics in (''F'', ''T'') then ''2''
              else ''2'' end logistics,
        tt.goods_code as goodsid,
        replace_ts(tt.goods_name)as goodsname,
        tt.category_id as categoryid,
        '''' as categoryname,
        '''' as low_categoryid,
        '''' as low_categoryname,
        '''' as middle_categoryid,
        '''' as middle_categoryname,
        '''' as order_shopid,
        to_char(tt.pknum,''FM999999990.0000'') as pknum,
        ''0.0000'' as costtaxrate,
        ''0.0000'' as saletaxrate,
        ''xg'' as datasource,
        to_char(tt.check_scatter_num,''FM999999990.0000'')  as rpt_qty,
        to_char(tt.cost * tt.check_scatter_num ,''FM999999990.0000'')  as rpt_costvalue,
        to_char(tt.cost * tt.check_scatter_num,''FM999999990.0000'')  as rpt_costtaxvalue,
        to_char(tt.check_box_num,''FM999999990.0000'')  as rpt_boxes
  from dw.xg_check_t tt
where tt.logistics in (''G'', ''F'', ''T'')
  and tt.check_date >= to_char(to_date(''{DAY}'', ''yyyy-mm-dd'') - {PRE_DAYS}, ''yyyymmdd'')
  and tt.check_date < to_char(to_date(''{DAY}'', ''yyyy-mm-dd'') + 1, ''yyyymmdd'')', 'etl', 1, null, null, 'checkdate,sheetid,operator,editor,dc_id,venderid,vendername,logistics,goodsid,goodsname,categoryid,categoryname,low_categoryid,low_categoryname,middle_categoryid,middle_categoryname,order_shopid,pknum,costtaxrate,saletaxrate,datasource,rpt_qty,rpt_costvalue,rpt_costtaxvalue,rpt_boxes');
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (96, 8, 'CK_JVDMS_RATION_L', 'JVDMS配送清单', 'SELECT to_char(t.outexedate,''yyyy-mm-dd'') as deliver_date,
        t.sheetid,
        case when t.rationtype = ''I'' then t.outshopid else t.inshopid end outshopid,
        case when t.rationtype = ''I'' then t.inshopid else t.outshopid end inshopid,
        replace(replace(t.editor, ''\\'', ''''), chr(13),'''') as editor,
        replace(replace(t.operator, ''\\'', ''''), chr(13),'''') as operator,
        t2.paytypeid||'''' as paytypeid,
        t2.goodsid||'''' as goodsid,
        to_char(t2.cost,''FM999999990.0000'') as cost,
        t1.LOGISTICS||'''' as item_logistics,
        t2.logistics||'''' as logistics,
        t2.venderid,
        to_char(t2.costtaxrate,''FM999999990.0000'')  as costtaxrate,
        to_char(t2.saletaxrate,''FM999999990.0000'')  as saletaxrate,
        ''1'' as bun_type,
        to_char(t2.outqty,''FM999999990.0000'') as rt_qty,
        to_char(t2.cost * t2.outqty,''FM999999990.0000'') as rt_costvalue,
        to_char(t2.cost * t2.outqty / (1 + t2.costtaxrate / 100),''FM999999990.0000'') as rt_cost,
        to_char(t1.pknum,''FM999999990.0000'') as pknum,
        t2.CATEGORYID
  FROM dw.jv_ration     t
    inner join   dw.jv_rationitem t1 on t.SHEETID = t1.SHEETID
    left join dw.JV_RATIONACC  t2 on  t.sheetid = t2.sheetid AND t1.goodsid = t2.goodsid
 WHERE  t.outexedate >= to_date(''{DAY}'', ''yyyy-mm-dd'') - {PRE_DAYS}
   AND t.outexedate   < to_date(''{DAY}'', ''yyyy-mm-dd'') + 1
   AND t.rationtype in ( ''I'')', 'etl', 1, null, null, 'deliver_date,sheetid,outshopid,inshopid,editor,operator,paytypeid,goodsid,cost,logistics_item,logistics,venderid,costtaxrate,saletaxrate,bun_type,rt_qty,rt_costvalue,rt_cost,pknum,low_categoryid');
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (97, 8, 'CK_RPT_PACKING', '仓储系统验收包装规则', 'select  to_char(DAY, ''yyyy-mm-dd'') as stat_day,
        DC_ID,
        ARTICLE_NO,
        to_char(PACKING_QTY,''FM999999990.00'') as PACKING_QTY,
        GOODSID||'''' as GOODSID
 from mk.bm_article_packing_t
where DAY >=to_date(''{DAY}'', ''yyyy-mm-dd'') - {PRE_DAYS}
  and DAY  < to_date(''{DAY}'', ''yyyy-mm-dd'') + 1', 'etl', 1, null, null, 'stat_day,dc_id,article_no,packing_qty,goodsid');
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (98, 8, 'CK_STK_ITBI', 'bi库存明细', 'SELECT to_char(sday,''yyyy-mm-dd'') as SDAY,
      SHOPID||'''' as SHOPID,
      GOODSID||'''' as GOODSID,
      to_char(QTY,''FM999999990.00'') as QTY,
      to_char(COST,''FM999999990.00'') as COST,
      to_char(COSTVALUE,''FM999999990.00'') as COSTVALUE,
      BU||'''' as BU
  FROM dw.DMS_BI_SHOPSTOCK t
 WHERE t.SDAY >= to_date(''{DAY}'', ''yyyy-mm-dd'') - {PRE_DAYS}
  AND t.SDAY  < to_date(''{DAY}'', ''yyyy-mm-dd'') + 1', 'etl', 1, null, null, 'sday,shopid,goodsid,qty,cost,costvalue,bu');
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (99, 8, 'CK_SG_SHOP', '维表 - CK苏果店铺信息', 'select shop_id,
       shop_name,
       shop_from_id,
       shop_from_name,
       provinces,
       city
  from mk.sg_shop tt', 'etl', 1, null, null, 'shop_id,shop_name,shop_from_id,shop_from_name,provinces,city');
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (100, 8, 'IM_IMPORT_M_NWMS', 'IM_IMPORT_M_NWMS记录数', 'select ''{DAY}'' as stat_day_s,
       ''{TOPIC_ENNAME}'' as metric_topic,
       ''{SYSTEM_ENNAME}'' as metric_source,
       metric_date,
       metric_key,
       ''day'' as agg_type,
       ''{"logs":'' || logs || ''}'' as metric_value
  from (select metric_date, metric_key, ''day'' as agg_type, sum(1) as logs
          from (select t2.BATCH_SERIAL_NO as metric_key,
                       to_char(t1.rgst_date, ''yyyymmdd'') as metric_date
                  from dw.IM_IMPORT_M_NWMS t1,
                       dw.BM_DEFDCSHOP_NWMS t2
                 where t1.LOCNO=t2.LOCNO
                   and t1.rgst_date >= trunc(sysdate) - 90
                   and t1.rgst_date < trunc(sysdate))
         GROUP BY metric_date, metric_key)', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (101, 8, 'IM_IMPORT_D_NWMS', 'IM_IMPORT_D_NWMS记录数', 'select ''{DAY}'' as stat_day_s,
       ''{TOPIC_ENNAME}'' as metric_topic,
       ''{SYSTEM_ENNAME}'' as metric_source,
       metric_date,
       metric_key,
       ''day'' as agg_type,
       ''{"logs":'' || logs || ''}'' as metric_value
  from (select metric_date, metric_key, ''day'' as agg_type, sum(1) as logs
          from (select t3.BATCH_SERIAL_NO as metric_key,
                       to_char(t1.rgst_date, ''yyyymmdd'') as metric_date
                  from dw.IM_IMPORT_M_NWMS t1, dw.IM_IMPORT_D_NWMS T2,dw.BM_DEFDCSHOP_NWMS T3
                 where t1.locno = t2.locno
                   and t1.owner_no = t2.owner_no
                   and t1.import_no = t2.import_no
                   and t1.LOCNO=t3.LOCNO
                   and t1.rgst_date >= trunc(sysdate) - 90
                   and t1.rgst_date < trunc(sysdate))
         GROUP BY metric_date, metric_key)', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (102, 8, 'IM_CHECK_M_NWMS', 'IM_CHECK_M_NWMS记录数', 'select ''{DAY}'' as stat_day_s,
       ''{TOPIC_ENNAME}'' as metric_topic,
       ''{SYSTEM_ENNAME}'' as metric_source,
       metric_date,
       metric_key,
       ''day'' as agg_type,
       ''{"logs":'' || logs || ''}'' as metric_value
  from (select metric_date, metric_key, ''day'' as agg_type, sum(1) as logs
          from (select t2.BATCH_SERIAL_NO as metric_key,
                       to_char(t1.rgst_date, ''yyyymmdd'') as metric_date
                  from dw.IM_CHECK_M_NWMS t1,
                       dw.BM_DEFDCSHOP_NWMS t2
                 where t1.LOCNO=t2.LOCNO
                   and t1.rgst_date >= trunc(sysdate) - 90
                   and t1.rgst_date < trunc(sysdate))
         GROUP BY metric_date, metric_key)', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (103, 8, 'IM_CHECK_D_NWMS', 'IM_CHECK_D_NWMS记录数', 'select ''{DAY}'' as stat_day_s,
       ''{TOPIC_ENNAME}'' as metric_topic,
       ''{SYSTEM_ENNAME}'' as metric_source,
       metric_date,
       metric_key,
       ''day'' as agg_type,
       ''{"logs":'' || logs || ''}'' as metric_value
  from (select metric_date, metric_key, ''day'' as agg_type, sum(1) as logs
          from (select t3.BATCH_SERIAL_NO as metric_key,
                       to_char(t1.rgst_date, ''yyyymmdd'') as metric_date
                  from dw.IM_CHECK_M_NWMS t1, dw.IM_CHECK_D_NWMS T2,dw.BM_DEFDCSHOP_NWMS T3
                 where t1.locno = t2.locno
                   and t1.owner_no = t2.owner_no
                   and t1.CHECK_NO =t2.CHECK_NO
                   and t1.LOCNO=t3.LOCNO
                   and t1.rgst_date >= trunc(sysdate) - 90
                   and t1.rgst_date < trunc(sysdate))
         GROUP BY metric_date, metric_key)', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (104, 8, 'OM_DELIVER_M_NWMS', 'OM_DELIVER_M_NWMS记录数', 'select ''{DAY}'' as stat_day_s,
       ''{TOPIC_ENNAME}'' as metric_topic,
       ''{SYSTEM_ENNAME}'' as metric_source,
       metric_date,
       metric_key,
       ''day'' as agg_type,
       ''{"logs":'' || logs || ''}'' as metric_value
  from (select metric_date, metric_key, ''day'' as agg_type, sum(1) as logs
          from (select t2.BATCH_SERIAL_NO as metric_key,
                       to_char(t1.rgst_date, ''yyyymmdd'') as metric_date
                  from dw.OM_DELIVER_M_NWMS t1,
                       dw.BM_DEFDCSHOP_NWMS t2
                 where t1.LOCNO=t2.LOCNO
                   and t1.rgst_date >= trunc(sysdate) - 90
                   and t1.rgst_date < trunc(sysdate))
         GROUP BY metric_date, metric_key)', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (105, 8, 'OM_DELIVER_D_NWMS', 'OM_DELIVER_D_NWMS记录数', 'select ''{DAY}'' as stat_day_s,
       ''{TOPIC_ENNAME}'' as metric_topic,
       ''{SYSTEM_ENNAME}'' as metric_source,
       metric_date,
       metric_key,
       ''day'' as agg_type,
       ''{"logs":'' || logs || ''}'' as metric_value
  from (select metric_date, metric_key, ''day'' as agg_type, sum(1) as logs
          from (select t3.BATCH_SERIAL_NO as metric_key,
                       to_char(t1.rgst_date, ''yyyymmdd'') as metric_date
                  from dw.OM_DELIVER_M_NWMS t1, dw.OM_DELIVER_D_NWMS T2,dw.BM_DEFDCSHOP_NWMS T3
                 where t1.locno = t2.locno
                   and t1.owner_no = t2.owner_no
                   and t1.DELIVER_NO =t2.DELIVER_NO
                   and t1.LOCNO=t3.LOCNO
                   and t1.rgst_date >= trunc(sysdate) - 90
                   and t1.rgst_date < trunc(sysdate))
         GROUP BY metric_date, metric_key)', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (106, 8, 'IM_IMPORT_M', 'IM_IMPORT_M记录数', 'select ''{DAY}'' as stat_day_s,
       ''{TOPIC_ENNAME}'' as metric_topic,
       ''{SYSTEM_ENNAME}'' as metric_source,
       metric_date,
       metric_key,
       ''day'' as agg_type,
       ''{"logs":'' || logs || ''}'' as metric_value
  from (select metric_date, metric_key, ''day'' as agg_type, sum(1) as logs
          from (select t1.dc_id as metric_key,
                       to_char(t1.rgst_date, ''yyyymmdd'') as metric_date
                  from dw.IM_IMPORT_M t1
                 where t1.rgst_date >= trunc(sysdate) - 90
                   and t1.rgst_date < trunc(sysdate))
         GROUP BY metric_date, metric_key)', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (107, 8, 'IM_IMPORT_D', 'IM_IMPORT_D记录数', 'select ''{DAY}'' as stat_day_s,
       ''{TOPIC_ENNAME}'' as metric_topic,
       ''{SYSTEM_ENNAME}'' as metric_source,
       metric_date,
       metric_key,
       ''day'' as agg_type,
       ''{"logs":'' || logs || ''}'' as metric_value
  from (select metric_date, metric_key, ''day'' as agg_type, sum(1) as logs
          from (select t1.dc_id as metric_key,
                       to_char(t1.rgst_date, ''yyyymmdd'') as metric_date
                  from dw.IM_IMPORT_M t1, dw.IM_IMPORT_D T2
                 where t1.import_no = t2.import_no and t1.dc_id=t2.dc_id
                   and t1.rgst_date >= trunc(sysdate) - 90
                   and t1.rgst_date < trunc(sysdate))
         GROUP BY metric_date, metric_key)', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (108, 8, 'IM_CHECK_M', 'IM_CHECK_M记录数', 'select ''{DAY}'' as stat_day_s,
       ''{TOPIC_ENNAME}'' as metric_topic,
       ''{SYSTEM_ENNAME}'' as metric_source,
       metric_date,
       metric_key,
       ''day'' as agg_type,
       ''{"logs":'' || logs || ''}'' as metric_value
  from (select metric_date, metric_key, ''day'' as agg_type, sum(1) as logs
          from (select t1.dc_id as metric_key,
                       to_char(t1.rgst_date, ''yyyymmdd'') as metric_date
                  from dw.IM_CHECK_M t1
                 where t1.rgst_date >= trunc(sysdate) - 90
                   and t1.rgst_date < trunc(sysdate))
         GROUP BY metric_date, metric_key)', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (109, 8, 'IM_CHECK_D', 'IM_CHECK_D记录数', 'select ''{DAY}'' as stat_day_s,
       ''{TOPIC_ENNAME}'' as metric_topic,
       ''{SYSTEM_ENNAME}'' as metric_source,
       metric_date,
       metric_key,
       ''day'' as agg_type,
       ''{"logs":'' || logs || ''}'' as metric_value
  from (select metric_date, metric_key, ''day'' as agg_type, sum(1) as logs
          from (select t1.dc_id as metric_key,
                       to_char(t1.rgst_date, ''yyyymmdd'') as metric_date
                  from dw.IM_CHECK_M t1, dw.IM_CHECK_D T2
                 where t1.CHECK_NO = t2.CHECK_NO and t1.dc_id=t2.dc_id
                   and t1.rgst_date >= trunc(sysdate) - 90
                   and t1.rgst_date < trunc(sysdate))
         GROUP BY metric_date, metric_key)', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (110, 8, 'OM_DELIVER_M', 'OM_DELIVER_M记录数', 'select ''{DAY}'' as stat_day_s,
       ''{TOPIC_ENNAME}'' as metric_topic,
       ''{SYSTEM_ENNAME}'' as metric_source,
       metric_date,
       metric_key,
       ''day'' as agg_type,
       ''{"logs":'' || logs || ''}'' as metric_value
  from (select metric_date, metric_key, ''day'' as agg_type, sum(1) as logs
          from (select t1.dc_id as metric_key,
                       to_char(t1.rgst_date, ''yyyymmdd'') as metric_date
                  from dw.OM_DELIVER_M t1
                 where t1.rgst_date >= trunc(sysdate) - 90
                   and t1.rgst_date < trunc(sysdate))
         GROUP BY metric_date, metric_key)', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (111, 8, 'OM_DELIVER_D', 'OM_DELIVER_D记录数', 'select ''{DAY}'' as stat_day_s,
       ''{TOPIC_ENNAME}'' as metric_topic,
       ''{SYSTEM_ENNAME}'' as metric_source,
       metric_date,
       metric_key,
       ''day'' as agg_type,
       ''{"logs":'' || logs || ''}'' as metric_value
  from (select metric_date, metric_key, ''day'' as agg_type, sum(1) as logs
          from (select t1.dc_id as metric_key,
                       to_char(t1.rgst_date, ''yyyymmdd'') as metric_date
                  from dw.om_deliver_m t1, dw.om_deliver_d T2
                 where t1.DELIVER_NO = t2.DELIVER_NO
                   and t1.dc_id=t2.dc_id
                   and t1.rgst_date >= trunc(sysdate) - 90
                   and t1.rgst_date < trunc(sysdate))
         GROUP BY metric_date, metric_key)', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (112, 5, 'OM_LOCATE_M_NWMS', 'OM_LOCATE_M_NWMS出货头表', 'select  locno,
      locate_no,
      exp_type,
      status,
      locate_name,
      locate_date,
      fast_flag,
      divide_flag,
      specify_cell,
      exp_date,
      hm_manual_flag,
      task_batch,
      source_type,
      tms_seqid
from NWMS_WMS.om_locate_m llm
where llm.LOCATE_DATE >= to_date(''{DAY}'', ''yyyy-mm-dd'')
  and llm.LOCATE_DATE  < to_date(''{DAY}'', ''yyyy-mm-dd'') + 1
union
select  locno,
      locate_no,
      exp_type,
      status,
      locate_name,
      locate_date,
      fast_flag,
      divide_flag,
      specify_cell,
      exp_date,
      hm_manual_flag,
      task_batch,
      source_type,
      tms_seqid
from NWMS_WMS.OM_LOCATE_MHTY llm
where llm.LOCATE_DATE >= to_date(''{DAY}'', ''yyyy-mm-dd'')
  and llm.LOCATE_DATE  < to_date(''{DAY}'', ''yyyy-mm-dd'') + 1', 'etl', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (113, 5, 'OM_LOCATE_D_NWMS', 'OM_LOCATE_D_NWMS出货明细', 'select  old.LOCNO,
        old.OWNER_NO,
        old.LOCATE_NO,
        old.ROW_ID,
        old.CUST_NO,
        old.SUB_CUST_NO,
        old.EXP_NO,
        old.ARTICLE_NO,
        old.PLAN_QTY,
        old.LOCATED_QTY,
        old.STATUS,
        old.LINE_NO,
        old.BATCH_NO,
        old.CONDITION,
        old.SPECIAL_BATCH,
        old.B_OUT_FLAG,
        old.PRIORITY,
        old.ADD_EXP_NO,
        old.ITEM_TYPE,
        old.EXP_DATE,
        old.PLAN_EXPORT_QTY,
        old.EXPORT_QTY,
        old.IMPORT_NO,
        old.STOCK_TYPE,
        old.QUALITY
from NWMS_WMS.om_locate_m llm,
     NWMS_WMS.om_locate_d old
where llm.LOCNO = old.LOCNO
  and llm.LOCATE_NO = old.LOCATE_NO
  and llm.LOCATE_DATE >= to_date(''{DAY}'', ''yyyy-mm-dd'')
  and llm.LOCATE_DATE  < to_date(''{DAY}'', ''yyyy-mm-dd'') + 1
union
select  old.LOCNO,
        old.OWNER_NO,
        old.LOCATE_NO,
        old.ROW_ID,
        old.CUST_NO,
        old.SUB_CUST_NO,
        old.EXP_NO,
        old.ARTICLE_NO,
        old.PLAN_QTY,
        old.LOCATED_QTY,
        old.STATUS,
        old.LINE_NO,
        old.BATCH_NO,
        old.CONDITION,
        old.SPECIAL_BATCH,
        old.B_OUT_FLAG,
        old.PRIORITY,
        old.ADD_EXP_NO,
        old.ITEM_TYPE,
        old.EXP_DATE,
        old.PLAN_EXPORT_QTY,
        old.EXPORT_QTY,
        old.IMPORT_NO,
        old.STOCK_TYPE,
        old.QUALITY
from NWMS_WMS.OM_LOCATE_MHTY llm,
     NWMS_WMS.OM_LOCATE_DHTY old
where llm.LOCNO = old.LOCNO
  and llm.LOCATE_NO = old.LOCATE_NO
  and llm.LOCATE_DATE >= to_date(''{DAY}'', ''yyyy-mm-dd'')
  and llm.LOCATE_DATE  < to_date(''{DAY}'', ''yyyy-mm-dd'') + 1', 'etl', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (114, 1, 'CK_RET_L', '退供日清单表', 'select to_char(retcheckdate,''%Y-%m-%d'') as retcheckdate,
       trim(t.sheetid) as sheetid,
       trim(t.refsheetid) as refsheetid,
       t.askshopid,
       t.shopid,
       t.tdcshopid tdcshopid,
       t1.reasontypeid,
       t.logistics||'''' as logistics,
       t.venderid,
       t2.goodsid||'''' as goodsid,
       to_char(t2.cost,''<<<<<<<<<<<.****'')   as  acc_goodcost,
       to_char(t2.COSTTAXRATE,''<<<<<<<<<<<.****'')   as costtaxrate,
       to_char(t2.SALETAXRATE,''<<<<<<<<<<<.****'')   as saletaxrate,
       t1.categoryid||''''  as categoryid,
       t.flag||''''     as flag,
       t.rettype||''''  as rettype,
       t.badflag||''''  as badflag,
       ''0.0000''       as pknum,
       to_char(t1.ASKQTY,''<<<<<<<<<<<.****'')   as item_askqty,
       to_char(t1.RETQTY,''<<<<<<<<<<<.****'')   as item_retqty,

       to_char(t2.qty,''<<<<<<<<<<<.****'')     as acc_qty,
       to_char(t2.cost * t2.qty,''<<<<<<<<<<<.****'')   as ret_cost,
       to_char(t2.cost * t2.qty / (1 + t2.costtaxrate / 100),''<<<<<<<<<<<.****'')   as  ret_costvalue,
       ''{DC_ID}'' as systemsource
from ret      t,
     retitem  t1,
     retacc   t2
where t.sheetid = t1.sheetid

  and t.sheetid = t2.sheetid

  and t1.goodsid = t2.goodsid

  and t.retcheckdate >= date(''{DAY}'') - {PRE_DAYS} 
  and t.retcheckdate  < date(''{DAY}'') + 1 UNITS day', 'etl', 1, null, null, 'retcheckdate,sheetid,refsheetid,askshopid,shopid,tdcshopid,reasontypeid,logistics,venderid,goodsid,acc_goodcost,costtaxrate,saletaxrate,categoryid,flag,rettype,badflag,pknum,ret_item_askqty,ret_item_retqty,ret_acc_qty,ret_costvalue,ret_cost,systemsource');
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (115, 3, 'CK_JV_RET_L', 'ACC退供清单表', 'SELECT  to_char(t.sdate, ''yyyy-mm-dd'') as sdate,
         t.sheetid,
         t.shopid,
          t.venderid,
         t1.goodsid||'''' as goodsid,
         t1.categoryid||'''' as categoryid,
         ''0.0000'' pknum,
         to_char(t1.costtaxrate,''FM99999990.0000'') AS costtaxrate,
         to_char(t1.saletaxrate,''FM99999990.0000'') AS saletaxrate,
         to_char(t1.qty,''FM99999990.0000'') AS qty,
         to_char(t1.costvalue * (1 + t1.costtaxrate / 100),''FM99999990.0000'') AS cost, --含税
         to_char(t1.costvalue,''FM99999990.0000'') AS costvalue                        --不含税
    from JVD2.acc_jv_ret     t,
         JVD2.acc_jv_retitem t1
   where t.sheetid = t1.sheetid
     and t.sdate >= to_date(''{DAY}'', ''yyyy-mm-dd'') - {PRE_DAYS}
     and t.sdate  <  to_date(''{DAY}'', ''yyyy-mm-dd'') + 1', 'etl', 1, null, null, 'sdate,sheetid,shopid,venderid,goodsid,categoryid,pknum,costtaxrate,saletaxrate,qty,costvalue,cost');
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (116, 8, 'CK_SG_RET_L', '苏果退供清单表', 'select to_char(to_date(tt.check_date, ''yyyymmdd''), ''yyyy-mm-dd'') as checkdate,
        tt.CHECK_ID as sheetid,
        '''' as operator,
        '''' as editor,
        tt.shop_id as dc_id,
        tt.venderid,
        replace_ts(tt.vendername) AS vendername,
        case when tt.logistics in (''3调整'', ''0存储'') then ''3''
            when  tt.logistics in (''1直流'') then ''2''
              else ''2'' end logistics,
        tt.goods_code as goodsid,
        replace_ts(tt.goods_name) as goodsname,
        tt.category_id as categoryid,
         tt.category_name as categoryname,
        tt.category_low_id as low_categoryid,
        tt.category_low_name as low_categoryname,
        tt.category_middle_id as middle_categoryid,
        tt.category_middle_name as middle_categoryname,
        '''' as order_shopid,
        to_char(pknum,''FM999999990.0000'') as pknum,
        ''0.0000'' as costtaxrate,
        ''0.000'' as saletaxrate,
        ''sg'' as datasource,
        to_char(tt.check_scatter_num,''FM999999990.0000'') as ret_qty,
        to_char(tt.check_taxamount,''FM999999990.0000'')  as ret_costvalue,
        to_char(tt.check_cost,''FM999999990.0000'') as ret_costtaxvalue
  from dw.sg_check_t tt
where  tt.logistics  in (''2退货'')
 and tt.check_date >= to_char( to_date(''{DAY}'', ''yyyy-mm-dd'') - {PRE_DAYS}, ''yyyymmdd'')
 and tt.check_date < to_char(to_date(''{DAY}'', ''yyyy-mm-dd'') + 1, ''yyyymmdd'')', 'etl', 1, null, null, 'checkdate,sheetid,operator,editor,dc_id,venderid,vendername,logistics,goodsid,goodsname,categoryid,categoryname,low_categoryid,low_categoryname,middle_categoryid,middle_categoryname,order_shopid,pknum,costtaxrate,saletaxrate,datasource,ret_qty,ret_costvalue,ret_costtaxvalue');
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (117, 5, 'OM_LOCATE_M_NWMS', 'OM_LOCATE_M_NWMS记录数', 'select ''{DAY}'' as stat_day_s,
       ''{TOPIC_ENNAME}'' as metric_topic,
       ''{SYSTEM_ENNAME}'' as metric_source,
       metric_date,
       metric_key,
       ''day'' as agg_type,
       ''{"logs":'' || logs || ''}'' as metric_value
  from (select metric_date, metric_key, ''day'' as agg_type, sum(1) as logs
          from (select t2.BATCH_SERIAL_NO as metric_key,
                       to_char(t1.LOCATE_DATE, ''yyyymmdd'') as metric_date
                  from (select  *  from NWMS_WMS.om_locate_m
                        union
                        select  * from NWMS_WMS.OM_LOCATE_MHTY) t1,
                       NWMS_WMS.BM_DEFDCSHOP t2
                 where t1.LOCNO=t2.LOCNO
                   and t1.LOCATE_DATE >= trunc(sysdate) - 90
                   and t1.LOCATE_DATE < trunc(sysdate))
         GROUP BY metric_date, metric_key)', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (118, 8, 'OM_LOCATE_M_NWMS', 'OM_LOCATE_M_NWMS记录数', 'select ''{DAY}'' as stat_day_s,
       ''{TOPIC_ENNAME}'' as metric_topic,
       ''{SYSTEM_ENNAME}'' as metric_source,
       metric_date,
       metric_key,
       ''day'' as agg_type,
       ''{"logs":'' || logs || ''}'' as metric_value
  from (select metric_date, metric_key, ''day'' as agg_type, sum(1) as logs
          from (select t2.BATCH_SERIAL_NO as metric_key,
                       to_char(t1.LOCATE_DATE, ''yyyymmdd'') as metric_date
                  from dw.OM_LOCATE_M_NWMS t1,
                       dw.BM_DEFDCSHOP_NWMS t2
                 where t1.LOCNO=t2.LOCNO
                   and t1.LOCATE_DATE >= trunc(sysdate) - 90
                   and t1.LOCATE_DATE < trunc(sysdate))
         GROUP BY metric_date, metric_key)', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (119, 5, 'OM_LOCATE_D_NWMS', 'OM_LOCATE_D_NWMS记录数', 'select ''{DAY}'' as stat_day_s,
       ''{TOPIC_ENNAME}'' as metric_topic,
       ''{SYSTEM_ENNAME}'' as metric_source,
       metric_date,
       metric_key,
       ''day'' as agg_type,
       ''{"logs":'' || logs || ''}'' as metric_value
  from (select metric_date, metric_key, ''day'' as agg_type, sum(1) as logs
          from (select t2.BATCH_SERIAL_NO as metric_key,
                       to_char(t1.LOCATE_DATE, ''yyyymmdd'') as metric_date
                  from (select  old.*,llm.LOCATE_DATE  from NWMS_WMS.om_locate_m llm, NWMS_WMS.om_locate_d old
                         where llm.LOCNO = old.LOCNO
                           and llm.LOCATE_NO = old.LOCATE_NO
                         union
                        select  old.*,llm.LOCATE_DATE from NWMS_WMS.OM_LOCATE_MHTY llm, NWMS_WMS.OM_LOCATE_DHTY old
                         where llm.LOCNO = old.LOCNO
                           and llm.LOCATE_NO = old.LOCATE_NO) t1,
                       NWMS_WMS.BM_DEFDCSHOP t2
                 where t1.LOCNO=t2.LOCNO
                   and t1.LOCATE_DATE >= trunc(sysdate) - 90
                   and t1.LOCATE_DATE < trunc(sysdate))
         GROUP BY metric_date, metric_key)', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (120, 8, 'OM_LOCATE_D_NWMS', 'OM_LOCATE_D_NWMS记录数', 'select ''{DAY}'' as stat_day_s,
       ''{TOPIC_ENNAME}'' as metric_topic,
       ''{SYSTEM_ENNAME}'' as metric_source,
       metric_date,
       metric_key,
       ''day'' as agg_type,
       ''{"logs":'' || logs || ''}'' as metric_value
  from (select metric_date, metric_key, ''day'' as agg_type, sum(1) as logs
          from (select t3.BATCH_SERIAL_NO as metric_key,
                       to_char(t1.LOCATE_DATE, ''yyyymmdd'') as metric_date
                  from dw.OM_LOCATE_M_NWMS t1, dw.OM_LOCATE_D_NWMS T2,dw.BM_DEFDCSHOP_NWMS T3
                 where t1.locno = t2.locno
                   and t1.LOCATE_NO =t2.LOCATE_NO
                   and t1.LOCNO=t3.LOCNO
                   and t1.LOCATE_DATE >= trunc(sysdate) - 90
                   and t1.LOCATE_DATE < trunc(sysdate))
         GROUP BY metric_date, metric_key)', 'monitor', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (121, 8, 'CK_SALE_L', '事实表 - 日销售清单', 'select to_char(hsday, ''yyyy-mm-dd'') as hsday,
        shopid,
        placeid||'''' as placeid,
        brandid||'''' as brandid,
        familyid||'''' as familyid,
        goodsid||'''' as goodsid,
        shelfid||'''' as shelfid,
        venderid||'''' as venderid,
        payflag||'''' as payflag,
        paytypeid||'''' as paytypeid,
        to_char(costtaxrate,''FM999999990.0000'') as costtaxrate,
        to_char(saletaxrate,''FM999999990.0000'') as saletaxrate,
        logistics||'''' as logistics,
        disctype||'''' as disctype,
        pickflag||'''' as pickflag,
        to_char(saleqty,''FM999999990.0000'') as saleqty,
        to_char(salevalue,''FM999999990.0000'') as salevalue,
        to_char(discvalue,''FM999999990.0000'') as discvalue,
        to_char(saletax,''FM999999990.0000'') as saletax,
        to_char(salecostvalue,''FM999999990.0000'') as salecostvalue,
        to_char(costdisc,''FM999999990.0000'') as costdisc,
        to_char(costtax,''FM999999990.0000'') as costtax,
        to_char(discticketvalue,''FM999999990.0000'') as discticketvalue,
        source
 from dw.SALE_SHOP_SKU_DAY
where hsday >=to_date(''{DAY}'', ''yyyy-mm-dd'') - {PRE_DAYS}
  and hsday  < to_date(''{DAY}'', ''yyyy-mm-dd'') + 1', 'etl', 1, null, null, 'hsday,shopid,placeid,brandid,familyid,goodsid,shelfid,venderid,payflag,paytypeid,costtaxrate,saletaxrate,logistics,disctype,pickflag,saleqty,salevalue,discvalue,saletax,salecostvalue,costdisc,costtax,discticketvalue,source');
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (122, 8, 'MAPPING_CATETORYDEPART_INFO', '品类部门映射表', 'select ''crv'' as system,
       jli.FIELDVALUE as depart_id,
       jli.LOOKUP as depart_name
  from dw.JV_LOOKUPITEM jli
where jli.LOOKUPID=''CATETORYDEPART''
  and jli.BUID = 88
union all
select ''jv'' as system,
       jli.FIELDVALUE as depart_id,
       jli.LOOKUP as depart_name
  from dw.JV_LOOKUPITEM jli
where jli.LOOKUPID=''CATETORYDEPART''
  and jli.BUID = 88
union all
select ''ole'' as system,
       jli.FIELDVALUE as depart_id,
       jli.LOOKUP as depart_name
  from dw.JV_LOOKUPITEM jli
where jli.LOOKUPID=''CATETORYDEPART''
  and jli.BUID = 88', 'etl', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (123, 8, 'CK_WMS_NWMS_PACKAGE', 'WMS,NWMS包装表', 'select  bap.dc_id ,
        bap.article_no||'''' as  article_no,
        bap.owner_article_no as owner_article_no,
        to_char(bap.packing_qty ,''FM99999990'') as packing_qty,
        bap.packing_unit,
        to_char(bap.a_length,''FM99999990.0000'') as a_length,
        to_char(bap.a_width,''FM99999990.0000'') as a_width,
        to_char(bap.a_height,''FM99999990.0000'') as a_height,
        to_char(bap.packing_weight,''FM99999990.0000'') as packing_weight
 from dw.bm_article_packing_l bap', 'etl', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (124, 8, 'CK_WMS_NWMS_GOODS', 'WMS,NWMS商品称重', 'select mm.batch_serial_no,
        tt.article_no||'''' as article_no,
        tt.owner_article_no||'''' as owner_article_no,
        tt.unit,
        tt.group_no||'''' as group_no
  from dw.bm_defarticle_nwms tt,
       dw.bm_defdcshop_nwms  mm
where tt.locno = mm.locno
  and tt.MEASURE_MODE = 1
', 'etl', 1, null, null, 'dc_id,article_no,owner_article_no,unit,group_no');
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (125, 4, 'RPT_KEEPPOCONTENT', 'WMS验收预约', 'select    OPERATE_DATE,
            to_char(L_GROUP_NO) as L_GROUP_NO,
            PO_NO,
            ARTICLE_NO,
            BARCODE,
            ARTICLE_NAME,
            PO_QTY,
            PO_QTYS,
            PO_VALUE,
            SUPPLIER_NO,
            SUPPLIER_NAME,
            END_DATE,
            LAST_DATE,
            PO_TYPE,
            ORDERTYPENAME,
            ORDER_SERIAL,
             '''' as STATUS,
            ''{DC_ID}'' AS DC_ID
    from CRCWMS.v_rpt_keeppocontent', 'etl', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (126, 5, 'RPT_KEEPPOCONTENT', 'NWMS验收预约', 'select   OPERATE_DATE,
        to_char(L_GROUP_NO) as L_GROUP_NO,
        PO_NO,
        ARTICLE_NO,
        BARCODE,
        ARTICLE_NAME,
        PO_QTY,
        PO_QTYS,
        PO_VALUE,
        SUPPLIER_NO,
        SUPPLIER_NAME,
        END_DATE,
        LAST_DATE,
        PO_TYPE,
        ORDER_TYPE_NAME as ORDERTYPENAME,
        ORDER_SERIAL,
         STATUS,
         DC_ID
  FROM NWMS_WMS.v_rpt_keeppocontent T', 'etl', 1, null, null, null);
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (127, 8, 'CK_KPI_CC_RT_RATIO', 'kpi考核 - 存储配送完成率', 'select months as stat_mon,
        dc_id ,
         to_char(po_qty,''FM99999999990.0000'') as order_qty,
        to_char(locate_qty, ''FM99999999990.0000'') as loc_qty,
        to_char(deliver_qty,''FM99999999990.0000'') as rt_qty,
         to_char(deliver_qty_24,''FM99999999990.0000'') as rt_24_qty,
         to_char(deliver_qty_48,''FM99999999990.0000'') as rt_48_qty,
        ''crv'' as datasource
    from mk.delivery_rate_report_t
union all
     select stat_mon,
            dc_id,
            to_char(order_qty,''FM99999999990.0000'')  as order_qty,
            to_char(loc_qty,''FM99999999990.0000'')  as loc_qty,
            to_char(rt_qty,''FM99999999990.0000'')  as rt_qty,
            to_char(rt_24_qty,''FM99999999990.0000'')  as rt_24_qty,
            to_char(rt_48_qty,''FM99999999990.0000'')  as rt_48_qty,
            datasource
      from mk.oss_kpi_cc_complete_ratio_mon', 'etl', 1, null, null, 'stat_mon,dc_id,order_qty,loc_qty,rt_qty,rt_24_qty,rt_48_qty,datasource');
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (128, 8, 'CK_KPI_ZT_RT_RATIO', 'kpi考核 - 直通配送完成率', 'select stat_mon,
        dc_id,
        to_char(order_qty,''FM99999999990.0000'')  as order_qty,
        to_char(rpt_order_qty,''FM99999999990.0000'')  as rpt_order_qty,
        to_char(err_order_qty,''FM99999999990.0000'')  as err_order_qty,
        to_char(rpt_qty,''FM99999999990.0000'')  as rpt_qty,
        to_char(rt_qty,''FM99999999990.0000'')  as rt_qty,
        to_char(rt_24_qty,''FM99999999990.0000'')  as rt_24_qty,
        to_char(rt_48_qty,''FM99999999990.0000'')  as rt_48_qty,
        ''sg'' as datasource
  from mk.oss_kpi_zt_complete_ratio_mon
  union all
select months as stat_mon,
        dc_id,
        to_char(order_qty,''FM99999999990.0000'')  as order_qty,
        to_char(crv.order_qty_check,''FM99999999990.0000'')  as rpt_order_qty,
        to_char(crv.order_qty_cancel,''FM99999999990.0000'')  as err_order_qty,
        to_char(crv.check_qty,''FM99999999990.0000'')  as rpt_qty,
        to_char(crv.deliver_qty,''FM99999999990.0000'')  as rt_qty,
        to_char(crv.deliver_qty_24,''FM99999999990.0000'')  as rt_24_qty,
        to_char(crv.deliver_qty_48,''FM99999999990.0000'')  as rt_48_qty,
        ''crv'' as datasource
  from mk.through_efficiency_report_t crv', 'etl', 1, null, null, 'stat_mon,dc_id,order_qty,rpt_order_qty,err_order_qty,rpt_qty,rt_qty,rt_24_qty,rt_48_qty,datasource');
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (129, 8, 'CK_GOODSHOP', '维表 - 商品合同信息', 'select jg.shopid,
       jg.goodsid,
       to_char(jg.cost, ''FM99999990.0000'') as cost,
       to_char(jg.costtaxrate,''FM99999990.0000'') as costtaxrate,
       to_char(jg.contractcost,''FM99999990.0000'') as contractcost,
       to_char(jg.concost,''FM99999990.0000'') as concost,
       to_char(jg.price,''FM99999990.0000'') as price,
       jg.venderid,
       jg.dcshopid,
       jg.vbuid||'''' as vbuid,
       jg.logistics||'''' as logistics,
       jg.goodsstatus||'''' as goodsstatus,
       jg.ORDERFLAG||'''' as orderflag,
       jg.RETFLAG||'''' as retflag
  from dw.jv_goodsshop jg,
       dw.jv_shop js
  where jg.shopid = js.shopid
    and js.shopformid = 9
', 'etl', 1, null, null, 'shopid,goodsid,cost,costtaxrate,contractcost,concost,price,venderid,dcshpid,vbuid,logistics,goodsstatus,orderflag, retflag');
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (130, 8, 'CK_SPOMS_STK', '事实表 - 大宗批发库存', 'select to_char(tt.inventory_date,''yyyy-mm-dd'') as inventory_date,
       tt.shopid||'''' as shopid,
       tt.goodsid||'''' as goodsid,
       tt.categoryid||'''' as categoryid,
       to_char(tt.qty, ''FM9999999990.0000'') as stk_qty
from mk.oss_oms_inventory tt
where tt.inventory_date >=  date''{DAY}'' - {PRE_DAYS}
  and tt.inventory_date  <  date''{DAY}'' + 1', 'etl', 1, null, null, 'stk_day,shopid,goodsid,categoryid,stk_qty');
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (131, 8, 'CK_GOODSINFO', '维表 - 商品信息', 'select tmp.sysgoodsid,
       tmp.categorytreeid,
       tmp.buid,
       tmp.brandid,
       tmp.goodsid,
       tmp.goodscode,
       tmp.barcode,
       replace_ts(tmp.goodsname) as GOODSNAME,
       replace_ts(tmp.goodsspec) as goodsspec,
       replace_ts(tmp.unitname) as unitname,
       tmp.saletaxrate,
       replace_ts(tmp.producingarea) as producingarea,
       tmp.advicecost,
       tmp.addrate,
       to_char(tmp.begindate, ''yyyy-mm-dd'') as begindate,
       to_char(tmp.endtodate, ''yyyy-mm-dd'')as endtodate, 
       tmp.categoryid as low_categoryid,
       nvl(tmp2.low_categoryname, ''未知'') as low_categoryname,
       nvl(tmp2.middle_categoryid, ''未知'') as middle_categoryid,
       nvl(tmp2.middle_categoryname,''未知'') as middle_categoryname,
       nvl(tmp2.high_categoryid,''未知'') as high_categoryid,
       nvl(tmp2.high_categoryname,''未知'') as high_categoryname,
       nvl(tmp2.catetorydepart, ''待维护'') as catetorydepart
  from 
(select jg.sysgoodsid,
       jb.categorytreeid,
       jg.buid,
       jg.brandid,
       jg.goodsid,
       jg.goodscode,
       jg.barcode,
       jg.goodsname,
       jg.goodsspec,
       jg.unitname,
       jg.saletaxrate,
       jg.producingarea,
       jg.advicecost,
       jg.addrate,
       jg.begindate,
       jg.endtodate,
       jg.categoryid
  from dw.jv_goods jg,
       dw.jv_buinfo jb
       where jg.buid = jb.buid
       ) tmp,
       (select jc_low.categorytreeid,
           jc_low.deptlevelid,
           jc_low.categoryid as low_categoryid,
           jc_low.categoryname as low_categoryname,

           to_char(jc_middle.categoryid) as middle_categoryid,
           jc_middle.categoryname as middle_categoryname,
           
           to_char(jc_high.categoryid) as high_categoryid,
           jc_high.categoryname as high_categoryname,
            jc_low.catetorydepart||'''' as catetorydepart
      from dw.jv_category jc_low,
           dw.jv_category jc_middle,
           dw.jv_category jc_high
     where jc_low.categorytreeid = jc_middle.categorytreeid
       and jc_low.headcatid = jc_middle.categoryid
       and jc_middle.categorytreeid = jc_high.categorytreeid
       and jc_middle.headcatid = jc_high.categoryid
       ) tmp2
    where tmp.categorytreeid = tmp2.categorytreeid(+)
      and tmp.categoryid  = tmp2.low_categoryid(+)', 'etl', 1, null, null, 'sysgoodsid,categorytreeid,buid,brandid,goodsid,goodscode,barcode,goodsname,goodsspec,unitname,saletaxrate,producingarea,advicecost,addrate,begindate,endtodate,low_categoryid,low_categoryname,middle_categoryid,middle_categoryname,high_categoryid,high_categoryname,catetorydepart');
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (132, 8, 'CK_GOODS_PACKAGE', '维表 - 订货规格信息', 'select BUID,
       BARCODE,
       GOODSID,
       SYSGOODSID,
       PACKAGEID,
       to_single_byte(replace(PKNAME, chr(10), ''''))  as PKNAME,
       to_single_byte(replace(PACKAGESPEC, chr(10), '''')) as PACKAGESPEC,
       PKNUMBER,
       PKWEIGHT,
       replace(TRANSSPEC, chr(10), '''')  as TRANSSPEC,
       SHELFSPECL,
       SHELFSPECW,
       SHELFSPECH,
       LFLAG,
       MFLAG,
       SFLAG,
       TFLAG,
       ORDERFLAG,
       replace(TPND, chr(13), '''')  as TPND
from dw.jv_gpackage
where lflag = 1', 'etl', 1, null, null, 'buid,barcode,goodsid,sysgoodsid,packageid,pkname,packagespec,pknumber,pkweight,transspec,shelfspecl,shelfspecw,shelfspech,lflag,mflag,sflag,tflag,orderflag,tpnd');
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (133, 8, 'CK_VENDERINFO', '维表 - 供应商信息', 'select tt.sysvenderid,
       tt.buid,
       tt.venderid,
       tt.vendercode,
       replace(replace(replace(tt.vendername, '''''''', ''''), ''\\'',''-''), chr(13), '''')  as vendername,
       case when tt.headbuid = ''-1'' then ''0'' else tt.headbuid end headbuid,
       tt.headvendercode,
       tt.vendertype,
       tt.regionid,
       tt.vmanagelevel,
       replace(replace(replace(tt.address, chr(13), ''''), ''\\'',''-''),chr(13), '''')  as address,
       replace(replace(replace(tt.faxno, '''''''', ''''), ''\\'',''-''),chr(13), '''') as  faxno,
       replace(replace(replace(tt.mobilephone, '''''''', ''''), ''\\'',''-''),chr(13), '''') as mobilephone,
       replace(replace(replace(tt.inbranch, '''''''', ''''), ''\\'',''-'') ,chr(13), '''') as inbranch,
       to_char(tt.indate, ''yyyy-mm-dd'') as indate
  from dbusrmaster.vender@JVDMS tt', 'etl', 1, null, null, 'sysvenderid,buid,venderid,vendercode,vendername,headbuid,headvendercode,vendertype,regionid,vmanagelevel,address,faxno,mobilephone,inbranch,indate');
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (134, 8, 'CK_DC_SHOP', '维表 - 仓库店铺信息', 'select tt.sysshopid,
       jb.categorytreeid,
       tt.buid,
       jb.buname,
       tt.shopid,
       tt.shopcode,
       tt.shopname,
       tt.itshoptype,
       tt.shopstatus,
       tt.shopformid,
       jl.shopformname,
       tt.regionid,
       tt.payshopid,
       jb_2.categorytreeid as ts_categorytreeid,
       tt.tsbuid,
       jb_2.buname as tsbuname,
       jb_3.categorytreeid as in_categorytreeid ,
       tt.inbuid,
       jb_3.buname as inbuname,
       case when tt.shopid in ( ''995125'', ''8S9012'') then ''干货'' else nvl(jd.dctype, '''') end dctype,
       case when tt.shopid in ( ''995125'', ''8S9012'') then ''租赁'' else nvl(jd.propertype, '''') end propertype,
       replace(nvl(tt.province,''需维护''), chr(13), 0) as province,
       replace(nvl(tt.city,''需维护''), chr(13), '''') as city,
       replace(tt.provinces_regions_id,chr(13), '''') as provinces_regions_id,
       replace(tt.provinces_regions,chr(13), '''') as provinces_regions,
       to_char(tt.fstartdate, ''yyyy-mm-dd'') as fstartdate,
       to_char(tt.startdate, ''yyyy-mm-dd'') as startdate,
       to_char(tt.enddate, ''yyyy-mm-dd'') as enddate,
       tr.tms_city_id||'''' as tms_city_id,
       tr.position_address,
       tr.longitude,
       tr.latitude
  from dw.jv_shop tt
  left join mk.jt_dcinfo jd on tt.shopid = jd.shopid
  left join (select t.fieldvalue as shopformid,
                   max(t.lookup)  as shopformname
                   from dw.JV_LOOKUPITEM t where t.lookupid = ''SHOPFORM'' and t.buid <> 19
            group by t.fieldvalue) jl on tt.shopformid = jl.shopformid
  left join dw.jv_buinfo jb on tt.buid = jb.buid
  left join dw.jv_buinfo jb_2 on tt.tsbuid = jb_2.buid
  left join dw.jv_buinfo jb_3 on tt.inbuid = jb_3.buid
  left join dw.tms_receiver tr on tt.shopid = tr.code', 'etl', 1, null, null, 'sysshopid,categorytreeid,buid,buname,shopid,shopcode,shopname,itshoptype,shopstatus,shopformid,shopformname,regionid,payshopid,ts_categorytreeid,tsbuid,tsbuname,in_categorytreeid,inbuid,inbuname,dctype,propertype,province,city,provinces_regions_id,provinces_regions,fstartdate,startdate,enddate,tms_city_id,position_address,latitude,longitude');
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (135, 8, 'CK_JV_GOODS_PACKAGE', '维度 - JV订货规格信息', 'select buid,
        barcode,
        goodsid,
        sysgoodsid,
        packageid,
        to_single_byte(replace(pkname, chr(10) , '''')) as pkname,
        to_single_byte(replace(packagespec, chr(10), '''')) as packagespec,
        pknumber,
        pkweight,
        replace(transspec, chr(10), '''') as transspec,
        shelfspecl,
        shelfspecw,
        shelfspech,
        lflag,
        mflag,
        sflag,
        tflag,
        orderflag,
        replace(tpnd, chr(13) ,'''') as tpnd
from dw.acc_jv_gpackage
where lflag = 1
', 'etl', 1, null, null, 'buid,barcode,goodsid,sysgoodsid,packageid,pkname,packagespec,pknumber,pkweight,transspec,shelfspecl,shelfspecw,shelfspech,lflag,mflag,sflag,tflag,orderflag,tpnd');
INSERT INTO etl_monitor.etl_monitor_topic_sql_config (topic_id, system_id, topic_enname, topic_chname, sql_text, model_type, status, create_time, modify_time, target_table_fields) VALUES (136, 8, 'CK_RATION_RET_L_T', 'ODS - crv返配信息', 'select to_char(t.outexedate,''yyyy-mm-dd'') as deliver_date,
       to_char(t.outcheckdate, ''yyyy-mm-dd'') as  outcheckdate,
       to_char(t.incheckdate, ''yyyy-mm-dd'') as incheckdate,
       to_char(t.inexedate, ''yyyy-mm-dd'') as inexedate,
       t.sheettype||'''' as sheettype,
       t.esflag||'''' as esflag,
       trim(t.sheetid) as sheetid,
       case when t.rationtype = ''I'' then t.outshopid else t.inshopid end outshopid,
       case when t.rationtype = ''I'' then t.inshopid else t.outshopid end inshopid,
       replace(replace(t.editor, ''\\'', ''''), chr(13),'''') as editor,
       replace(replace(t.operator, ''\\'', ''''), chr(13),'''') as operator,
       t2.paytypeid||'''' as paytypeid,
       t2.goodsid||'''' as goodsid,
       to_char(t2.cost,''FM999999990.0000'') as cost,
       t2.logistics||'''' as logistics,
       t2.venderid,
       to_char(t2.costtaxrate,''FM999999990.0000'') as costtaxrate,
       to_char(t2.saletaxrate,''FM999999990.0000'') as saletaxrate,
       ''6''  as bun_type,
       to_char(t2.outqty,''FM999999990.0000'') as  rt_qty,
       to_char(t2.cost * t2.outqty,''FM999999990.0000'') as rt_costvalue,
       to_char(t2.cost * t2.outqty / (1 + t2.costtaxrate / 100), ''FM999999990.0000'') rt_cost,
       to_char(t1.pknum,''FM999999990.0000'') as  pknum,
       t2.CATEGORYID||'''' as CATEGORYID,
       retflag||'''' as retflag,
       badflag||'''' as badflag,
       nvl(isdiff, 0)||'''' as isdiff
FROM dw.ration     t,
     dw.rationitem t1,
     dw.rationacc  t2
WHERE t.sheetid = t1.sheetid
AND t.sheetid = t2.sheetid
AND t1.goodsid = t2.goodsid
AND t.outexedate >= to_date(''20180101'', ''yyyymmdd'')
AND t.outexedate  < add_months(to_date(''20180101'', ''yyyymmdd'') ,1)
AND t.outshopid != ''995045''
AND t.rationtype in (''O'')', 'etl', 1, null, null, 'deliver_date,outcheckdate,incheckdate,inexedate,sheettype,esflag,sheetid,outshopid,inshopid,editor,operator,paytypeid,goodsid,cost,logistics,venderid ,costtaxrate,saletaxrate,bun_type,rt_qty ,rt_costvalue ,rt_cost ,pknum ,low_categoryid ,retflag ,badflag ,isdiff');