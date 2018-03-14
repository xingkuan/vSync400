insert into sync_db 
(db_id, db_desc, db_usr, db_pwd, db_conn, db_info, db_type, db_cmd_queue) 
values (1, 'VERTU', 'dbadmin', '3Astere**s', 'jdbc:vertica://vertu01:5433/vertu', 'VERTU - VERTICA - vert schema, target', 2, 'vertsnap.cmd_queue')
;
insert into sync_db 
(db_id, db_desc, db_usr, db_pwd, db_conn, db_info, db_type, db_cmd_queue) 
values (2, 'CRMCLON2', 'vertsnap', 'BAtm0B1L#', 'jdbc:oracle:thin:@crmdbclonetest02:1521:crmp65', 'CRM test - ORACLE - source', 1, 'vertsnap.cmd_queue')
;
insert into sync_db 
(db_id, db_desc, db_usr, db_pwd, db_conn, db_info, db_type, db_cmd_queue) 
values (3, 'CRM', 'vertsnap', 'BAtm0B1L#', 'jdbc:oracle:thin:@crmdb2:1521:crmp64', 'CRM prod - ORACLE - source', 1, 'vertsnap.cmd_queue')
;
insert into sync_db 
(db_id, db_desc, db_usr, db_pwd, db_conn, db_info, db_type, db_cmd_queue) 
values (4, 'VERTX', 'dbadmin', 'Bre@ker321', 'jdbc:vertica://vertx1:5433/vertx', 'VERTX prod - vert replicatation DB', 2, '<obselete col 2018/01/31>')
;

commit;
