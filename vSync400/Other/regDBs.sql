insert into lu_db_type(db_type, db_type_desc) values (3, 'DB2/AS400');

insert into sync_db (db_id, db_desc, db_usr, db_pwd, db_conn, db_info, db_type, db_cmd_queue)
values 
(7, 'DB2T', 'johnlee2', 'Ch@ng1966', 'jdbc:as400://DEVELOPM:2551/DB2_RETSYS', 'DB2/AS400 Test', 3, 'not used!');


commit;
