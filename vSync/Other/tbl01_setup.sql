--set heading off
--set feedback off
--set echo off
--set verify off
--set lines 300


define tblOwner = &1
define tblName = &2
define tblShort = &3
define tgtOwner = &4
define poolID = &5
define refType = &6
-- define tblTX = &3._TX_LOG2   the old ...
-- define tblTr = GU2V_&3
define tblTX = &3._TX_LOG3
define tblTr = GV2V_&3

-- Important Note:
--     Please provide the DB tns name/user/passwd for both the source and meta repository
-- | srcDBtns = 'JOTPP';    -- JOTPP prod
-- |            system/cal618
-- | srcDBtns = 'CRMP65'   -- the clone db for test
-- | srcDBtns = 'CRMP64'
-- |            system/lanchong
define srcDBtns = 'CRMP64';
define srcDBuser = 'system';
define srcDBpwd = 'lanchong';
define repDBtns = 'RMAN01'
define repDBuser = 'vertsnap'
define repDBpwd = 'BAtm0B1L#'

-- Important Note:
--     Please find the SOURCE_DB_ID and TARGET_DB_ID from sync_db
define srcDBid = 3
define tgtDBid = 4
-- VERTU     1
-- CRMCLON2  2
-- CRM       3
-- VertX     4
-- JOTPP     5
-- VERTR     6


connect &srcDBuser/&srcDBpwd@&srcDBtns;
Accept foo PROMPT "Press [Enter]-key to create log table and trigger... "
--
CREATE TABLE &tblOwner..&tblTX  
   (  M_ROW VARCHAR2(255 BYTE),  SNAPTIME DATE) TABLESPACE SNAPLOG 
;

CREATE OR REPLACE TRIGGER &tblOwner..&tblTr 
    AFTER  INSERT OR UPDATE OR DELETE ON &tblOwner..&tblName 
       FOR EACH ROW 
         BEGIN  INSERT INTO &tblOwner..&tblTX (  M_ROW, SNAPTIME  ) 
         VALUES ( :new.rowid, sysdate   );  END; 
/

alter TRIGGER &tblOwner..&tblTr disable; 

connect &repDBuser/&repDBpwd@&repDBtns;
Accept foo PROMPT "Press [Enter]-key to regist table ... "
var tbl_id number;
begin
  select case when max(table_id) is null then 1 else max(table_id)+1 end into :tbl_id from sync_table;
end;
/
insert into VERTSNAP.SYNC_TABLE
     (SOURCE_SCHEMA, TARGET_SCHEMA, SOURCE_TABLE, TARGET_TABLE, CURR_STATE, TABLE_WEIGHT, TABLE_ID, 
      SOURCE_TRIGGER_NAME, T_ORDER, ORA_DELIMITER, EXP_TYPE, EXP_TIMEOUT, VERT_DELIMITER, PARTITIONED, 
      SOURCE_LOG_TABLE, TARGET_PK, ERR_CNT, ERR_LIM, source_db_id, target_db_id, pool_id, refresh_type)
values ( '&tblOwner', '&tgtOwner', '&tblName', '&tblName', 0, 1, (select :tbl_id from dual), 
          '&tblOwner..&tblTr', '', '|', 1, 500, '|', 'N', 
          '&tblTX', 'gu_rowid', 0, 5 , &srcDBid, &tgtDBid, &poolID, &refType)
;
commit;

-- populate table fiedls to Rep Meta.
-- first generate DDL by connecting to the source DB
-- then connect to RepMeta to run the generated DDL.
connect &srcDBuser/&srcDBpwd@&srcDBtns;
Accept foo PROMPT "Press [Enter]-key to generate DDL for fields registration ... "

set heading off;
set feedback off;
set echo off;
set verify off;
set lines 300;
set pages 80
spool wrkFieldDDL.sql
select 'insert into SYNC_TABLE_FIELD (FIELD_ID, TABLE_ID, SOURCE_FIELD, TARGET_FIELD, XFORM_FCTN, XFORM_TYPE ) values ( ' 
   || column_id  || ', '  || (select :tbl_id from dual) || ', '  
   || '''' || column_name  || '''' || ', '  
   || '''' || column_name  || '''' || ', '  
   || decode(substr(data_type,1,3),
            'NUM',     '''nvl(to_char(' || column_name || '), ''''NULL'''' )''' ,
            'VAR',     '''nvl('         || column_name || ' , ''''NULL'''' )''' ,
            'CLO',     '''nvl('         || column_name || ' , ''''NULL'''' )''' ,
            'CHA',     '''nvl('         || column_name || ' , ''''NULL'''' )''' ,
            'DAT',     '''nvl(to_char(' || column_name || ' , ''''dd-mon-yyyy hh24:mi:ss''''),''''NULL'''' )''' ,
            'TIM',     '''nvl(to_char(' || column_name || ' , ''''dd-mon-yyyy hh24:mi:ss''''),''''NULL'''' )'''
       ) || ', '   
   || decode(substr(data_type,1,3),
            'NUM',  decode(data_scale,
            		null, 1,
                    0, 2,
                       5
                  ) ,
            'VAR',     1 ,
            'CLO',     1 ,
            'CHA',     1 ,
            'DAT',     6 ,
            'TIM',     6
       )  || '  ) ;'
from   dba_tab_columns    
where  table_name = upper('&tblName')
and    owner      = upper('&tblOwner')
-- order by a.column_id
union
select 'insert into SYNC_TABLE_FIELD (FIELD_ID, TABLE_ID, SOURCE_FIELD, TARGET_FIELD, XFORM_FCTN, XFORM_TYPE ) values ( ' 
    || (select max(column_id) +1 from dba_tab_columns where table_name = upper('&tblName') and owner = upper('&tblOwner')) || ', '  ||  (select :tbl_id from dual)  || ', '  
    || ''''  || 'rowid' || '''' || ', ' 
    || ''''  || 'gu_rowid' || '''' || ', ' 
    || '''nvl(ROWID, ''''NULL'''' )''' || ', 1  ) ;'
from   dual
;
spool off

connect &repDBuser/&repDBpwd@&repDBtns;
Accept foo PROMPT "view wrkFieldDDL.sql, then press [Enter]-key to regist table fields ... "
@wrkFieldDDL.sql 
commit;


Accept foo PROMPT "Press [Enter]-key to generate DDL for Vertica ... "
connect &srcDBuser/&srcDBpwd@&srcDBtns;
set lines 300
set feedback off
spool wrkVertica.sql
select txt from (
select 0 as cid, 'CREATE TABLE ' ||'&tgtOwner'|| '.' || '&tblName' || ' ('  txt from dual 
union
select column_id cid, column_name || ' ' ||        
       decode(data_type,
          'NUMBER', decode(data_scale,
                  null, 'number(22,10)',
                  0, 'NUMBER(' || decode(data_precision,38,22,data_precision) ||')',
                     'NUMBER(' || decode(data_precision,38,22,data_precision) ||','|| data_scale ||')'
                  ),
           'VARCHAR2', 'VARCHAR2'||'('||data_length||')'       ,
           'CHAR',     'CHAR'    ||'('||data_length||')'       ,
           'CLOB',     'LONG VARCHAR'                          ,
           'DATE',     'TIMESTAMP'                             ,
            data_type
       )    || ', '  txt
from  dba_tab_columns
where table_name  = upper('&tblName')
and   owner       = upper('&tblOwner')
union
select 999 as cid, 'gu_rowid VARCHAR2 (255), primary key(gu_rowid) ' txt from dual 
union
select 1000 as cid, ');' txt from dual
) order by cid asc;
spool off


Accept foo PROMPT "Press [Enter]-key to grant ... "
-- Grants
grant select, insert, update, delete on &tblOwner..&tblName to vertsnap ;
grant all on &tblOwner..&tblTX to vertsnap ;

select 'Please note down the table_id:' from dual;
print   :tbl_id
Accept foo PROMPT "Done with Oracle. Remenber to run wrkVertica.sql against Vertica. Press [Enter]-key to grant ... "

exit

