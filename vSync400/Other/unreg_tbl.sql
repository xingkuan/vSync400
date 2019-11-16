define tblOwner = &3
define tblName = &5
--# the 4th parameter, TGTSCH, is not used here.
-- Important Note:
--     Please provide the DB tns name/user/passwd for both the source and meta repository
-- | srcDBtns = 'JOTPP';    -- JOTPP prod
-- |            system/cal618
-- | srcDBtns = 'CRMP65'   -- the clone db for test
-- | srcDBtns = 'CRMP64'
-- |            system/lanchong
--# define srcDBtns = 'CRMP64'
--# define srcDBuser = 'system';
--# define srcDBpwd = 'lanchong';
--# take srcDBurl from commandline
define srcDBurl = &1
define repDBtns = 'RMAN01'
define repDBuser = 'vertsnap'
define repDBpwd = 'BAtm0B1L#'

define srcDBid = &2   --not used
-- has only one tgtDB, vertX, tgtDBid=4
define tgtDBid = 4     -- not used
-- VERTU     1
-- CRMCLON2  2
-- CRM       3
-- VertX     4
-- JOTPP     5
-- VERTR     6

set heading off;
set feedback off;
set echo off;
set verify off;
set lines 300;
set pages 80

connect &repDBuser/&repDBpwd@&repDBtns;
select 'sdb_id: '||source_db_id||';;;; pool_id: '||pool_id||';;;; table_ID: '||table_id from sync_table where source_schema='&tblOwner' and source_table='&tblName';

var tbl_id number;
var src_trg varchar2(50);
var src_log varchar2(50);
begin
  select table_id, source_trigger_name, source_log_table into :tbl_id,:src_trg,:src_log from sync_table
  where source_schema='&tblOwner' and source_table='&tblName';
end;
/

print :tbl_id
print :src_trg
print :src_log
Accept foo PROMPT "Sure want to drop vsync for: &tblOwner &tblName? Press [Enter]-key to grant ... "
connect &srcDBurl;
select :tbl_id from dual;
spool /tmp/tempVSYNC.sql
select 'drop trigger '||:src_trg||';' from dual;
select 'drop table &tblOwner'||'.'||:src_log||';' from dual;
spool off
--alter TRIGGER :src_trg disable;
@/tmp/tempVSYNC.sql

connect &repDBuser/&repDBpwd@&repDBtns;
delete VERTSNAP.SYNC_TABLE_FIELD 
where table_id=:tbl_id
;
delete VERTSNAP.SYNC_TABLE
 where table_id=:tbl_id;
commit;

exit

