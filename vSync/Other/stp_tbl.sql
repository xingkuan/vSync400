define tblOwner = &1
define tblName = &2

-- Important Note:
--     Please provide the DB tns name/user/passwd for both the source and meta repository
-- | srcDBtns = 'JOTPP';    -- JOTPP prod
-- |            system/cal618
-- | srcDBtns = 'CRMP65'   -- the clone db for test
-- | srcDBtns = 'CRMP64'
-- |            system/lanchong
define srcDBtns = 'CRMP64'
define srcDBuser = 'system';
define srcDBpwd = 'lanchong';
define repDBtns = 'RMAN01'
define repDBuser = 'vertsnap'
define repDBpwd = 'BAtm0B1L#'


-- Important Note:
--     Please find the SOURCE_DB_ID and TARGET_DB_ID from sync_db
define srcDBid = 3
define tgtDBid = 6
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
var tbl_id number;
var src_trg varchar2(50);
begin
  select table_id, source_trigger_name into :tbl_id,:src_trg from sync_table
  where source_schema='&tblOwner' and source_table='&tblName';
end;
/

print :tbl_id
print :src_trg
Accept foo PROMPT "Are you sure want to stop: &tblOwner &tblName? Press [Enter]-key to grant ... "
select :tbl_id from dual;
update VERTSNAP.SYNC_TABLE
     set CURR_STATE=0
     where table_id=:tbl_id;
commit;

connect &srcDBuser/&srcDBpwd@&srcDBtns;
spool /tmp/tempVSYNC.sql
select 'alter trigger '||:src_trg||' disable;' from dual;
spool off
--alter TRIGGER :src_trg disable;
@/tmp/tempVSYNC.sql
exit

