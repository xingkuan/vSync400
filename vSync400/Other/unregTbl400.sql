define tblOwner = &3
define tblName = &5
--# the 4th parameter, TGTSCH, is not used here.
-- Important Note:
--     Please provide the DB tns name/user/passwd for both the source and meta repository
--     (for DB2/AS400, that is not needed. But we keep it here for now.)
-- | srcDBtns = 'JOTPP';    -- JOTPP prod
-- |            system/cal618
-- | srcDBtns = 'CRMP65'   -- the clone db for test
-- | srcDBtns = 'CRMP64'
-- |            system/lanchong
-- | DB2/AS400 = 'DB2T'  
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
-- DB2T      7

set heading off;
set feedback off;
set echo off;
set verify off;
set lines 300;
set pages 80

connect &repDBuser/&repDBpwd@&repDBtns;
select 'sdb_id: '||source_db_id||';;;; pool_id: '||pool_id||';;;; table_ID: '||table_id from sync_table where source_schema='&tblOwner' and source_table='&tblName';

var tbl_id number;
begin
  select table_id into :tbl_id from sync_table
  where source_schema='&tblOwner' and source_table='&tblName';
end;
/

print :tbl_id
Accept foo PROMPT "Sure want to drop vSync for: &tblOwner &tblName? Press [Enter]-key to continue ... "
connect &repDBuser/&repDBpwd@&repDBtns;
delete VERTSNAP.SYNC_TABLE_FIELD 
where table_id=:tbl_id
;
delete VERTSNAP.SYNC_TABLE
 where table_id=:tbl_id;
commit;

exit

