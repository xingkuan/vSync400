export ORACLE_HOME=/opt/oracle/instantclient_11_2
export TNS_ADMIN=$ORACLE_HOME
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$ORACLE_HOME
export PATH=$PATH:$ORACLE_HOME:/opt/vertica/bin/
export SQLPATH=$ORACLE_HOME

#
# This is the main script for stopping a replication of a table
# example usage: stpTbl.sh system/lanchong@CRMP64 CRMDEV CST_EMAIL
#

echo stop replicating $2 $3
#read -p "pre check. Press enter to continue ..."

sqlplus -s /nolog <<SQLSTMT

define srcDB = $1
define tblOwner = $2
define tblName = $3

define repDBtns = 'RMAN01'
define repDBuser = 'vertsnap'
define repDBpwd = 'BAtm0B1L#'


set heading off;
set feedback off;
set echo off;
set verify off;
set lines 300;
set pages 80


connect &repDBuser/&repDBpwd@&repDBtns;
select '&srcDB' from dual;
select '&tblOwner' from dual;
select '&tblName' from dual;

var tbl_id number;
var src_trg varchar2(50);
begin
  select table_id, source_trigger_name into :tbl_id,:src_trg from sync_table
  where source_schema='&tblOwner' and source_table='&tblName';
end;
/

print :tbl_id
print :src_trg
--Accept foo PROMPT "Are you sure want to stop: &tblOwner &tblName? Press [Enter]-key to grant ... "
select :tbl_id from dual;
update VERTSNAP.SYNC_TABLE
     set CURR_STATE=0
     where table_id=:tbl_id;
commit;

connect &srcDB;
spool /tmp/tempVSYNC.sql
select 'alter trigger '||:src_trg||' disable;' from dual;
spool off
@/tmp/tempVSYNC.sql

set heading off;
set feedback off;
spool /tmp/VSYNC/TBLID.tobeResynced
select :tbl_id from dual;
spool off

exit

SQLSTMT
