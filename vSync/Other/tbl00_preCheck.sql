--set heading off
--set feedback off
--set echo off
--set verify off
--set lines 250

-- Usage:
--     sqlplus /nolog
--     @tbl01_preCheck <srcSch> <srcTbl> <shortName> <tgtName>
-- 
define tblOwner = upper('&1')
define tblName = upper('&2')
define tblShort = upper('&3')
-- define tblTX = upper('&3')||'_TX_LOG2';  This is the old
define tblTX = upper('&3')||'_TX_LOG3'  -- Use a different one so we can havae parallel... 
define tgtOwner = upper('&4')

-- Important Note:
--     Please provide the correct source DB connection
-- | srcDBtns = 'JOTPP';    -- JOTPP prod
-- |            system/cal618
-- | srcDBtns = 'CRMP65'   -- the clone db for test
-- | srcDBtns = 'CRMP64'
-- |            system/lanchong
define srcDBtns = 'CRMP64';
define srcDBuser = 'system';
define srcDBpwd = 'lanchong';
define repDBtns = 'RMAN01';
define repDBuser = 'vertsnap';
define repDBpwd = 'BAtm0B1L#';

connect &srcDBuser/&srcDBpwd@&srcDBtns;
select 'Fix the following issues before proceeding : ' from dual;
-- 
-- Table name can't be longger than 21 chars
--
select '!!! name too long',
       length(&tblName)   len
from   dual
where  length(&tblName)   > 21
and    length(&tblShort)  > 21
;

--
-- Check the source
-- 
Accept foo PROMPT "Press [Enter]-key to check source and log table exist ... "
select '!!! table not in : ',
        owner ||'.'||table_name
from (
select &tblOwner as owner, &tblName as table_name from dual
minus
select  owner, table_name
from   dba_tables 
where  owner = &tblOwner
  and  table_name = &tblName ) a
;
select '!!! '|| &tblTX ||' exists already: ' table_name 
from   dba_tables 
where  owner = &tblOwner
  and  table_name = &tblTX
;
  
--
-- Check it is not already registered
-- 
Accept foo PROMPT "Press [Enter]-key to check if table registered in MetaRep ... "
connect &repDBuser/&repDBpwd@&repDBtns;
select '!!! table already in sync_table: ',
        source_schema ||'.'||source_table
from sync_table
where  source_schema = &tblOwner
  and  source_table = &tblName
;

exit

