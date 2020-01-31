
repURL='vertsnap/BAtm0B1L#@RMAN01'

get_detailOfTblID () {
    sqlplus -s $repURL <<!
    set heading off
    set feedback off
    set pages 0
    select d.DB_DESC, t.SOURCE_SCHEMA||'.'||source_table, t.TARGET_SCHEMA||'.'||t.TARGET_TABLE
from sync_table t, sync_db d
where d.DB_ID = t.SOURCE_DB_ID
and t.table_id=$1 ;
!
}

dtls=$(get_detailOfTblID $1)
echo $dtls
rslt=($dtls)
SRCDB=${rslt[0]}
SRCTBL=${rslt[1]}
TGTTBL=${rslt[2]}
echo Src DB : $SRCDB
echo Src Tbl: $SRCTBL
echo Tgt Tbl: $TGTTBL

# delete from Repository:
sqlplus -s $repURL <<!
set pages 0
delete VERTSNAP.SYNC_TABLE_FIELD
where table_id=$1 ;
delete VERTSNAP.SYNC_TABLE
where table_id=$1 ;
commit;
!


#
# drop target table in VertX
#
VHOST=vertx1
VUSER=dbadmin
VPASS="Bre@ker321"

#read -p "create tgt tbl in vertica. Press enter to continue ..."
#echo vsql -h$VHOST -U$VUSER -w -U dbadmin -w$VPASS $TGTTBL
vsql -h$VHOST -U$VUSER -w -U dbadmin -w$VPASS -c "drop table $TGTTBL"
