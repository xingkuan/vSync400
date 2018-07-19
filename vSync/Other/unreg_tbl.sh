#
# This is the main script for registering table for replication.
# example usage: stpTbl.sh CRMDEV CST_EMAIL
#

SRCDBURL=$1
SRCDBID=$2
SRCSCH=$3
TGTSCH=$4
TBL=$5

echo unregister  $SRCSCH $TBL
read -p "pre check. Press enter to continue ..."

sqlplus /nolog @unreg_tbl.sql $SRCDBURL $SRCDBID $SRCSCH $TGTSCH $TBL

VHOST=vertx1
VUSER=dbadmin
VPASS="Bre@ker321"
vsql -h$VHOST -U$VUSER -w -U dbadmin -w$VPASS -c "drop table ${TGTSCH}.${TBL}"