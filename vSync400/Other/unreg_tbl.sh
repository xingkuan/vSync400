#
# This is the main script for registering table for replication.
# example usage: stpTbl.sh CRMDEV CST_EMAIL
#

SRCDB=$1    #SRCDBURL=$1
SRCDBID=$2
SRCSCH=$3
TGTSCH=$4
TBL=$5

fun_getDBConn()
{
    if [ "$1" == "CRM" ]
    then
      echo "system/lanchong@CRMP64"
    elif [ "$1" == "JOTPP" ]
    then
      echo "system/cal618@JOTPP"
    else
      echo not correct DB!
      exit 1
    fi
}

set -e
SRCDBURL=$(fun_getDBConn $SRCDB)

echo unregister  $SRCSCH $TBL
read -p "pre check. Press enter to continue ..."

sqlplus /nolog @unreg_tbl.sql $SRCDBURL $SRCDBID $SRCSCH $TGTSCH $TBL

VHOST=vertx1
VUSER=dbadmin
VPASS="Bre@ker321"
vsql -h$VHOST -U$VUSER -w -U dbadmin -w$VPASS -c "drop table ${TGTSCH}.${TBL}"