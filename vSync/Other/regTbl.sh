
#
# This is the main script for registering table for replication.
#

JPOOL=4
RTYPE=1
TBL=$1
SRCSCH=CRMDEV
TGTSCH=VCRM
VHOST=vertu01
VUSER=dbadmin
VPASS="3Astere**s"

echo $TBL
read -p "pre check. Press enter to continue ..."
sqlplus /nolog @tbl00_preCheck $SRCSCH $TBL $TBL $TGTSCH

read -p "regsiter. Press enter to continue ..."
sqlplus /nolog @tbl01_setup $SRCSCH $TBL $TBL $TGTSCH $JPOOL $RTYPE

read -p "create tgt tbl in vertica. Press enter to continue ..."
# fixing SQL errors
ed wrkVertica.sql <<STMT
,s/NUMBER(,)/NUMBER/g
wq
STMT
vsql -h$VHOST -U$VUSER -w -U dbadmin -w$VPASS -f wrkVertica.sql
