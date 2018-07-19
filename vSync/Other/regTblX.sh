
#
# This is the main script for registering table for replication.
#

JPOOL=$5
RTYPE=1
TBL=$6
TBLSHORT=$7
SRCSCH=$3
TGTSCH=$4
SRCURL=$2
SRCDBID=$1
TGTDBID=4  #we have only one, that is VERTX below, which is 4
VHOST=vertx1
VUSER=dbadmin
VPASS="Bre@ker321"

echo $TBL
read -p "pre check. Press enter to continue ..."
sqlplus /nolog @tbl00_preCheck $SRCSCH $TBL $TBLSHORT $TGTSCH $SRCURL

read -p "regsiter. Press enter to continue ..."
sqlplus /nolog @tbl01_setup $SRCSCH $TBL $TBLSHORT $TGTSCH $JPOOL $RTYPE $SRCURL $SRCDBID $TGTDBID

read -p "create tgt tbl in vertica. Press enter to continue ..."
#fixing SQL errors
#g/NUMBER\(,\)/s/NUMBER/
ed wrkVertica.sql  <<STMT
g/(,)/s//
g/   *$/s//
w
STMT

vsql -h$VHOST -U$VUSER -w -U dbadmin -w$VPASS -f wrkVertica.sql
