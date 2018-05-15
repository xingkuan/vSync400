#
# This is the main script for registering table for replication.
# example usage: stpTbl.sh CRMDEV CST_EMAIL
#

TBL=$2
SRCSCH=$1

echo unregister  $SRCSCH $TBL
read -p "pre check. Press enter to continue ..."

sqlplus /nolog @unreg_tbl.sql $SRCSCH $TBL
