#
# This is the main script for registering table for replication.
# example usage: stpTbl.sh CRMDEV CST_EMAIL
#

TBL=$2
SRCSCH=$1

echo stop replicating $SRCSCH $TBL
read -p "pre check. Press enter to continue ..."

sqlplus /nolog @stp_tbl.sql $SRCSCH $TBL
