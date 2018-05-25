
# ... to be improved. For now, hardcoded one for different srouce DB:
# dbID 3: crm CRMDEV VCRM system/lanchong@CMP64
#      5: sewn SEWN VSEWN system/cal618@JOTPP
dbID=5
sschName=SEWN
tschName=VSEWN
srcDBUrl=system/cal618@JOTPP


fun_genRowIDs() {
  local sschName=$1
  local tschName=$2
  local tblName=$3

#  echo ${sschName}
#  echo ${tschName}
#  echo ${tblName}

  vsql -A -t -d vertx -h vertx1 -U dbadmin -w Bre@ker321 -o /tmp/XXX -c "select gu_rowid from ${tschName}.${tblName} tablesample(10) limit 100"

ed /tmp/XXX <<STMT
1,\$s/^/'
1,\$s/\$/',
\$
s/,//
w
q
STMT

#local rslt=`wc -l < /tmp/XXX`
#
#echo $rslt
}


fun_oraCnt() {
  local sschName=$1
  local tschName=$2
  local tblName=$3

  echo "select count(1) from ${sschName}.$tblName a where rowid in (" > /tmp/${tblName}O.sql
  cat /tmp/XXX >> /tmp/${tblName}O.sql
  echo ") ; " >> /tmp/${tblName}O.sql
    sqlplus -s $srcDBUrl <<STMT
set heading off
set feedback off
set echo off
set verify off
set pages 0
set lines 2000
set trimspool on
set trimout on
set termout off
set colsep   "|"
alter session set nls_date_format='yyyy-mm-dd hh24:mi:ss';
spool /tmp/${tblName}O.txt
@/tmp/${tblName}O.sql
exit
STMT

#local rslt=$(cat /tmp/${tblName}O.txt | sed 's/[^0-9]*//g')
#
#echo $rslt
}

fun_sendMatrix()
{
    curl -i -XPOST http://grafana01:8086/write?db=vsync --data-binary "vSyncMismatch,key=$1 value=$2"

    return $?
}


#### MAIN ####
##############
# generate a list of table to /tmp/tmpTbls.lst by calling mon1.sql
sqlplus -s /nolog <<STMT
define repDBtns = 'RMAN01'
define repDBuser = 'vertsnap'
define repDBpwd = 'BAtm0B1L#'

connect &repDBuser/&repDBpwd@&repDBtns;
set termout off
set heading off
set feedback off
set echo off
set verify off
set lines 300

spool /tmp/tmpTbls.lst
select source_table from sync_table where source_db_id=$dbID and rownum<3 ;
exit;
STMT

vi /tmp/tmpTbls.lst -c ':1,$s/  *$//g' -c ':1,$/^$/d'  -c':wq'
#vi /tmp/tmpTbls.lst -c ':1,$/^$/d' -c':wq'

grep -v ^$ /tmp/tmpTbls.lst | while read ln
do
    echo $ln
    # get samle rowid, and data from vertX
    #./mon1.sh $sschName $tschName $ln
    fun_genRowIDs $sschName $tschName $ln
    rowCnt=`wc -l < /tmp/XXX`
    echo row count $rowCnt

    if [[ $rowCnt == 0 ]] ; then
      echo skip!
    else
      echo compare $ln ...
      fun_oraCnt $sschName $tschName $ln
      mis=`cat /tmp/${ln}O.txt|grep ERROR|wc -l`
      if [[ $mis > 0 ]] ; then
        echo mismatch $ln
        fun_sendMatrix $ln 10   ## just hard code a number here!
      else
        srcCnt=$(cat /tmp/${ln}O.txt | sed 's/[^0-9]*//g')
        echo source count $srcCnt
        diff=$(( $rowCnt - $srcCnt ))
        echo difference $diff
        if [[ $diff == 0 ]] ; then
          echo $ln okay!
        else
          echo $ln $diff
          # echo and report to Grafana
          fun_sendMatrix $ln $diff
        fi
      fi
    fi

done
