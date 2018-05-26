import subprocess
import requests
import time
import sys, getopt
from oraJobGlobalVars import dbs

def run_sqlplus(dbN, strSQL):
    dbN="DPRD"
    uName=dbs.get(dbN).get("user")
    pwd=dbs.get(dbN).get("pwd")
    url="//"+dbs.get(dbN).get("url")

    sqlScript = strSQL + "\n"  \
#                "select sysdate from dual;\n"  \
                "exit;\n"

#    p = subprocess.Popen(['sqlplus','/nolog'],stdin=subprocess.PIPE,
    p = subprocess.Popen(['sqlplus',uName+"/"+pwd+"@"+url],stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    (stdout,stderr) = p.communicate(sqlScript.encode('utf-8'))
    stdout_lines = stdout.decode('utf-8').split("\n")
    for line in stdout_lines:
        print(line)

    print(stderr)
    return stdout_lines

#sqlplus_script="""
#--'vertsnap', 'BAtm0B1L#', 'crmrman01:1521/RMAN01'
#--connect vertsnap/BAtm0B1L#@//crmrman01:1521/RMAN01
#--connect """+uName+"/"+pwd+"@"+url+"""
#select sysdate from dual;
#exec testp();
#exit
#"""

#curl -i -XPOST http://grafana01:8086/write?db=guess --data-binary "sinceLast,key=$1 value=$2"
def sendMatric(keyNam, db, cat, job, val):
    #keyNam='duration'
    #dbNam='TEST'
    #jobNam='Job1'
    #val=10
    url='http://grafana01:8086/write?db=guess'
    dataStr = keyNam+',DB='+db+',jobCat='+cat+',jobName='+job+' value='+str(val)
    r = requests.post(url, data=dataStr)
    return


def main():
    dbName =''
    jobName = ''
    jobCat = ''
    cmdLine = ''

    try:
        opts, args = getopt.getopt(sys.argv[1:],"hd:j:c:l:",["dbName","jobName","jobCat","cmdLine"])
    except getopt.GetoptError:
        print('essenJob.py -d <dbName> -j <jobName> -c <jobCat> -l <command>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('essenJob.py -d <dbName> -j <jobName> -c <jobCat> -l <command>')
            sys.exit()
        elif opt in ("-d", "--dbName"):
            dbName = arg
        elif opt in ("-j", "--jobName"):
            jobName = arg
        elif opt in ("-c", "--jobCat"):
            jobCat = arg
        elif opt in ("-l", "--cmdLine"):
            cmdLine = arg
    print('db: ', dbName)
    print('job: ', jobName)
    print('cat: ', jobCat)
    print('cmd: ', cmdLine)

    start = time.time()
    #runOracleJob('vertsnap', 'BAtm0B1L#', 'crmrman01:1521/RMAN01')
    #runOracleJob('SELECT count(1) FROM sync_table where curr_state!=5')
    run_sqlplus(dbName, cmdLine)

    end = time.time()
    duration = end - start
    print(duration)

    sendMatric("duration", dbName, jobCat, jobName, duration)

if __name__ == "__main__":
   main()