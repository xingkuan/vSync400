package com.guess.vsync400;

import java.io.*;
import java.util.*;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.text.*;
import java.sql.*;
import oracle.jdbc.*;
import oracle.jdbc.pool.OracleDataSource;

/*
 class OVSmeta
 
 OVSmeta instantiates the source and target table classes, and handles the repository meta data
   - instantiates and initializes OVSsrc and OVStgt
   - creates the sql strings for the OVSsrc and OVStgt instances
   - handles all updates to the repository record for the current table
   - handles retrieval of requests
   - 
*/

class OVSmeta {
   private  OVScred repCred;
   private  Connection repConn;
   private  Statement repStmt;
   private static String OVSTable = new String("vertsnap.sync_table");
   private static String OVSTableField = new String("vertsnap.sync_table_field");
   //private static String OVSRequest = new String("vertsnap.cmd_queue");
   private  String OVSdb = new String("vertsnap.sync_db");
   private  ResultSet rRset;
   private int tableID;
   private int currState=0;
   private int fldCnt;
   private int refreshCnt;
   private int[] xformType;
   private String[] fldName;
   private String sqlSelectSource;
   private String sqlInsertTarget;
   private String sqlInsertTargetAlt;
   private String sqlCopyTarget;
   private String sqlCopySource;
   private String srcSchema;
   private String srcTable;
   private String tgtSchema;
   private String tgtTable;
   private String tgtTableAlt;
   private String srcTrigger;
   private String srcLogTable;
   private String tgtPK;
   private String oraDelimiter;
   private String vrtDelimiter;
   private int defInitType;
   private int refreshType;
   private int recordCountThreshold;  
   private int minPollInterval; 
   private Timestamp tsLastAudit;
   private int auditExp;
   private Timestamp tsLastRefresh;   
   private long seqLastRef;
   private int poolID;   
   private int prcTimeout;
   private long startMS;
   private long endMS;
//.   private static String logDir="/u01/sync/logs";
   private int srcDBid;
   private int tgtDBid;
   private OVSrepo dbMeta;
   private OVScred srcCred;
   private OVScred tgtCred;
   private boolean tgtUseAlt;
   private String label;
   private String srcTblAb7;
   
   private String journalLib, journalName;

   private Timestamp tsThisRefresh;   
   private long seqThisRef;

   private static final Logger ovLogger = LogManager.getLogger();

   private static final OVSmetrix metrix = OVSmetrix.getInstance();
   
   public boolean init() {
      label=">";
      return linit();
   }

   public boolean init(String lbl) {
      label=lbl;
      return linit();
   }
   private boolean linit() {
      // initialize variables
      currState=0;
	  boolean rtv = true;
      java.sql.Timestamp ts = new java.sql.Timestamp(System.currentTimeMillis()); 
      java.sql.Timestamp ats;
      long ets;
      
      tgtUseAlt=false;
      // test for Oracle repository type, then load oracle driver
      repCred = dbMeta.getCred(0);   // 0 is the default repcred db id

      if (repCred.getType() == 1) {
         try {
            Class.forName("oracle.jdbc.OracleDriver"); 
         } catch(ClassNotFoundException e){
            ovLogger.error("Driver error has occured when loading Oracle JDBC.");
            e.printStackTrace();
		    rtv=false;
		    
            return rtv;
         }
      } else {
            ovLogger.info("repository db type not supported");
            rtv=false;
            return rtv;
      }

      try {
         //establish Oracle connection
         repConn = DriverManager.getConnection(repCred.getURL(), repCred.getUser(), repCred.getPWD());
         repConn.setAutoCommit(false);
         repStmt = repConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
         rRset = repStmt.executeQuery("select SOURCE_SCHEMA, TARGET_SCHEMA, SOURCE_TABLE, TARGET_TABLE, TS_LAST_REF400, SEQ_LAST_REF, "
         		+ "	TS_LAST_AUDIT, AUDIT_EXP, POOL_ID, REFRESH_TYPE, MIN_POLL_INTERVAL, REC_CNT_THRESHOLD, TARGET_TABLE_ALT, "
        	    + "	TS_LAST_INIT,  CURR_STATE, TABLE_WEIGHT, LAST_REFRESH_DURATION, LAST_INIT_DURATION, REFRESH_CNT,  "
         		+ "	AUD_SOURCE_RECORD_CNT, AUD_TARGET_RECORD_CNT, TABLE_ID, SOURCE_TRIGGER_NAME, T_ORDER, ORA_DELIMITER, "
         		+ "	EXP_TYPE, LINESIZE, EXP_TIMEOUT, VERT_DELIMITER, PARTITIONED, PART_BY_CLAUSE, SOURCE_LOG_TABLE, TARGET_PK, "
         		+ "	HEARTBEAT, LAST_INIT_TYPE, DEFAULT_INIT_TYPE, SOURCE_DB_ID, TARGET_DB_ID "
         		+ " from " + OVSTable 
         		+ " where table_id=" + tableID);   
         if (rRset.next()) {
            srcSchema = rRset.getString("source_schema");
            srcTable = rRset.getString("source_table");
            srcTrigger = rRset.getString("source_trigger_name");
            srcLogTable = rRset.getString("source_log_table");
            tgtSchema = rRset.getString("target_schema");
            tgtTable = rRset.getString("target_table");
            tgtTableAlt = rRset.getString("target_table_alt");
            tgtPK = rRset.getString("target_pk");
            prcTimeout = rRset.getInt("exp_timeout");
            defInitType=rRset.getInt("default_init_type");
            currState=rRset.getInt("curr_state");
            oraDelimiter = rRset.getString("VERT_DELIMITER");
            vrtDelimiter = rRset.getString("ORA_DELIMITER");
            srcDBid=rRset.getInt("SOURCE_DB_ID");
            tgtDBid=rRset.getInt("TARGET_DB_ID");
            refreshType=rRset.getInt("REFRESH_TYPE");
            recordCountThreshold=rRset.getInt("REC_CNT_THRESHOLD");
            minPollInterval=rRset.getInt("MIN_POLL_INTERVAL"); 
            poolID=rRset.getInt("POOL_ID");
            tsLastAudit=rRset.getTimestamp("TS_LAST_AUDIT");
            //2020.02.21 quick&dirty solution: TS_LAST_REFRESH (a DATE) is info only anymore! we need TS to track the processed TS in DB2.
            tsLastRefresh=rRset.getTimestamp("TS_LAST_REF400");
            //2020.02.24: the above change is not safe enough. Switch to using DB2's SEQUENCE_NUMBER, which is kept in SEQ_LAST_REF:
            seqLastRef=rRset.getLong("SEQ_LAST_REF");
            
            auditExp=rRset.getInt("AUDIT_EXP");
            
            srcTblAb7 = srcTable.substring(0, Math.min(7, srcTable.length()));
            
           // ovLogger.log(label + " Table " + tableID + " checking for audit");
            if (currState==5 && auditExp!=0){  //. "refreshed" and ?
               //ovLogger.log(label + " Table " + tableID + " checking for audit refresh type");
               if (refreshType==1  || refreshType==2 || refreshType==3){   //.trickle down ?
                  // if audit has expired, push an audit command
                  ets=tsLastAudit.getTime() + ((long) auditExp * 3600000);
                  ats=new java.sql.Timestamp(ets); 
                  //ovLogger.log(label + " Table " + tableID + " @@@la " + tsLastAudit + "   exp " +  auditExp + "   exptime " + ets + "   ats " + ats + "   ts" + ts.getTime() );
                  if (ts.getTime() > ets) {
                     ovLogger.info(label + " Table " + tableID + " @@@setting audit");
//.                     putAuditRequest();
                  }
               }
            }
            getFieldMetaData();
            initDBcreds();
            //System.out.println("---META: " + tgtSchema + "." + tgtTable);
		   } else {
		      ovLogger.error(label + " Table " + tableID + " does not appear to exist");
		      rtv=false;
         }
         //rRset.close();
      } catch(SQLException e) {
         ovLogger.error(e.getMessage());
         rtv=false;
      }

      return rtv;
   }
//for injecting data into Kafka (for DB2/AS400), instead of table level; read all entries of a Journal (which could be for many tables 
   public boolean initForKafka(int dbID, String jLib, String jName) {
	   srcDBid = dbID;   
	   label="Inject Kafka DBid: " + dbID;
	   
	   journalLib = jLib;
	   journalName = jName;
       srcCred=dbMeta.getCred(srcDBid);

	   return true;
   }

   
   private void initDBcreds() {
      // sets the db credentials for source and target classes
      srcCred=dbMeta.getCred(srcDBid);
      tgtCred=dbMeta.getCred(tgtDBid);
   }

   private void getFieldMetaData() throws SQLException {
      // creates select and insert strings 
      Connection lrepConn;
      Statement lrepStmt;
      ResultSet lrRset;
      int i;

      lrepConn = DriverManager.getConnection(repCred.getURL(), repCred.getUser(), repCred.getPWD());
      lrepStmt = lrepConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

      // get field count
      lrRset = lrepStmt.executeQuery("select count(*) from " + OVSTableField + " where table_id=" + tableID);
      //System.out.println("select count(*) from " + OVSTableField + " where table_id=" + tableID);
      if (lrRset.next()) {
         fldCnt=lrRset.getInt(1);
      }
      lrRset.close();

      // get field info
      xformType = new int[fldCnt];
      fldName = new String[fldCnt];
      //System.out.println("field cnt: " + fldCnt);
      i=0;
      lrRset = lrepStmt.executeQuery("select xform_fctn, target_field, source_field, xform_type  from " + OVSTableField 
    		  + " where table_id=" + tableID 
    		  + " order by field_id");
      if (lrRset.next()) {                                                                  
         sqlSelectSource = "select \n"    + lrRset.getString("source_field");      
         sqlInsertTarget = "insert into " + tgtSchema + "." + tgtTable ;
         sqlInsertTargetAlt = "insert into " + tgtSchema + "." + tgtTableAlt ;
         sqlInsertTarget +=            "\n( "          + "\"" + lrRset.getString("target_field") + "\"";
         sqlInsertTargetAlt +=            "\n( "       + "\"" + lrRset.getString("target_field") + "\"";
         xformType[i] = lrRset.getInt("xform_type");
         fldName[i] = lrRset.getString("target_field");
         sqlCopySource = "select \n" + lrRset.getString("xform_fctn");
         sqlCopyTarget = "copy " + tgtSchema + "." + tgtTable + " (" +  "\"" + lrRset.getString("target_field") + "\"";
      }
      while (lrRset.next()) {
         sqlSelectSource += " \n , " + lrRset.getString("source_field") ;    
         sqlInsertTarget += " \n , " + "\"" + lrRset.getString("target_field") + "\"";   
         sqlInsertTargetAlt += " \n , " + "\"" + lrRset.getString("target_field") + "\""; 
         sqlCopySource += " || " + oraDelimiter + " || \n" + lrRset.getString("xform_fctn");
         sqlCopyTarget += ", " + "\"" + lrRset.getString("target_field") + "\"";
         i++;
         //System.out.println(i);
         xformType[i] = lrRset.getInt("xform_type");
         fldName[i] = lrRset.getString("target_field");
      }
      lrRset.close();
      sqlCopyTarget += ") FROM LOCAL STDIN DELIMITER " + vrtDelimiter + "DIRECT ENFORCELENGTH";
      sqlCopySource += "\nfrom " + srcSchema + "." + srcTable + " a";
      sqlSelectSource += " \n from " + srcSchema + "." + srcTable + " a";
      sqlInsertTarget += ") \n    Values ( " ;
      sqlInsertTargetAlt += ") \n    Values ( " ;
      for ( i=1; i<=fldCnt; i++ ) {
         if (i==1) {
            sqlInsertTarget += "?";
            sqlInsertTargetAlt += "?";
         } else {
            sqlInsertTarget += ",?";
            sqlInsertTargetAlt += ",?";
         }
      }
      sqlInsertTarget += ") ";
      sqlInsertTargetAlt += ") ";
      lrepConn.close();
   }
   public void markStartTime() {
      Calendar cal = Calendar.getInstance();
      startMS = cal.getTimeInMillis();
      
   }
   public void markEndTime() {
	  Calendar cal = Calendar.getInstance();
      endMS = cal.getTimeInMillis();
   }
   
   //public void saveInitStats(String jobID, java.sql.Timestamp hostTS) {
   public void saveInitStats(String jobID) {
	  int duration =  (int) (endMS - startMS)/1000;
      ovLogger.info(label + " duration: " + duration + " seconds");
      
      //Save to InfluxDB:
      metrix.sendMX("initDuration,jobId="+jobID+",tblID="+srcTblAb7+"~"+tableID+" value="+duration+"\n");
      metrix.sendMX("initRows,jobId="+jobID+",tblID="+srcTblAb7+"~"+tableID+" value="+refreshCnt+"\n");
      metrix.sendMX("JurnalSeq,jobId="+jobID+",tblID="+srcTblAb7+"~"+tableID+" value="+seqThisRef+"\n");

      //Save to MetaRep:
      java.sql.Timestamp ts = new java.sql.Timestamp(System.currentTimeMillis()); 
      try {                     
         rRset.updateInt("LAST_INIT_DURATION", (int)((endMS - startMS)/1000));
         rRset.updateTimestamp("TS_LAST_INIT",tsThisRefresh);
         rRset.updateTimestamp("TS_LAST_REF400",tsThisRefresh);
         rRset.updateLong("SEQ_LAST_REF",seqThisRef);
         rRset.updateTimestamp("TS_LAST_AUDIT",ts);
         rRset.updateInt("AUD_SOURCE_RECORD_CNT",refreshCnt);     
         rRset.updateInt("AUD_TARGET_RECORD_CNT",refreshCnt);

         rRset.updateRow();
         repConn.commit();   
      } catch (SQLException e) {
         ovLogger.error(label + e.getMessage());
      }
   }
   
   public void saveRefreshStats(String jobID) {
	   int duration = (int)(int)((endMS - startMS)/1000);

	   metrix.sendMX("syncDuration,jobId="+jobID+",tblID="+srcTblAb7+"~"+tableID+" value="+duration+"\n");
	   metrix.sendMX("syncCount,jobId="+jobID+",tblID="+srcTblAb7+"~"+tableID+" value="+refreshCnt+"\n");
	   metrix.sendMX("JurnalSeq,jobId="+jobID+",tblID="+srcTblAb7+"~"+tableID+" value="+seqThisRef+"\n");

      try {                     
         rRset.updateInt("LAST_REFRESH_DURATION", duration);
         //2020.02.21
         //rRset.updateTimestamp("TS_LAST_REFRESH",hostTS);
         rRset.updateTimestamp("TS_LAST_REF400",tsThisRefresh);
         rRset.updateLong("SEQ_LAST_REF",seqThisRef);
         rRset.updateInt("REFRESH_CNT",refreshCnt);
        
         rRset.updateRow();
         repConn.commit();   
      } catch (SQLException e) {
         System.out.println(e.getMessage());
      }
   }

   public void saveAudit(int srcRC, int tgtRC) {
      java.sql.Timestamp ts = new java.sql.Timestamp(System.currentTimeMillis()); 
      ovLogger.info(label + "source record count: " + srcRC + "     target record count: " + tgtRC );
//TODO: matrix
      try {                     
         rRset.updateTimestamp("TS_LAST_AUDIT",ts);
         rRset.updateInt("AUD_SOURCE_RECORD_CNT",srcRC);
         rRset.updateInt("AUD_TARGET_RECORD_CNT",tgtRC);
         rRset.updateRow();
         repConn.commit();   
         //System.out.println("audit info saved");
      } catch (SQLException e) {
         ovLogger.error(label + e.getMessage());
      }
   }
  
   
   public void setRefreshTS(Timestamp thisRefreshHostTS) {
		tsThisRefresh = thisRefreshHostTS;
	}
	public void setRefreshSeq(long thisRefreshSeq) {
		if(thisRefreshSeq>0) {
			seqThisRef=thisRefreshSeq;
		}else {
			seqThisRef=seqLastRef;
			ovLogger.info("...hmm, got a 0 for SEQ# for srcTbl " + srcTable + ". The last one: " + seqLastRef);
		}
	}

   
   public int getPrcTimeout() {
      return prcTimeout;
   }
   public int getRefreshType() {
      return refreshType;
   }
   public int getRecordCountThreshold() {
      return recordCountThreshold;
   }
   public int getMinPollInterval() {
      return minPollInterval;
   }
   public Timestamp getLastAudit() {
      return tsLastAudit;
   }
   public Timestamp getLastRefresh() {
      return tsLastRefresh; 
   }
   public long getSeqLastRefresh() {
	      return seqLastRef; 
	   }
   public int getPoolID() {
      return poolID;
   }
   public String getLogTable() {
      return srcLogTable;
   }
   public String getSrcTrigger() {
      return srcTrigger;
   }
   public String getSrcSchema() {
      return srcSchema;
   }
   public String getTgtSchema() {
      return tgtSchema;
   }
   public String getSrcTable() {
      return srcTable;
   }
   public String getTgtTable() {
      return tgtTable;
   }
   public String getTgtTableAlt() {
      return tgtTableAlt;
   }
   public String getSQLInsert() {
      return sqlInsertTarget;
   }
   public String getSQLInsertAlt() {
      return sqlInsertTargetAlt;
   }
   public void setTgtUseAlt() {
      tgtUseAlt=true;
   }
   public boolean getTgtUseAlt() {
      return tgtUseAlt;
   }
   public String getSQLSelect() {
      return sqlSelectSource;
   }
   public String getSQLCopySource() {
      return sqlCopySource;
   }
   public String getSQLCopyTarget() {
      return sqlCopyTarget;
   }
   public String getPK() {
      return tgtPK;
   }
   public String getSQLSelectXForm() {
      return sqlSelectSource;
   }
   public int getCurrState() {
      return currState;
   }
   public void setCurrentState(int cs) {
      try {
         currState=cs;
         rRset.updateInt("CURR_STATE",cs);
         rRset.updateRow();
         repConn.commit();   
      } catch (SQLException e) {
         ovLogger.error(label + e.getMessage());
      }
   }
   public int getCurrentState() {
      return currState;
   }
/*JLEE 7/14: to be removed
   public void setHeartBeat() {
      java.util.Date date = new java.util.Date();
      long t = date.getTime();
	  java.sql.Timestamp sqlTimestamp = new java.sql.Timestamp(t);
      try {
         rRset.updateTimestamp("HEARTBEAT",sqlTimestamp);
         rRset.updateRow();
         repConn.commit();   
      } catch (SQLException e) {
         ovLogger.log(label + e.getMessage());
      }
   }
*/   
   public void setCred(OVScred ovsc) {
      repCred=ovsc;
   }
   public void setDbMeta(OVSrepo ovsdb){
      dbMeta=ovsdb;
   }
   public void setTableID(int tid) {
      tableID=tid;
   }
   public int getTableID() {
	      return tableID;
   }
   public int getDefInitType() {
      return defInitType;
   }
//.   public void setLogDir(String s) {
//.      logDir=s;
//.   }
//.   public void setInitLogDir(String s) {
//.      initLogDir=s;
//.  }
//.   public String getLogDir() { 
//.     return logDir;
//.   }
   public int getFldCnt() {
      return fldCnt;
   }
   public int getFldType(int i) {
      return xformType[i];
   }  
   public String getFldName(int i) {
      return fldName[i];
   }  
//.   public String getInitLogDir() {
//.      return initLogDir;
//.   }
   public void setRefreshCnt(int i) {
      refreshCnt=i;
   }
   public OVScred getSrcCred() {
      return srcCred;
   }
   public OVScred getTgtCred() {
      return tgtCred;
   }
   public String getSrcDbDesc() {
      return srcCred.getDesc();
   }
   public int getSrcDBid() {
	   return srcDBid;
   }
   public void close() {
     try {                     
         rRset.close();
         repStmt.close();
         repConn.close();
      } catch (SQLException e) {
         ovLogger.error(label + e.getMessage());
      }

   }
   public String getLabel() {
	   return label;
   }
   public String getJournalLib() {
	   return journalLib;
   }
   public String getJournalName() {
	   return journalName;
   }

}    