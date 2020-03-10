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

class OVSmetaJournal400 {
   private  OVScred repCred;
   private  Connection repConn;
   private  Statement repStmt;
   private static String OVSTable = new String("vertsnap.sync_journal400");
   private  ResultSet rRset;
   private String sqlSelectSource;
   private String srcLogTable;
   private long seqLastRef;
   private int prcTimeout;
   private long startMS;
   private long endMS;
   private int srcDBid;
   private OVSrepo dbMeta;
   private OVScred srcCred;
   private String label;
     
   private String journalLib, journalName;
   private Timestamp tsLastRef;
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
	  boolean rtv = true;
      
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
         rRset = repStmt.executeQuery("select SOURCE_DB_ID, SOURCE_LOG_TABLE, SEQ_LAST_REF, TS_LAST_REF400 "
         		+ " from " + OVSTable 
         		+ " where SOURCE_DB_ID=" + srcDBid + " and SOURCE_LOG_TABLE='" + srcLogTable +"'");   
         if (rRset.next()) {
            srcLogTable = rRset.getString("source_log_table");
            srcDBid=rRset.getInt("SOURCE_DB_ID");
            tsLastRef=rRset.getTimestamp("TS_LAST_REF400");
            seqLastRef=rRset.getLong("SEQ_LAST_REF");
            
            srcCred=dbMeta.getCred(srcDBid);
		   } else {
		      ovLogger.error(label + " dbID " + srcDBid + " does not appear to exist");
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
	   label=dbID+"."+jLib+"."+jName;
	   
	   journalLib = jLib;
	   journalName = jName;
	   srcLogTable=jLib+"."+jName;
       srcCred=dbMeta.getCred(srcDBid);

	   return linit();
   }

   
   public void markStartTime() {
      Calendar cal = Calendar.getInstance();
      startMS = cal.getTimeInMillis();
      
   }
   public void markEndTime() {
	  Calendar cal = Calendar.getInstance();
      endMS = cal.getTimeInMillis();
   }
   
   public void saveReplicateKafka() {
      int duration = (int)(int)((endMS - startMS)/1000);
      try {                     
         rRset.updateTimestamp("TS_LAST_REF400",tsThisRefresh);
         rRset.updateLong("SEQ_LAST_REF",seqThisRef);
        
         rRset.updateRow();
         repConn.commit();   
      } catch (SQLException e) {
         System.out.println(e.getMessage());
      }
	   metrix.sendMX("duration,jobId="+label+" value="+duration+"\n");
	   metrix.sendMX("Seq#,jobId="+label+" value="+seqThisRef+"\n");
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
   
	public void setThisRefreshSeq(long thisRefreshSeq) {
		if(thisRefreshSeq>0) {
			seqThisRef=thisRefreshSeq;
		}else {
			seqThisRef=seqLastRef;
			ovLogger.info("...hmm, got a 0 for SEQ# for srcTbl " + srcLogTable + ". The last one: " + seqLastRef);
		}
	}
	
   public int getPrcTimeout() {
      return prcTimeout;
   }
   public String getLogTable() {
      return srcLogTable;
   }
   public String getSQLSelect() {
      return sqlSelectSource;
   }

   public void setCred(OVScred ovsc) {
      repCred=ovsc;
   }
   public void setDbMeta(OVSrepo ovsdb){
      dbMeta=ovsdb;
   }
   public OVScred getSrcCred() {
      return srcCred;
   }
   public String getSrcDbDesc() {
      return srcCred.getDesc();
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
   public long getSeqLastRefresh() {
	   return seqLastRef;
   }

}    