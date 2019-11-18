package com.guess.vsync400;

import java.io.*;
import java.util.*;
import java.text.*;
import java.sql.*;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

/*
  class: OVStable
  
  Instantiates OVSmeta instance and handles all high-level functions
     - implements 
         table initialization
         stop table
         loadswap
         refresh
*/

class OVStable {
   OVScred srcCred;
   OVScred tgtCred;
   OVScred repCred;
   int tableID;
   OVSsrc tblSrc;
   OVStgt tblTgt;
   OVSmeta tblMeta;
   OVSrepo dbMeta;
   String jobID;
   String srcTblAb7;

   private static final OVSmetrix metrix = OVSmetrix.getInstance();
   private static final Logger ovLogger = LogManager.getLogger();

	public boolean init(String jID) {
      boolean rtv=true;
 
      jobID = jID;
      
      tblMeta = new OVSmeta();
      tblMeta.setDbMeta(dbMeta);
      

      tblMeta.setTableID(tableID);
      rtv = tblMeta.init(jobID);
      srcTblAb7 = tblMeta.getSrcTable().substring(0,Math.min(7,tblMeta.getSrcTable().length()));

      // initialize source object
      tblSrc = new OVSsrc();
      tblSrc.setMeta(tblMeta);
      rtv = (tblSrc.init(jobID) && rtv);
      ovLogger.info("connected to source. tblID: " + tableID + " - " + tblMeta.getSrcDbDesc() + ". JobID: "+ jobID  );
     
      // initialize target object
      tblTgt = new OVStgt();
      tblTgt.setMeta(tblMeta);
      rtv = (tblTgt.init(jobID) && rtv);
      ovLogger.info("connected to target. tblID: " + tableID + " - " + tblMeta.getTgtTable() + ". JobID: "+ jobID  );

      return rtv;
   }
   
   public boolean tblInitType1(){
      boolean rtv=true;
      int recordCnt;
      int errorCnt;
      
      if ( (tblMeta.getCurrState() == 0)       ||    // setup, but not initialized 
    	   ( (tblMeta.getCurrState() == 2            //or (initialized 
    	      || tblMeta.getCurrState() == 5 ) &&    //     or refreshed) 
    	      tblMeta.getTgtUseAlt()                 //   and use swap ? 
    	   ) 
      ){
         tblMeta.setCurrentState(1);   // set current state to initializing
         tblMeta.markStartTime();
         try {
//TOTO: Change back, DEBUG USE
            tblSrc.initSrcQuery("");
//            tblSrc.initSrcQuery("where rowid='AAAptlAAiAAJyJ3AAF'");
            
            ovLogger.info("src query initialized. tblID: " + tableID +". Job " + jobID );
//2019.11.18, John:
            java.sql.Timestamp hostTS = tblSrc.getHostTS();
            
            tblTgt.setSrcRset(tblSrc.getSrcResultSet());
            recordCnt=tblTgt.initLoadType1();

            tblMeta.setRefreshCnt(recordCnt);
            errorCnt=tblTgt.getErrCnt();
            if(errorCnt>0)
            	metrix.sendMX("errCnt,jobId="+jobID+",tblID="+srcTblAb7+"~"+tableID+" value=" + errorCnt + "\n");
            //ovLogger.info("JobID: " + jobID + ", tblID: " + tableID  + " stats saved");

            tblTgt.commit();
            tblSrc.commit();
            ovLogger.info("Refreshed tblID: " + tableID + ". JobID: " + jobID);
            
            tblMeta.markEndTime();
            tblMeta.saveInitStats(jobID, hostTS);

            if (recordCnt < 0) {
               tblMeta.setCurrentState(7);   //broken - suspended
               rtv=false;
            } else {
               tblMeta.setCurrentState(2);   //initialized
            }
         } catch (SQLException e) {
            rtv=false;
            ovLogger.error("exception in tblInitType1() for (tblID: " + tableID +"): "  + e.getMessage());
         } finally {
		      if (!rtv) {
                 try {
                    tblTgt.rollback();
                    tblSrc.rollback();
// .			        tblSrc.setTriggerOff(); 
                   tblMeta.setCurrentState(0);
                 } catch(SQLException e) {
                    ovLogger.error("JobID: " + jobID + ", tblID: " + tableID + e.getMessage());
                 }
			  }
         }
      } else { 
         ovLogger.error("Cannot initialize. tblID " + tableID +" not in correct state.");
         rtv=false;
      }
      return rtv;
  }
  
  /* JLEE, 07/14:
   *   replicate changes since this initilization. < is there such use case ?>
   *   and it seems never used.
   */
  public boolean tblInitType2(){
      if (tblMeta.getCurrState() == 0){
         //  initialize table
         ovLogger.info("JobID: " + jobID + ", tblID: " + tableID + " init type 2");
         tblMeta.setCurrentState(1);   // set current state to initializing

         tblMeta.markStartTime();
         try {
            tblTgt.truncate();
// .            tblSrc.truncateLog();
            tblSrc.setTriggerOn();
         } catch (SQLException e) {

         }
      } else { 
         //System.out.println(label + "Cannot initialize... not in correct state");
         ovLogger.error("JobID: " + jobID + ", tblID: " + tableID + " Cannot initialize... not in correct state");
      }
      return true;
   }
  
   public void tblLoadSwap() {
      tblMeta.setTgtUseAlt();
      if (tblInitType1()) {
         ovLogger.info("tblID: " + tableID + " Init successful. JobID: " + jobID );
      } else {
          ovLogger.info("tblID: " + tableID + " Init failed. JobID: " + jobID );
      }
      tblTgt.swapTable();
   }

   public void audit(String jobID) {
	   int srcRC;
	   int tgtRC;
	   int rowDiff;
	   
       srcRC=tblSrc.getRecordCount();
       tgtRC=tblTgt.getRecordCount();

       tblMeta.saveAudit(srcRC, tgtRC);
       rowDiff = srcRC - tgtRC;

      metrix.sendMX("rowDiff,jobId="+jobID+",tblID="+srcTblAb7+"~"+tableID+" value=" + rowDiff + "\n");
      metrix.sendMX("rowSrc,jobId="+jobID+",tblID="+srcTblAb7+"~"+tableID+" value=" + srcRC + "\n");
      metrix.sendMX("rowTgt,jobId="+jobID+",tblID="+srcTblAb7+"~"+tableID+" value=" + tgtRC + "\n");
 
//TODO: remove it .. it is here for immediate Prod needs. 08/02/2017
      writeAudit(srcRC, tgtRC, rowDiff);
   }
   
   //only used for now, 2017/08/01 for the current Prod issue.
   OVSconf conf = OVSconf.getInstance();
   private String initLogDir = conf.getConf("initLogDir");
   private void writeAudit(int srcRC, int tgtRC, int diffRC){
	   try{
	   FileWriter fstream = new FileWriter(initLogDir + "/vSyncAudit.log", true);
	   BufferedWriter out = new BufferedWriter(fstream);
	   out.write("TableID: " + tableID + ", TableName: " + tblMeta.getSrcSchema() + '.' + tblMeta.getSrcTable() 
	       + ", srcCnt: " + srcRC 
	       + ", tgtCnt: " + tgtRC
	       + ", diffCnt: " + diffRC
	       + "\r\n");
	   out.close();
   } catch (Exception e){
       System.out.println("Error writing audit file: " + e.getMessage());
    }
   }

   public boolean tblRefresh() {
      boolean rtv=true;
      int recordCnt;
      int errorCnt;
      int srcRC;
      int tgtRC;
      
      srcRC=0;
      tgtRC=0;
      
      if (tblMeta.getCurrState() == 2 || tblMeta.getCurrState() == 5) {
         tblMeta.setCurrentState(3);   // set current state to being refreshed
         tblMeta.markStartTime();
         try {
            tblSrc.initSrcLogQuery();

          //2019.11.18, John:
            java.sql.Timestamp hostTS = tblSrc.getHostTS();

            tblTgt.setSrcRset(tblSrc.getSrcResultSet());
            tblTgt.dropStaleRecords();
            
            tblSrc.initSrcQuery("  where ( ROWID ) in ( select distinct M_ROW " 
            		+ " from "  +  tblMeta.getSrcSchema() + "." + tblMeta.getLogTable() 
            		+ " where  snaptime = '01-JUN-1910'  )" );  
            ovLogger.info("Source query initialized. tblID: " + tableID + " - " + tblMeta.getSrcDbDesc() );
            tblTgt.setSrcRset(tblSrc.getSrcResultSet());
            
            recordCnt=tblTgt.initLoadType1();
            ovLogger.info("Refreshed tblID: " + tableID + ", record Count: " + recordCnt);

            errorCnt=tblTgt.getErrCnt();
            if(errorCnt>0)
            	metrix.sendMX("errCnt,jobId="+jobID+",tblID="+srcTblAb7+"~"+tableID+" value=" + errorCnt + "\n");

            tblMeta.markEndTime();

            tblMeta.setRefreshCnt(tblTgt.getRefreshCnt());
            tblMeta.saveRefreshStats(jobID, hostTS);

            tblSrc.delConsumedLog();
            tblTgt.commit();
            tblSrc.commit();
            ovLogger.info("tblID: " + tableID + ", " + tableID + " - " + tblMeta.getSrcDbDesc() + " commited" );

            if (recordCnt < 0) {
               tblMeta.setCurrentState(7);   //broken - suspended
               //System.out.println(label + "refresh not succesfull");
               ovLogger.info("JobID: " + jobID + ", tblID: " + tableID + "refresh not succesfull");
            } else {
               tblMeta.setCurrentState(5);   //initialized
               //System.out.println(label + " <<<<<<<<<<<<  refresh successfull");
               ovLogger.info("JobID: " + jobID + ", tblID: " + tableID + " <<<<<  refresh successfull");
            }
         } catch (SQLException e) {
            rtv=false;
			ovLogger.error("TblRefresh() failed for tblID: " + tableID + e.getMessage());
         } finally {
		      if (!rtv) {
               try {
                  ovLogger.error("TblRefresh() failed for tblID: " + tableID + " JobID: " + jobID + ", tblID: " + tableID
                    + " exception handling started " );
                  tblTgt.rollback();
                  ovLogger.error(" tblID: " + tableID + " - " + tblMeta.getSrcDbDesc()  
                    + " exception handling tgt rolled back " );
                  tblSrc.rollback();
               //2018.02.02 John: don't turn it off. The next run will resume!
 		       //   tblSrc.setTriggerOff(); 
               //   tblMeta.setCurrentState(0);
                  tblMeta.setCurrentState(5);
               } catch(SQLException e) {
                  ovLogger.error("JobID: " + jobID + ", tblID: " + tableID + e.getMessage());
               }
			   }
		   }
     } else { 
         ovLogger.error("JobID: " + jobID + ", tblID: " + tableID  + " No refresh for table state");
         rtv=false;
     }
      return rtv;
   }

   public void tblStop(){
      if (tblMeta.getCurrState() != 0){
         //  initialize table
         tblMeta.setCurrentState(0);   // set current state to initializing
         tblMeta.markStartTime();
         try {
// .            tblSrc.truncateLog();
// .            tblSrc.setTriggerOff();
            tblSrc.commit();
            //System.out.println(label + "Table stopped");
            ovLogger.info("Log for " + tableID + " stopped");
         } catch (SQLException e) {
         }

      } else { 
         //System.out.println(label + "Cannot stop.. not in correct state");
         ovLogger.error("Cannot stop.. not in correct state");
      }
   }
   
   public int getLogCnt() {
      return tblSrc.getThreshLogCount();
   }
   public int getRecordCountThreshold() {
      return tblMeta.getRecordCountThreshold();
   }
   public int getRefreshType() {
      return tblMeta.getRefreshType();
   }
   public Timestamp getLastAudit() {
      Timestamp ts;
      Timestamp cts;
      ts=tblMeta.getLastAudit();

      if (ts == null) {
         cts = new Timestamp(0);
         return cts;
      } else {
         return ts;
      }       
   }
   public int getMinPollInterval() {
      return tblMeta.getMinPollInterval();
   }
   public Timestamp getLastRefresh() {
      Timestamp ts;
      Timestamp cts;
      ts = tblMeta.getLastRefresh();
      
      if (ts == null) {
         cts = new Timestamp(0);
         return cts;
      } else {
         return ts;
      } 
   }
   public void setDbMeta(OVSrepo ovsdb){
      dbMeta=ovsdb;
   }
   public void setsrcCred(OVScred ovsc) {
      srcCred=ovsc;
   }
  public void setRepCred(OVScred ovsc) {
      repCred=ovsc;
   }
   public void settgtCred(OVScred ovsc) {
      tgtCred=ovsc;
   }
   public void setTableID(int tid) {
      tableID=tid;
   }
//JLEE, 07/18
   public int getTableID() {
	      return tableID;
   }
   public String getTgtTableName() {
      return tblMeta.getTgtSchema() + "." + tblMeta.getTgtTable();
   }
   public String getSrcTableName() {
      return tblMeta.getSrcSchema() + "." + tblMeta.getSrcTable();
   }
   public int getDefInitType() {
      return tblMeta.getDefInitType();
   }
   public int getCurrentState() {
      return tblMeta.getCurrentState();
   }
   public void closeMeta() {
      //ovLogger.log(label + "closing table metadata");
      tblMeta.close();
      //ovLogger.log(label + "table metadata closed");
   }
   public void close() {
      ovLogger.info("closing tgt. tblID: " + tableID );
      try {
         tblTgt.close();
      } catch (SQLException e) {
         System.out.println(e.getMessage());
      }
      ovLogger.info("closing src. tblID: " + tableID);
      try {
         tblSrc.close();
      } catch (SQLException e) {
         System.out.println(e.getMessage());
      }
   }
}