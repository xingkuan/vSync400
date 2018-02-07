package com.guess.vsync;

import java.io.*;
import java.util.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/*
  class OVStableProcessor
  
  Instantiates an OVStable instance, and wraps it inside a theadable class
  exposes high level table operators (init, stop, refresh)
*/

class OVStableProcessor {//. extends Thread {
   OVStable ovTable = new OVStable();
   OVSrepo dbMeta = new OVSrepo();

   String jobID="";
   int tblID=-1;

   private static final Logger ovLogger = LogManager.getLogger();

// 07/18: go through eatch table in the pool, and sync them accordingly.
   public void syncTblsByPoolID(int poolID) {
	  List<Integer> tblIDs = dbMeta.getTblsByPoolID(poolID); 
	  for (int tid: tblIDs){
		  syncTblByID(tid, poolID);
	  }
   }
   
   //put toolID there, so Grafana dashboard can be serperated according to poolID 
   private void syncTblByID(int tblID, int pID) {
	  init(tblID, "syncTbl"+pID);
	  refresh();
   }
//JOHNLEE, 07/24, replace the run() {}? not a good abstraction; should be re-orged!
   public void refresh() {
	   procAll();
	   close();
   }
   
 
//.   public boolean init(int tblID, String lbl) {
   private boolean init(int tid, String lbl) {
      // initializes table tblID.  lbl is simply a label to make it easier to filter logs and output based on the lbl content
      boolean rtv;
      jobID = lbl;
      tblID = tid;  //.JLEE
      
      ovLogger.info("Initializing tblID: " + tblID + ". jobID " + jobID + ".");
      
      rtv=true;
      ovTable=null;
      
      ovTable = new OVStable();
      ovTable.setDbMeta(dbMeta);
      ovTable.setTableID(tblID);

      if (ovTable.init(jobID)) {
      } else {
         close();
         rtv=false;
         ovLogger.error("failed: init of ovTable: " + tblID);
      }
      return rtv;
   }
   
   public void setDbMeta(OVSrepo dbm) {
      dbMeta=dbm;
   }
   public void close() {
      ovTable.closeMeta();
      ovTable.close();
      ovTable=null;
   }

   public void auditTbls(int pID) {
	  List<Integer> tblIDs = dbMeta.getTblsByPoolID(pID);
	  for (int tid: tblIDs){
		  // TOTO, John, 2018.02.01: very confusing logic; Need re-org the code here.
		  init(tid, "AudTbl"+pID);
		  ovTable.audit(jobID);
		  close();
	  }	      
   }
   
   public void deactivate(int tblID, String jobID) {
		  init(tblID, jobID);
	      ovTable.tblStop();
	}
   
//.   public void procInitDefault() {
   public void procInitDefault(int tblID, String jobID) {
	  init(tblID, jobID);   //. JLEE. 07/18: moved it there so don't need to call is by caller.
	   
      int i = ovTable.getDefInitType();  //. JLEE 07/14: from the table, all are 1 
      switch (i) {
         case 1:
            initType1();
            break;
         case 2:
            initType2();
            break;
      }   
      close();   //.JLEE, 07/18: moved it there so don't need to call is by caller.
   }
   public void procStop() {
      ovTable.tblStop();
   }
   
   public void tblLoadSwap() {
      ovTable.tblLoadSwap();
   }
   public void procAll() {
      int rt;
      int rc;
      boolean flg=true;
      
      rt = ovTable.getRefreshType();

      switch(rt){
      	case 1:  // regular refresh
            flg=true;    // just to make it easy to read
      		break;
      	case 2:   // smart refresh, depends on the number of rows changed
            rc=ovTable.getLogCnt();
            ovLogger.info("Log record count is: " + rc);
            if (rc> ovTable.getRecordCountThreshold()) {
               ovLogger.info("Threshold Exceeded - Reloading Table");
               procStop();
               procInitDefault(tblID, jobID);
               flg=false;
            }
      		break;
      	case 3:   // load swap
            rc=ovTable.getLogCnt();
            if (rc>ovTable.getRecordCountThreshold()) {
               ovLogger.info("Threshold Exceeded - LoadSwaping Table " + tblID);
               tblLoadSwap();
               flg=false;
            }
      		break;
      	case 4:   // full refresh only. A special case of type 2
            ovLogger.warn("Type 4. is for reloading. Table " + tblID);
            ovLogger.warn("use OVSinit utility.");
      	    //procStop();
            //procInitDefault(tblID, jobID);
      	    flg=false;
      		break;
      }
 
/* do not remove it yet. I need to make sure the above new codes covers the logic here.      
      // test for refresh type trickle...   otherwise skip refresh  - refresh_type = 1, 2 or 3
      if (rt==1 || rt==2 || rt==3) {
         // test for poll interval
//.         if ((ovTable.getLastRefresh().getTime() + (ovTable.getMinPollInterval() * 60000)) < cd.getTime()) {   //. JLEE, 07/14 shouldn't it be ">"?
    	 // ... not going to rely on the interval anyway; instead, Jenkin schedule dictate the run. 
            if (rt==2) {
            //reload - test threshold
               ovLogger.info("Testing Threshold for reload for tblID: " + ovTable.getTableID() + " tblName: " + ovTable.getTgtTableName());
               ovLogger.info("  allowable limit: " + ovTable.getRecordCountThreshold());
               rc=ovTable.getLogCnt();
               ovLogger.info("Log record count is: " + rc);
               if (rc> ovTable.getRecordCountThreshold()) {
                  //System.out.println(label +"Threshold Exceeded - Reloading Table");
                  ovLogger.info("Threshold Exceeded - Reloading Table");
                  procStop();
                  procInitDefault(tblID, jobID);
                  flg=false;
               }
            }
            if (rt==3) {     // loadswap 
               rc=ovTable.getLogCnt();
               if (rc>ovTable.getRecordCountThreshold()) {
                  ovLogger.info("Threshold Exceeded - LoadSwaping Table " + tblID);
                  tblLoadSwap();
                  flg=false;
               }
            }
      } else {
         flg=false;
      }
*/
      
      if (flg) {
         ovTable.tblRefresh();
      } 
      
   }

   private  void initType1() {
      if (ovTable.tblInitType1()) {
    	  ovLogger.info("Init type1successful. tblID: " + tblID );
      } else {
    	ovLogger.error("Init type1 not successful. tblID: " + tblID);
      }
   }
   
   private  void initType2() {
      if (ovTable.tblInitType2()) {
         ovLogger.info("jobID: " + jobID + " Init type2 successful");
      } else {
         ovLogger.info("jobID: " + jobID + " Init type2 not successful");
      }
   }

 }    