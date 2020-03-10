package com.guess.vsync400;

import java.io.*;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/*
  class OVStableProcessor
  
  Instantiates an OVStable instance, and wraps it inside a theadable class
  exposes high level table operators (init, stop, refresh)
*/

class OVStableProcessor {//. extends Thread {
   OVSmeta tblMeta;	
   OVStable ovTable = new OVStable();
   OVSrepo dbMeta = new OVSrepo();
   
   private KafkaConsumer<Long, String> kafkaConsumer;
   
   String jobID="";
   int tblID=-1;

   private static final Logger ovLogger = LogManager.getLogger();

// 07/18: go through eatch table in the pool, and sync them accordingly.
   public void syncTblsByPoolID(int poolID, String jID) {
	  jobID=jID+poolID;
	  
	  List<Integer> tblIDs = dbMeta.getTblsByPoolID(poolID); 
	  for (int tid: tblIDs){
		  //syncTblByID(tid);
		  init(tid);
		  procAll();
	  }
	  
	  close();
   }
   
   //private boolean init(int tid, String lbl) {
   private boolean init(int tid) {
      boolean rtv;
      tblID = tid;  
      
      ovLogger.info("Initializing tblID: " + tblID + ". jobID " + jobID + ".");
      
      rtv=true;

      tblMeta.setTableID(tblID);
      tblMeta.init(jobID);
      
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
   public void setOVSmeta(OVSmeta md) {
	   tblMeta=md;
	   ovTable.setOVSmeta(md);
   }
   
   public void setDB2KafkaMeta(OVSmetaJournal400 km) {
	   ovTable.setDB2KafkaMeta(km);
   }   
/*   public void saveReplicateKafka()
   {
	   db2KafkaMeta.saveReplicateKafka();
   }
   */
   public void close() {
      ovTable.closeMeta();
      ovTable.close();
      ovTable=null;
   }

   public void auditTbls(int pID, String jID) {
	  jobID=jID+"_"+pID;
	  List<Integer> tblIDs = dbMeta.getTblsByPoolID(pID);
	  for (int tid: tblIDs){
		  init(tid);
		  ovTable.audit(jobID);
	  }	      
	  close();
   }
   
   public void deactivate(int tID, String jID) {
	    jobID=jID+"_"+tID;
		init(tblID);
	    ovTable.tblStop();
	}
   
//.   public void procInitDefault() {
   public void procInitDefault(int tID, String jID) {
	  jobID= jID + "_" + tID;
	  int tblID = tblMeta.getTableID();  //which should be the same as tID 
	  init(tblID);   //. JLEE. 07/18: moved it there so don't need to call is by caller.
	   
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
   private void procAll() {
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
               procInitDefault(tblMeta.getTableID(), jobID);
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

	//ovTable.tblRefreshTry();
      if (flg) {
         try {
			ovTable.tblRefresh();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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

public void setKafkaConsumer(KafkaConsumer<Long, String> consumer) {
	kafkaConsumer=consumer;
}

 }    