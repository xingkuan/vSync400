package com.guess.vsync400;

import java.io.*;
import java.util.*;
import java.text.*;
import java.time.Duration;
import java.sql.*;

import org.apache.logging.log4j.Logger;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
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
 //  int tableID;
   OVSsrc tblSrc;
   OVStgt tblTgt;
   OVSmeta tblMeta;
   OVSrepo dbMeta;
   String jobID;
   String srcTblAb7;

   OVSmetaJournal400 db2KafkaMeta;
   
   private static final OVSmetrix metrix = OVSmetrix.getInstance();
   private static final Logger ovLogger = LogManager.getLogger();

   private int totalDelCnt=0, totalInsCnt=0, totalErrCnt=0;
   
   private int giveUp;
   private int kafkaMaxPollRecords;
   private int pollWaitMil;
   
	public boolean init(String jID) {
      boolean rtv=true;
 
      jobID = jID;
 
      totalDelCnt=0; totalInsCnt=0; totalErrCnt=0;
/*20200303      
      tblMeta = new OVSmeta();
      tblMeta.setDbMeta(dbMeta);
      

      tblMeta.setTableID(tableID);
      rtv = tblMeta.init(jobID);
*/      
      srcTblAb7 = tblMeta.getSrcTable().substring(0,Math.min(7,tblMeta.getSrcTable().length()));

      // initialize source object
      tblSrc = new OVSsrc();
      tblSrc.setMeta(tblMeta);
      rtv = (tblSrc.init(jobID) && rtv);
      ovLogger.info("connected to source. tblID: " + tblMeta.getTableID() + " - " + tblMeta.getSrcDbDesc() + ". JobID: "+ jobID  );
     
      // initialize target object
      tblTgt = new OVStgt();
      tblTgt.setMeta(tblMeta);
      rtv = (tblTgt.init(jobID) && rtv);
      ovLogger.info("connected to target. tblID: " + tblMeta.getTableID() + " - " + tblMeta.getTgtTable() + ". JobID: "+ jobID  );

      return rtv;
   }
   
   public boolean tblInitType1(){
      boolean rtv=true;
      int recordCnt;
      int errorCnt;
      
      // Make sure we had started replicating this Journal before initializing the table.
      //  This is to gurantee no data loss due to gap in replicating journal.
      long curLogSeq = tblSrc.getThisRefreshSeq();
      if (db2KafkaMeta.getSeqLastRefresh() == 0 ) {
    	  ovLogger.info("Before initializing a table, please make sure the Journal has been started replicating to Kafka!!!" );
    	  rtv = false;
      }else {
	      if ( (tblMeta.getCurrState() == 0)       ||    // setup, but not initialized 
	    	   ( (tblMeta.getCurrState() == 2            //or (initialized 
	    	      || tblMeta.getCurrState() == 5 ) &&    //     or refreshed) 
	    	      tblMeta.getTgtUseAlt()                 //   and use swap ? 
	    	   ) 
	      ){
	         tblMeta.setCurrentState(1);   // set current state to initializing
	         tblMeta.markStartTime();
	      
	         //Timestamp ts = new Timestamp(System.currentTimeMillis());
	         try {
	            tblSrc.initSrcQuery("");
	            ovLogger.info("src query initialized. tblID: " + tblMeta.getTableID() +". Job " + jobID );
	
	            //db2KafkaMeta.setRefreshTS(tblSrc.getThisRefreshHostTS());  This is Journal level info. Don't do it here!
	            //db2KafkaMeta.setRefreshSeq(tblSrc.getThisRefreshSeq());    This is Journal level info. Don't do it here!
	            tblMeta.setRefreshTS(tblSrc.getThisRefreshHostTS());   //and it is info only
	            tblMeta.setRefreshSeq(curLogSeq);     //    it is info only. 
	
	            tblTgt.setSrcRset(tblSrc.getSrcResultSet());
	            recordCnt=tblTgt.initLoadType1();
	
	            tblMeta.setRefreshCnt(recordCnt);
	            errorCnt=tblTgt.getErrCnt();
	            if(errorCnt>0)
	            	metrix.sendMX("errCnt,jobId="+jobID+",tblID="+srcTblAb7+"~"+tblMeta.getTableID()+" value=" + errorCnt + "\n");
	            //ovLogger.info("JobID: " + jobID + ", tblID: " + tableID  + " stats saved");
	
	            tblTgt.commit();
	            tblSrc.commit();
	            ovLogger.info("Refreshed tblID: " + tblMeta.getTableID() + ". JobID: " + jobID);
	            
	            tblMeta.markEndTime();
	            tblMeta.saveInitStats(jobID); 
	            //db2KafkaMeta.saveReplicateKafka();    Initialize table has nothing to do with Journal level info. Don't do it here.
	            
	            if (recordCnt < 0) {
	               tblMeta.setCurrentState(7);   //broken - suspended
	               rtv=false;
	            } else {
	               tblMeta.setCurrentState(2);   //initialized
	            }
	         } catch (SQLException e) {
	            rtv=false;
	            ovLogger.error("exception in tblInitType1() for (tblID: " + tblMeta.getTableID() +"): "  + e.getMessage());
	         } finally {
			      if (!rtv) {
	                 try {
	                    tblTgt.rollback();
	                    tblSrc.rollback();
	// .			        tblSrc.setTriggerOff(); 
	                   tblMeta.setCurrentState(0);
	                 } catch(SQLException e) {
	                    ovLogger.error("JobID: " + jobID + ", tblID: " + tblMeta.getTableID() + e.getMessage());
	                 }
				  }
	         }
	      } else { 
	         ovLogger.error("Cannot initialize. tblID " + tblMeta.getTableID() +" not in correct state.");
	         rtv=false;
	      }
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
         ovLogger.info("JobID: " + jobID + ", tblID: " + tblMeta.getTableID() + " init type 2");
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
         ovLogger.error("JobID: " + jobID + ", tblID: " + tblMeta.getTableID() + " Cannot initialize... not in correct state");
      }
      return true;
   }
  
   public void tblLoadSwap() {
      tblMeta.setTgtUseAlt();
      if (tblInitType1()) {
         ovLogger.info("tblID: " + tblMeta.getTableID() + " Init successful. JobID: " + jobID );
      } else {
          ovLogger.info("tblID: " + tblMeta.getTableID() + " Init failed. JobID: " + jobID );
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

      metrix.sendMX("rowDiff,jobId="+jobID+",tblID="+srcTblAb7+"~"+tblMeta.getTableID()+" value=" + rowDiff + "\n");
      metrix.sendMX("rowSrc,jobId="+jobID+",tblID="+srcTblAb7+"~"+tblMeta.getTableID()+" value=" + srcRC + "\n");
      metrix.sendMX("rowTgt,jobId="+jobID+",tblID="+srcTblAb7+"~"+tblMeta.getTableID()+" value=" + tgtRC + "\n");
 
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
	   out.write("TableID: " + tblMeta.getTableID() + ", TableName: " + tblMeta.getSrcSchema() + '.' + tblMeta.getSrcTable() 
	       + ", srcCnt: " + srcRC 
	       + ", tgtCnt: " + tgtRC
	       + ", diffCnt: " + diffRC
	       + "\r\n");
	   out.close();
   } catch (Exception e){
       System.out.println("Error writing audit file: " + e.getMessage());
    }
   }
   
   private KafkaConsumer<Long, String> createKafkaConsumer(String topic){
	   String consumerGrp = jobID+tblMeta.getTableID();
		Properties propsx = new Properties();
		propsx.put("bootstrap.servers", "usir1xrvkfk01:9092,usir1xrvkfk02:9092");
	    propsx.put("group.id", consumerGrp);
	    //propsx.put(ConsumerConfig.GROUP_ID_CONFIG, jobID);
	    propsx.put("client.id", jobID);
	    propsx.put("enable.auto.commit", "true");
	    propsx.put("auto.commit.interval.ms", "1000");
	    propsx.put("auto.offset.reset", "earliest");    //if we do this, we better reduce msg retention to just one day.
	    propsx.put("max.poll.records", kafkaMaxPollRecords);
	    propsx.put("session.timeout.ms", "30000");
	    propsx.put("key.deserializer", "org.apache.kafka.common.serialization.LongDeserializer");
	    propsx.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	    
	    KafkaConsumer<Long, String> consumerx = new KafkaConsumer<Long, String>(propsx);
	    //consumerx.subscribe(Arrays.asList("JOHNLEE2.TESTTBL2"));
	    consumerx.subscribe(Arrays.asList(topic));
	    
	    return consumerx;
   }
   public boolean tblRefreshTry() {
	   KafkaConsumer<Long, String> consumerx = createKafkaConsumer("JOHNLEE2.TESTTBL2");
	   
	    int noRecordsCount=0, cntRRN=0;
	    int giveUp=10; 
	    String rrnList="";
	    long lastJournalSeqNum=0l;
	    
	    while (true) {
	      ConsumerRecords<Long, String> records = consumerx.poll(0);
	      
	        if (records.count()==0) {
	            noRecordsCount++;
	            if (noRecordsCount > giveUp) break;
	            else continue;
	        }
	        
	        for (ConsumerRecord<Long, String> record : records) {
	            //System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
	        	if(cntRRN==0)
	        		rrnList = record.value();
	        	else
	        		rrnList = rrnList + "," + record.value();
	            lastJournalSeqNum=record.key();
	            cntRRN++;
	        }
	      
		        //in case there are more in Kafka broker:
		        rrnList="";
		        noRecordsCount=0;
	      
	      for (ConsumerRecord<Long, String> record : records) {

	        System.out.println("key: " + record.key()); 
	        System.out.println("value: " + record.value()); 

	        System.out.println("Partition: " + record.partition() 
	            + " Offset: " + record.offset()
	            + " Value: " + record.value() 
	            + " ThreadID: " + Thread.currentThread().getId()  );
	         
	      }
	    }
	   
	   return true;
   }
   //public boolean tblRefresh(KafkaConsumer<Long, String> kConsumer) throws SQLException {
   public boolean tblRefresh() throws SQLException {
   //public boolean tblRefresh() throws SQLException {
      boolean rtv=true;
     
	   
	    int noRecordsCount=0, cntRRN=0;
	    boolean firstItem=true; 
	    String rrnList="";
	    long lastJournalSeqNum=0l;
		boolean success=true;
	    
      
      giveUp = Integer.parseInt(conf.getConf("kafkaMaxEmptyPolls"));
      kafkaMaxPollRecords = Integer.parseInt(conf.getConf("kafkaMaxPollRecords"));
      pollWaitMil = Integer.parseInt(conf.getConf("kafkaPollWaitMill"));
      

      if (tblMeta.getCurrState() == 2 || tblMeta.getCurrState() == 5) {
      	  String srcLog = tblMeta.getLogTable();
      	  String[] res = srcLog.split("[.]", 0);
      	  //String jLibName = res[0];
      	  //String jName = res[1];

		  //String strTS = new SimpleDateFormat("yyyy-MM-dd-HH.mm.ss.SSSSSS").format(tblMeta.getLastRefresh());
		  tblMeta.markStartTime();
		
		  //build list of RRN is the format of "1,2,3"
	   	   //KafkaConsumer<Long, String> consumerx = createKafkaConsumer("JOHNLEE2.TESTTBL2");
		  String topic=tblMeta.getSrcSchema()+"."+tblMeta.getSrcTable();
		   KafkaConsumer<Long, String> consumerx = createKafkaConsumer(topic);

          ovLogger.info("    START on this table.");
		  while (true) {     
				//blocking call:
		        ConsumerRecords<Long, String> records =	consumerx.poll(Duration.ofMillis(pollWaitMil));
		        //ConsumerRecords<Long, String> records =	consumerx.poll(0);
		        if (records.count()==0) {
		            noRecordsCount++;
		            ovLogger.info("    consumer poll cnt: " + noRecordsCount);
		            if (noRecordsCount > giveUp) break;    //no more records. exit 
		            else continue;
		        }
		        
		        for (ConsumerRecord<Long, String> record : records) {
		            //System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
		            lastJournalSeqNum=record.key();
		            
			        //if( lastJournalSeqNum == db2KafkaMeta.getSeqLastRefresh()) {  //stop by the SEQ number indicated in sync_journal400 table:
			    	//   break;    //break from the for loop 
			        //}       ///let's stop where ever it is. really, it does not matter! 

			        if(firstItem) {
		        		rrnList = record.value();
		        		firstItem=false;
		        	}else
		        		rrnList = rrnList + "," + record.value();
		        	
		        	cntRRN++;
		        }
		        ovLogger.info("    processing to: " + cntRRN);
	        	success = replicateRRNList(rrnList);
	        	if (!success) break;
	        	//in case there are more in Kafka broker, start the next cycle:
	        	rrnList="";
	        	noRecordsCount=0;
	        	firstItem=true;
		  }
 		  
          ovLogger.info("    COMPLETE on this table.");
		  consumerx.close();  

		  java.sql.Timestamp ts = new java.sql.Timestamp(System.currentTimeMillis()); 
		  //if no entries from Kafka, then simply mark this run:
          tblMeta.markEndTime();
		  if(cntRRN==0) {
		   	ovLogger.info("tblID: " + tblMeta.getTableID() + "_____" + tblMeta.getSrcSchema() + "." + tblMeta.getSrcTable() + " has no change since last sync." );

		   	tblMeta.setRefreshCnt(0);
		   	//tblMeta.setRefreshTS(tblSrc.getThisRefreshHostTS()); 
		   	//tblMeta.setRefreshSeq(tblSrc.getThisRefreshSeq());
		   	tblMeta.setRefreshTS(ts); 
		   	tblMeta.setRefreshSeq(0);   //0 means keep the original value.
		   	tblMeta.saveRefreshStats(jobID);
		   	
	 	       metrix.sendMX("delCnt,jobId="+jobID+",tblID="+srcTblAb7+"~"+tblMeta.getTableID()+" value=0\n");
	 	       metrix.sendMX("insCnt,jobId="+jobID+",tblID="+srcTblAb7+"~"+tblMeta.getTableID()+" value=0\n");
	 	       metrix.sendMX("errCnt,jobId="+jobID+",tblID="+srcTblAb7+"~"+tblMeta.getTableID()+" value=0\n");

		  }else{
	        tblMeta.setRefreshCnt(cntRRN);
	        tblMeta.setRefreshTS(ts);
	        tblMeta.setRefreshSeq(lastJournalSeqNum);
 	       metrix.sendMX("delCnt,jobId="+jobID+",tblID="+srcTblAb7+"~"+tblMeta.getTableID()+" value=" + totalDelCnt + "\n");
 	       metrix.sendMX("insCnt,jobId="+jobID+",tblID="+srcTblAb7+"~"+tblMeta.getTableID()+" value=" + totalInsCnt + "\n");
 	       metrix.sendMX("errCnt,jobId="+jobID+",tblID="+srcTblAb7+"~"+tblMeta.getTableID()+" value=" + totalErrCnt + "\n");
	        tblMeta.saveRefreshStats(jobID);
	        
	    	ovLogger.info("Refreshed tblID: " + tblMeta.getTableID() + ", Del Cnt: " + totalDelCnt);
	    	ovLogger.info("Refreshed tblID: " + tblMeta.getTableID() + ", Ins Cnt: " + totalInsCnt);
	    	ovLogger.info("Refreshed tblID: " + tblMeta.getTableID() + ", Err Cnt: " + totalErrCnt);

		  }
	       if(lastJournalSeqNum>0)
	    	   metrix.sendMX("JournalSeq,jobId="+jobID+",tblID="+srcTblAb7+"~"+tblMeta.getTableID()+" value=" + lastJournalSeqNum + "\n");

		 	   ovLogger.info("tblID: " + tblMeta.getTableID() + ", "  + " - " + tblMeta.getSrcDbDesc() + " commited" );

	       if (!success) {
	           tblMeta.setCurrentState(7);   //broken - suspended
	           //System.out.println(label + "refresh not succesfull");
	           ovLogger.info("JobID: " + jobID + ", tblID: " + tblMeta.getTableID() + "refresh not succesfull");
	        } else {
	           tblMeta.setCurrentState(5);   //initialized
	           //System.out.println(label + " <<<<<<<<<<<<  refresh successfull");
	           ovLogger.info("JobID: " + jobID + ", tblID: " + tblMeta.getTableID() + " <<<<<  refresh successfull");
	        }

     } else { 
         ovLogger.error("JobID: " + jobID + ", tblID: " + tblMeta.getTableID()  + " No refresh for table state");
         rtv=false;
     }
     
      return rtv;
   }

   private boolean replicateRRNList(String rrns) {
	   boolean success=true;
	   
	   int rowCnt;
	   try {
			rowCnt = tblTgt.dropStaleRecordsOfRRNlist(rrns);
	//	   ovLogger.info(jobID +  " deleted - " + rowCnt );
		   totalDelCnt = totalDelCnt + rowCnt;
		   
	       //tblSrc.initSrcQueryOfRRNList(rrns);
	       tblSrc.initSrcQuery(rrns);
	//       ovLogger.info("Source query initialized. tblID: " + tblMeta.getTableID() + " - " + tblMeta.getSrcDbDesc() );
	       tblTgt.setSrcRset(tblSrc.getSrcResultSet());
	       rowCnt=tblTgt.initLoadType1();
	//      ovLogger.info("Refreshed tblID: " + tblMeta.getTableID() + ", record Count: " + rowCnt);
	       if(rowCnt<0)
	    	   success=false;
	       totalInsCnt = totalInsCnt + rowCnt;
	
	       rowCnt=tblTgt.getErrCnt();
	       totalErrCnt = totalErrCnt + rowCnt;
	
           tblTgt.commit();
	       tblSrc.commit();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
       
       return success;
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
            tblTgt.commit();
            //System.out.println(label + "Table stopped");
            ovLogger.info("Log for " + tblMeta.getTableID() + " stopped");
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
   /*
   public void setTableID(int tid) {
      tableID=tid;
   }
   public int getTableID() {
	      return tableID;
   }*/
   public void setOVSmeta(OVSmeta md) {
	   tblMeta = md;
   }
   public void setDB2KafkaMeta(OVSmetaJournal400 km){
	   db2KafkaMeta=km;
   }

   public String getJournalLib() {
	   return tblMeta.getJournalLib();
   }
   public String getJournalName() {
	   return tblMeta.getJournalName();
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
      ovLogger.info("closing tgt. tblID: " + tblMeta.getTableID() );
      try {
         tblTgt.close();
      } catch (SQLException e) {
         System.out.println(e.getMessage());
      }
      ovLogger.info("closing src. tblID: " + tblMeta.getTableID());
      try {
         tblSrc.close();
      } catch (SQLException e) {
         System.out.println(e.getMessage());
      }
   }
}