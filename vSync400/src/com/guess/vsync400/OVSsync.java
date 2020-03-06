package com.guess.vsync400;

import java.io.*;
import java.util.*;
import java.text.*;
import java.time.Duration;

import org.apache.logging.log4j.Logger;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;


class OVSsync
{
   private static OVStableProcessor tProc = new OVStableProcessor();
   private static OVSrepo dbMeta = new OVSrepo();
   private static OVSmeta tblMeta;
   private static final Logger ovLogger = LogManager.getLogger();
 
	private static OVSconf conf = OVSconf.getInstance();
    
    private static String jobID = "KToV";
    private static int poolID;
    
   public static void main (String args[]) {   
      if (args.length != 1) { 
         System.out.println("Usage:   OVSinit <int poolID>  ");   
         System.out.println("example:   OVSinit 2 ");
         return;
      } 
      
      // -1: replicate all; >1: refresh all in a the pool
      poolID = -9 ;
      try{
    	  poolID = Integer.parseInt(args[0]);
      } catch (NumberFormatException nfe) { 
    	  ovLogger.error("invalid poolID. "+nfe);
      }
      if (poolID < -1){
    	  ovLogger.error("invalid poolID! ");
    	  return;
      }
      
	  dbMeta.init();
      tblMeta = new OVSmeta();
      tblMeta.setDbMeta(dbMeta);
      
	  tProc.setDbMeta(dbMeta);
	  tProc.setOVSmeta(tblMeta);

	  tProc.syncTblsByPoolID(poolID, jobID);
	  
//	  consumer.close();

	  
//	  test();

   }
   

	
	private static void test() {
		Properties propsx = new Properties();
		propsx.put("bootstrap.servers", "usir1xrvkfk01:9092,usir1xrvkfk02:9092");
	    propsx.put("group.id", "group3");
	    propsx.put("enable.auto.commit", "true");
	    propsx.put("auto.commit.interval.ms", "1000");
	    propsx.put("auto.offset.reset", "earliest");
	    propsx.put("session.timeout.ms", "30000");
	    propsx.put("key.deserializer", "org.apache.kafka.common.serialization.LongDeserializer");
	    propsx.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	    
	    KafkaConsumer<Long, String> consumerx = new KafkaConsumer<Long, String>(propsx);
	    consumerx.subscribe(Arrays.asList("JOHNLEE2.TESTTBL2"));
	    
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
	}

}