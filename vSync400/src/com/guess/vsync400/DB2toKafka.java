package com.guess.vsync400;


import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class DB2toKafka {
	private static final Logger ovLogger = LogManager.getLogger();
	private static final OVSmetrix metrix = OVSmetrix.getInstance();
	
	Properties props = new Properties();
    OVSconf conf = OVSconf.getInstance();
    
    KafkaProducer<Long, String> producer;

    	
    OVSrepo dbMeta = new OVSrepo();
    
    private static String jobID = "db2ToKafka";
    private int poolID;
    
    public static void main (String args[]) {   
        

        if (args.length != 1) { 
           System.out.println("Usage:   DB2toKafka <int pool;ID>  ");
           System.out.println("example:   DB2toKafka 99 ");
           return ;
        } 
       
        DB2toKafka replicateFromDB2toKafka = new DB2toKafka();

        replicateFromDB2toKafka.setPoolId(Integer.parseInt(args[0]));
        replicateFromDB2toKafka.setupKafka();
        replicateFromDB2toKafka.replicate();
        replicateFromDB2toKafka.close();
        
        //replicateFromDB2toKafka.test();
        return ;
     }
    
    private void setPoolId(int pID) {
    	poolID=pID;
    }
	private void setupKafka(){
        String kafkaURL = conf.getConf("kafkaURL");
	    String strVal = conf.getConf("kafkaMaxBlockMS");
		int kafkaMaxBlockMS = Integer.parseInt(strVal);
		String kafkaACKS = conf.getConf("kafkaACKS");
		String kafkaINDEM = conf.getConf("kafkaINDEM");
		strVal = conf.getConf("kafkaSNDS");
		int kafkaSendTries =Integer.parseInt(strVal);

		//props.put("request.timeout.ms", 60000);
	    props.put("bootstrap.servers", kafkaURL);
	    props.put("client.id", "producer"+poolID);
	//    props.put("transactional.id", "DtoK"+poolID);
	    //props.put("acks", "all");
	    props.put("acks", kafkaACKS);
	    props.put("enable.idempotence", kafkaINDEM);
	    //props.put("message.send.max.retries", kafkaSendTries);
	//    props.put("retries", kafkaSendTries);
	    props.put("batch.size", 1638400);
	    props.put("linger.ms", 1);
	    props.put("buffer.memory", 33554432);
	    props.put("max.block.ms", kafkaMaxBlockMS );     //default 60000 ms
	    //props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
	    //props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
	    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
	    props.put(ProducerConfig.CLIENT_ID_CONFIG, "vSync400_"+poolID);

	    producer = new KafkaProducer<Long, String>(props);
	}
	
	private void close() {
		producer.close();
	}

	//open jdbc connection to DB2, and send to each journal entry to the a topic
	//   of the topic of the same name.
	private void replicate(){
		int srcDBid;
		String jLib;
		String jFile;
		boolean goodRun=true;
        dbMeta.init();
		
		List<String> journals = dbMeta.getAS400JournalsByPoolID(poolID);
		  for (String jName : journals){
			//1. although technically can handle different source DB, but it is not; instead, simply error out;
			String[] tokens = jName.split("[.]");
			if(tokens.length==3) {
				srcDBid=Integer.parseInt(tokens[0]);
				jLib=tokens[1];
				jFile=tokens[2];
	
				ovLogger.info(" replicating: " + jName);
				goodRun=replicate(srcDBid, jLib, jFile);
				if(!goodRun)
					break;   //Something is wrong with the infrastructure; no need to continue.
				ovLogger.info(" finished: " + jName);

			 }else {
  			    ovLogger.info(" invalide name: dbID: " + jName);
			 }
		}
	}

	
	private boolean replicate(int dbID, String jLib, String jName){
		boolean success=true;
		
	    List<String> tblList;
	    
		//The DB2 journal
		OVSmetaJournal400 tblMeta;   
	    tblMeta = new OVSmetaJournal400();
	    tblMeta.setDbMeta(dbMeta);
	    tblMeta.initForKafka(dbID, jLib, jName);
	    tblMeta.markStartTime();
	    
		//JDBC result of "Display_jounral(), and iterate through it:
		OVSsrc tblSrc = new OVSsrc();
	    tblSrc.setMeta400(tblMeta);
	    tblSrc.setLabel400(jobID+"."+poolID+"."+dbID+"."+jLib+"."+jName);
		tblSrc.linit400();   //initialize src DB conn
		boolean hasWork = tblSrc.initForKafkaMeta();
		if(hasWork) {
		    tblList = dbMeta.getDB2TablesOfJournal(dbID, jLib+"."+jName);
	        
	    	//the message to be formated as: SEQ#:RRN:TS
	
	        int rrn=0;
	        long seq=0l;
	        int cnt=0, xcnt=0, chgCnt=0;
	        String srcTbl="";
	        tblSrc.initSrcLogQuery400(tblList);
			ovLogger.info("      BEGING");
			RecordMetadata metadata;
		    //ProducerRecord<Long, String> aMsg;
		    //ProducerRecord<String, String> aMsg;
		   
	        ResultSet srcRset = tblSrc.getSrcResultSet();   //the journal lib and member names are in thetblMeta.
        	//producer.initTransactions();
	        try {
	        	//producer.beginTransaction();
				while (srcRset.next()) {
		        	cnt++;
					rrn=srcRset.getInt("RRN");
					seq=srcRset.getLong("SEQNBR");
					srcTbl=srcRset.getString("SRCTBL");
					// ignore those for unregister tables:
					if (tblList.contains(srcTbl)) {
						xcnt++;
						chgCnt++;
					final	ProducerRecord<Long, String>	aMsg = new ProducerRecord<Long, String>(srcTbl, seq, String.valueOf(rrn));
						//metadata = producer.send(aMsg).get();
						//producer.send(aMsg);
						producer.send(aMsg, new Callback() {
		                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
		                        //execute everytime a record is successfully sent or exception is thrown
		                        if(e == null){
		                           // No Exception
		                        }else{
		    						ovLogger.error("      exception at " + " " + aMsg.key() + ". Exit here!");
		    						ovLogger.error(e);
		    						//set the aMsg.key and exit. Question: will the loop stop when encounter this exception?
		    						tblMeta.setThisRefreshSeq(aMsg.key());
		                        }
		                    }
		                });
					}
					if(cnt==5000) {   //when the job is too big, this progress reporter could be helpful.
						ovLogger.info("      +5000" + ", " + xcnt + ", " + srcTbl);
						cnt=0; xcnt=0;
					}
				}
				//ovLogger.info("   last Journal Seq #: " + seq);
				ovLogger.info("   last Journal Seq #: " + tblSrc.getCurrSeq());
				ovLogger.info("   cnt of changes sent: " + chgCnt);
				//metrix.sendMX("JournalSeq,jobId="+jobID+",journal="+jLib+"."+jName+" value=" + seq + "\n");
				metrix.sendMX("JournalSeq,jobId="+jobID+",journal="+jLib+"."+jName+" value=" + tblSrc.getCurrSeq() + "\n");
				//producer.commitTransaction();
			} catch (SQLException e) {
				ovLogger.error("   failed to retrieve from DB2: " + e);
				success=true;   // ignore this one, and move on to the next one.
			} catch (Exception e1) {
				//producer.abortTransaction();
				ovLogger.error("   failed to write to kafka E: " + e1);
			}finally{
				producer.flush();
				//producer.close();
			}
	        /*catch (InterruptedException e) {
			}
				ovLogger.error("   failed to write to kafka I: " + e);
				success=false;
			} catch (ExecutionException e) {
				ovLogger.error("   failed to write to kafka E: " + e);
				success=false;
			}*/
		}
        tblMeta.markEndTime();
        //Timestamp ts = new Timestamp(System.currentTimeMillis());
        //tblMeta.setRefreshTS(ts);
        //tblMeta.setRefreshSeq(seq);
        if(success)
        	tblMeta.saveReplicateKafka();

		return success;
	}
	
	private void test() {
		Properties propx = new Properties();
	    KafkaProducer<Long, String> producerx;

	    String strVal = conf.getConf("kafkaMaxBlockMS");
		int kafkaMaxBlockMS = Integer.parseInt(strVal);
		String kafkaURL = conf.getConf("kafkaURL");
		//String kafkaURL="usir1xrvkfk04:9092";
		String kafkaACKS = conf.getConf("kafkaACKS");
        
	    propx.put("bootstrap.servers", kafkaURL);
	    //propx.put("acks", "all");
	    propx.put("acks", kafkaACKS);
	    propx.put("retries", 0);
	    propx.put("batch.size", 16384);
	    propx.put("linger.ms", 1);
	    propx.put("buffer.memory", 33554432);
	    propx.put("max.block.ms", kafkaMaxBlockMS );     //default 60000 ms
	    //props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	    propx.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
	    //props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
	    propx.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
	    propx.put(ProducerConfig.CLIENT_ID_CONFIG, "vSync400_"+poolID);

	    producerx = new KafkaProducer<Long, String>(propx);

		ProducerRecord<Long, String> aMsgx;
		aMsgx = new ProducerRecord<Long, String>("TEST", "1");
		try {
			RecordMetadata metadata = producerx.send(aMsgx).get();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

