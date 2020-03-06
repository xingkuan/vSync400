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

        return ;
     }
    
    private void setPoolId(int pID) {
    	poolID=pID;
    }
	private void setupKafka(){
        String kafkaURL = conf.getConf("kafkaURL");
        
	    props.put("bootstrap.servers", kafkaURL);
	    props.put("acks", "all");
	    props.put("retries", 0);
	    props.put("batch.size", 16384);
	    props.put("linger.ms", 1);
	    props.put("buffer.memory", 33554432);
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
				replicate(srcDBid, jLib, jFile);
				ovLogger.info(" finished: " + jName);

			 }else {
  			    ovLogger.info(" invalide name: dbID: " + jName);
			 }
		}
	}

	
	private void replicate(int dbID, String jLib, String jName){
	    ProducerRecord<Long, String> aMsg;
	    //ProducerRecord<String, String> aMsg;
	   
	    List<String> tblList;
	    
		//JDBC result of "Display_jounral(), and iterate through it:
		OVSsrc tblSrc = new OVSsrc();
		//The DB2 journal
		OVSmetaJournal400 tblMeta;   
	    tblMeta = new OVSmetaJournal400();
	    tblMeta.setDbMeta(dbMeta);
	    tblMeta.initForKafka(dbID, jLib, jName);
	    tblMeta.markStartTime();
	    
	    tblSrc.setMeta400(tblMeta);
		tblSrc.initForKafka();

	    tblSrc.initSrcLogQuery400();
	    tblList = dbMeta.getDB2TablesOfJournal(dbID, jLib+"."+jName);
        
    	//the message to be formated as: SEQ#:RRN:TS

        int rrn=0;
        long seq=0l;
        String srcTbl="";
        ResultSet srcRset = tblSrc.getSrcResultSet();
        try {
			while (srcRset.next()) {
				rrn=srcRset.getInt("RRN");
				seq=srcRset.getLong("SEQNBR");
				srcTbl=srcRset.getString("SRCTBL");
				// ignore those for unregister tables:
				if (tblList.contains(srcTbl)) {
					aMsg = new ProducerRecord<Long, String>(srcTbl, seq, String.valueOf(rrn));
					RecordMetadata metadata = producer.send(aMsg).get();
				}
			}
			ovLogger.info("   last Journal Seq #: " + seq);
			metrix.sendMX("JournalSeq,jobId="+jobID+",journal="+srcTbl+" value=" + seq + "\n");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    
        tblMeta.markEndTime();
        Timestamp ts = new Timestamp(System.currentTimeMillis());
        tblMeta.setRefreshTS(ts);
        tblMeta.setRefreshSeq(seq);
		tblMeta.saveReplicateKafka();
	}
}
