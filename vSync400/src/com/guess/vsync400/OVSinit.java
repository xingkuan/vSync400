package com.guess.vsync400;

import java.sql.Timestamp;

class OVSinit
{
   private static OVStableProcessor tProc = new OVStableProcessor();
   private static OVSrepo dbMeta = new OVSrepo();
   private static OVSmeta tblMeta;
   
   
   private static OVSmetaJournal400 db2KafkaMeta = new OVSmetaJournal400();
   
   public static void main (String args[]) {   
	  String jobID="init";
	  
      if (args.length != 1) { 
         System.out.println("Usage:   OVSinit <int TableID>  ");
         System.out.println("example:   OVSinit 2 ");
         return ;
      } 
     
      int tblID = Integer.parseInt(args[0]);

      dbMeta.init();

      tblMeta = new OVSmeta();
      tblMeta.setDbMeta(dbMeta);
      tblMeta.setTableID(tblID);
      tblMeta.init(jobID);
      
      String srcLog=tblMeta.getLogTable();
      String[] tokens = srcLog.split("[.]");
	  db2KafkaMeta.setDbMeta(dbMeta);
	  db2KafkaMeta.initForKafka(tblMeta.getSrcDBid(), tokens[0], tokens[1]);
	   
      
      tProc.setDbMeta(dbMeta);
      tProc.setOVSmeta(tblMeta);
      tProc.setDB2KafkaMeta(db2KafkaMeta);
      tProc.procInitDefault(tblID, jobID) ;
      
      return ;
   }

}