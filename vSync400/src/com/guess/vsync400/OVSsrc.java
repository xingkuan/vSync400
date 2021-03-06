package com.guess.vsync400;

import java.io.*;
import java.util.*;
import java.text.*;
import java.sql.*;
import oracle.jdbc.*;
import oracle.jdbc.pool.OracleDataSource;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

/*
 class OVSsrc
 
 OVSsrc handles all db interaction with the source database/table
   -  establishes the db connection
   -  turns log trigger on/off
   -  commits/rolls back  transactions on the log table
   -  initializes the data source query 
   -  initializes the data source recordset
   -  exposes the data source recordset object (to be passed to the OVStgt class for data load)
   -  wraps all OVSsrc and OVStgt functions and exposes them as necessary
  
*/

class OVSsrc {
   private OVScred srcCred;
   private OVSmeta tblMeta;
   private OVSmetaJournal400 jMeta400;
   private Connection srcConn;
   private Statement srcStmt;
   private boolean srcConnOpen;
   private boolean srcStmtOpen;
   private ResultSet sRset;
   private int tableID;
   private boolean recover=false;
   private int currState=0;
   private boolean isError=false;
   private int fldCnt;
   private int[] xformType;
   private String[] xformFctn;
   private int logCnt;
   private String label;

   
   private int connAtmptLim=5;
   private int AtmptDelay=5000;
   
   private String jLibName ;
   private String jName ;

   private long seqThisFresh=0;
   private java.sql.Timestamp tsThisRefesh=null;

   
   private static final Logger ovLogger = LogManager.getLogger();
   
   public boolean init() {
      label=">";
      return linit();
   }
   public boolean init(String lbl) {
      label=lbl;
      
	  String srcLog = tblMeta.getLogTable();
	  String[] res = srcLog.split("[.]", 0);
	  //String jLibName = "JOHNLEE2";
	  //String jName = "QSQJRN";
	  jLibName = res[0];
	  jName = res[1];

      
      return linit();
   }

   private boolean linit() {
      int attempts;
      //  initializes the connection
      
      // initialize variables
      isError=false;
      currState=0;
	  boolean rtv = true;
     srcConnOpen=false;
     srcStmtOpen=false;
     

      //test for db type oracle and if it is load oracle driver
      srcCred=tblMeta.getSrcCred();
      if (srcCred.getType() ==1) {
         try {
            Class.forName("oracle.jdbc.OracleDriver"); 
         } catch(ClassNotFoundException e){
            ovLogger.error(label + " Driver error has occured");
            e.printStackTrace();
	         rtv = false;
            return rtv;
         }
      } else if (srcCred.getType() == 3) {
          try {
        	  Class.forName("com.ibm.as400.access.AS400JDBCDriver");  
           } catch(ClassNotFoundException e){
              ovLogger.error(label + " Driver error has occured");
              e.printStackTrace();
  	         rtv = false;
              return rtv;
           }
    	  
      }else {
         ovLogger.error(label + " source db type not supported");
         rtv=false;
         return rtv;
      }
      
      attempts=0;
      while (attempts<connAtmptLim ) {
         attempts++;
         
      try {
         ovLogger.info(label + " conn attempt " + attempts);
         // this attempts a reset from a prior exception
         close();
         //establish DB connection
         srcConn = DriverManager.getConnection(srcCred.getURL(), srcCred.getUser(), srcCred.getPWD());
         srcConnOpen=true;
         srcConn.setAutoCommit(false);
         srcStmt = srcConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
         srcStmtOpen=true;
         // all success, burn rest of attempts
         attempts=connAtmptLim;
      } catch(SQLException e) {
         //System.out.println(label + " tgt cannot connect to db");
         //System.out.println(label + e.getMessage());
//.         ovLogger.log(label + " src cannot connect to db - init failed ");
         ovLogger.error(label + " src cannot connect to db - init failed ");
//.         ovLogger.log(label + e.getMessage());
         ovLogger.error(label + e.getMessage());
         rtv=false;
         msWait(AtmptDelay);
      }
      
      }

      return rtv;
   }
   //used only by DB2toKafka,
   public boolean initForKafkaMeta() {
	   boolean proceed=false;
	   label=jMeta400.getLabel();
	      
	   jLibName = jMeta400.getJournalLib();
	   jName = jMeta400.getJournalName();

	   setThisRefreshHostTS();
	   setThisRefreshSeq();  
	   if( jMeta400.getSeqLastRefresh()==0 ){ //this means the Journal is to be first replicated. INIT run!
	       if( (seqThisFresh==0) )  // .. display_journal did not return, perhaps the journal is archived.
	    	   setThisRefreshSeqInitExt();          // try the one with *CURCHAIN
	       
	       jMeta400.setThisRefreshSeq(seqThisFresh);            //set last to the current seqNum
	       jMeta400.setSeqLastRefresh(seqThisFresh);           //and this too
	   }
	   if( seqThisFresh > jMeta400.getSeqLastRefresh())
		   proceed=true;
	  //return linit400();
	   
	   return proceed;
   }
   public void setLabel400(String lbl) {  //TODO: too dirty code
	   label=lbl;
   }
   public boolean linit400() {
	      int attempts;
	      //  initializes the connection
	      
	      // initialize variables
	      isError=false;
	      currState=0;
		  boolean rtv = true;
	     srcConnOpen=false;
	     srcStmtOpen=false;
	     

	      //test for db type oracle and if it is load oracle driver
	      srcCred=jMeta400.getSrcCred();
	      if (srcCred.getType() ==1) {
	         try {
	            Class.forName("oracle.jdbc.OracleDriver"); 
	         } catch(ClassNotFoundException e){
	            ovLogger.error(label + " Driver error has occured");
	            e.printStackTrace();
		         rtv = false;
	            return rtv;
	         }
	      } else if (srcCred.getType() == 3) {
	          try {
	        	  Class.forName("com.ibm.as400.access.AS400JDBCDriver");  
	           } catch(ClassNotFoundException e){
	              ovLogger.error(label + " Driver error has occured");
	              e.printStackTrace();
	  	         rtv = false;
	              return rtv;
	           }
	    	  
	      }else {
	         ovLogger.error(label + " source db type not supported");
	         rtv=false;
	         return rtv;
	      }
	      
	      attempts=0;
	      while (attempts<connAtmptLim ) {
	         attempts++;
	         
	      try {
	         //ovLogger.info(label + " conn attempt " + attempts);
	         // this attempts a reset from a prior exception
	         close();
	         //establish DB connection
	         srcConn = DriverManager.getConnection(srcCred.getURL(), srcCred.getUser(), srcCred.getPWD());
	         srcConnOpen=true;
	         srcConn.setAutoCommit(false);
	         srcStmt = srcConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
	         srcStmtOpen=true;
	         // all success, burn rest of attempts
	         attempts=connAtmptLim;
	      } catch(SQLException e) {
	         ovLogger.error(label + " src cannot connect to db - init failed ");
	         ovLogger.error(label + e.getMessage());
	         rtv=false;
	         msWait(AtmptDelay);
	      }
	      
	      }

	      return rtv;
	   }
/*   public void markThisRun(){
	   setThisRefreshHostTS();
	   setThisRefreshSeq();  
   }
   */
/*   public boolean initSrcQueryOfRRNList(String rrns) {
	      boolean rtv=true;
	      String sqlStmt = tblMeta.getSQLSelect() + " where rrn(a) in (" + rrns + ")";
	      //String sqlStmt = "select * from johnlee2.testtbl2";
	      try {
	    	 srcStmt = srcConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
	         sRset=srcStmt.executeQuery(sqlStmt);
	      } catch(SQLException e) {
	         ovLogger.error(label + " recordset not created");
	         ovLogger.error(e);
	         rtv=false;
	      }
	      
	      return rtv;
   }
   */
   //public boolean initSrcQuery(boolean isInit){
   public boolean initSrcQuery(String rrns){
	   String whereClause;

	   if(rrns.equals("")) {      //empty where clause is used only for initializing a table
		   setThisRefreshHostTS();
		   setThisRefreshSeq();  
		   whereClause = "";
	   }else{    //otherwise, will be a list of RRN like "1,2,3"
		   whereClause = " where rrn(a) in (" + rrns + ")";
	   }
	   
      boolean rtv=true;
      String sqlStmt = tblMeta.getSQLSelect() + " " + whereClause;
      //String sqlStmt = "select * from johnlee2.testtbl2";
      try {
    	 //2020.04.23:
    	 //It is open already! srcStmt = srcConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
         sRset=srcStmt.executeQuery(sqlStmt);
      } catch(SQLException e) {
         ovLogger.error(label + " recordset not created");
         ovLogger.error(e);
         ovLogger.error(label + " \n\n\n" + tblMeta.getSQLSelect() + " " + whereClause + "\n\n\n");
         rtv=false;
      }
      
      return rtv;
   }
/*   
   //now, this is only used by DB2toKafka
   public boolean initSrcLogQuery() {
	   markThisRun();
	   
      // initializes the source log query
      boolean rtv=true;
      
      String strLastSeq;
      String strReceiver;
      if (tblMeta.getSeqLastRefresh() == 0) {
    	  strLastSeq = "null";
    	  strReceiver="";
      }else {
    	  strLastSeq =  Long.toString(tblMeta.getSeqLastRefresh());
    	  strReceiver="*CURCHAIN";
      }
      
      try {
   	 //  String strTS = new SimpleDateFormat("yyyy-MM-dd-HH.mm.ss.SSSSSS").format(tblMeta.getLastRefresh());
    	  srcStmt = srcConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    	  String StrSQLRRN =  " select COUNT_OR_RRN as RRN,  SEQUENCE_NUMBER AS SEQNBR"
    	              		+ " FROM table (Display_Journal('" + jLibName + "', '" + jName + "', "
    	              		+ "   '', '" + strReceiver + "', "
    	              		//+ "   cast('" + strTS +"' as TIMESTAMP), "    //pass-in the start timestamp;
    	              		+ "   cast(null as TIMESTAMP), "    //pass-in the start timestamp;
    	              		+ "   cast(" + strLastSeq + " as decimal(21,0)), "    //starting SEQ #
    	              		+ "   'R', "   //JOURNAL CODE: record operation
    	              		+ "   '',"    //JOURNAL entry: UP,DL,PT,PX,UR,DR,UB
    	              		+ "   '" + tblMeta.getSrcSchema() + "', '" + tblMeta.getSrcTable() + "', '*QDDS', '',"  //Object library, Object name, Object type, Object member
    	              		+ "   '', '', ''"   //User, Job, Program
    	              		+ ") ) as x where SEQUENCE_NUMBER >=" + strLastSeq + " and SEQUENCE_NUMBER <=" + Long.toString(seqThisFresh) + " order by 2 asc"   // something weird with DB2 function: the starting SEQ number seems not takining effect
    	              		;
    	              //		"  where ( ROWID ) in ( select distinct M_ROW " 
    	              //		+ " from "  +  tblMeta.getSrcSchema() + "." + tblMeta.getLogTable() 
    	              //		+ " where  snaptime = '01-JUN-1910'  )";    
         sRset=srcStmt.executeQuery(StrSQLRRN);
      } catch(SQLException e) {
         ovLogger.error("initSrcLogQuery() failure: " + e);
         rtv=false;
      }
      return rtv;
   }
   */
   public boolean initSrcLogQuery400(List<String> tblList) {  
   //now, this is only used by DB2toKafka   
	  linit400();  
	   
      // initializes the source log query
      boolean rtv=true;
      
      String strLastSeq;
      String strReceiver;

	  String whereClauseT = " (";
	  for (int i = 0; i < tblList.size(); i++) {
		  if (i==0)
			  whereClauseT = whereClauseT + "'" + tblList.get(i) + "'";
		  else
			  whereClauseT = whereClauseT + ", '" + tblList.get(i) + "'";
      }
	  whereClauseT = whereClauseT + ") ";
	  
      if (jMeta400.getSeqLastRefresh() == 0) {
          ovLogger.error("initSrcLogQuery(): " + jLibName + "." + jName + " is not initialized.");
    	  rtv=false; //then initilize it     	  setThisRefreshSeqInitExt();
      }else {
          ovLogger.info("initSrcLogQuery(): " + jLibName + "." + jName + " last Seq: " + jMeta400.getSeqLastRefresh());
    	  strLastSeq =  Long.toString(jMeta400.getSeqLastRefresh());
    	  strReceiver="*CURCHAIN";
      
	      try {
	   	 //  String strTS = new SimpleDateFormat("yyyy-MM-dd-HH.mm.ss.SSSSSS").format(tblMeta.getLastRefresh());
	    	  srcStmt = srcConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
	    	  String StrSQLRRN =  " select COUNT_OR_RRN as RRN,  SEQUENCE_NUMBER AS SEQNBR, trim(both from SUBSTR(OBJECT,11,10))||'.'||trim(both from SUBSTR(OBJECT,21,10)) as SRCTBL"
	    	              		+ " FROM table (Display_Journal('" + jLibName + "', '" + jName + "', "
	    	              		+ "   '', '" + strReceiver + "', "
	    	              		//+ "   cast('" + strTS +"' as TIMESTAMP), "    //pass-in the start timestamp;
	    	              		+ "   cast(null as TIMESTAMP), "    //pass-in the start timestamp;
	    	              		+ "   cast(" + strLastSeq + " as decimal(21,0)), "    //starting SEQ #
	    	              		+ "   'R', "   //JOURNAL CODE: record operation
	    	              		+ "   '',"    //JOURNAL entry: UP,DL,PT,PX,UR,DR,UB
	    	              		+ "   '', '', '*QDDS', '',"  //Object library, Object name, Object type, Object member
	    	              		+ "   '', '', ''"   //User, Job, Program
	    	              		+ ") ) as x where SEQUENCE_NUMBER > " + strLastSeq + " and SEQUENCE_NUMBER <=" + seqThisFresh 
	    	              		+ " and trim(both from SUBSTR(OBJECT,11,10))||'.'||trim(both from SUBSTR(OBJECT,21,10)) in " +  whereClauseT 
	    	              		+ " order by 2 asc"   // something weird with DB2 function: the starting SEQ number seems not takining effect
	    	              		;
	    	 // ovLogger.info(StrSQLRRN);
	         sRset=srcStmt.executeQuery(StrSQLRRN);
	         if(sRset.isBeforeFirst())     // this check can throw exception, and do the needed below.
	        	 ovLogger.info("   opened src jrnl recordset: " + label);
	      } catch(SQLException e) {
	         ovLogger.error("initSrcLogQuery() failure: " + e);
	         //2020.04.12:
	         //looks like it is possible that a Journal 0f the last entry can be deleted by this time,--which mayhappen if that journal was never used -- which will result in error.
	         //  one way is to NOT use -- cast(" + strLastSeq + " as decimal(21,0)), . 
	         //The code do it here in the hope of doing good thing. But the user should be the one to see if that is appropreate.
	         ovLogger.warn("Posssible data loss! needed journal " + jLibName + "." + jName + " must have been deleted.");
	         ovLogger.warn("  try differently of " + jLibName + "." + jName + ":");
		      try {
		 	    	  //srcStmt = srcConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		 	    	  String StrSQLx =  " select COUNT_OR_RRN as RRN,  SEQUENCE_NUMBER AS SEQNBR, trim(both from SUBSTR(OBJECT,11,10))||'.'||trim(both from SUBSTR(OBJECT,21,10)) as SRCTBL"
		 	    	              		+ " FROM table (Display_Journal('" + jLibName + "', '" + jName + "', "
		 	    	              		+ "   '', '" + strReceiver + "', "
		 	    	              		+ "   cast(null as TIMESTAMP), " 
		 	    	              		+ "   cast(null as decimal(21,0)), "   
		 	    	              		+ "   'R', "   
		 	    	              		+ "   '',"    
		 	    	              		+ "   '', '', '*QDDS', '',"  
		 	    	              		+ "   '', '', ''"   
		 	    	              		+ ") ) as x where SEQUENCE_NUMBER > " + strLastSeq + " and SEQUENCE_NUMBER <=" + seqThisFresh + " order by 2 asc"   // something weird with DB2 function: the starting SEQ number seems not takining effect
		 	    	              		;
		 	         sRset=srcStmt.executeQuery(StrSQLx);
		 	         ovLogger.info("   opened src jrnl recordset on ultimate try: " + label);
		 	      } catch(SQLException ex) {
			 	     ovLogger.error("  ultimate failure: " + jLibName + "." + jName + " !");
		 	         ovLogger.error("  initSrcLogQuery() failure: " + ex);
		 	         
		 	         rtv=false;
		 	      }
		       }
	         //rtv=false;
	      }
      //}
      return rtv;
   }
   
   public int getThreshLogCount() {
      // counts and returns the number of records in the source log table
      
      int lc=0;
      try {
         // set snaptime, count the number of records in the log table, then create recordset
         
         sRset  = srcStmt.executeQuery( " select count(distinct M_ROW)   from   "  + tblMeta.getSrcSchema() + "." +  tblMeta.getLogTable() );
         sRset.next();
         lc = Integer.parseInt(sRset.getString(1));
     //TODO:    sRset.close();
      } catch(SQLException e) {
         //System.out.println(label + " error during threshlogcnt");
//.         ovLogger.log(label + " error during threshlogcnt");
         ovLogger.error(label + " error during threshlogcnt");
      }
      //System.out.println(label + " theshold log count: " + lc);
//.      ovLogger.log(label + " theshold log count: " + lc);
      ovLogger.info(label + " theshold log count: " + lc);
      return lc;
   }

   
   public int getRecordCount(){
      // counts and returns the number of records in the source table
      
      int rtv;
    //  Connection lConn;
    //  Statement lStmt;
      ResultSet lrRset;
      int i;

      rtv=0;
      try {
     //should use the srcConn and srcStmt
     //	  lConn = DriverManager.getConnection(srcCred.getURL(), srcCred.getUser(), srcCred.getPWD());
     //   lStmt = lConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    	  srcStmt = srcConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

     //   lrRset=lStmt.executeQuery("select count(*) from " + tblMeta.getSrcSchema() + "." + tblMeta.getSrcTable());
         lrRset=srcStmt.executeQuery("select count(*) from " + tblMeta.getSrcSchema() + "." + tblMeta.getSrcTable());
         if (lrRset.next()) {
            rtv = Integer.parseInt(lrRset.getString(1));  
         }
         lrRset.close();
     //    lStmt.close();
     //    lConn.close();
      } catch(SQLException e) {
         //System.out.println(label + " error during src audit"); 
//.         ovLogger.log(label + " error during src audit"); 
         ovLogger.error(label + " error during src audit: "+ e); 
      }
      return rtv;
   }
   public void setTriggerOn() throws SQLException {
      srcStmt.executeUpdate("alter trigger "  + tblMeta.getSrcTrigger() + " enable");    
      //System.out.println("========>>> trigger turned on");      
   }
   public int getLogCnt() {
      return logCnt;
   }
  
   //To be safe, let's get the TS from journal, (instead of "CURRENT TIMESTAM" (later maybe the corresponding highest SEQ # as well).   
   public java.sql.Timestamp getThisRefreshHostTS(){
	   return tsThisRefesh;
   }
   
   private void setThisRefreshHostTS(){
	   //no more need to get it from the host
	   Timestamp ts = new Timestamp(System.currentTimeMillis());
	   /*
	      int rtv;
	      ResultSet lrRset;
	      java.sql.Timestamp hostTS = null;
	      
	      try {
	    	 srcStmt = srcConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
	    	 
	    	 String strSQL = "SELECT CURRENT TIMESTAMP FROM SYSIBM.SYSDUMMY1";
	    	 lrRset=srcStmt.executeQuery(strSQL);
	         
	         if (lrRset.next()) {
	            hostTS = lrRset.getTimestamp(1);  
	      }
	         lrRset.close();
	         srcStmt.close();
	      } catch(SQLException e) {
	         ovLogger.error(label + " error during src audit: "+ e); 
	      }

	      tsThisRefesh = hostTS;
*/
	   tsThisRefesh = ts;
	   if(jMeta400!=null)   //that means it is from DB2toKafka
		   jMeta400.setRefreshTS(ts);
	   if(tblMeta!=null)   //when called from OVSsysnc, OVSinit
		   tblMeta.setRefreshTS(ts);   
   }
   public long getThisRefreshSeq(){
	   return seqThisFresh;
   }
   private void setThisRefreshSeq(){
	      ResultSet lrRset;
	      
	      String strSQL;
   
	      try {
	    	 srcStmt = srcConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
	    	 //locate the ending SEQUENCE_NUMBER of this run:
	    	 strSQL = " select max(SEQUENCE_NUMBER) "
	            		+ " FROM table (Display_Journal('" + jLibName + "', '" + jName + "', "
	            		+ "   '', '', "   //it looks like possible the journal can be switched and this SQL return no row.
	            		+ "   cast(null as TIMESTAMP), "    //pass-in the start timestamp;
	            		+ "   cast(null as decimal(21,0)), "    //starting SEQ #
	            		+ "   'R', "   //JOURNAL cat: record operations
	            		+ "   '',"    //JOURNAL entry: UP,DL,PT,PX,UR,DR,UB 
	            + "   '', '', '*QDDS', '',"  
	      		+ "   '', '', ''"   //User, Job, Program
	      		+ ") ) as x "
	      		;
	    	 	lrRset=srcStmt.executeQuery(strSQL);
	            //I guess it could be 0 when DB2 just switched log file.
	    	 	if (lrRset.next()) {
	    	 		seqThisFresh = lrRset.getLong(1);  
	    	 		if(jMeta400!=null)    //only if called from DB2toKafaka
	    	 			jMeta400.setThisRefreshSeq(seqThisFresh);   
	    	 		if(tblMeta!=null)     //from init, sync
	    	 			tblMeta.setRefreshSeq(seqThisFresh);   
	    	 	}
	    	 	lrRset.close();

	         srcStmt.close();
	      } catch(SQLException e) {
	         ovLogger.error(label + " error in setThisRefreshSeq(): "+ e); 
	      }
   }
   private void setThisRefreshSeqInitExt(){
	      ResultSet lrRset;
	      
	      String strSQL;

	      try {
	    	 srcStmt = srcConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
	    	 //locate the ending SEQUENCE_NUMBER of this run:
	    	 strSQL = " select max(SEQUENCE_NUMBER) "
	            		+ " FROM table (Display_Journal('" + jLibName + "', '" + jName + "', "
	            		+ "   '', '*CURCHAIN', "   //it looks like possible the journal can be switched and this SQL return no row.
	            		+ "   cast(null as TIMESTAMP), "    //pass-in the start timestamp;
	            		+ "   cast(null as decimal(21,0)), "    //starting SEQ #
	            		+ "   'R', "   //JOURNAL cat: record operations
	            		+ "   '',"    //JOURNAL entry: UP,DL,PT,PX,UR,DR,UB 
	            + "   '', '', '*QDDS', '',"  
	      		+ "   '', '', ''"   //User, Job, Program
	      		+ ") ) as x "
	      		;
	    	 	lrRset=srcStmt.executeQuery(strSQL);
	            //I guess it could be 0 when DB2 just switched log file.
	    	 	if (lrRset.next()) {
	    	 		seqThisFresh = lrRset.getLong(1);  
	    	 		if(jMeta400!=null)    //only if called from DB2toKafaka
	    	 			jMeta400.setThisRefreshSeq(seqThisFresh);   
	    	 		if(tblMeta!=null)     //from init, sync
	    	 			tblMeta.setRefreshSeq(seqThisFresh);   
	    	 	}
	    	 	lrRset.close();

	         srcStmt.close();
	      } catch(SQLException e) {
	         ovLogger.error(label + " error in setThisRefreshSeq(): "+ e); 
	      }
}
   public long getCurrSeq(){
	      return seqThisFresh;
   }
   
   
//   public void delConsumedLog() throws SQLException {
//      srcStmt.executeUpdate(" DELETE FROM " +  tblMeta.getSrcSchema() + "." +  tblMeta.getLogTable() +  " where  snaptime = '01-JUN-1910' "); 
//   }
   public ResultSet getSrcResultSet() {
      return sRset;
   }
   public void closeSrcResultSet() throws SQLException {
       sRset.close();
   }
   public void OVSsrc(OVScred ovsc) {
      srcCred=ovsc;
   }
   public void setRecover(boolean rcvr) {
      recover=rcvr;
   }
   public void setCred(OVScred ovsc) {
      srcCred=ovsc;
   }
   public void setMeta(OVSmeta mta) {
      tblMeta=mta;
   }
   public void setMeta400(OVSmetaJournal400 mta) {
	   jMeta400=mta;
	   }
   public void commit() throws SQLException {
      srcConn.commit();
   }
   public void rollback() throws SQLException {
      srcConn.rollback();
   }
   public void close() throws SQLException {
      if (srcStmtOpen) {
         srcStmt.close();
         srcStmtOpen=false;
      }
      if (srcConnOpen) {
         srcConn.close();
         srcConnOpen=false;
         ovLogger.info(label + " closed src db src");
      }
   }
   private  void msWait(int mSecs) {
      try {
         Thread.sleep(mSecs);
      } catch (InterruptedException e) {
      }
   }
}