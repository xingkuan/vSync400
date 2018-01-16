package com.guess.vsync;

import java.io.*;
import java.util.*;
import java.text.*;
import java.sql.*;
import oracle.jdbc.*;
import oracle.jdbc.pool.OracleDataSource;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;


/*
 class: OVSdb
 Loads all database connection information into array of OVScred from repository
*/

class OVSrepo {
   private  Connection repConn;
   private  Statement repStmt;
   private  String OVSdbTable = new String("vertsnap.sync_db");
   private  ResultSet rRset;
   private  OVScred dbCred[]  = new OVScred[11];

   private static final Logger ovLogger = LogManager.getLogger();

   public boolean init() {
      // initialize variables
      boolean rtv = true;
      int dbid;
      

     // loads the repository credentials into first element of the credential array
     //dbCred[0]  = new OVScred("vertsnap","BAtm0B1L#","jdbc:oracle:thin:@rhjsd:1523:jotpp",1,"Repository","vertsnap.mstr_cmd_queue");
      OVSconf conf = OVSconf.getInstance();
      String uID = conf.getConf("repDBuser");
      String uPW = conf.getConf("repDBpasswd");
      String url = conf.getConf("repDBurl");
      dbCred[0]  = new OVScred(uID, uPW, url, 1, "Repository", "vertsnap.mstr_cmd_queue");

      try {
          Class.forName("oracle.jdbc.OracleDriver"); 
      } catch(ClassNotFoundException e){
       	ovLogger.error("Driver error has occured");
         e.printStackTrace();
	    rtv=false;
           return rtv;
      }

      try {
         //establish Oracle connection
         repConn = DriverManager.getConnection(dbCred[0].getURL(), dbCred[0].getUser(), dbCred[0].getPWD());
         repConn.setAutoCommit(false);
         repStmt = repConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
         rRset = repStmt.executeQuery("select DB_ID, DB_USR, DB_PWD, DB_CONN, DB_TYPE,DB_DESC, DB_CMD_QUEUE "
        		 			+ "from " + OVSdbTable);   
         
         // load all the credential information for all the databases listed in the repository into the credential array
         while (rRset.next()) {
            dbid=rRset.getInt("DB_ID");      
            dbCred[dbid]= new OVScred(rRset.getString("DB_USR"),rRset.getString("DB_PWD"),rRset.getString("DB_CONN"),
            		rRset.getInt("DB_TYPE"),rRset.getString("DB_DESC"),rRset.getString("DB_CMD_QUEUE"));
		 } 
         rRset.close();
      } catch(SQLException e) {
    	  ovLogger.error(e.getMessage());
         rtv=false;
      }

      return rtv;
   }
   public void setCred(OVScred ovsc) {
      // sets repository database credentials
      dbCred[0]=ovsc;
   }
   public OVScred getCred(int dbid) {
      // returns the OVScred of the specified array pointer
      return dbCred[dbid];
   }
   
//.   public void putAuditRequest(int tableID){
//.      putMstrRequest(tableID, 4, 2);
//.   } 
//.   
//.   private void putMstrRequest(int objID, int request, int cmd_type) {
//.      // creates an audit request entry into table's cmd queue
//.      Connection lrepConn;
//.      Statement lrepStmt;
//.      try {
//.         lrepConn = DriverManager.getConnection(dbCred[0].getURL(), dbCred[0].getUser(), dbCred[0].getPWD());
//.         //lrepConn.setAutoCommit(false);
//.         lrepStmt = lrepConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
//.         lrepStmt.executeUpdate(" insert into " + dbCred[0].getCmdQueue()  + " (object_id, request, cmd_type) values (" 
//.        		 + objID + "," + request + "," + cmd_type + ")" );
//.         lrepConn.commit();
//.         lrepStmt.close();
//.         lrepConn.close();
//.      } catch(SQLException e) {
//.         //ovLogger.log(label + e.getMessage());
 //.     }
 //.  }
   
   /*
    * 07/24: return list of tbls belongs to a pool
    */
   public List<Integer> getTblsByPoolID(int poolID) {
	   List<Integer> tList = new ArrayList<Integer>();
	   String strSQL;
       OVScred repCred = dbCred[0];
       
	   if(poolID < 0)
		   strSQL = "select TABLE_ID,CURR_STATE from sync_table order by t_order";
	   else
	      strSQL = "select TABLE_ID,CURR_STATE from sync_table where pool_id = " + poolID + " order by t_order";
	      
       // This shortterm solution is only for Oracle databases (as the source)
	   try {
           Class.forName("oracle.jdbc.OracleDriver"); 
           repConn = DriverManager.getConnection(repCred.getURL(), repCred.getUser(), repCred.getPWD());
           repConn.setAutoCommit(false);
           repStmt = repConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
           rRset = repStmt.executeQuery(strSQL);
           while(rRset.next()){
               //Retrieve by column name
               int id  = rRset.getInt("TABLE_ID");
               tList.add(id);
            }
        } catch(SQLException se){
           ovLogger.error("OJDBC driver error has occured" + se);
        }catch(Exception e){
            //Handle errors for Class.forName
        	ovLogger.error(e);
         }finally {
        	// make sure the resources are closed:
        	 try{
        		if(repStmt !=null)
        			repStmt.close();
        	 }catch(SQLException se2){
        	 }
             try{
                 if(repConn!=null)
                    repConn.close();
             }catch(SQLException se){
             }
         }
	   
	   return tList;
   }  
   public List<Integer> getTableIDsAll() {
	   return getTblsByPoolID(-1);
   }
 }    