package com.guess.vsync400;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ibm.as400.access.*;

import java.io.FileWriter;
import java.io.IOException;
import java.io.File;

/* 2019.11.15 John Lee:
 * This code only responsible for generate the DDL into text files.
 * The operator is supposedly to review it and then submit it to REPO db and target DB 
 * accordingly
 */
public class RegisterTbl400 {
   private static final Logger ovLogger = LogManager.getLogger();

   private static String outPath;
   private static int tblID;
   
   static FileWriter verticaDDL ;
   static FileWriter repoInsTbl ;
   static FileWriter repoInsCols;

   
   private OVScred srcCred;

   private Connection srcConn;
   private Statement srcStmt;
   private ResultSet sRset;
   private int tableID;

   private static OVSrepo dbMeta = new OVSrepo();
   
   private int genDDL(int srcDBid, String srcSch, String srcTbl, String journal, int tgtDBid, String tgtSch, String tgtTbl, int poolID, int refType) throws IOException {
      // get the src db cred
      srcCred=dbMeta.getCred(srcDBid);
      
      // connect to src DB
      if (srcCred.getType() != 3) {  // This code is for DB2/AS400 only, so far!
    	  return 1;
      }
      
      //register driver
      try {
         Class.forName("com.ibm.as400.access.AS400JDBCDriver"); 
      } catch(ClassNotFoundException e){
         ovLogger.error("Driver error has occured");
         e.printStackTrace();
         return 2;
      }
       
      try {
          //ovLogger.info("conn attempt ");
          srcConn = DriverManager.getConnection(srcCred.getURL(), srcCred.getUser(), srcCred.getPWD());
          srcConn.setAutoCommit(false);
          srcStmt = srcConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
          ////
          String sqlStmt = "select " + 
          		"       c.ordinal_position," + 
          		"       c.column_name," + 
          		"       k.ordinal_position as key_column," + 
          		"       k.asc_or_desc      as key_order," + 
          		"       c.data_type, c.length, c.numeric_scale, c.is_nullable, c.column_text" + 
          		"  from qsys2.syscolumns   c" + 
          		"  join qsys2.systables    t" + 
          		"    on c.table_schema = t.table_schema" + 
          		"   and c.table_name   = t.table_name" + 
          		"  left outer join sysibm.sqlstatistics k" + 
          		"    on c.table_schema = k.table_schem" + 
          		"   and c.table_name   = k.table_name" + 
          		"   and c.table_name   = k.index_name " + 
          		"   and c.column_name  = k.column_name" + 
          		" where c.table_schema = '" + srcSch + "' "  +
          		"   and c.table_name   = '" + srcTbl + "' " +
          		" order by ordinal_position asc"
          ;		
          sRset=srcStmt.executeQuery(sqlStmt );

          
          //For sync_table
          String sqlRepoDML1 = "insert into VERTSNAP.SYNC_TABLE\r\n" + 
            		"  (SOURCE_SCHEMA, TARGET_SCHEMA, SOURCE_TABLE, TARGET_TABLE, CURR_STATE, TABLE_WEIGHT, TABLE_ID,\r\n" + 
            		"   SOURCE_TRIGGER_NAME, T_ORDER, ORA_DELIMITER, EXP_TYPE, EXP_TIMEOUT, VERT_DELIMITER, PARTITIONED,\r\n" + 
            		"   SOURCE_LOG_TABLE, TARGET_PK, ERR_CNT, ERR_LIM, source_db_id, target_db_id, pool_id, refresh_type)\r\n" + 
            		"values \r\n" +
            		"  ('" + srcSch +"', '" + tgtSch + "', '" + srcTbl + "', '" + tgtTbl + "', 0, 1, " + tblID + ", \r\n" +
            		"  '',  '', '|', 1, 500, '|', 'N', " +    // no SOURCE_TRIGGER_NAME
            		"    '" + journal + "', 'DB2RRN', 0, 5, " + srcDBid + ", " + tgtDBid + ", " + poolID + ", " + refType + ")\r\n;";           //SOURCE_LOG_TABLE contains JOURNAL info	
          System.out.println(sqlRepoDML1);
          repoInsTbl.write(sqlRepoDML1);
          
          // for sync_table_field:
          String sqlRepoDMLTemp = "insert into SYNC_TABLE_FIELD "
            		+ "(FIELD_ID, TABLE_ID, SOURCE_FIELD, TARGET_FIELD, XFORM_FCTN, XFORM_TYPE ) "
            		+ "values ("
            		+ "";
          String sqlRepoDMLfield = "";
          // prepare target DDL(Vertica):
          String sqlDDLv = "create table " + tgtSch + "." + tgtTbl + "\n ( ";
          String strDataSpec;
          int scal;
          String sDataType, tDataType;
          String xForm=""; int xType;
          int fieldCnt = 0;
          while (sRset.next() ) {
        	  sqlRepoDMLfield = "";
        	  fieldCnt++;
        	  
        	  sDataType = sRset.getString("data_type");
        	  
        	  if (sDataType.equals("VARCHAR")) {
        		  strDataSpec = "VARCHAR2(" + sRset.getInt("length") + ")"; 

        		  xType = 1;
        		  xForm ="nvl(" + sRset.getString("column_name") + ", NULL)" ;
        	 } else if (sDataType.equals("TIMESTMP")) {
        		  strDataSpec = "TIMESTAMP";
        		  xType = 6;
        		  xForm ="nvl(to_char(" + sRset.getString("column_name") + ",''dd-mon-yyyy hh24:mi:ss''), NULL)" ;
        	  } else if (sDataType.equals("NUMERIC")) {
	        	  scal = sRset.getInt("numeric_scale");
	        	  if (scal > 0) {
	        		  strDataSpec = "NUMBER(" +  sRset.getInt("length") + ", " + sRset.getInt("numeric_scale") + ")"; 

	        		  xType = 5;
	        		  xForm ="nvl(to_char(" + sRset.getString("column_name") + "), NULL)" ;
	        	  } else {
	        		  strDataSpec = "NUMBER(" +  sRset.getInt("length") + ")";

	        		  xType = 1;  //or 2
		        	  //xForm = ;
	        	  }
        	  } else if (sDataType.equals("CHAR")) {
        		  strDataSpec = "CHAR(" +  sRset.getInt("length") + ")";
        		  
        		  xType = 1;
        		  xForm ="nvl(" + sRset.getString("column_name") + "), NULL)" ;
        	  }  else {
        		  strDataSpec = sDataType; 

        		  xType = 1;
        		  xForm ="nvl(to_char(" + sRset.getString("column_name") + "), NULL)" ;
        	  }
        	  sqlDDLv = sqlDDLv + sRset.getString("column_name") + " " + strDataSpec + ",\n";
        	  
        	  sqlRepoDMLfield = sqlRepoDMLTemp  
        			 +  sRset.getInt("ordinal_position") + ", " + tblID + ", '" + sRset.getString("column_name")  + "', '" 
        			 + sRset.getString("column_name")  + "', '" + xForm + "', " + xType + ") ;\r\n";
        			 
   	    	  System.out.println(sqlRepoDMLfield);
   	    	  repoInsCols.write(sqlRepoDMLfield);
          }
          sqlDDLv = sqlDDLv + 
        		  " DB2RRN int ) \n;";
    	  System.out.println(sqlDDLv);
    	  verticaDDL.write(sqlDDLv);

    	  fieldCnt++;
    	  sqlRepoDMLfield = sqlRepoDMLTemp + fieldCnt + ", "+ tblID + ", 'RRN(" + srcTbl + ") as DB2RRN', 'DB2RRN', " 
    			  + " 'nvl(rrn( " + srcTbl + "), NULL)', 1);"; 
    	  System.out.println(sqlRepoDMLfield);
    	  repoInsCols.write(sqlRepoDMLfield);
    	  
          sRset.close();
          srcStmt.close();
          srcConn.close();
       } catch(SQLException e) {
          ovLogger.error(e.getMessage());

          
          return 1;
       }
       
      return 0;
   }   
   
	   
   private int getNextTblID() {
	   // the repod DB cred, an Oracle DB
	   OVScred repoCred;
	   repoCred=dbMeta.getCred(0);
      
	   Connection repConn;
	   Statement repStmt;
	   ResultSet rRset;

	   int tblID = 0;
	   
	   //load JDBC driver
      try {
          Class.forName("oracle.jdbc.OracleDriver"); 
      } catch(ClassNotFoundException e){
       	 //ovLogger.error("Driver error has occured");
         e.printStackTrace();
         return -1;
      }

      try {
	     repConn = DriverManager.getConnection(repoCred.getURL(), repoCred.getUser(), repoCred.getPWD());
	     repConn.setAutoCommit(false);
	     repStmt = repConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
	     rRset = repStmt.executeQuery("select max(table_id) from vertsnap.sync_table ");
	         
	     rRset.next();
	     tblID=rRset.getInt(1);      
         rRset.close();
         repStmt.close();
         repConn.close();
      } catch(SQLException e) {
    	  ovLogger.error(e.getMessage());
      }

	   return tblID;
   }
	
	public static void main(String[] args) throws IOException {

      if (args.length == 10) {
    	  tblID = 0;  //have to be set later.
      } else if (args.length == 11) {
    	  tblID = Integer.parseInt(args[10]);
      } else {
          System.out.println("Usage:   RegisterTbl400 <sdbID> <ssch> <stbl> <jrnl> <tdbID> <tsch> <ttbl> <pID> <rtype> <opath>");
          System.out.println("   or:   RegisterTbl400 <sdbID> <ssch> <stbl> <jrnl> <tdbID> <tsch> <ttbl> <pID> <rtype> <opath> <tblID>");
          System.out.println("E. g.:   RegisterTbl400 7 JOHNLEE2 TESTTBL1 JOHNLEE2.QSQJRN 4 DB2T TESTTBL1 11 1 c:\\Users\\johnlee\\");
          System.out.println("   or:   RegisterTbl400 7 JOHNLEE2 TESTTBL1 JOHNLEE2.QSQJRN 4 DB2T TESTTBL1 11 1 c:\\Users\\johnlee\\  285");
              
          return ;
      } 

      int srcDBid = Integer.parseInt(args[0]);
      String srcSch = args[1];
      String srcTbl = args[2];
      String jrnlName = args[3];
      int tgtDBid = Integer.parseInt(args[4]);
      String tgtSch = args[5];
      String tgtTbl = args[6];
      int poolID = Integer.parseInt(args[7]);
      int refType = Integer.parseInt(args[8]);
      outPath = args[9];

      dbMeta.init();
      RegisterTbl400 regTbl = new RegisterTbl400();
      
      //check if the Journal can be accessed 
      boolean isJournalOK = regTbl.verifyJournal (srcDBid, srcSch, srcTbl, jrnlName);
      if(isJournalOK) {
	      if (tblID == 0) {
	    	  tblID = regTbl.getNextTblID();
	      }
	      
	      // make sure tableID is not used
	      if (dbMeta.isNewTblID(tblID)) {
		      File dir = new File (outPath);
		      verticaDDL = new FileWriter(new File(dir, "verticaDDL.sql"));
		      repoInsTbl = new FileWriter(new File(dir, "repoTblDML.sql"));
		      repoInsCols = new FileWriter(new File(dir, "repoColsDML.sql"));
		      
		      regTbl.genDDL(srcDBid, srcSch, srcTbl, jrnlName, tgtDBid, tgtSch, tgtTbl, poolID, refType);
		
		      verticaDDL.close();
		      repoInsTbl.close();
		      repoInsCols.close();
	      }else {
	    	  System.out.println("TableID " + tblID + " has been used already!");
	      }
      }else {
    	  System.out.println("Is the journal for " + srcSch + "." + srcTbl + ": " + jrnlName + " right?" );
      }
	}

	private boolean verifyJournal (int srcDBid, String srcSch, String srcTbl, String srcLog) {
		boolean rslt = false;
		String[] res = srcLog.split("[.]", 0);
  	    //String jLibName = "JOHNLEE2";
  	    //String jName = "QSQJRN";
  	    String jLibName = res[0];
  	    String jName = res[1];
  	  
        srcCred=dbMeta.getCred(srcDBid);
		try {
			srcConn = DriverManager.getConnection(srcCred.getURL(), srcCred.getUser(), srcCred.getPWD());
			srcConn.setAutoCommit(false);
			srcStmt = srcConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			////
	    	String rLib="", rName="";  //all receiver?
	    	// try to read journal of the last 4 hours(I know I'm using the client time; that does not matter)
	    	Calendar cal = Calendar.getInstance();
	    	cal.add(Calendar.HOUR_OF_DAY, -4);

	    	String strTS = new SimpleDateFormat("yyyy-MM-dd-HH.mm.ss.SSSSSS").format(cal.getTime());
	    	String sqlStmt =  " select COUNT_OR_RRN as RRN,  SEQUENCE_NUMBER AS SEQNBR"
	    	              		+ " FROM table (Display_Journal('" + jLibName + "', '" + jName + "', "
	    	              		+ "   '" + rLib + "', '" + rName + "', "
	    	              		+ "   cast('" + strTS +"' as TIMESTAMP), "    //pass-in the start timestamp;
	    	              		+ "   cast(null as decimal(21,0)), "    //starting SEQ #
	    	              		+ "   'R', "   //JOURNAL CODE: PT, DL, UP, PX?
	    	              		+ "   'UP,DL,PT,PX',"    //JOURNAL entry ?
	    	              		+ "   '" + srcSch + "', '" + srcTbl + "', '*QDDS', '',"  //Object library, Object name, Object type, Object member
	    	              		+ "   '', '', ''"   //User, Job, Program
	    	              		+ ") ) as x order by 2 asc"
	    	              		;
	
			sRset=srcStmt.executeQuery(sqlStmt );		
        
			if (sRset.next() ) {
			//	rslt = true;
			}
			rslt = true;
			
			sRset.close();
			srcStmt.close();
			srcConn.close();
		} catch(SQLException e) {
			ovLogger.error(e.getMessage());
		}
		
		return rslt;
	}

}