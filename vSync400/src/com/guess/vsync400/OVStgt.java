package com.guess.vsync400;

import java.io.*;
import java.util.*;
import java.text.*;
import java.sql.*;
import com.vertica.jdbc.VerticaConnection;
import com.vertica.jdbc.VerticaCopyStream;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;


class OVStgt {
   private OVScred tgtCred;
   private OVSmeta tblMeta;
   private Connection tgtConn;
   private Statement tgtStmt;
   private boolean isError=false;
   private ResultSet srcRset;
   private int errCnt;
   private int refreshCnt;
   //private int MaxInCount = 5000;  //10000;    2018.04.30: use batchSize
   private String label;

   OVSconf conf = OVSconf.getInstance();
   private String initLogDir = conf.getConf("initLogDir");
   private int batchSize = Integer.parseInt(conf.getConf("batchSize"));

   private static final Logger ovLogger = LogManager.getLogger();
   private static final OVSmetrix metrix = OVSmetrix.getInstance();
   
   public boolean init() {
      label=">";
      return linit();
   }
   public boolean init(String lbl) {
      label=lbl;
      return linit();
   }
   private boolean linit() {
      // initialize variables
      isError=false;
	   boolean rtv = true;
      
      // make sure target db type is supported
      tgtCred=tblMeta.getTgtCred();
      if (tgtCred.getType() == 2)  {
         // if it is supported, load Vertica driver
         try {
            Class.forName("com.vertica.jdbc.Driver"); 
         } catch(ClassNotFoundException e){
            ovLogger.error(label + " Driver error has occured");
            e.printStackTrace();
	   	   rtv=false;
            return rtv;
         }
      } else {
         ovLogger.error(label + " target type not of Vertica... not supported");
         rtv=false;
         return rtv;
      }
      try {
         //establish Vertica connection
         tgtConn = DriverManager.getConnection(tgtCred.getURL(), tgtCred.getUser(), tgtCred.getPWD());
         tgtConn.setAutoCommit(false);
         tgtStmt = tgtConn.createStatement();
      } catch(SQLException e) {
         ovLogger.error("Cannot connect to target - init failed, jobID: " + label);
         ovLogger.error(e.getMessage());
         rtv=false;
      }
	  return rtv;
   }
   
   /* returns -1 if non-recoverable error, 
    * 0 if all records loaded, 
    * or number of records which did not load
    */
   public int initLoadType1() throws SQLException {   
      // performs a load of Type 1 of the table
      int[] batchResults = null;
      String[] RowIDs = new String[batchSize];
      int curRecCnt;
      int i = 0;
      String ts;
      String sDebug;
      
      PreparedStatement tgtPStmt;
      boolean commFlag;
      
      errCnt=0;
      ((VerticaConnection) tgtConn).setProperty("DirectBatchInsert", true );
      
      if (tblMeta.getTgtUseAlt()) { 
         tgtPStmt = tgtConn.prepareStatement(tblMeta.getSQLInsertAlt());
         //System.out.println(label + " inserting into alternate table " + tblMeta.getTgtTableAlt());
         ovLogger.info(label + " inserting into alternate table " + tblMeta.getTgtTableAlt());
      } else {
         tgtPStmt = tgtConn.prepareStatement(tblMeta.getSQLInsert());
      }
      curRecCnt = 0;
      refreshCnt = 0;
      commFlag = true;
int tmpInt;     
float tmpFloat;
double tmpDouble;
long tmpLong;
      // insert records into batch
      while (srcRset.next()) {
         i=0;
         //RowIDs[curRecCnt]=srcRset.getString("rowid");
         RowIDs[curRecCnt]=srcRset.getString("DB2RRN");
         try {

            for ( i=1; i<=tblMeta.getFldCnt(); i++ ) {
               //  accomodate different data type per mapping
               switch (tblMeta.getFldType(i-1)) {
                  case 1:     //String
//String x1 =srcRset.getString(i);
                     tgtPStmt.setString(i,srcRset.getString(i));
                     break;
                  case 2:     //int
//                     tgtPStmt.setInt(i,srcRset.getInt(i));
                	 tmpInt=srcRset.getInt(i);
                	 if ((tmpInt == 0) && srcRset.wasNull())
                		 tgtPStmt.setNull(i, java.sql.Types.INTEGER);
                	 else
                		 tgtPStmt.setInt(i,tmpInt);
//int x2 =srcRset.getInt(i);
                     break;
                  case 3:     //Long
                     //tgtPStmt.setLong(i,srcRset.getLong(i));
//long x3 =srcRset.getLong(i);
                 	 tmpLong=srcRset.getLong(i);
                 	 if ((tmpLong == 0) && srcRset.wasNull())
                 		 tgtPStmt.setNull(i, java.sql.Types.NULL);
                 	 else
                 		 tgtPStmt.setDouble(i,tmpLong);
                     break;
                  case 4:     //Double
                     //tgtPStmt.setDouble(i,srcRset.getDouble(i));
                	 tmpDouble=srcRset.getDouble(i);
                	 if ((tmpDouble == 0) && srcRset.wasNull())
                		 tgtPStmt.setNull(i, java.sql.Types.DOUBLE);
                	 else
                		 tgtPStmt.setDouble(i,tmpDouble);
                     break;
                  case 5:     //Float
                     //tgtPStmt.setFloat(i,srcRset.getFloat(i));
                	 tmpFloat=srcRset.getFloat(i);
                	 if ((tmpFloat == 0) && srcRset.wasNull())
                		 tgtPStmt.setNull(i, java.sql.Types.FLOAT);
                	 else
                		 tgtPStmt.setFloat(i,tmpFloat);
                     break;
                  case 6:     //Timestamp
                     tgtPStmt.setTimestamp(i,srcRset.getTimestamp(i));
//Timestamp x6 =srcRset.getTimestamp(i);
                     break;
                  case 7:     //Date
                      tgtPStmt.setDate(i,srcRset.getDate(i));
java.sql.Date x7 =srcRset.getDate(i);
                      break;
                  case 100:     //alternate encoding
                     tgtPStmt.setString(i,new String(srcRset.getString(i).getBytes("ISO-8859-15"), "UTF-8"));
                     break;
                  case 1000:     //alternate encoding w debugging
                     sDebug=new String(srcRset.getString(i).getBytes("ISO-8859-15"), "UTF-8");
                     tgtPStmt.setString(i,sDebug);
                     System.out.println(sDebug);
                     break;
                  case 101:     //alternate encoding
                     //System.out.println("type 101 - " + i + " |" + srcRset.getString(i) + "|");
                     ts=srcRset.getString(i);
                     if (ts != null){
                        tgtPStmt.setString(i,new String(srcRset.getString(i).getBytes("US-ASCII"), "UTF-8"));
                     } else {
                        tgtPStmt.setString(i,"");
                     }
                     break;
                  case 1010:     //alternate encoding w debugging
                     //System.out.println("type 101 - " + i + " |" + srcRset.getString(i) + "|");
                     ts=srcRset.getString(i);
                     if (ts != null){
                        sDebug=new String(srcRset.getString(i).getBytes("US-ASCII"), "UTF-8");
                        tgtPStmt.setString(i,sDebug);
                        System.out.println(sDebug);
                     } else {
                        tgtPStmt.setString(i,"");
                     }
                     break;
                  case 102:     //alternate encoding
                     tgtPStmt.setString(i,new String(srcRset.getString(i).getBytes("UTF-16"), "UTF-8"));
                     break;
                  case 103:     //alternate encoding
                     ts=srcRset.getString(i);
                     if (ts != null){
                        tgtPStmt.setString(i,new String(srcRset.getString(i).getBytes("UTF-8"), "UTF-8"));
                     } else {
                        tgtPStmt.setString(i,"");
                     }   
                     break;
                  case 1030:     //alternate encoding w debugging
                     sDebug=new String(srcRset.getString(i).getBytes("UTF-8"), "UTF-8");
                     tgtPStmt.setString(i,sDebug);
                     System.out.println(sDebug);
                     break;
                  case 104:     //alternate encoding
                     tgtPStmt.setString(i,new String(srcRset.getString(i).getBytes("ISO-8859-1"), "UTF-8"));
                     break;
                  case 1040:     //alternate encoding w debugging
                     sDebug=new String(srcRset.getString(i).getBytes("ISO-8859-1"), "UTF-8");
                     tgtPStmt.setString(i,sDebug);
                     System.out.println(sDebug);
                     break;
                  case 105:     //alternate encoding
                     tgtPStmt.setString(i,new String(srcRset.getString(i).getBytes("ISO-8859-2"), "UTF-8"));
                     break;
                  case 1050:     //alternate encoding w debugging
                     sDebug=new String(srcRset.getString(i).getBytes("ISO-8859-2"), "UTF-8");
                     tgtPStmt.setString(i,sDebug);
                     System.out.println(sDebug);
                     break;
                  case 106:     //alternate encoding
                     tgtPStmt.setString(i,new String(srcRset.getString(i).getBytes("ISO-8859-4"), "UTF-8"));
                     break;
                  case 1060:     //alternate encoding w debugging
                     sDebug=new String(srcRset.getString(i).getBytes("ISO-8859-4"), "UTF-8");
                     tgtPStmt.setString(i,sDebug);
                     System.out.println(sDebug);
                     break;
                  case 107:     //alternate encoding
                     tgtPStmt.setString(i,new String(srcRset.getString(i).getBytes("ISO-8859-1"), "ISO-8859-1"));
                     break;
                  case 1070:     //alternate encoding w debugging
                     sDebug=new String(srcRset.getString(i).getBytes("ISO-8859-1"), "ISO-8859-1");
                     tgtPStmt.setString(i,sDebug);
                     System.out.println(sDebug);
                     break;
                  //case 125:     //alternate encoding - unicode stream
                  //   tgtPStmt.setUnicodeStream(i,srcRset.getUnicodeStream(i),srcRset.getUnicodeStream(i).available());
                     //tgtPStmt.setString(i,new String(srcRset.getString(i).getBytes("ISO-8859-4"), "UTF-8"));
                  //   break;
                  case 999:   //set string blank  more for testing purposes
                     tgtPStmt.setString(i,new String(""));
                     break;
                  default:      //default (String)
                     tgtPStmt.setString(i,srcRset.getString(i)); 
                     break;
               }
            }  
         } catch (Exception e) {
            ovLogger.error("initLoadType1 Exception. Rollback.");
            ovLogger.error(label + " " + e.toString());
            ovLogger.error( "    ****************************");
            ovLogger.error( "    rowid: " +RowIDs[curRecCnt]);
            ovLogger.error( "    fieldno: " + i + "  " + tblMeta.getFldName(i-1));
            return -1;
         }
         // insert batch into target table
         tgtPStmt.addBatch();                
         refreshCnt++;
         curRecCnt++ ;

         if (curRecCnt == batchSize) {
            curRecCnt=0;
            ovLogger.info(label +  " adding recs (accumulating) - " + refreshCnt );
            try {
               batchResults = tgtPStmt.executeBatch();
            } catch(BatchUpdateException e) {
               ovLogger.error(label + " executeBatch Error... ");
               ovLogger.error(label + e.toString());
               commFlag=false;
               for ( i=1; i<=tblMeta.getFldCnt(); i++ ) {
                  ovLogger.error(label + " " + srcRset.getString(i));
               }                                                
               int[] iii;
               iii=e.getUpdateCounts();
               for ( i=1; i<=batchSize; i++ ) {
                  if (iii[i-1]==-3) {     // JLEE, 07/24: the failed row.
                     ovLogger.info(label + " " + (i-1) +  " : " + iii[i-1] + " - " + RowIDs[i-1]);
                     putROWID(RowIDs[i-1]);
                     errCnt++;
                  }
               }                                                
            }
         }
////to be removed. for debug purpose
//if (refreshCnt > 10000) {break;}
      }
      try {
         batchResults = tgtPStmt.executeBatch();
      } catch(BatchUpdateException e) {
         ovLogger.error(label + " Error... rolling back");
         ovLogger.error(label + e.getMessage());
         
         commFlag=false;
         int[] iii;
         iii=e.getUpdateCounts();
         ovLogger.error(label + " Number of records in batch: " + curRecCnt);
         
         for ( i=1; i<=curRecCnt; i++ ) {
            if (iii[i-1]==-3) {
               ovLogger.error(label + " " + (i-1) +  " : " + iii[i-1] + " - " + RowIDs[i-1]);
               putROWID(RowIDs[i-1]);
               errCnt++;
            }
         } 
         return -1;         
      }
    //if(errCnt>0){
    //	  metrix.sendMX("errCnt,jobId="+jobID+",tblID="+srcTblAb7+"~"+tableID+" value=" + errCnt + "\n");
    //  }
      
      return refreshCnt;
   }
   
/* returns -1 if non-recoverable error, 
 * 0 if all records loaded, 
 * or number of records which did not load
 */
   public int initLoadType2() throws SQLException {   
      // perform load type 2 on table (this is no longer used)
      int[] batchResults = null;
      
      int curRecCnt;
      int i = 0;
      String sqlCopyTgt = tblMeta.getSQLCopyTarget();
      PreparedStatement tgtPStmt;
      boolean commFlag;
      
      errCnt=0;

      try {
         VerticaCopyStream stream = new VerticaCopyStream( (VerticaConnection) tgtConn, sqlCopyTgt);
      } catch (Exception e) {
         ovLogger.error(label + e.getMessage());
      }
      
      return refreshCnt;
   }

   public int dropStaleRecordsOfRRNlist(String rrnList) throws SQLException {
	  int delCnt=0;
      String DeleteTargetTable  = " delete from "  + tblMeta.getTgtSchema() + "." + tblMeta.getTgtTable() 
			   + " where "  + tblMeta.getPK() + " in (" + rrnList + ")";
      
      delCnt = tgtStmt.executeUpdate (DeleteTargetTable) ;
      tgtConn.commit();
      
      return delCnt;
   }
   public Long dropStaleRecords(String jobID,String srcTblAb7,int tableID) throws SQLException {  //2019.12.18: added the parameters purely for sending them to influxDB
      // deletes records to be replaced in target table
      boolean NeedsProcessing;
      int TotalRecordsProcessed;
      int CurrentRecordCount;
      int lRefreshCnt;
      String DeleteTargetTable = new String(""); 
      
      Long journalSeqNum=0l;
      
      TotalRecordsProcessed = 0;
      CurrentRecordCount = 0;
      NeedsProcessing = false;
      lRefreshCnt=0;
      
      DeleteTargetTable  = " delete from "  + tblMeta.getTgtSchema() + "." + tblMeta.getTgtTable() 
      					   + " where "  + tblMeta.getPK() + " in (";
      if (srcRset.next()) {
         NeedsProcessing    = true;
         CurrentRecordCount++ ;
         TotalRecordsProcessed++ ;
         DeleteTargetTable += "'" + srcRset.getInt("RRN") + "'" ;
         journalSeqNum = srcRset.getLong("SEQNBR");
      }
      while (srcRset.next()) {
         TotalRecordsProcessed++ ;
         CurrentRecordCount++ ;
         NeedsProcessing    = true;
         DeleteTargetTable += ", '" +  srcRset.getInt("RRN") + "'" ;

         journalSeqNum = srcRset.getLong("SEQNBR");

         //if ( CurrentRecordCount == MaxInCount ) {
         if ( CurrentRecordCount == batchSize ) {
            DeleteTargetTable += ") " ;
            lRefreshCnt = lRefreshCnt + CurrentRecordCount;
            //putLog(new java.util.Date() + "    - " + CurrentRecordCount + " - " + lRefreshCnt + " / " + oLogCnt);
            tgtStmt.executeUpdate (DeleteTargetTable) ;
            tgtConn.commit();
            ovLogger.info(label +  " deleted - " + TotalRecordsProcessed );
            CurrentRecordCount = 0 ;
            NeedsProcessing    = false;
            DeleteTargetTable  = " delete  from "  + tblMeta.getTgtSchema() + "." + tblMeta.getTgtTable();
            DeleteTargetTable += " where   "  + tblMeta.getPK() + " in (";
            if (srcRset.next()) {
               NeedsProcessing    = true;
               CurrentRecordCount++ ;
               TotalRecordsProcessed++ ;
               DeleteTargetTable += "'" + srcRset.getString("RRN") + "'" ;

               journalSeqNum = srcRset.getLong("SEQNBR");
            }
         }
      }
      if (NeedsProcessing) {
         DeleteTargetTable += " ) " ;
         lRefreshCnt = lRefreshCnt + CurrentRecordCount;
         //putLog(new java.util.Date() + "    - " + CurrentRecordCount + " - " + lRefreshCnt + " / " + oLogCnt);
         tgtStmt.executeUpdate (DeleteTargetTable) ;
         tgtConn.commit();
         ovLogger.info(label +  " deleted - " + TotalRecordsProcessed );
         metrix.sendMX("deleted,jobId="+jobID+",tblID="+srcTblAb7+"~"+tableID+" value=" + TotalRecordsProcessed + "\n");
      }
      srcRset.close();
      
      return journalSeqNum;
   }
   
   private void putROWID(String rowid) {
      try {
//.         FileWriter fstream = new FileWriter(tblMeta.getInitLogDir() + "/" + tblMeta.getTgtSchema() + "." + tblMeta.getTgtTable() + ".row", true);
         FileWriter fstream = new FileWriter(initLogDir + tblMeta.getTgtSchema() + "." + tblMeta.getTgtTable() + ".row", true);
         BufferedWriter out = new BufferedWriter(fstream);
         out.write(rowid + "\n");
         out.close();
      } catch (Exception e){
         System.out.println(label + " Error: " + e.getMessage());
      }
   }
   public void truncate() {
      // truncates target table
      String sql;
      try {
         if (tblMeta.getTgtUseAlt()){ 
            sql = "truncate table  " + tblMeta.getTgtSchema() + "." + tblMeta.getTgtTableAlt() + ";";
            //System.out.println(label + " truncating alternate table " +  tblMeta.getTgtTableAlt()) ;
            ovLogger.info(label + " truncating alternate table " +  tblMeta.getTgtTableAlt()) ;
         } else {
            sql = "truncate table  " + tblMeta.getTgtSchema() + "." + tblMeta.getTgtTable() + ";";
         }
         tgtStmt.execute(sql);
      } catch (SQLException e) {
         //System.out.println(label + " Truncate table failure");
         //System.out.println(e.getMessage());
         ovLogger.error(label + " Truncate table failure");
         ovLogger.error(label + e.getMessage());
      }
   }
   public void swapTable() {
      String sql;
      try {
         sql = "alter table " 
             + tblMeta.getTgtSchema() + "." + tblMeta.getTgtTable() + ", " 
             + tblMeta.getTgtSchema() + "." + tblMeta.getTgtTableAlt() + ", " 
             + tblMeta.getTgtSchema() + "." + tblMeta.getTgtTableAlt() + "_gv_tmp rename to "  
             + tblMeta.getTgtTableAlt() + "_gv_tmp, "  
             + tblMeta.getTgtTable() + ", "  
             + tblMeta.getTgtTableAlt() + ";"      ;
         ovLogger.info(" swap table sql: " + sql);
         tgtStmt.execute(sql);
         ovLogger.info(label + " table swapped" +  tblMeta.getTgtTableAlt()) ;
      } catch (SQLException e) {
         ovLogger.error(label + " swap table failure");
         ovLogger.error(label + e.getMessage());
      }
   }
   public int getRecordCount(){
      int rtv;
   //   Connection lConn;
   //   Statement lStmt;
      ResultSet lrRset;
      int i;

      rtv=0;
      try {
    // should us tgtConn and tgtStmt
    // 	  lConn = DriverManager.getConnection(tgtCred.getURL(), tgtCred.getUser(), tgtCred.getPWD());
    //     lStmt = tgtConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
         tgtStmt = tgtConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

    //     lrRset=lStmt.executeQuery("select count(*) from " + tblMeta.getTgtSchema() + "." + tblMeta.getTgtTable());
         lrRset=tgtStmt.executeQuery("select count(*) from " + tblMeta.getTgtSchema() + "." + tblMeta.getTgtTable());
         if (lrRset.next()) {
            rtv = Integer.parseInt(lrRset.getString(1));  
         }
         lrRset.close();
    //     lStmt.close();
    //     lConn.close();
      } catch(SQLException e) {
         //System.out.println(label + " error during src audit"); 
         ovLogger.error(label + " error connect to target: " +e); 
      }
      return rtv;
   }
   public void setMeta(OVSmeta mta) {
      tblMeta=mta;
   }
   public void OVStgt(OVScred ovsc) {
      tgtCred=ovsc;
   }
   public void setCred(OVScred ovsc) {
      tgtCred=ovsc;
   }
   public void setSrcRset(ResultSet rs) {
      srcRset=rs;
   }
   public void setBatchSize(int i) {
      batchSize=i;
   }
   public int getErrCnt(){
      return errCnt;
   }
   public void commit() throws SQLException {
      tgtConn.commit();
   }
   public void rollback() throws SQLException {
      tgtConn.rollback();
   }
   public void close() throws SQLException {
      tgtStmt.close();
      tgtConn.commit();
      tgtConn.close();
      ovLogger.info(label + " closed tgt db src");
   }
   public int getRefreshCnt() {
      return refreshCnt;
   }
}