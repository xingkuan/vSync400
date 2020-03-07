package com.guess.vsync400;

import java.io.*;
import java.util.*;
import java.text.*;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;


class OVSaudit
{
   private static OVStableProcessor tProc = new OVStableProcessor();
   private static OVSrepo dbMeta = new OVSrepo();
   private static OVSmeta tblMeta;
   
   private static final Logger log4j = LogManager.getLogger();
   private static String jobID="AudTbl";
   
   public static void main (String args[]) {   
      if (args.length != 1) { 
         System.out.println("Usage:   OVSaudit <-1|the positive pool ID> ");
         return ;
      } 

      // -1 for all; >1 for a the poolID
      int poolID = -9 ;
      try{
    	  poolID = Integer.parseInt(args[0]);
      } catch (NumberFormatException nfe) { 
    	  log4j.error("invalid poolID. "+nfe);
      }
      if (poolID < -1){
    	  log4j.error("invalid poolID! ");
    	  return;
      }
      
      dbMeta.init(); 
      tblMeta = new OVSmeta();
      tblMeta.setDbMeta(dbMeta);
      
      tProc.setDbMeta(dbMeta);
	  tProc.setOVSmeta(tblMeta);
	  
      tProc.auditTbls(poolID, jobID);
      
      return ;
   }

}