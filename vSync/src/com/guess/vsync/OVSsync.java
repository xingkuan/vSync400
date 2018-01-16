package com.guess.vsync;

import java.io.*;
import java.util.*;
import java.text.*;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;


class OVSsync
{
   private static OVStableProcessor tProc = new OVStableProcessor();
   private static OVSrepo dbMeta = new OVSrepo();
   private static final Logger ovLogger = LogManager.getLogger();
   
   public static void main (String args[]) {   
      if (args.length != 1) { 
         System.out.println("Usage:   OVSinit <int poolID>  ");   
         System.out.println("example:   OVSinit 2 ");
         return;
      } 
      
      // -1: replicate all; >1: refresh all in a the pool
      int poolID = -9 ;
      try{
    	  poolID = Integer.parseInt(args[0]);
      //or tProc.syncTblsAll();
      } catch (NumberFormatException nfe) { 
    	  ovLogger.error("invalid poolID. "+nfe);
      }
      if (poolID < -1){
    	  ovLogger.error("invalid poolID! ");
    	  return;
      }
	  dbMeta.init();
	  tProc.setDbMeta(dbMeta);
	  tProc.syncTblsByPoolID(poolID);

   }

}