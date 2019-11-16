package com.guess.vsync400;

import java.io.*;
import java.util.*;
import java.text.*;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

// JohnLee, 07/27: This may never be fully developed. It is better handled by SQLscript.!
class OVSdeact
{
   private static OVStableProcessor tProc = new OVStableProcessor();
   private static OVSrepo dbMeta = new OVSrepo();
   
   public static int main (String args[]) {   
      int argLength=args.length;
      
      if (argLength != 1) { 
         System.out.println("Usage:   OVSdeact <int TableID>  ");
         System.out.println("example:   OVSdeact 2 ");
         
         return -1;
      } 

      try {
         dbMeta.init();
         System.out.println("db initialized");

         tProc.setDbMeta(dbMeta);
         tProc.deactivate(Integer.parseInt(args[0]), "deact");
         tProc.close();
      } catch (Exception e) {
    	  System.out.println(e); 
      }
      return 0;
   }

}