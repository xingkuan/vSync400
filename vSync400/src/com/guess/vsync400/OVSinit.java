package com.guess.vsync400;


class OVSinit
{
   private static OVStableProcessor tProc = new OVStableProcessor();
   private static OVSrepo dbMeta = new OVSrepo();
   
   public static void main (String args[]) {   
      if (args.length != 1) { 
         System.out.println("Usage:   OVSinit <int TableID>  ");
         System.out.println("example:   OVSinit 2 ");
         return ;
      } 
     
      dbMeta.init();
      tProc.setDbMeta(dbMeta);
      tProc.procInitDefault(Integer.parseInt(args[0]), "initJob") ;
      
      return ;
   }

}