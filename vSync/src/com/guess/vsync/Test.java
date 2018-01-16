package com.guess.vsync;


class Test
{
   private static final OVSmetrix metrix = OVSmetrix.getInstance();
   
   public static void main (String args[]) {   
	  metrix.sendMX("initDuration,jobId=test,tblID=0 value=6\n");
//	  metrix.sendMXrest("initDuration,jobId=test,tblID=0 value=6\n");
      
      return ;
   }

}