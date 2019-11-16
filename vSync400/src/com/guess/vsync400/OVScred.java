package com.guess.vsync400;

/*
 class OVScred
 
 Stores all necessary connection information for a single database
 
*/
class OVScred {
   String credURL;
   String credUser;
   String credPassword;
   String credDesc;
   String cmdQueue;
   int credType;
   
   public OVScred(String cUser, String cPWD, String cURL) {
      // creates a simple OVScred with only user, password and URL
      credUser=cUser;
      credPassword=cPWD;
      credURL=cURL;
   }
   public OVScred(String cUser, String cPWD, String cURL, int tp, String dsc, String cqueue) {
      // creates a more complex OVScred adding database type, description and command queue attributes
      credUser=cUser;
      credPassword=cPWD;
      credURL=cURL;
      credType=tp;
      credDesc=dsc;
      cmdQueue=cqueue;
   }
   // All of the following subroutines simply set/get individual attributes of the class instance
   public void setURL(String s) {
      credURL=s;
   }
   public void setUser(String s) {
      credUser=s;
   }
   public void setPWD(String s) {
      credPassword=s;
   }
   public void setType(int i) {
      credType=i;
   }
   public String getURL() {
      return credURL;
   }
   public String getUser() {
      return credUser;
   }
   public String getPWD() {
      return credPassword;
   }
   public int getType() {
      return credType;
   }
   public String getDesc() {
      return credDesc;
   }
   public String getCmdQueue() {
      return cmdQueue;
   }
}