package com.guess.vsync400;

import java.io.*;
import java.net.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;



import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;



public class OVSmetrix {
    DatagramSocket sock = null;
    private InetAddress address;
    OVSconf conf = OVSconf.getInstance();
    
    private byte[] buf;
    
    private static final Logger ovLogger = LogManager.getLogger();
    
    
    private static OVSmetrix instance = null;  // use lazy instantiation new OVSmetrix();
    
    public static OVSmetrix getInstance() {
        if(instance == null) {
           instance = new OVSmetrix();
        }
        return instance;
     }
    
    private OVSmetrix() {  // and defeat instantiation.
    }
    
    public void sendMX(String mx){
        String portStr = conf.getConf("influxDBport");
        String hostName = conf.getConf("influxDBhost");
        
        //ovLogger.info(hostName + ":" + portStr);
        int portNum = Integer.parseInt(portStr);
        
    	try {
    		sock = new DatagramSocket();
    		address = InetAddress.getByName(hostName);

           buf = mx.getBytes();
           DatagramPacket dp 
             = new DatagramPacket(buf, buf.length, address, portNum);

           sock.send(dp);
        } catch (Exception e) {
    		ovLogger.error("Exception " + e);
    	}
    }
    
    public void sendMXrest(String mx){

    	String portNum = conf.getConf("influxDBport");
        String hostName = conf.getConf("influxDBhost");
        String dbName = conf.getConf("influxDB");
        String urlStr = "http://"+hostName+":"+portNum+"/write?db="+dbName;
    	try {
	    URL url = new URL(urlStr);
		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
		conn.setDoOutput(true);
		conn.setRequestMethod("POST");
	    conn.setRequestProperty("Content-Type", "application/text");

	    //String input = "{\"qty\":100,\"name\":\"iPad 4\"}";

	    OutputStream os = conn.getOutputStream();
	    os.write(mx.getBytes());
	    os.flush();

	    if (conn.getResponseCode() != HttpURLConnection.HTTP_CREATED) {
			throw new RuntimeException("Failed : HTTP error code : "
			+ conn.getResponseCode());
	    }

	    //BufferedReader br = new BufferedReader(new InputStreamReader(
		// 		(conn.getInputStream())));

        //String output;
        //System.out.println("Output from Server .... \n");
        //while ((output = br.readLine()) != null) {
    	//  System.out.println(output);
        //}
        conn.disconnect();
     } catch (MalformedURLException e) {
  		e.printStackTrace();
     } catch (IOException e) {
  	    e.printStackTrace();
     }
   }
}
