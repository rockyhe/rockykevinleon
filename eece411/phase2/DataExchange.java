package phase2Pack;

import java.io.*;
import java.net.*;
import java.util.*;
import java.security.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.List;
import java.util.ArrayList;

public class DataExchange implements Runnable {
	private Socket clntSock;
	private ConcurrentHashMap<String,byte[]> store;
	private static final int RECV_BUFSIZE = 1057;   // Size of receive buffer
	private static final int SEND_BUFSIZE = 1025;
	DataExchange(Socket clientSocket,ConcurrentHashMap<String,byte[]> KVstore) {
      		this.clntSock=clientSocket;
		store = KVstore;
   	}

	public void run(){
	try{	
            byte[] sendBuffer = new byte[SEND_BUFSIZE];
            byte[] recvBuffer = new byte[RECV_BUFSIZE];
	            //declare subset byte[]
             byte[] key = new byte[32];
             byte[] value = new byte[1024];
	     String keyStr;
	    //Get the return message from the server
            InputStream in = clntSock.getInputStream();

	    int recvMsgSize = 0;

	    while ((recvMsgSize = in.read(recvBuffer)) != -1){
			System.out.println("reading input ");
		
			//call package classes
			//StringUtils objStringUtils = new StringUtils();
			//ByteOrder objByteOrder = new ByteOrder();
		    
			//declare subset byte[]
	    		
			 System.out.println("byte stream received:");
			System.out.println(Arrays.toString(recvBuffer));
			 //split byte stream
			key = Arrays.copyOfRange(recvBuffer, 1,32);
			value = Arrays.copyOfRange(recvBuffer, 33, recvBuffer.length);
			
			keyStr = Arrays.toString(key).replaceAll("(^\\[|\\]$)", "").replace(", ", "_");	
			 System.out.println("convert key bytes to string:");

			System.out.println(keyStr);
			if(recvBuffer[0]==(byte)0x01){
				store.put(keyStr,value);
				 System.out.println("using store.get(key) :");
				System.out.println(Arrays.toString(store.get(keyStr)));
			}
	    }

	    clntSock.close();
	}catch(IOException ioe) {
	    System.out.println("IOException!!!");
	}
	}
}

