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

import com.matei.eece411.util.*;

public class Server {
    
    private static int port=5000;
    private static final int BUFSIZE = 1057;   // Size of receive buffer
    private static ConcurrentHashMap<String, byte[]> store; 
    
    public static void main(String[] args)
    {
		try {
        	// Create a server socket to accept client connection requests
        	ServerSocket servSock = new ServerSocket(port);
			Socket clntSock;
			store = new ConcurrentHashMap<String,byte[]>();
        	int recvMsgSize;   // Size of received message
        	int threadcnt = 0;
        	System.out.println("listening...");
       		
    		for (;;)
    		{
    			// Run forever, accepting and servicing connections
    			clntSock = servSock.accept();     // Get client connection
    			DataExchange connection = new DataExchange(clntSock, store);
    			Thread t = new Thread(connection);
    			t.start();
    			threadcnt++;
    			System.out.println("thread "+ threadcnt);
    			System.out.println(Arrays.toString(store.get("1_0_0_0_0_0_0_0_0_0_0_0_0_0_0_0_0_0_0_0_0_0_0_0_0_0_0_0_0_0_0")));
    		}
		} catch(IOException e) {
			System.out.println("IOException on socket listen");
		}
    }
}

