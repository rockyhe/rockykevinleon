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
import java.lang.Error;

import com.matei.eece411.util.*;

public class DataExchange implements Runnable {
	private Socket clntSock;
	private ConcurrentHashMap<String, byte[]> store;
	private static final int RECV_BUFSIZE = 1057;   // Size of receive buffer
	private static final int SEND_BUFSIZE = 1025;
	
	//Constructor
	DataExchange(Socket clientSocket, ConcurrentHashMap<String,byte[]> KVstore)
	{
  		this.clntSock = clientSocket;
		this.store = KVstore;
   	}
	
	/**
	* Puts the given value into the store, mapped to the given key.
	* If there is already a value corresponding to the key, then the value is overwritten.
	*/
	public void put(String key, byte[] value)
	{
		store.put(key,value);
		System.out.println("using store.get(key) :");
		System.out.println(Arrays.toString(store.get(key)));
	}
	
	/**
	* Returns the value associated with the given key.
	* If there is no such key in the store, the store returns error - not found.
	*/
	public byte[] get(String key) throws Error
	{
		if (store.containsKey(key))
		{
			return store.get(key);
		}
		else
		{
			throw new Error("Not found");
		}
	}
	
	/**
	* Removes the value associated with the given key.
	* If there is no such key in the store, the store returns error - not found.
	*/
	public void remove(String key) throws Error
	{
		if (store.containsKey(key))
		{
			store.remove(key);
		}
		else
		{
			throw new Error("Not found");
		}
	}

	public void run()
	{
		try {
	        byte[] sendBuffer = new byte[SEND_BUFSIZE];
	        byte[] recvBuffer = new byte[RECV_BUFSIZE];
            //declare subset byte[]
        	byte[] key = new byte[32];
			byte[] value = new byte[1024];
			String keyStr;
			//Get the return message from the server
			InputStream in = clntSock.getInputStream();
			
			int recvMsgSize = 0;
	
		    while ((recvMsgSize = in.read(recvBuffer)) != -1)
		    {
		    	System.out.println("reading input ");
		    	
				//declare subset byte[]
				System.out.println("byte stream received:");
				System.out.println(Arrays.toString(recvBuffer));
				
				//Split byte stream				
				//Get the command byte
				int cmd = ByteOrder.leb2int(recvBuffer, 0, 1);

				//Get the key bytes
				key = Arrays.copyOfRange(recvBuffer, 1,32);
				System.out.println("convert key bytes to string:");
				keyStr = Arrays.toString(key).replaceAll("(^\\[|\\]$)", "").replace(", ", "_");
				System.out.println(keyStr);
				
				//1 - put operation
				switch(cmd)
				{
				case 1:
					//Get the value bytes
					//Only do this if the command is put
					value = Arrays.copyOfRange(recvBuffer, 33, recvBuffer.length);
					put(keyStr, value);
					break;
				case 2:
					//2 - get operation
					get(keyStr);
					break;
				case 3:
					//3 - remove operation
					remove(keyStr);
					break;
				}
		    }
	     	clntSock.close();
		} catch(IOException ioe) {
		    System.out.println("IOException!!!");
		}
	}
}

