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
	private static final int REQUEST_BUFFSIZE = 1057;   // Size of receive buffer
	private static final int REPLY_BUFFSIZE = 1025;
	private static final int CMD_SIZE = 1;
	private static final int KEY_SIZE = 32;
	private static final int VALUE_SIZE = 1024;
	
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
	private byte[] put(String key, byte[] value)
	{
		store.put(key,value);
		System.out.println("using store.get(key) :");
		System.out.println(Arrays.toString(store.get(key)));
	}
	
	/**
	* Returns the value associated with the given key.
	* If there is no such key in the store, the store returns error - not found.
	*/
	private byte[] get(String key)
	{
		if (store.containsKey(key))
		{
			return store.get(key);
		}
		else
		{
		}
	}
	
	/**
	* Removes the value associated with the given key.
	* If there is no such key in the store, the store returns error - not found.
	*/
	private byte[] remove(String key)
	{
		if (store.containsKey(key))
		{
			store.remove(key);
			return 0x00;
		}
		else
		{
			return 0x01;
		}
	}
	
	private String ReplyMessage(byte errCode)
	{
		switch(errCode)
		{
		case 0x00:
			return "Operation successful";
		case 0x01:
			return "Inexistent key requested in a get or remove operation";
		case 0x02:
			return "Out of space for put operation";
		case 0x03:
			return "System Overload";
		case 0x04:
			return "Internal KVStore Failure";
		case 0x05:
			return "Unrecognized command";
		default:
			return "Error code not handled";	
		}
	}

	public void run()
	{
		try {
	        byte[] requestBuffer = new byte[REQUEST_BUFFSIZE];
	        byte[] replyBuffer = new byte[REPLY_BUFFSIZE];
            //declare subset byte[]
        	byte[] key = new byte[KEY_SIZE];
			byte[] value = new byte[VALUE_SIZE];
			String keyStr;
			
			//Get the return message from the server
			InputStream in = clntSock.getInputStream();
			
			int requestMessageSize = 0; //TODO: Do we want to explicitly check correct message size depending on the command?
		    if ((requestMessageSize = in.read(requestBuffer)) != -1)
		    {	
				System.out.println("Request received:");
				//System.out.println(Arrays.toString(requestBuffer));
				
				//Split byte stream
				int offset = 0;
				//Get the command byte
				int cmd = ByteOrder.leb2int(requestBuffer, offset, offset + CMD_SIZE);
				offset += CMD_SIZE;
				System.out.println("cmd: " + cmd);

				//Get the key bytes
				//NOTE: As stated by Matei in class, assume that client is responsible for providing hashed keys so not re-hashing here.
				key = Arrays.copyOfRange(requestBuffer, offset, offset + KEY_SIZE);
				offset += KEY_SIZE;
				//System.out.println("convert key bytes to string:");
				keyStr = StringUtils.byteArrayToHexString(key);//Arrays.toString(key).replaceAll("(^\\[|\\]$)", "").replace(", ", "_");
				System.out.println("key: " + keyStr);
				
				//1 - put operation
				switch(cmd)
				{
				case 1:
					//Get the value bytes
					//Only do this if the command is put
					value = Arrays.copyOfRange(requestBuffer, offset, offset + VALUE_SIZE);
					offset += VALUE_SIZE;
					System.out.println("value: " + StringUtils.byteArrayToHexString(value));
					put(keyStr, value);
					break;
				case 2:
					//2 - get operation
					value = get(keyStr);					
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

