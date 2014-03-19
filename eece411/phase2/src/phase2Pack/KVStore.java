package phase2Pack;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.*;

public class KVStore implements Runnable {
	//Constants
	private static final int CMD_SIZE = 1;
	private static final int KEY_SIZE = 32;
	private static final int VALUE_SIZE = 1024;
	private static final int MIN_REQUEST_BUFFSIZE = CMD_SIZE + KEY_SIZE; //Min size of request message
	private static final int KVSTORE_SIZE = 40000;

	//Private members
	private Socket clntSock;
	private ConcurrentHashMap<String, byte[]> store;
	private byte errCode = 0x00; //Set default errCode to 0x00, so we can assume that operation is successful unless errCode is explicitly changed

	private AtomicInteger clientCnt;
	private AtomicInteger shutdown;

	//Constructor
	KVStore(Socket clientSocket, ConcurrentHashMap<String,byte[]> KVstore, AtomicInteger concurrentClientCount, AtomicInteger shutdownFlag)
	{
		this.clntSock = clientSocket;
		this.store = KVstore;
		this.clientCnt = concurrentClientCount;
		this.shutdown = shutdownFlag;
	}

	/**
	 * Puts the given value into the store, mapped to the given key.
	 * If there is already a value corresponding to the key, then the value is overwritten.
	 * If the number of key-value pairs is KVSTORE_SIZE, the store returns out of space error.
	 */
	private void put(String key, byte[] value)
	{
		if (store.size() < KVSTORE_SIZE)
		{
			store.put(key, value);
		}
		else
		{
			errCode = 0x02;
		}
	}

	/**
	 * Returns the value associated with the given key.
	 * If there is no such key in the store, the store returns key not found error.
	 */
	private byte[] get(String key)
	{
		if (!store.containsKey(key))
		{
			errCode = 0x01;
			return null;
		}
		else
		{
			return store.get(key);
		}
	}

	/**
	 * Removes the value associated with the given key.
	 * If there is no such key in the store, the store returns key not found error.
	 */
	private void remove(String key)
	{
		if (!store.containsKey(key))
		{
			errCode = 0x01;
		}
		else
		{
			store.remove(key);
		}
	}

	private void shutdown()
	{
		//make a flag for shutdown command
		//refuse all incomes
		shutdown.getAndIncrement();
		//check if current thread is 0
		//System.out.println("Shut");
		while(true){
			if(clientCnt.get() == 1){
				System.exit(0);
			}
		}
		//once it reaches 0, shutdown the program	
	}
	public void run()
	{
		try {
			clientCnt.getAndIncrement();
			//Get the request message from the client
			InputStream in = clntSock.getInputStream();
			int totalRequestBytesRcvd = 0;
			int requestBytesRcvd = 0;
			byte[] requestBuffer = new byte[MIN_REQUEST_BUFFSIZE]; //Get the minimum bytes
			while (totalRequestBytesRcvd < requestBuffer.length)
			{
				if ((requestBytesRcvd = in.read(requestBuffer, totalRequestBytesRcvd, requestBuffer.length - totalRequestBytesRcvd)) != -1)
				{
					totalRequestBytesRcvd += requestBytesRcvd;
				}
			}
			//System.out.println("Request received:");
			//System.out.println("requestBuffer: " + StringUtils.byteArrayToHexString(requestBuffer));

			//Track offset position for splitting byte stream0
			//Get the command byte
			int cmd = ByteOrder.leb2int(requestBuffer, 0, CMD_SIZE);
			//System.out.println("cmd: " + cmd);

			//Get the key bytes
			//NOTE: As stated by Matei in class, assume that client is responsible for providing hashed keys so no need to perform re-hashing here.
			byte[] key = new byte[KEY_SIZE];
			System.arraycopy(requestBuffer, CMD_SIZE, key, 0, KEY_SIZE); //Use System.arraycopy for better performance instead of Arrays.copyOfRange

			//Convert key bytes to string
			String keyStr = StringUtils.byteArrayToHexString(key);//Arrays.toString(key).replaceAll("(^\\[|\\]$)", "").replace(", ", "");
			//System.out.println("key: " + keyStr);

			byte[] value = new byte[VALUE_SIZE];
			switch(cmd)
			{
			case 1: //Put command
				//Get the value bytes (only do this if the command is put)
				int totalValueBytesRcvd = 0;
				int valueBytesRcvd = 0;
				while (totalValueBytesRcvd < value.length)
				{
					if ((valueBytesRcvd = in.read(value, totalValueBytesRcvd, value.length - totalValueBytesRcvd)) != -1)
					{
						totalValueBytesRcvd += valueBytesRcvd;
					}
				}
				//System.out.println("value: " + StringUtils.byteArrayToHexString(value));
				put(keyStr, value);
				break;
			case 2: //Get command
				value = get(keyStr); //Store into value byte array
				break;
			case 3: //Remove command
				remove(keyStr);
				break;
			case 4: //shutdown command
				shutdown();
				break;
			default: //Unrecognized command
				errCode = 0x05;
				break;
			}

			//Send the reply message to the client
			//Only send value if command was get and value returned wasn't null
			if (cmd == 2 && value != null)
			{
				sendErrCodeAndValue(value);
			}
			else
			{
				sendErrCode();
			}
			//System.out.println("--------------------");
		} catch (Exception e) {
			//If any exception happens, return internal KVStore error
			errCode = 0x04;
			try {
				sendErrCode();
			} catch (Exception e2) { } //If we get an exception trying to send reply for internal error then do nothing
		} finally {
			//Close the socket
			try {
				if (clntSock != null)
				{
					clntSock.close();
					clientCnt.getAndDecrement();
				}
			} catch (Exception e) {
				//If any exception happens, return internal KVStore error
				errCode = 0x04;
				try {
					sendErrCode();
				} catch (Exception e2) { } //If we get an exception trying to send reply for internal error then do nothing
			}
		}
	}

	private void sendErrCode() throws IOException
	{
		//System.out.println("errCode: " + errorMessage(errCode));
		//Send errCode to client
		//System.out.println("Sending reply:");
		OutputStream out = clntSock.getOutputStream();
		//System.out.println("errCode: " + StringUtils.byteArrayToHexString(replyBuffer) + " - " + errorMessage(errCode));
		out.write(new byte[] {errCode});
	}

	private void sendErrCodeAndValue(byte[] value) throws IOException
	{
		//Send errCode to client
		//System.out.println("Sending reply:");
		OutputStream out = clntSock.getOutputStream();
		//System.out.println("errCode: " + StringUtils.byteArrayToHexString(replyBuffer) + " - " + errorMessage(errCode));
		out.write(new byte[] {errCode});

		//Send value to client
		if (value != null)
		{
			//System.out.println("value: " + StringUtils.byteArrayToHexString(value));
			out.write(value);
		}
	}

	public static String errorMessage(byte errCode)
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
}

