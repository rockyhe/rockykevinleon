package phase2Pack;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class KVStore implements Runnable {
	//Constants
	private static final int CMD_SIZE = 1;
	private static final int KEY_SIZE = 32;
	private static final int VALUE_SIZE = 1024;
	private static final int ERR_SIZE = 1;
	private static final int MIN_REQUEST_BUFFSIZE = CMD_SIZE + KEY_SIZE; //Min size of request message
	private static final int MIN_REPLY_BUFFSIZE = ERR_SIZE; //Min size of reply message

	//Private members
	private Socket clntSock;
	private ConcurrentHashMap<String, byte[]> store;
	private byte errCode = 0x00; //Set default errCode to 0x00, so we can assume that operation is successful unless errCode is explicitly changed

	//Constructor
	KVStore(Socket clientSocket, ConcurrentHashMap<String,byte[]> KVstore)
	{
		this.clntSock = clientSocket;
		this.store = KVstore;
	}

	/**
	 * Puts the given value into the store, mapped to the given key.
	 * If there is already a value corresponding to the key, then the value is overwritten.
	 */
	private void put(String key, byte[] value)
	{
		store.put(key, value);
	}

	/**
	 * Returns the value associated with the given key.
	 * If there is no such key in the store, the store returns key not found error.
	 */
	private byte[] get(String key)
	{
		byte[] result = new byte[VALUE_SIZE];
		if (!store.containsKey(key))
		{
			errCode = 0x01;
			return null;
		}
		else
		{
			result = store.get(key);
		}
		return result;
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

	public void run()
	{
		try {
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
			System.out.println("Request received:");
			System.out.println("requestBuffer: " + StringUtils.byteArrayToHexString(requestBuffer));

			//Track offset position for splitting byte stream
			int offset = 0;
			//Get the command byte
			int cmd = ByteOrder.leb2int(requestBuffer, offset, offset + CMD_SIZE);
			offset += CMD_SIZE;
			System.out.println("cmd: " + cmd);

			//Get the key bytes
			//NOTE: As stated by Matei in class, assume that client is responsible for providing hashed keys so no need to perform re-hashing here.
			byte[] key = new byte[KEY_SIZE];
			key = Arrays.copyOfRange(requestBuffer, offset, offset + KEY_SIZE);
			offset += KEY_SIZE;

			//Convert key bytes to string
			String keyStr;
			keyStr = StringUtils.byteArrayToHexString(key);//Arrays.toString(key).replaceAll("(^\\[|\\]$)", "").replace(", ", "_");
			System.out.println("key: " + keyStr);

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
				System.out.println("value: " + StringUtils.byteArrayToHexString(value));
				put(keyStr, value);
				break;
			case 2: //Get command
				value = get(keyStr); //Store into value byte array
				break;
			case 3: //Remove command
				remove(keyStr);
				break;
			default: //Unrecognized command
				errCode = 0x05;
				break;
			}

			//Send the reply message to the client
			System.out.println("Sending reply:");
			OutputStream out = clntSock.getOutputStream();
			byte[] replyBuffer = new byte[MIN_REPLY_BUFFSIZE];

			//Send errCode to client
			System.arraycopy(new byte[] {errCode}, 0, replyBuffer, 0, ERR_SIZE);
			System.out.println("errCode: " + StringUtils.byteArrayToHexString(replyBuffer) + " - " + errorMessage(errCode));
			out.write(replyBuffer);
			
			//Send value to client if command is get and the value returned from get isn't null
			if (cmd == 2 && value != null)
			{
				System.out.println("value: " + StringUtils.byteArrayToHexString(value));
				out.write(value);
			}
			clntSock.close();
			System.out.println("--------------------");
		} catch(IOException ioe) {
			System.out.println("IOException!!!");
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

