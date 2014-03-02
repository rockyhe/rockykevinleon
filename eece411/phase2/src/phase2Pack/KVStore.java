package phase2Pack;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import com.matei.eece411.util.*;

public class KVStore implements Runnable {
	//Constants
	private static final int CMD_SIZE = 1;
	private static final int KEY_SIZE = 32;
	private static final int VALUE_SIZE = 1024;
	private static final int ERR_SIZE = 1;
	private static final int REQUEST_BUFFSIZE = CMD_SIZE + KEY_SIZE + VALUE_SIZE; //Size of request message
	private static final int REPLY_BUFFSIZE = ERR_SIZE + VALUE_SIZE; //Size of reply message

	//Private members
	private Socket clntSock;
	private ConcurrentHashMap<String, byte[]> store;
	private byte[] errCode = new byte[]{0x00}; //Set default errCode to 0x00, so we can assume that operation is successful unless errCode is explicitly changed

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
		byte[] result;
		if (!store.containsKey(key))
		{
			errCode[0] = 0x01;
			return null;
		}
		else
		{
			result = new byte[REPLY_BUFFSIZE];
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
			errCode[0] = 0x01;
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
			int totalRequestBytesRcvd = 0; //TODO: Do we want to explicitly check correct message size depending on the command?
			int requestBytesRcvd = 0;
			byte[] requestBuffer = new byte[REQUEST_BUFFSIZE];
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

			byte[] putValue = new byte[VALUE_SIZE];
			byte[] getValue = new byte[VALUE_SIZE];
			switch(cmd)
			{
			case 1: //Put command
				//Get the value bytes (only do this if the command is put)
				putValue = Arrays.copyOfRange(requestBuffer, offset, offset + VALUE_SIZE);
				offset += VALUE_SIZE;
				System.out.println("put value: " + StringUtils.byteArrayToHexString(putValue));

				put(keyStr, putValue);
				break;
			case 2: //Get command
				getValue = get(keyStr);
				break;
			case 3: //Remove command
				remove(keyStr);
				break;
			default: //Unrecognized command
				errCode[0] = 0x05;
				break;
			}

			//Send the reply message to the client
			System.out.println("Sending reply:");
			OutputStream out = clntSock.getOutputStream();
			byte[] replyBuffer = new byte[REPLY_BUFFSIZE];

			//Copy errCode into the reply buffer array
			System.arraycopy(errCode, 0, replyBuffer, 0, ERR_SIZE);
			System.out.println("errCode: " + StringUtils.byteArrayToHexString(errCode) + " - " + errorMessage(errCode[0]));
			//If command is get and the value returned from get isn't null, copy it into the reply buffer
			if (cmd == 2 && getValue != null)
			{
				System.arraycopy(getValue, 0, replyBuffer, ERR_SIZE, getValue.length);
				System.out.println("getValue: " + StringUtils.byteArrayToHexString(getValue));
			}

			System.out.println("replyBuffer: " + StringUtils.byteArrayToHexString(replyBuffer));
			out.write(replyBuffer);
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

