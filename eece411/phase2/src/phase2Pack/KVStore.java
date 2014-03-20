package phase2Pack;

import java.io.*;
import java.net.*;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.*;

public class KVStore implements Runnable {
	//Class for node info
	public static class Node
	{
		public InetSocketAddress address;
		public boolean online;

		Node(InetSocketAddress addr, boolean alive)
		{
			this.address = addr;
			this.online = alive;
		}
	}

	public static enum KVCommands
	{
		PUT (0x01),
		GET (0x02),
		REMOVE (0x03),
		SHUTDOWN (0x04);

		private static KVCommands[] values = null;
		private final int numVal;
		
		KVCommands(int numVal)
		{
			this.numVal = numVal;
		}
		
		public static KVCommands fromInt(int i)
		{
			if (KVCommands.values == null)
			{
				KVCommands.values = KVCommands.values();
			}
			return KVCommands.values[i];
		}
	}

	//Constants
	private static final int CMD_SIZE = 1;
	private static final int KEY_SIZE = 32;
	private static final int VALUE_SIZE = 1024;
	private static final int ERR_SIZE = 1;
	private static final int KVSTORE_SIZE = 40000;

	//Private members
	private Socket clntSock;
	private ConcurrentHashMap<String, byte[]> store;
	private AtomicInteger clientCnt;
	private AtomicInteger shutdown;
	private ConcurrentSkipListMap<String, Node> nodes;

	private byte errCode = 0x00; //Set default errCode to 0x00, so we can assume that operation is successful unless errCode is explicitly changed

	//Constructor
	KVStore(Socket clientSocket, ConcurrentHashMap<String,byte[]> KVstore, ConcurrentSkipListMap<String, Node> nodes, AtomicInteger concurrentClientCount, AtomicInteger shutdownFlag)
	{
		this.clntSock = clientSocket;
		this.store = KVstore;
		this.nodes = nodes;
		this.clientCnt = concurrentClientCount;
		this.shutdown = shutdownFlag;
	}

	/**
	 * Puts the given value into the store, mapped to the given key.
	 * If there is already a value corresponding to the key, then the value is overwritten.
	 * If the number of key-value pairs is KVSTORE_SIZE, the store returns out of space error.
	 */
	private void put(byte[] key, byte[] value) throws IOException //Propagate the exceptions to main
	{
		//Convert key bytes to string
		String keyStr = StringUtils.byteArrayToHexString(key);//Arrays.toString(key).replaceAll("(^\\[|\\]$)", "").replace(", ", "");
		//Re-hash the key using our hash function so it's consistent
		String rehashedKeyStr = getHash(keyStr);
		//System.out.println("key: " + keyStr);

		//Get the node with hashed value that is greater than or equal to the key
		//since each node stores keys up to its hashed value
		Map.Entry<String, Node> entry = nodes.ceilingEntry(rehashedKeyStr);
		//If ceiling entry is null, then we've wrapped around the entire node ring, so set to first node
		if (entry == null)
		{
			entry = nodes.firstEntry();
		}

		//Check if the node that should contain it is this one, or if we need to do a remote call
		if (entry.getValue().address.getHostName() == clntSock.getInetAddress().getHostName())
		{
			if (store.size() < KVSTORE_SIZE)
			{
				store.put(rehashedKeyStr, value);
				System.out.println("Put command succeeded!");
			}
			else
			{
				errCode = 0x02;
				System.out.println("Error 0x02!");
			}
		}
		else
		{
			System.out.println("Forwarding put command!");
			forward(entry.getValue(), KVCommands.PUT, key, value);
		}
	}

	/**
	 * Returns the value associated with the given key.
	 * If there is no such key in the store, the store returns key not found error.
	 */
	private byte[] get(byte[] key) throws IOException //Propagate the exceptions to main
	{
		//Convert key bytes to string
		String keyStr = StringUtils.byteArrayToHexString(key);//Arrays.toString(key).replaceAll("(^\\[|\\]$)", "").replace(", ", "");
		//Re-hash the key using our hash function so it's consistent
		String rehashedKeyStr = getHash(keyStr);
		//System.out.println("key: " + keyStr);

		//If key doesn't exist on this node's local store, then route to node that should contain it
		if (!store.containsKey(rehashedKeyStr))
		{
			//Get the node with hashed value that is greater than or equal to the key
			//since each node stores keys up to its hashed value
			Map.Entry<String, Node> entry = nodes.ceilingEntry(rehashedKeyStr);
			//If ceiling entry is null, then we've wrapped around the entire node ring, so set to first node
			if (entry == null)
			{
				entry = nodes.firstEntry();
			}

			//If the node that should contain it is this, then key doesn't exist
			if (entry.getValue().address.getHostName() == clntSock.getInetAddress().getHostName())
			{
				errCode = 0x01;
				System.out.println("Error 0x01!");
				return null;
			}
			System.out.println("Forwarding get command!");
			return forward(entry.getValue(), KVCommands.GET, key, null);
		}
		System.out.println("Get command succeeded!");
		return store.get(rehashedKeyStr);
	}

	/**
	 * Removes the value associated with the given key.
	 * If there is no such key in the store, the store returns key not found error.
	 */
	private void remove(byte[] key) throws IOException //Propagate the exceptions to main
	{
		//Convert key bytes to string
		String keyStr = StringUtils.byteArrayToHexString(key);//Arrays.toString(key).replaceAll("(^\\[|\\]$)", "").replace(", ", "");
		//Re-hash the key using our hash function so it's consistent
		String rehashedKeyStr = getHash(keyStr);
		//System.out.println("key: " + keyStr);

		if (!store.containsKey(rehashedKeyStr))
		{
			//Get the node with hashed value that is greater than or equal to the key
			//since each node stores keys up to its hashed value
			Map.Entry<String, Node> entry = nodes.ceilingEntry(rehashedKeyStr);
			//If ceiling entry is null, then we've wrapped around the entire node ring, so set to first node
			if (entry == null)
			{
				entry = nodes.firstEntry();
			}

			//If the node that should contain it is this, then key doesn't exist
			if (entry.getValue().address.getHostName() == clntSock.getInetAddress().getHostName())
			{
				errCode = 0x01;
				System.out.println("Error 0x01!");
			}
			else
			{
				System.out.println("Forwarding remove command!");
				forward(entry.getValue(), KVCommands.REMOVE, key, null);
			}
		}
		else
		{
			store.remove(rehashedKeyStr);
			System.out.println("Remove command succeeded!");
		}
	}

	private byte[] forward(Node remoteNode, KVCommands cmd, byte[] key, byte[] value) throws IOException //Propagate the exceptions to main
	{
		System.out.println("Forwarding to " + remoteNode.address.toString());
		
		if (!remoteNode.online)
		{
			errCode = 0x21;
			return null;
		}

		//Open a socket connection to the other server
		Socket socket = new Socket(remoteNode.address.getHostName(), remoteNode.address.getPort());
		System.out.println("Connected to server: " + socket.getInetAddress().toString());

		//Route the message
		//If command is put, then increase request buffer size to include value bytes
		byte[] requestBuffer;
		if (cmd == KVCommands.PUT)
		{
			requestBuffer = new byte[CMD_SIZE + KEY_SIZE + VALUE_SIZE];
			ByteOrder.int2leb(cmd.numVal, requestBuffer, 0); 	//Command byte - 1 byte
			System.arraycopy(key, 0, requestBuffer, CMD_SIZE, KEY_SIZE); //Key bytes - 32 bytes
			System.arraycopy(value, 0, requestBuffer, CMD_SIZE + KEY_SIZE, VALUE_SIZE); //Value bytes - 1024 bytes
		}
		else
		{
			requestBuffer = new byte[CMD_SIZE + KEY_SIZE];
			ByteOrder.int2leb(cmd.numVal, requestBuffer, 0); 	//Command byte - 1 byte
			System.arraycopy(key, 0, requestBuffer, CMD_SIZE, KEY_SIZE); //Key bytes - 32 bytes
		}

		//Send the encoded string to the server
		sendBytes(socket, requestBuffer);
		System.out.println("Forwarding request");

		//Get the return message from the server
		//Get the error code byte
		byte[] errorCode = new byte[ERR_SIZE];
		receiveBytes(socket, errorCode);
		System.out.println("Received reply from forwarded request");
		errCode = errorCode[0];
		System.out.println("Error Code: " + errorMessage(errCode));

		//If command was get and ran successfully, then get the value bytes
		if (cmd == KVCommands.GET && errCode == 0x00)
		{
			byte[] getValue = new byte[VALUE_SIZE];
			receiveBytes(socket, getValue);
			System.out.println("Value for GET: " + StringUtils.byteArrayToHexString(getValue));
			return getValue;
		}
		return null;
	}

	private void shutdown()
	{
		//make a flag for shutdown command
		//refuse all incomes
		shutdown.getAndIncrement();
		//check if current thread is 0
		//System.out.println("Shut");
		while (true)
		{
			if (clientCnt.get() == 1)
			{
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
			//System.out.println("Request received:");
			//System.out.println("requestBuffer: " + StringUtils.byteArrayToHexString(requestBuffer));

			//Get the command byte
			byte[] command = new byte[CMD_SIZE];
			receiveBytes(clntSock, command);
			KVCommands cmd = KVCommands.fromInt(ByteOrder.leb2int(command, 0, CMD_SIZE)); //Cast command int to enum
			//System.out.println("cmd: " + cmd);

			//NOTE: As stated by Matei in class, assume that client is responsible for providing hashed keys so not necessary to perform re-hashing.
			byte[] key = null;
			byte[] value = null;
			switch (cmd)
			{
			case PUT: //Put command
				//Get the key bytes
				key = new byte[KEY_SIZE];
				receiveBytes(clntSock, key);
				//Get the value bytes (only do this if the command is put)
				value = new byte[VALUE_SIZE];
				receiveBytes(clntSock, value);
				//System.out.println("value: " + StringUtils.byteArrayToHexString(value));
				put(key, value);
				break;
			case GET: //Get command
				//Get the key bytes
				key = new byte[KEY_SIZE];
				receiveBytes(clntSock, key);
				//Store get result into value byte array
				value = new byte[VALUE_SIZE];
				value = get(key);
				break;
			case REMOVE: //Remove command
				//Get the key bytes
				key = new byte[KEY_SIZE];
				receiveBytes(clntSock, key);
				remove(key);
				break;
			case SHUTDOWN: //shutdown command
				shutdown();
				break;
			default: //Unrecognized command
				errCode = 0x05;
				break;
			}

			//Send the reply message to the client
			//Only send value if command was get and value returned wasn't null
			if (cmd == KVCommands.GET && value != null)
			{
				byte[] combined = new byte[ERR_SIZE + VALUE_SIZE];
				System.arraycopy(new byte[] {errCode}, 0, combined, 0, ERR_SIZE);
				System.arraycopy(value, 0, combined, ERR_SIZE, VALUE_SIZE);
				sendBytes(clntSock, combined);
			}
			else
			{
				sendBytes(clntSock, new byte[] {errCode} );
			}
			//System.out.println("--------------------");
		} catch (Exception e) {
			//If any exception happens, return internal KVStore error
			errCode = 0x04;
			try {
				sendBytes(clntSock, new byte[] {errCode} );
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
					sendBytes(clntSock, new byte[] {errCode} );
				} catch (Exception e2) { } //If we get an exception trying to send reply for internal error then do nothing
			}
		}
	}

	private void receiveBytes(Socket srcSock, byte[] dest) throws IOException
	{
		InputStream in = srcSock.getInputStream();
		int totalBytesRcvd = 0;
		int bytesRcvd = 0;
		while (totalBytesRcvd < dest.length)
		{
			if ((bytesRcvd = in.read(dest, totalBytesRcvd, dest.length - totalBytesRcvd)) != -1)
			{
				totalBytesRcvd += bytesRcvd;
			}
		}
	}

	private void sendBytes(Socket destSock, byte[] src) throws IOException
	{
		OutputStream out = destSock.getOutputStream();
		out.write(src);
	}

	public static String getHash(String msg)
	{
		String result = null;
		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
			byte[] hash = md.digest(msg.getBytes("UTF-8"));
			result = StringUtils.byteArrayToHexString(hash);
		} catch (Exception e) {
			System.out.println("Error trying to get hash of string: " + msg);
		}
		return result;
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
		case 0x21:
			return "Node is offline";
		default:
			return "Error code not handled";
		}
	}
}

