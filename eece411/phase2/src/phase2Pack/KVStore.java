package phase2Pack;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.security.MessageDigest;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class KVStore implements Runnable
{
	// Class for node info
	public static class Node
	{
		public InetSocketAddress address;
		public boolean online;
		public Timestamp t = new Timestamp(new Date().getTime());
		public boolean rejoin = false;

		Node(InetSocketAddress addr, boolean alive)
		{
			this.address = addr;
			this.online = alive;
		}

		public boolean Equals(InetAddress addr)
		{
			if (addr != null)
			{
				if (addr.getHostName().equals(this.address.getHostName()))
				{
					return true;
				}
			}
			return false;
		}

		public boolean Equals(Node node)
		{
			if (node != null)
			{
				if (this == node)
				{
					return true;
				}

				if (node.address.getHostName().equals(this.address.getHostName()))
				{
					return true;
				}
			}
			return false;
		}
	}

	// Constants
	private static final int CMD_SIZE = 1;
	private static final int KEY_SIZE = 32;
	private static final int VALUE_SIZE = 1024;
	private static final int ERR_SIZE = 1;
	private static final int KVSTORE_SIZE = 40000;

	// Private members
	private Socket clntSock;
	private ConcurrentHashMap<String, byte[]> store;
	private AtomicInteger clientCnt;
	private AtomicInteger shutdown;
	private ConcurrentSkipListMap<String, Node> nodeMap;
	private CopyOnWriteArrayList<Node> membership;
	private ConcurrentSkipListMap<String, ArrayList<String>> successorListMap;

	private byte errCode = 0x00; // Set default errCode to 0x00, so we can assume that operation is successful unless errCode is explicitly changed

	// Constructor
	KVStore(Socket clientSocket, ConcurrentHashMap<String, byte[]> KVstore, ConcurrentSkipListMap<String, Node> nodeMap, AtomicInteger concurrentClientCount, AtomicInteger shutdownFlag, CopyOnWriteArrayList<Node> membershipList, ConcurrentSkipListMap<String, ArrayList<String>> successorListMap)
	{
		this.clntSock = clientSocket;
		this.store = KVstore;
		this.nodeMap = nodeMap;
		this.clientCnt = concurrentClientCount;
		this.shutdown = shutdownFlag;
		this.membership = membershipList;
		this.successorListMap = successorListMap;
	}

	/**
	 * Puts the given value into the store, mapped to the given key. If there is already a value corresponding to the key, then the value is overwritten. If the number of key-value pairs is KVSTORE_SIZE, the store returns out of space error.
	 */
	private void put(byte[] key, byte[] value) throws IOException // Propagate the exceptions to main
	{
		// Re-hash the key using our hash function so it's consistent
		String rehashedKeyStr = getHash(StringUtils.byteArrayToHexString(key));

		// Get the node responsible for the partition with first hashed value that is greater than or equal to the key (i.e. clockwise on the ring)
		Map.Entry<String, Node> primary = getNodeEntryForHash(rehashedKeyStr);

		// Check if this node is the primary partition for the hash key, or if we need to do a remote call
		if (primary.getValue().Equals(clntSock.getLocalAddress()))
		{
			if (store.size() < KVSTORE_SIZE)
			{
				store.put(rehashedKeyStr, value);
				//System.out.println("before backup");
				updateReplicas(key, value);
				//System.out.println("after backup");
			}
			else
			{
				errCode = 0x02;
			}
		}
		else
		{
			//System.out.println("Forwarding put command!");
			forward(primary.getValue(), 1, key, value);
		}
	}

	/**
	 * Returns the value associated with the given key. If there is no such key in the store, the store returns key not found error.
	 */
	private byte[] get(byte[] key) throws IOException // Propagate the exceptions to main
	{
		// Re-hash the key using our hash function so it's consistent
		String rehashedKeyStr = getHash(StringUtils.byteArrayToHexString(key));

		// If key doesn't exist on this node's local store
		if (!store.containsKey(rehashedKeyStr))
		{
            System.out.println("I dont have the key");
			// Get the node responsible for the partition with first hashed value that is greater than or equal to the key (i.e. clockwise on the ring)
			Map.Entry<String, Node> primary = getNodeEntryForHash(rehashedKeyStr);

			// Check if this node is the primary partition for the hash key (so we know to forward to successors)
            System.out.println("primary = "+clntSock.getLocalAddress().toString());
			if (primary.getValue().Equals(clntSock.getLocalAddress()))
			{
				// Iterate through each replica, by getting the successor list belonging to this partition, and check for key
				ArrayList<String> successors = successorListMap.get(primary.getKey());
				byte[] replyFromReplica = new byte[VALUE_SIZE];
      
				for (String nextSuccessor : successors)
				{
					// If a replica returns a value, then return that as the result
					replyFromReplica = forward(nodeMap.get(nextSuccessor), 2, key, null);
					if (replyFromReplica != null)
					{
						return replyFromReplica;
					}
				}
			}
			// Otherwise only route the command if this node is neither the primary partition nor one of the replicas of the primary
			else if (!isReplica(primary))
			{
				// Otherwise route to node that should contain
				// System.out.println("Forwarding get command!");
				return forward(primary.getValue(), 2, key, null);
			}

			// If the key doesn't exist of any of the replicas, then key doesn't exist
			errCode = 0x01;
			return null;
		}
		// System.out.println("Get command succeeded!");
		return store.get(rehashedKeyStr);
	}

	private boolean isReplica(Map.Entry<String, Node> primary)
	{
		ArrayList<String> successors = successorListMap.get(primary.getKey());
		for (String successor : successors)
		{
			if (nodeMap.get(successor).Equals(clntSock.getLocalAddress()))
			{
				return true;
			}
		}
		return false;
	}

	/**
	 * Removes the value associated with the given key. If there is no such key in the store, the store returns key not found error.
	 */
	private void remove(byte[] key) throws IOException // Propagate the exceptions to main
	{
		// Re-hash the key using our hash function so it's consistent
		String rehashedKeyStr = getHash(StringUtils.byteArrayToHexString(key));

		// Get the node responsible for the partition with first hashed value that is greater than or equal to the key (i.e. clockwise on the ring)
		Map.Entry<String, Node> primary = getNodeEntryForHash(rehashedKeyStr);

		// Check if this node is the primary partition for the hash key, or if we need to do a remote call
		if (primary.getValue().Equals(clntSock.getLocalAddress()))
		{
			if (!store.containsKey(rehashedKeyStr))
			{
				errCode = 0x01; // Key doesn't exist on primary
			}
			else
			{
				store.remove(rehashedKeyStr);
				// System.out.println("Remove command succeeded!");
			}

			//System.out.println("before backup");
			updateReplicas(key, null);
			//System.out.println("after backup");
		}
		else
		{
			// System.out.println("Forwarding remove command!");
			forward(primary.getValue(), 3, key, null);
		}
	}

	private void updateReplicas(byte[] key, byte[] value) throws IOException // Propagate the exceptions to main
	{
		byte[] sendBuffer;
		if (value != null)
		{
            //update existing key or put a new key
			sendBuffer = new byte[CMD_SIZE + KEY_SIZE + VALUE_SIZE];
			ByteOrder.int2leb(101, sendBuffer, 0); // Command byte - 1 byte
			System.arraycopy(key, 0, sendBuffer, CMD_SIZE, KEY_SIZE); // Key bytes - 32 bytes
			System.arraycopy(value, 0, sendBuffer, CMD_SIZE + KEY_SIZE, VALUE_SIZE); // Value bytes - 1024 bytes
		}
		else
		{
            //get value of existing key
			sendBuffer = new byte[CMD_SIZE + KEY_SIZE];
			ByteOrder.int2leb(103, sendBuffer, 0); // Command byte - 1 byte
			System.arraycopy(key, 0, sendBuffer, CMD_SIZE, KEY_SIZE); // Key bytes - 32 bytes
		}

		// Re-hash the key using our hash function so it's consistent
		String rehashedKeyStr = getHash(StringUtils.byteArrayToHexString(key));
		// Get the id of the primary partition
		Map.Entry<String, Node> primary = getNodeEntryForHash(rehashedKeyStr);

		Socket socket = null;
		// Get the successor list of the primary partition so we know where to place the replicas
		ArrayList<String> successors = successorListMap.get(primary.getKey());
		for (String nextSuccessor : successors)
		{
			//NOTE: What happens if we try to connect to a successor that happens to be offline at this time?
            //check if sendBytes is successful, if not, loop to next on the successor list
            System.out.println("replicate to "+nodeMap.get(nextSuccessor).address.getHostName().toString());
			socket = new Socket(nodeMap.get(nextSuccessor).address.getHostName(), nodeMap.get(nextSuccessor).address.getPort());
			sendBytes(socket, sendBuffer);
        }
	}

	private byte[] forward(Node remoteNode, int cmd, byte[] key, byte[] value)
	{
		System.out.println("Forwarding to " + remoteNode.address.toString());
		// System.out.println("cmd: " + cmd);
		//System.out.println("key: " + StringUtils.byteArrayToHexString(key));
		// if (value != null)
		// {
		// System.out.println("value: " + StringUtils.byteArrayToHexString(value));
		// }

		// Open a socket connection to the other server
		// System.out.println("Creating socket");
		try {
			Socket socket = new Socket(remoteNode.address.getHostName(), remoteNode.address.getPort());
			// System.out.println("Connected to server: " + socket.getInetAddress().toString());

			// Route the message
			// If command is put, then increase request buffer size to include value bytes
			byte[] requestBuffer;
			if (cmd == 1)
			{
				requestBuffer = new byte[CMD_SIZE + KEY_SIZE + VALUE_SIZE];
				ByteOrder.int2leb(cmd, requestBuffer, 0); // Command byte - 1 byte
				System.arraycopy(key, 0, requestBuffer, CMD_SIZE, KEY_SIZE); // Key bytes - 32 bytes
				System.arraycopy(value, 0, requestBuffer, CMD_SIZE + KEY_SIZE, VALUE_SIZE); // Value bytes - 1024 bytes
			}
			else
			{
				requestBuffer = new byte[CMD_SIZE + KEY_SIZE];
				ByteOrder.int2leb(cmd, requestBuffer, 0); // Command byte - 1 byte
				System.arraycopy(key, 0, requestBuffer, CMD_SIZE, KEY_SIZE); // Key bytes - 32 bytes
			}

			// Send the encoded string to the server
			// System.out.println("Forwarding request");
			// System.out.println("Request buffer: " + StringUtils.byteArrayToHexString(requestBuffer));
			sendBytes(socket, requestBuffer);

			// Get the return message from the server
			// Get the error code byte
			byte[] errorCode = new byte[ERR_SIZE];
			receiveBytes(socket, errorCode);
			// System.out.println("Received reply from forwarded request");
			errCode = errorCode[0];
			// System.out.println("Error Code: " + errorMessage(errCode));

			// If command was get and ran successfully, then get the value bytes
			if (cmd == 2 && errCode == 0x00)
			{
				byte[] getValue = new byte[VALUE_SIZE];
				receiveBytes(socket, getValue);
				// System.out.println("Value for GET: " + StringUtils.byteArrayToHexString(getValue));
				return getValue;
			}
		} catch (Exception e) {
			// System.out.println("Forwarding to a node that is offline!");
			errCode = 0x04; //Just set to internal KVStore error if we fail to connect to the node we want to forward to
			int index = membership.indexOf(remoteNode);
			membership.get(index).online = false;
			membership.get(index).t = new Timestamp(0);
			//System.out.println(membership.get(index).address.getHostName().toString() + " left");
			return null;
		}
		return null;
	}

	private void shutdown()
	{
		// Increment the shutdown flag to no longer accept incoming connections
		shutdown.getAndIncrement();

		// Update online status to false and timestamp to 0 of self node, so it propagates faster
		int index = membership.indexOf(clntSock.getInetAddress().getHostName());
		if (index >= 0)
		{
			Node self = membership.get(index);
			self.online = false;
			self.t = new Timestamp(0);
		}

		while (true)
		{
			// Wait until the only client left is the one initiating the shutdown command
			// i.e. all other existing client requests have finished
			// System.out.println("clientcnt: "+clientCnt.get());

			if (clientCnt.get() == 1)
			{
				break;
			}
		}
	}

	private void gossip()
	{
		for (Node node : membership)
		{
			// System.out.println("client sock: "+clntSock.getInetAddress().getHostName().toString());
			if (node.Equals(clntSock.getInetAddress()))
			{
				if (!node.online)
				{
					node.rejoin = true;
				}
				node.online = true;
				node.t = new Timestamp(new Date().getTime());
				// System.out.println("timestamp: "+onlineNodeList.get(onlineNodeList.indexOf(node)).t.toString());
				break;
			}
		}
	}

	private void replicatedWrite(byte[] key, byte[] value)
	{
		System.out.println("replicating: " + StringUtils.byteArrayToHexString(key));
		// Convert key bytes to string
		String keyStr = StringUtils.byteArrayToHexString(key);// Arrays.toString(key).replaceAll("(^\\[|\\]$)", "").replace(", ", "");
		// Re-hash the key using our hash function so it's consistent
		String rehashedKeyStr = getHash(keyStr);
		if (value != null)
		{
			store.put(rehashedKeyStr, value);
		}
		else
		{
			store.remove(rehashedKeyStr);
		}
	}

	public void run()
	{
		try {
			clientCnt.getAndIncrement();
			// Get the request message from the client
			// System.out.println("Request received:");
			// System.out.println("requestBuffer: " + StringUtils.byteArrayToHexString(requestBuffer));

			// Get the command byte
			byte[] command = new byte[CMD_SIZE];
			receiveBytes(clntSock, command);
			int cmd = ByteOrder.leb2int(command, 0, CMD_SIZE);
			// System.out.println("cmd: " + cmd);

			// NOTE: As stated by Matei in class, assume that client is responsible for providing hashed keys so not necessary to perform re-hashing.
			byte[] key = null;
			byte[] value = null;
			switch (cmd)
			{
			case 1: // Put command
				// Get the key bytes
				key = new byte[KEY_SIZE];
				receiveBytes(clntSock, key);
				// Get the value bytes (only do this if the command is put)
				value = new byte[VALUE_SIZE];
				receiveBytes(clntSock, value);
				// System.out.println("value: " + StringUtils.byteArrayToHexString(value));
				put(key, value);
				break;
			case 2: // Get command
				// Get the key bytes
				key = new byte[KEY_SIZE];
				receiveBytes(clntSock, key);
				// Store get result into value byte array
				value = new byte[VALUE_SIZE];
				value = get(key);
				break;
			case 3: // Remove command
				// Get the key bytes
				key = new byte[KEY_SIZE];
				receiveBytes(clntSock, key);
				remove(key);
				break;
			case 4: // shutdown command
				shutdown();
				break;
			case 254:// FIXME

			case 255: // gossip signal
				gossip();
				break;
			case 101: // write to replica
				key = new byte[KEY_SIZE];
				receiveBytes(clntSock, key);
				value = new byte[VALUE_SIZE];
				receiveBytes(clntSock, value);
				replicatedWrite(key, value);
			case 103: // remove from replica
				key = new byte[KEY_SIZE];
				receiveBytes(clntSock, key);
				replicatedWrite(key, null);
			default: // Unrecognized command
				errCode = 0x05;
				break;
			}

			// Send the reply message to the client
			// Only send value if command was get and value returned wasn't null
			if (cmd == 2 && value != null)
			{
				byte[] combined = new byte[ERR_SIZE + VALUE_SIZE];
				System.arraycopy(new byte[] { errCode }, 0, combined, 0, ERR_SIZE);
				System.arraycopy(value, 0, combined, ERR_SIZE, VALUE_SIZE);
				sendBytes(clntSock, combined);
			}
			else
			{
				sendBytes(clntSock, new byte[] { errCode });
				// If command was shutdown, then increment the flag after sending success reply
				// so that Server knows it's safe to shutdown
				if (cmd == 4)
				{
					shutdown.getAndIncrement();
				}
			}
			// System.out.println("--------------------");
		} catch (Exception e) {
			// If any exception happens, return internal KVStore error
			errCode = 0x04;
			try {
				sendBytes(clntSock, new byte[] { errCode });
			} catch (Exception e2) { } // If we get an exception trying to send reply for internal error then do nothing
		} finally {
			// Close the socket
			try {
				if (clntSock != null)
				{
					clntSock.close();
					clientCnt.getAndDecrement();
				}
			} catch (Exception e) {
				// If any exception happens, return internal KVStore error
				errCode = 0x04;
				try {
					sendBytes(clntSock, new byte[] { errCode });
				} catch (Exception e2) { } // If we get an exception trying to send reply for internal error then do nothing
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

	private Map.Entry<String, Node> getNodeEntryForHash(String hashedKey)
	{
		// Get the node responsible for the partition with first hashed value that is greater than or equal to the key (i.e. clockwise on the ring)
		// System.out.println("Hashed key string: " + hashedKey);
		Map.Entry<String, Node> entry = nodeMap.ceilingEntry(hashedKey);
		// If ceiling entry is null, then we've wrapped around the entire node ring, so set to first node
		if (entry == null)
		{
			// System.out.println("Setting entry to first entry");
			entry = nodeMap.firstEntry();
		}
		// System.out.println("Entry hash: " + entry.getKey());
		return entry;
	}

	public static String getHash(String msg)
	{
		String result = null;
		try {
			// Hash the id using SHA-256 to get a 32 byte hash
			// since the ring space is from 0 to (2^256)-1
			MessageDigest md = MessageDigest.getInstance("SHA-256");
			byte[] hash = md.digest(msg.getBytes("UTF-8"));
			result = StringUtils.byteArrayToHexString(hash);
		} catch (Exception e) {
			System.out.println("Error trying to get hash of string: " + msg);
		}
		return result;
	}

	public static String errorMessage(byte errCode)
	{
		switch (errCode)
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
