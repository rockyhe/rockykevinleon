package phase2Pack;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.security.MessageDigest;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;

import phase2Pack.nio.Dispatcher;

public class KVStore
{
    // Constants
    private static final String NODE_LIST_FILE = "nodeList.txt";
    // Make sure this value is larger than number of physical nodes
    // Since potential max nodes is 100, then use 100 * 100 = 10000
    private static final int NUM_PARTITIONS = 10000;
    private static final int REPLICATION_FACTOR = 3;
    private static final int KVSTORE_SIZE = 40000;
    private static final int CMD_SIZE = 1;
    private static final int KEY_SIZE = 32;
    private static final int VALUE_SIZE = 1024;
    private static final int ERR_SIZE = 1;

    // Private members
    private static String localHost;

    private ConcurrentHashMap<String, byte[]> store;

    private int partitionsPerNode;
    private CopyOnWriteArrayList<Node> membership;
    private ConcurrentSkipListMap<String, Node> nodeMap; // Sorted map for mapping hashed values to physical nodes
    private ConcurrentSkipListMap<String, ArrayList<String>> successorListMap; // Sorted map for mapping each partition to its successor partitions

    public KVStore(int port)
    {
        // Load the list of participating nodes and construct the membership list
        try
        {
            // Store the local host name for convenient access later
            localHost = InetAddress.getLocalHost().getHostName();

            Scanner s = new Scanner(new File(NODE_LIST_FILE));
            Node node;
            membership = new CopyOnWriteArrayList<Node>();
            while (s.hasNext())
            {
                String nodeName = s.next();
                node = new Node(new InetSocketAddress(nodeName, port), true);
                membership.add(node);
            }
            s.close();
        } catch (FileNotFoundException e) {
            System.out.println("Cannot find node list file!");
        } catch (UnknownHostException e) {
            System.out.println("Couldn't determine IP of local host!");
        } catch (Exception e) {
            System.out.println("Error loading node list!");
        }

        // Construct the initial ring by mapping the nodes to partitions
        constructRing();
        // displayRing();
        // verifyRing();
        // displaySuccessorListMap();
    }

    public CopyOnWriteArrayList<Node> getMembership()
    {
        return membership;
    }

    private void constructRing()
    {
        // Divide the hash space into NUM_PARTITIONS partitions
        // with each physical node responsible for (NUM_PARTITIONS / number of nodes) hash ranges
        // int partitionsPerNode = NUM_PARTITIONS / onlineNodeList.size();
        nodeMap = new ConcurrentSkipListMap<String, Node>();
        partitionsPerNode = NUM_PARTITIONS / membership.size();
        for (Node node : membership)
        {
            for (int i = 0; i < partitionsPerNode; ++i)
            {
                nodeMap.put(getHash(node.address.getHostName() + i), node);
            }
        }

        // Construct the successor list for each partition
        constructSuccessorLists();
    }

    // When constructing successor list, we may skip partitions to ensure only unique physical nodes
    private void constructSuccessorLists()
    {
        successorListMap = new ConcurrentSkipListMap<String, ArrayList<String>>();
        int numSuccessors = REPLICATION_FACTOR - 1;
        if (membership.size() <= numSuccessors)
        {
            // If the number of participating nodes is not larger than the backups desired,
            // then just set to number of backups to the number of participating nodes
            numSuccessors = membership.size() - 1;
        }

        // For each partition, construct its successor list
        // Ensure there are no duplicate physical nodes in the successor list
        HashMap<String, Node> successors;
        Iterator<Map.Entry<String, Node>> partitionIterator = nodeMap.entrySet().iterator();
        Map.Entry<String, Node> sourcePartition;

        while (partitionIterator.hasNext())
        {
            sourcePartition = partitionIterator.next();
            successors = new HashMap<String, Node>();

            Map.Entry<String, Node> lastSuccessor = sourcePartition;
            while (successors.size() < numSuccessors)
            {
                // Keep looking for the next successor if we already have a successor partition owned by the same physical node
                // or if the successor is owned by the same physical node that owns the current partition that we're generating successors for
                // This guarantees that the successors (and therefore the replicas) will be different physical nodes
                do
                {
                    lastSuccessor = nodeMap.higherEntry(lastSuccessor.getKey());
                    // If there are no high entries, then we're at the end of the ring, so wrap around
                    if (lastSuccessor == null)
                    {
                        lastSuccessor = nodeMap.firstEntry();
                    }

                } while (successors.values().contains(lastSuccessor.getValue()) || lastSuccessor.getValue().Equals(sourcePartition.getValue()));

                successors.put(lastSuccessor.getKey(), lastSuccessor.getValue());
            }

            successorListMap.put(sourcePartition.getKey(), new ArrayList<String>(successors.keySet()));
        }
    }

    public void put(byte[] key, byte[] value) throws IOException
    {
        // Re-hash the key using our hash function so it's consistent
        String rehashedKeyStr = getHash(StringUtils.byteArrayToHexString(key));

        // Get the node responsible for the partition with first hashed value that is greater than or equal to the key (i.e. clockwise on the ring)
        Map.Entry<String, Node> primary = getPrimary(rehashedKeyStr);

        // Check if this node is the primary partition for the hash key, or if we need to do a remote call
        if (primary.getValue().Equals(localHost))
        {
            if (store.size() < KVSTORE_SIZE)
            {
                store.put(rehashedKeyStr, value);
                // System.out.println("before backup");
                updateReplicas(key, value);
                // System.out.println("after backup");
            }
            else
            {
                errCode = 0x02;
            }
        }
        else
        {
            // System.out.println("Forwarding put command!");
            forward(primary.getValue(), 1, key, value);
        }
    }

    public byte[] get(byte[] key) throws IOException
    {
        // Re-hash the key using our hash function so it's consistent
        String rehashedKeyStr = getHash(StringUtils.byteArrayToHexString(key));

        // If key doesn't exist on this node's local store
        if (!store.containsKey(rehashedKeyStr))
        {
            System.out.println("I dont have the key");
            // Get the node responsible for the partition with first hashed value that is greater than or equal to the key (i.e. clockwise on the ring)
            Map.Entry<String, Node> primary = getPrimary(rehashedKeyStr);

            // Check if this node is the primary partition for the hash key (so we know to forward to successors)
            System.out.println("primary = " + localHost);
            if (primary.getValue().Equals(localHost))
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
            // Otherwise only route the command if this node is not one of the successors of the primary
            else if (!isSuccessor(primary))
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

    public void remove(byte[] key) throws IOException // Propagate the exceptions to main
    {
        // Re-hash the key using our hash function so it's consistent
        String rehashedKeyStr = getHash(StringUtils.byteArrayToHexString(key));

        // Get the node responsible for the partition with first hashed value that is greater than or equal to the key (i.e. clockwise on the ring)
        Map.Entry<String, Node> primary = getPrimary(rehashedKeyStr);

        // Check if this node is the primary partition for the hash key, or if we need to do a remote call
        if (primary.getValue().Equals(localHost))
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

            // System.out.println("before backup");
            updateReplicas(key, null);
            // System.out.println("after backup");
        }
        else
        {
            // System.out.println("Forwarding remove command!");
            forward(primary.getValue(), 3, key, null);
        }
    }

    public void shutdown()
    {
        // Increment the shutdown flag to no longer accept incoming connections
        Dispatcher.shutdown();

        // Update online status to false and timestamp to 0 of self node, so it propagates faster
        int index = membership.indexOf(localHost);
        if (index >= 0)
        {
            Node self = membership.get(index);
            self.online = false;
            self.t = new Timestamp(0);
        }
    }

    public void gossip()
    {
        for (Node node : membership)
        {
            // System.out.println("client sock: "+clntSock.getInetAddress().getHostName().toString());
            if (node.Equals(localHost))
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

    public void returnPartitions(int idx)
    {
        // foreach nodes in the nodeList
        // System.out.println("<<<<<<<<<<<<<<<<<<<<<<<rejoined node: "+onlineNodeList.get(idx).address.toString());
        for (Node node : membership)
        {

            // foreach partition in each node
            for (int i = 0; i < partitionsPerNode; ++i)
            {
                // if current partition's hash key's value (node) is the rejoin node
                if (node.Equals(membership.get(idx)))
                {
                    // replace it with the next node, or the first node
                    // System.out.println("rejoined node: "+onlineNodeList.get(idx).address.toString());
                    // System.out.println("hash key for rejoin node: "+KVStore.getHash(node.address.getHostName() + i).toString());

                    nodeMap.replace(getHash(node.address.getHostName() + i), membership.get(idx));
                }
            }
        }
    }

    public void takePartitions(int idx)
    {
        // get the node that is offline now
        // foreach nodes in the nodeList
        int j;

        // System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>offline  node: "+onlineNodeList.get(idx).address.toString());
        for (Node node : membership)
        {
            // foreach partition in each node
            for (int i = 0; i < partitionsPerNode; ++i)
            {
                j = 0;
                // if current partition's hash key's value (node) is the offline node
                if (nodeMap.get(getHash(node.address.getHostName() + i)).Equals(membership.get(idx)))
                {
                    // replace it with the next node, or the first node
                    // System.out.println("hash key for offline node: "+KVStore.getHash(node.address.getHostName() + i).toString());

                    if (idx < (membership.size() - 1))
                    {
                        j = idx + 1;
                    }
                    else
                    {
                        j = 0;
                    }

                    while (true)
                    {
                        if (membership.get(j).online)
                        {
                            nodeMap.replace(getHash(node.address.getHostName() + i), membership.get(j));
                            break;
                        }

                        if (j == (membership.size() - 1))
                        {
                            j = 0;
                        }
                        else
                        {
                            j++;
                        }
                    }
                }
            }
        }
    }

    private boolean isSuccessor(Map.Entry<String, Node> primary)
    {
        ArrayList<String> successors = successorListMap.get(primary.getKey());
        for (String successor : successors)
        {
            if (nodeMap.get(successor).Equals(localHost))
            {
                return true;
            }
        }
        return false;
    }

    private void updateReplicas(byte[] key, byte[] value) throws IOException // Propagate the exceptions to main
    {
        byte[] sendBuffer;
        if (value != null)
        {
            // update existing key or put a new key
            sendBuffer = new byte[CMD_SIZE + KEY_SIZE + VALUE_SIZE];
            ByteOrder.int2leb(101, sendBuffer, 0); // Command byte - 1 byte
            System.arraycopy(key, 0, sendBuffer, CMD_SIZE, KEY_SIZE); // Key bytes - 32 bytes
            System.arraycopy(value, 0, sendBuffer, CMD_SIZE + KEY_SIZE, VALUE_SIZE); // Value bytes - 1024 bytes
        }
        else
        {
            // get value of existing key
            sendBuffer = new byte[CMD_SIZE + KEY_SIZE];
            ByteOrder.int2leb(103, sendBuffer, 0); // Command byte - 1 byte
            System.arraycopy(key, 0, sendBuffer, CMD_SIZE, KEY_SIZE); // Key bytes - 32 bytes
        }

        // Re-hash the key using our hash function so it's consistent
        String rehashedKeyStr = getHash(StringUtils.byteArrayToHexString(key));
        // Get the id of the primary partition
        Map.Entry<String, Node> primary = getPrimary(rehashedKeyStr);

        Socket socket = null;
        // Get the successor list of the primary partition so we know where to place the replicas
        ArrayList<String> successors = successorListMap.get(primary.getKey());
        for (String nextSuccessor : successors)
        {
            // NOTE: What happens if we try to connect to a successor that happens to be offline at this time?
            // check if sendBytes is successful, if not, loop to next on the successor list
            System.out.println("replicate to " + nodeMap.get(nextSuccessor).address.getHostName().toString());
            socket = new Socket(nodeMap.get(nextSuccessor).address.getHostName(), nodeMap.get(nextSuccessor).address.getPort());
            sendBytes(socket, sendBuffer);
        }
    }

    private byte[] forward(Node remoteNode, int cmd, byte[] key, byte[] value)
    {
        System.out.println("Forwarding to " + remoteNode.address.toString());

        try
        {
            SocketChannel client;
            try {
                client = SocketChannel.open();

                System.out.println("connect to nio remote " + handle.isValid());
                client.configureBlocking(false);
                client.connect(new InetSocketAddress(host, KVStore.NIO_SERVER_PORT));
                ClientDispatcher.registerChannel(SelectionKey.OP_CONNECT, client,
                        handle, message, host);
            } catch (Exception e) {
                e.printStackTrace();
            }

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
        } catch (Exception e)
        {
            // System.out.println("Forwarding to a node that is offline!");
            errCode = 0x04; // Just set to internal KVStore error if we fail to connect to the node we want to forward to
            int index = membership.indexOf(remoteNode);
            membership.get(index).online = false;
            membership.get(index).t = new Timestamp(0);
            // System.out.println(membership.get(index).address.getHostName().toString() + " left");
            return null;
        }
        return null;
    }

    public void putToReplica(byte[] key, byte[] value)
    {
        System.out.println("replicating: " + StringUtils.byteArrayToHexString(key));
        // Convert key bytes to string
        String keyStr = StringUtils.byteArrayToHexString(key);
        // Re-hash the key using our hash function so it's consistent
        String rehashedKeyStr = getHash(keyStr);
        store.put(rehashedKeyStr, value);
    }

    public void removeFromReplica(byte[] key)
    {
        // Convert key bytes to string
        String keyStr = StringUtils.byteArrayToHexString(key);
        // Re-hash the key using our hash function so it's consistent
        String rehashedKeyStr = getHash(keyStr);
        store.remove(rehashedKeyStr);
    }

    private Map.Entry<String, Node> getPrimary(String hashedKey)
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
        try
        {
            // Hash the id using SHA-256 to get a 32 byte hash
            // since the ring space is from 0 to (2^256)-1
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] hash = md.digest(msg.getBytes("UTF-8"));
            result = StringUtils.byteArrayToHexString(hash);
        } catch (Exception e)
        {
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


    /*
     * For debugging purposes: print the number of partitions assigned to each physical node
     */
    private void verifyRing()
    {
        Map<Node, Integer> distribution = new HashMap<Node, Integer>();
        for (Node node : membership)
        {
            distribution.put(node, 0);
        }

        for (Map.Entry<String, Node> entry : nodeMap.entrySet())
        {
            Node node = entry.getValue();
            Integer count = distribution.get(node);
            distribution.put(node, count + 1);
        }

        for (Map.Entry<Node, Integer> node : distribution.entrySet())
        {
            System.out.println(node.getKey().address.getHostName() + " => " + node.getValue().toString());
        }
    }

    /*
     * For debugging purposes: print the mapping of hash value to physical node, for each partition
     */
    private void displayRing()
    {
        for (Map.Entry<String, Node> entry : nodeMap.entrySet())
        {
            String key = entry.getKey();
            InetSocketAddress addr = entry.getValue().address;
            // System.out.println(key + " => " + addr.toString() + " => " + entry.getValue().online);
            System.out.println(key + " => " + membership.indexOf(entry.getValue()));
        }
    }

    /*
     * For debugging purposes: print the successor list mapping of hash value to physical node, for each partition
     */
    private void displaySuccessorListMap()
    {
        for (Map.Entry<String, ArrayList<String>> entry : successorListMap.entrySet())
        {
            String key = entry.getKey();
            ArrayList<String> successors = entry.getValue();
            System.out.println(key + " => Successors:");
            for (String successor : successors)
            {
                System.out.println("\t" + successor + " => " + nodeMap.get(successor).address.toString());
            }
        }
    }
}
