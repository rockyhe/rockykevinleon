package phase2Pack;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;

import phase2Pack.Exceptions.InexistentKeyException;
import phase2Pack.Exceptions.InternalKVStoreException;
import phase2Pack.Exceptions.OutOfSpaceException;
import phase2Pack.Exceptions.UnrecognizedCmdException;
import phase2Pack.nio.Dispatcher;

public class ProcessRequest implements Runnable
{
    // Constants
    private static final int CMD_SIZE = 1;
    private static final int KEY_SIZE = 32;
    private static final int VALUE_SIZE = 1024;
    private static final int ERR_SIZE = 1;

    private SocketChannel socketChannel;
    private SelectionKey handle;
    private Selector selector;
    private ConsistentHashRing ring;
    private KVStore kvStore;

    private byte errCode = 0x00; // Set default errCode to 0x00, so we can assume that operation is successful unless explicitly changed

    public ProcessRequest(SocketChannel socketChannel, SelectionKey handle, Selector demultiplexer, ConsistentHashRing ring, KVStore kvStore)
    {
        this.socketChannel = socketChannel;
        this.handle = handle;
        this.selector = demultiplexer;
        this.ring = ring;
        this.kvStore = kvStore;
    }

    public void run()
    {
        try {
            // Read the command byte
            byte[] commandBytes = new byte[CMD_SIZE];
            receiveBytesNIO(commandBytes);
            int cmd = ByteOrder.leb2int(commandBytes, 0, CMD_SIZE);

            byte[] key = null;
            byte[] value = null;
            switch (cmd)
            {
            case 1: // Put command
                // Get the key bytes
                key = new byte[KEY_SIZE];
                receiveBytesNIO(key);
                // Get the value bytes (only do this if the command is put)
                value = new byte[VALUE_SIZE];
                receiveBytesNIO(value);
                put(key, value);
                break;
            case 2: // Get command
                // Get the key bytes
                key = new byte[KEY_SIZE];
                receiveBytesNIO(key);
                // Store get result into value byte array
                value = new byte[VALUE_SIZE];
                value = get(key);
                break;
            case 3: // Remove command
                // Get the key bytes
                key = new byte[KEY_SIZE];
                receiveBytesNIO(key);
                remove(key);
                break;
            case 4: // Shutdown command
                shutdown();
                break;
            case 101: // Put to replica command
                key = new byte[KEY_SIZE];
                receiveBytesNIO(key);
                value = new byte[VALUE_SIZE];
                receiveBytesNIO(value);
                putToReplica(key, value);
            case 103: // Remove from replica command
                key = new byte[KEY_SIZE];
                receiveBytesNIO(key);
                removeFromReplica(key);
            case 255: // Gossip signal
                gossip();
                break;
            default: // Unrecognized command
                throw new UnrecognizedCmdException();
            }

            // Send the reply message to the client
            // Only send value if command was get and value returned wasn't null
            if (cmd == 2 && value != null)
            {
                byte[] combined = new byte[ERR_SIZE + VALUE_SIZE];
                System.arraycopy(new byte[] { errCode }, 0, combined, 0, ERR_SIZE);
                System.arraycopy(value, 0, combined, ERR_SIZE, VALUE_SIZE);
                sendBytesNIO(combined);
            }
            else
            {
                sendBytesNIO(new byte[] { errCode });
                // If command was shutdown, then close the application after sending the reply
                if (cmd == 4)
                {
                    System.exit(0);
                }
            }
        } catch (InexistentKeyException e) {
            System.out.println("inexisted key");
            sendBytesNIO(new byte[] {0x01});
        } catch (OutOfSpaceException e) {
            System.out.println("Out Of Space");
            sendBytesNIO(new byte[] {0x02});
            //TODO: Figure out where to throw this
            //        } catch (SystemOverloadException e){
            //            System.out.println("System Overload");
            //            sendBytes(new byte[] {0x03});
        } catch (InternalKVStoreException e){
            System.out.println("Internal KVStore");
            sendBytesNIO(new byte[] {0x04});
        } catch (UnrecognizedCmdException e){
            System.out.println("Unrecognized Cmd");
            sendBytesNIO(new byte[] {0x05});
        } catch (Exception e){
            System.out.println("internal server error");
            e.printStackTrace();
        }
    }


    public void put(byte[] key, byte[] value) throws OutOfSpaceException, InternalKVStoreException
    {
        // Re-hash the key using our hash function so it's consistent
        String rehashedKeyStr = ConsistentHashRing.getHash(StringUtils.byteArrayToHexString(key));

        // Get the primary partition for the given key
        Map.Entry<String, Node> primary = ring.getPrimary(rehashedKeyStr);

        // Check if this node is the primary partition for the hash key, or if we need to do a remote call

        if (primary.getValue().Equals(ring.localHost))
        {
            kvStore.put(rehashedKeyStr, value);
            try {
                // System.out.println("before backup");
                updateReplicas(key, value);
                // System.out.println("after backup");
            } catch (IOException e) {
                e.printStackTrace();
                System.out.println("Error updating replicas");
            }
        }
        else
        {
            // System.out.println("Forwarding put command!");
            forward(primary.getValue(), 1, key, value);
        }
    }

    public byte[] get(byte[] key) throws InexistentKeyException, InternalKVStoreException
    {
        // Re-hash the key using our hash function so it's consistent
        String rehashedKeyStr = ConsistentHashRing.getHash(StringUtils.byteArrayToHexString(key));

        try {
            return kvStore.get(rehashedKeyStr);
        } catch (InexistentKeyException e) {
            // If key doesn't exist on this node's store
            // Get the primary partition for the given key
            Map.Entry<String, Node> primary = ring.getPrimary(rehashedKeyStr);

            // Check if this node is the primary partition for the hash key (so we know to forward to successors)
            if (primary.getValue().Equals(ring.localHost))
            {
                // Iterate through each replica, by getting the successor list belonging to this partition, and check for key
                ArrayList<String> successors = ring.getSuccessors(primary.getKey());
                byte[] replyFromReplica = new byte[VALUE_SIZE];

                for (String nextSuccessor : successors)
                {
                    // If a replica returns a value, then return that as the result
                    System.out.println("Forwarding get command to replica");
                    replyFromReplica = forward(ring.getNodeForPartition(nextSuccessor), 2, key, null);
                    if (replyFromReplica != null)
                    {
                        return replyFromReplica;
                    }
                }
            }
            // Otherwise only route the command if this node is not one of the successors of the primary
            else if (!ring.isSuccessor(primary))
            {
                // Otherwise route to node that should contain
                System.out.println("Routing get command!");
                return forward(primary.getValue(), 2, key, null);
            }

            // If the key doesn't exist of any of the replicas, then key doesn't exist
            throw new InexistentKeyException();
        }
    }

    public void remove(byte[] key) throws InexistentKeyException, InternalKVStoreException
    {
        // Re-hash the key using our hash function so it's consistent
        String rehashedKeyStr = ConsistentHashRing.getHash(StringUtils.byteArrayToHexString(key));

        // Get the node responsible for the partition with first hashed value that is greater than or equal to the key (i.e. clockwise on the ring)
        Map.Entry<String, Node> primary = ring.getPrimary(rehashedKeyStr);

        // Check if this node is the primary partition for the hash key, or if we need to do a remote call

        if (primary.getValue().Equals(ring.localHost))
        {
            kvStore.remove(rehashedKeyStr);
            try {
                // System.out.println("before backup");
                updateReplicas(key, null);
                // System.out.println("after backup");
            } catch (IOException e) {
                e.printStackTrace();
                System.out.println("Error updating replicas");
            }
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
        int index = ring.getMembership().indexOf(ring.localHost);
        if (index >= 0)
        {
            Node self = ring.getMembership().get(index);
            self.online = false;
            self.t = new Timestamp(0);
        }
    }

    public void gossip()
    {
        for (Node node : ring.getMembership())
        {
            // System.out.println("client sock: "+clntSock.getInetAddress().getHostName().toString());
            if (node.Equals(ring.localHost))
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

    public void putToReplica(byte[] key, byte[] value)
    {
        System.out.println("Put to replica: " + StringUtils.byteArrayToHexString(key));
        // Convert key bytes to string
        String keyStr = StringUtils.byteArrayToHexString(key);
        // Re-hash the key using our hash function so it's consistent
        String rehashedKeyStr = ConsistentHashRing.getHash(keyStr);
        try {
            kvStore.put(rehashedKeyStr, value);
        } catch (Exception e) {
            //NOTE: Do nothing if put to replica fails, eventual consistency
        }
    }

    public void removeFromReplica(byte[] key)
    {
        System.out.println("Remove from replica: " + StringUtils.byteArrayToHexString(key));
        // Convert key bytes to string
        String keyStr = StringUtils.byteArrayToHexString(key);
        // Re-hash the key using our hash function so it's consistent
        String rehashedKeyStr = ConsistentHashRing.getHash(keyStr);
        try {
            kvStore.remove(rehashedKeyStr);
        } catch (Exception e) {
            //NOTE: Do nothing if remove from replica fails, eventual consistency
        }
    }

    private void updateReplicas(byte[] key, byte[] value) throws IOException
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
        String rehashedKeyStr = ConsistentHashRing.getHash(StringUtils.byteArrayToHexString(key));
        // Get the id of the primary partition
        Map.Entry<String, Node> primary = ring.getPrimary(rehashedKeyStr);

        Socket socket = null;
        // Get the successor list of the primary partition so we know where to place the replicas
        for (String nextSuccessor : ring.getSuccessors(primary.getKey()))
        {
            // NOTE: What happens if we try to connect to a successor that happens to be offline at this time?
            // check if sendBytes is successful, if not, loop to next on the successor list
            System.out.println("replicate to " + ring.getNodeForPartition(nextSuccessor).hostname);
            socket = new Socket(ring.getNodeForPartition(nextSuccessor).hostname, Server.PORT);
            sendBytes(socket, sendBuffer);
        }
    }

    private byte[] forward(Node remoteNode, int cmd, byte[] key, byte[] value) throws InternalKVStoreException
    {
        System.out.println("Forwarding to " + remoteNode.hostname.toString());

        try
        {
            Socket socket = new Socket(remoteNode.hostname, Server.PORT);
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
            int index = ring.getMembership().indexOf(remoteNode);
            ring.getMembership().get(index).online = false;
            ring.getMembership().get(index).t = new Timestamp(0);
            // System.out.println(membership.get(index).address.getHostName().toString() + " left");

            // Just set to internal KVStore error if we fail to connect to the node we want to forward to
            throw new InternalKVStoreException();
        }
        return null;
    }

    private void receiveBytesNIO(byte[] dest) throws IOException
    {
        ByteBuffer buffer = ByteBuffer.allocate(dest.length);
        while (buffer.hasRemaining())
        {
            socketChannel.read(buffer);
        }
        buffer.flip();
        buffer.get(dest);
        buffer.clear();
    }

    private void sendBytesNIO(byte[] src)
    {
        Dispatcher.sendBytesNIO(handle, src);
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
}
