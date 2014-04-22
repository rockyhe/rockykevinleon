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
import phase2Pack.Exceptions.SystemOverloadException;
import phase2Pack.Exceptions.UnrecognizedCmdException;
import phase2Pack.enums.Commands;
import phase2Pack.enums.ErrorCodes;
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
    private byte[]  commandBytes = new byte[CMD_SIZE];
    byte[] key = new byte[KEY_SIZE];
    byte[] value = new byte[VALUE_SIZE];

    public ProcessRequest(SocketChannel socketChannel, SelectionKey handle, Selector demultiplexer, ConsistentHashRing ring, KVStore kvStore,byte[] cmd,byte[] key,byte[] value)
    {
        this.socketChannel = socketChannel;
        this.handle = handle;
        this.selector = demultiplexer;
        this.ring = ring;
        this.kvStore = kvStore;
        this.commandBytes = cmd;
        this.key = key;
        this.value = value;
    }

    public void run()
    {
        try {
            // Read the command byte
            Commands cmd = Commands.fromInt(ByteOrder.leb2int(commandBytes, 0, CMD_SIZE));
            
            switch (cmd)
            {
            case PUT: // Put command
                put(key, value);
                break;
            case GET: // Get command
                value = get(key);
                break;
            case REMOVE: // Remove command
                remove(key);
                break;
            case SHUTDOWN: // Shutdown command
                shutdown();
                break;
            case PUT_TO_REPLICA: // Put to replica command
                putToReplica(key, value);
                break;
            case REMOVE_FROM_REPLICA: // Remove from replica command
                removeFromReplica(key);
                break;
            case GOSSIP: // Gossip signal
                gossip();
                break;
            default: // Unrecognized command
                throw new UnrecognizedCmdException();
            }


            // Send the reply message to the client
            // Only send value if command was get and value returned wasn't null
            if (cmd == Commands.GET && value != null)
            {
                byte[] combined = new byte[ERR_SIZE + VALUE_SIZE];
                System.arraycopy(new byte[] { ErrorCodes.SUCCESS.toByte() }, 0, combined, 0, ERR_SIZE);
                System.arraycopy(value, 0, combined, ERR_SIZE, VALUE_SIZE);
                sendBytesNIO(combined);
            }
            else
            {
                sendBytesNIO(new byte[] { ErrorCodes.SUCCESS.toByte() });
                // If command was shutdown, then close the application after sending the reply
                if (cmd == Commands.SHUTDOWN)
                {
                    System.exit(0);
                }
            }
        } catch (InexistentKeyException e) {
            System.out.println("Inexistent Key");
            sendBytesNIO(new byte[] { ErrorCodes.INEXISTENT_KEY.toByte() });
        } catch (OutOfSpaceException e) {
            System.out.println("Out Of Space");
            sendBytesNIO(new byte[] { ErrorCodes.OUT_OF_SPACE.toByte() });
        } catch (InternalKVStoreException e){
            System.out.println("Internal KVStore");
            sendBytesNIO(new byte[] { ErrorCodes.INTERNAL_KVSTORE.toByte() });
        } catch (UnrecognizedCmdException e){
            System.out.println("Unrecognized Command");
            sendBytesNIO(new byte[] { ErrorCodes.UNRECOGNIZED_COMMAND.toByte() });
        } catch (Exception e){
            sendBytesNIO(new byte[] { ErrorCodes.INTERNAL_KVSTORE.toByte() });
            System.out.println("Internal Server Error");
            e.printStackTrace();
        }
    }

    private void put(byte[] key, byte[] value) throws InexistentKeyException, OutOfSpaceException, SystemOverloadException, InternalKVStoreException, UnrecognizedCmdException
    {
        System.out.println("in put");
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
            forward(primary.getValue(), Commands.PUT, key, value);
        }
    }

    private byte[] get(byte[] key) throws InexistentKeyException, OutOfSpaceException, SystemOverloadException, InternalKVStoreException, UnrecognizedCmdException
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
                    System.out.println(ring.getNodeForPartition(nextSuccessor).hostname);
                    replyFromReplica = forward(ring.getNodeForPartition(nextSuccessor), Commands.GET, key, null);
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
                return forward(primary.getValue(), Commands.GET, key, null);
            }

            // If the key doesn't exist of any of the replicas, then key doesn't exist
            throw new InexistentKeyException();
        }
    }

    private void remove(byte[] key) throws InexistentKeyException, OutOfSpaceException, SystemOverloadException, InternalKVStoreException, UnrecognizedCmdException
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
            forward(primary.getValue(), Commands.REMOVE, key, null);
        }
    }

    private void shutdown()
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

    private void gossip()
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

    private void putToReplica(byte[] key, byte[] value)
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

    private void removeFromReplica(byte[] key)
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
            ByteOrder.int2leb(Commands.PUT_TO_REPLICA.getValue(), sendBuffer, 0); // Command byte - 1 byte
            System.arraycopy(key, 0, sendBuffer, CMD_SIZE, KEY_SIZE); // Key bytes - 32 bytes
            System.arraycopy(value, 0, sendBuffer, CMD_SIZE + KEY_SIZE, VALUE_SIZE); // Value bytes - 1024 bytes
        }
        else
        {
            // get value of existing key
            sendBuffer = new byte[CMD_SIZE + KEY_SIZE];
            ByteOrder.int2leb(Commands.REMOVE_FROM_REPLICA.getValue(), sendBuffer, 0); // Command byte - 1 byte
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

    private byte[] forward(Node remoteNode, Commands cmd, byte[] key, byte[] value) throws InexistentKeyException, OutOfSpaceException, SystemOverloadException, InternalKVStoreException, UnrecognizedCmdException
    {
        System.out.println("Forwarding to " + remoteNode.hostname);

        try {
            Socket socket = new Socket(remoteNode.hostname, Server.PORT);
            // System.out.println("Connected to server: " + socket.getInetAddress().toString());

            // Route the message
            // If command is put, then increase request buffer size to include value bytes
            byte[] requestBuffer;
            if (cmd == Commands.PUT)
            {
                requestBuffer = new byte[CMD_SIZE + KEY_SIZE + VALUE_SIZE];
                ByteOrder.int2leb(cmd.getValue(), requestBuffer, 0); // Command byte - 1 byte
                System.arraycopy(key, 0, requestBuffer, CMD_SIZE, KEY_SIZE); // Key bytes - 32 bytes
                System.arraycopy(value, 0, requestBuffer, CMD_SIZE + KEY_SIZE, VALUE_SIZE); // Value bytes - 1024 bytes
            }
            else
            {
                requestBuffer = new byte[CMD_SIZE + KEY_SIZE];
                ByteOrder.int2leb(cmd.getValue(), requestBuffer, 0); // Command byte - 1 byte
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
            ErrorCodes errCode = ErrorCodes.fromByte(errorCode[0]);
            // System.out.println("Error Code: " + errorMessage(errCode));

            // If command was get and ran successfully, then get the value bytes
            if (cmd == Commands.GET && errCode == ErrorCodes.SUCCESS)
            {
                byte[] getValue = new byte[VALUE_SIZE];
                receiveBytes(socket, getValue);
                // System.out.println("Value for GET: " + StringUtils.byteArrayToHexString(getValue));
                return getValue;
            }
            else
            {
                switch (errCode)
                {
                case SUCCESS:
                    break;
                case INEXISTENT_KEY:
                    throw new InexistentKeyException();
                case OUT_OF_SPACE:
                    throw new OutOfSpaceException();
                case SYSTEM_OVERLOAD:
                    throw new SystemOverloadException();
                case INTERNAL_KVSTORE:
                    throw new InternalKVStoreException();
                case UNRECOGNIZED_COMMAND:
                    throw new UnrecognizedCmdException();
                default:
                    throw new InternalKVStoreException();
                }
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
            //if(socketChannel.isConnected()){
            socketChannel.read(buffer);
            //}
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
