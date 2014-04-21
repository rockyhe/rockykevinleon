package phase2Pack;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

public class ProcessRequest implements Runnable
{
    // Constants
    private static final int CMD_SIZE = 1;
    private static final int KEY_SIZE = 32;
    private static final int VALUE_SIZE = 1024;
    private static final int ERR_SIZE = 1;

    private SocketChannel socketChannel;
    private SelectionKey handle;
    private Selector demultiplexer;
    private KVStore kvStore;

    private byte errCode = 0x00; // Set default errCode to 0x00, so we can assume that operation is successful unless explicitly changed

    public ProcessRequest(SocketChannel socketChannel, SelectionKey handle, Selector demultiplexer, KVStore kvStore)
    {
        this.socketChannel = socketChannel;
        this.handle = handle;
        this.demultiplexer = demultiplexer;
        this.kvStore = kvStore;
    }

    public void run()
    {
        try {
            // Read the command byte
            byte[] commandBytes = new byte[CMD_SIZE];
            receiveBytes(commandBytes);
            int cmd = ByteOrder.leb2int(commandBytes, 0, CMD_SIZE);

            // NOTE: As stated by Matei in class, assume that client is responsible for providing hashed keys so not necessary to perform re-hashing.
            byte[] key = null;
            byte[] value = null;
            switch (cmd)
            {
            case 1: // Put command
                // Get the key bytes
                key = new byte[KEY_SIZE];
                receiveBytes(key);
                // Get the value bytes (only do this if the command is put)
                value = new byte[VALUE_SIZE];
                receiveBytes(value);
                kvStore.put(key, value);
                break;
            case 2: // Get command
                // Get the key bytes
                key = new byte[KEY_SIZE];
                receiveBytes(key);
                // Store get result into value byte array
                value = new byte[VALUE_SIZE];
                value = kvStore.get(key);
                break;
            case 3: // Remove command
                // Get the key bytes
                key = new byte[KEY_SIZE];
                receiveBytes(key);
                kvStore.remove(key);
                break;
            case 4: // shutdown command
                kvStore.shutdown();
                break;
            case 101: // put to replica
                key = new byte[KEY_SIZE];
                receiveBytes(key);
                value = new byte[VALUE_SIZE];
                receiveBytes(value);
                kvStore.putToReplica(key, value);
            case 103: // remove from replica
                key = new byte[KEY_SIZE];
                receiveBytes(key);
                kvStore.removeFromReplica(key);
            case 255: // gossip signal
                kvStore.gossip();
                break;
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
                sendBytes(combined);
            }
            else
            {
                sendBytes(new byte[] { errCode });
                // If command was shutdown, then close the application after sending the reply
                if (cmd == 4)
                {
                    System.exit(0);
                }
            }
        } catch (Exception e) {
            System.out.println("Internal Server Error!");
            e.printStackTrace();
        }
    }

    private void receiveBytes(byte[] dest) throws IOException
    {
        ByteBuffer buffer = ByteBuffer.allocate(dest.length);
        socketChannel.read(buffer);
        buffer.flip();
        buffer.get(dest);
        buffer.clear();
    }

    private void sendBytes(byte[] src) throws IOException
    {
        socketChannel.register(demultiplexer, SelectionKey.OP_WRITE, ByteBuffer.wrap(src));
    }
}
