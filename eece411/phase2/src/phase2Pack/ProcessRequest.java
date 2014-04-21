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
    private KVStore ring;

    public ProcessRequest(SocketChannel socketChannel, SelectionKey handle, Selector demultiplexer, KVStore ring)
    {
        this.socketChannel = socketChannel;
        this.handle = handle;
        this.demultiplexer = demultiplexer;
        this.ring = ring;
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
                ring.put(key, value);
                break;
            case 2: // Get command
                // Get the key bytes
                key = new byte[KEY_SIZE];
                receiveBytes(key);
                // Store get result into value byte array
                value = new byte[VALUE_SIZE];
                value = ring.get(key);
                break;
            case 3: // Remove command
                // Get the key bytes
                key = new byte[KEY_SIZE];
                receiveBytes(key);
                ring.remove(key);
                break;
            case 4: // shutdown command
                ring.shutdown();
                break;
            case 254:// FIXME

            case 255: // gossip signal
                gossip();
                break;
            case 101: // write to replica
                key = new byte[KEY_SIZE];
                receiveBytes(key);
                value = new byte[VALUE_SIZE];
                receiveBytes(value);
                replicatedWrite(key, value);
            case 103: // remove from replica
                key = new byte[KEY_SIZE];
                receiveBytes(key);
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
                sendBytes(combined);
            }
            else
            {
                sendBytes(new byte[] { errCode });
                // If command was shutdown, then increment the flag after sending success reply
                // so that Server knows it's safe to shutdown
                if (cmd == 4)
                {
                    shutdown.getAndIncrement();
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
