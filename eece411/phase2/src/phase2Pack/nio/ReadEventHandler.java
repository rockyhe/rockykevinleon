package phase2Pack.nio;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.ByteBuffer;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import phase2Pack.ByteOrder;
import phase2Pack.ConsistentHashRing;
import phase2Pack.KVStore;
import phase2Pack.ProcessRequest;
import phase2Pack.Exceptions.SystemOverloadException;
import phase2Pack.enums.Commands;
import phase2Pack.Exceptions.UnrecognizedCmdException;


/*
 * Read event handler for NIO, reactor pattern
 * Reference: http://chamibuddhika.wordpress.com/2012/08/11/io-demystified/
 */
public class ReadEventHandler implements EventHandler
{
    // Constants
    private static final int MAX_NUM_CLIENTS = 250;
    private static final int BACKLOG_SIZE = 250;
    private static final int CMD_SIZE = 1;
    private static final int KEY_SIZE = 32;
    private static final int VALUE_SIZE = 1024;

    // Private members
    private ThreadPoolExecutor threadPool;
    private Selector selector;
    private ConsistentHashRing ring;
    private KVStore kvStore;

    public ReadEventHandler(Selector selector, ConsistentHashRing ring, KVStore kvStore)
    {
        this.selector = selector;
        this.ring = ring;
        this.kvStore = kvStore;

        // Create a fixed thread pool since we'll have at most MAX_NUM_CLIENTS concurrent threads
        this.threadPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(MAX_NUM_CLIENTS);
    }

    @Override
    public void handleEvent(SelectionKey handle) throws SystemOverloadException, UnrecognizedCmdException, Exception
    {
        SocketChannel socketChannel = (SocketChannel) handle.channel();
        // Process the event on a thread
        if (threadPool.getQueue().size() < BACKLOG_SIZE)
        {
            byte[] commandBytes = new byte[CMD_SIZE];
            byte[] key = new byte[KEY_SIZE];
            byte[] value = new byte[VALUE_SIZE];

            receiveBytesNIO(socketChannel, commandBytes);
            int cmdInt = ByteOrder.leb2int(commandBytes, 0, CMD_SIZE);

            if (cmdInt != 0)
            {
                Commands command = Commands.fromInt(cmdInt);
                switch (command)
                {
                case PUT: case PUT_TO_REPLICA:
                    receiveBytesNIO(socketChannel,key);
                    receiveBytesNIO(socketChannel,value);
                    break;
                case GET: case REMOVE: case REMOVE_FROM_REPLICA:
                    receiveBytesNIO(socketChannel,key);
                    break;
                case GOSSIP:
                    break;
                default:
                    break;
                }
                threadPool.execute(new ProcessRequest(socketChannel, handle, selector, ring, kvStore,commandBytes,key,value));
            }
        }
        // If the thread pool queue reaches the max backlog size, then throw a system overload error
        else
        {
            throw new SystemOverloadException();
        }
    }

    private void receiveBytesNIO(SocketChannel socketChannel, byte[] dest) throws IOException
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
}
