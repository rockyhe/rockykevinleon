package phase2Pack.nio;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.ByteBuffer;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.nio.BufferUnderflowException;

import java.util.Arrays;
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
            
            receiveBytesNIO(socketChannel,commandBytes);
            
            if(leb2int(commandBytes, 0, CMD_SIZE) != 0){
                Commands cmd = Commands.fromInt(leb2int(commandBytes, 0, CMD_SIZE));
                switch(cmd){
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
    
    
    private void receiveBytesNIO (SocketChannel socketChannel,byte[] dest) throws IOException
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
    
    private int leb2int(byte[] x, int offset, int n)
    throws IndexOutOfBoundsException, IllegalArgumentException {
        if (n<1 || n>4)
            throw new IllegalArgumentException("No bytes specified");
        
        //Must mask value after left-shifting, since case from byte
        //to int copies most significant bit to the left!
        int x0=x[offset] & 0x000000FF;
        int x1=0;
        int x2=0;
        int x3=0;
        if (n>1) {
            x1=(x[offset+1]<<8) & 0x0000FF00;
            if (n>2) {
                x2=(x[offset+2]<<16) & 0x00FF0000;
                if (n>3)
                    x3=(x[offset+3]<<24);
            }
        }
        return x3|x2|x1|x0;
    }
    
}
