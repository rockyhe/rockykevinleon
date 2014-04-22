package phase2Pack.nio;

import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import phase2Pack.ConsistentHashRing;
import phase2Pack.KVStore;
import phase2Pack.ProcessRequest;
import phase2Pack.Exceptions.SystemOverloadException;

/*
 * Read event handler for NIO, reactor pattern
 * Reference: http://chamibuddhika.wordpress.com/2012/08/11/io-demystified/
 */
public class ReadEventHandler implements EventHandler
{
    // Constants
    private static final int MAX_NUM_CLIENTS = 250;
    private static final int BACKLOG_SIZE = 250;

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
    public void handleEvent(SelectionKey handle) throws SystemOverloadException, Exception
    {
        SocketChannel socketChannel = (SocketChannel) handle.channel();
        // Process the event on a thread
        if (threadPool.getQueue().size() < BACKLOG_SIZE)
        {
            threadPool.execute(new ProcessRequest(socketChannel, handle, selector, ring, kvStore));
        }
        // If the thread pool queue reaches the max backlog size, then throw a system overload error
        else
        {
            throw new SystemOverloadException();
        }
    }
}
