package phase2Pack.nio;

import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import phase2Pack.ConsistentHashRing;
import phase2Pack.KVStore;
import phase2Pack.ProcessRequest;

/*
 * Read event handler for NIO, reactor pattern
 * Reference: http://chamibuddhika.wordpress.com/2012/08/11/io-demystified/
 */
public class ReadEventHandler implements EventHandler
{
    // Constants
    private static final int MAX_NUM_CLIENTS = 250;

    private ExecutorService threadPool;

    private Selector demultiplexer;
    private ConsistentHashRing ring;
    private KVStore kvStore;

    public ReadEventHandler(Selector demultiplexer, ConsistentHashRing ring, KVStore kvStore)
    {
        this.demultiplexer = demultiplexer;
        this.ring = ring;
        this.kvStore = kvStore;

        // Create a fixed thread pool since we'll have at most MAX_NUM_CLIENTS concurrent threads
        this.threadPool = Executors.newFixedThreadPool(MAX_NUM_CLIENTS);
    }

    @Override
    public void handleEvent(SelectionKey handle) throws Exception
    {
        SocketChannel socketChannel = (SocketChannel) handle.channel();
        // Process the event on a thread
        threadPool.execute(new ProcessRequest(socketChannel, handle, demultiplexer, ring, kvStore));
    }
}
