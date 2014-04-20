package phase2Pack.nio;

import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.util.concurrent.ConcurrentHashMap;

/*
 * Reactor initiator for NIO, reactor pattern
 * Reference: http://chamibuddhika.wordpress.com/2012/08/11/io-demystified/
 */
public class ReactorInitiator
{
    private static final int PORT = 5000;
    private static ConcurrentHashMap<String, byte[]> store;
    private static final long TIMEOUT = 10000;

    public void initiateReactiveServer(int port) throws Exception
    {
        // Create the server socket channel
        ServerSocketChannel server = ServerSocketChannel.open();
        // host-port 8000
        server.socket().bind(new java.net.InetSocketAddress(PORT));
        // nonblocking I/O
        server.configureBlocking(false);
        System.out.println("Server connected to port 8000");

        Dispatcher dispatcher = new Dispatcher();
        dispatcher.registerChannel(SelectionKey.OP_ACCEPT, server);

        dispatcher.registerEventHandler(SelectionKey.OP_ACCEPT, new AcceptEventHandler(dispatcher.getDemultiplexer()));

        dispatcher.registerEventHandler(SelectionKey.OP_READ, new ReadEventHandler(dispatcher.getDemultiplexer()));

        dispatcher.registerEventHandler(SelectionKey.OP_WRITE, new WriteEventHandler());

        dispatcher.run(); // Run the dispatcher loop
    }

    public static void main(String[] args) throws Exception
    {
        System.out.println("Starting NIO server at port : " + PORT);
        new ReactorInitiator().initiateReactiveServer(PORT);
    }
}
