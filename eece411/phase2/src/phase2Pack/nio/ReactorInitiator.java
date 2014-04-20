package phase2Pack.nio;

import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;

/*
 * Reactor initiator for NIO, reactor pattern
 * Reference: http://chamibuddhika.wordpress.com/2012/08/11/io-demystified/
 */
public class ReactorInitiator
{
    public void initiateReactiveServer(int port) throws Exception
    {
        // Create the server socket channel
        ServerSocketChannel server = ServerSocketChannel.open();
        server.socket().bind(new java.net.InetSocketAddress(port));
        server.configureBlocking(false);
        System.out.println("Server connected to port : " + port);

        // Create the Dispatcher (selector) and register the channel and event handlers
        Dispatcher dispatcher = new Dispatcher();
        dispatcher.registerChannel(SelectionKey.OP_ACCEPT, server);
        dispatcher.registerEventHandler(SelectionKey.OP_ACCEPT, new AcceptEventHandler(dispatcher.getDemultiplexer()));
        dispatcher.registerEventHandler(SelectionKey.OP_READ, new ReadEventHandler(dispatcher.getDemultiplexer()));
        dispatcher.registerEventHandler(SelectionKey.OP_WRITE, new WriteEventHandler());

        // Run the dispatcher loop
        dispatcher.run();
    }
}
