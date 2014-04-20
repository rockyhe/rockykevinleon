package phase2Pack.nio;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Arrays;

/*
 * Read event handler for NIO, reactor pattern
 * Reference: http://chamibuddhika.wordpress.com/2012/08/11/io-demystified/
 */
public class ReadEventHandler implements EventHandler
{
    // Constants
    private static final int MAX_NUM_CLIENTS = 250;
    private static final int CMD_SIZE = 1;
    private static final int KEY_SIZE = 32;
    private static final int VALUE_SIZE = 1024;
    private static final int ERR_SIZE = 1;
    private static final int KVSTORE_SIZE = 40000;
    private static final int BUFFER_SIZE = 33;

    private Selector demultiplexer;

    public ReadEventHandler(Selector demultiplexer)
    {
        this.demultiplexer = demultiplexer;
    }

    @Override
    public void handleEvent(SelectionKey handle) throws Exception
    {
        SocketChannel socketChannel = (SocketChannel) handle.channel();

        // Read data from client
        ByteBuffer inputBuffer = ByteBuffer.allocate(BUFFER_SIZE);
        socketChannel.read(inputBuffer);

        inputBuffer.flip();
        // Rewind the buffer to start reading from the beginning

        byte[] buffer = new byte[inputBuffer.limit()];
        inputBuffer.get(buffer);
        inputBuffer.flip();
        System.out.println("Received message from client : " + Arrays.toString(buffer));

        // Rewind the buffer to start reading from the beginning
        // Register the interest for writable readiness event for
        // this channel in order to echo back the message

        socketChannel.register(demultiplexer, SelectionKey.OP_WRITE, inputBuffer);
    }
}
