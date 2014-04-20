package phase2Pack.nio;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

/*
 * Write event handler for NIO, reactor pattern
 * Reference: http://chamibuddhika.wordpress.com/2012/08/11/io-demystified/
 */
public class WriteEventHandler implements EventHandler
{
    @Override
    public void handleEvent(SelectionKey handle) throws Exception
    {
        SocketChannel socketChannel = (SocketChannel) handle.channel();
        ByteBuffer inputBuffer = (ByteBuffer) handle.attachment();
        socketChannel.write(inputBuffer);
        socketChannel.close();
    }
}
