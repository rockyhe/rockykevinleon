package phase2Pack.nio;

import java.nio.channels.SelectionKey;

/*
 * Event handler interface for NIO, reactor pattern
 * Reference: http://chamibuddhika.wordpress.com/2012/08/11/io-demystified/
 */
public interface EventHandler
{
    public void handleEvent(SelectionKey handle) throws Exception;
}
