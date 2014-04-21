package phase2Pack.nio;

import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/*
 * Dispatcher for NIO, reactor pattern
 * Reference: http://chamibuddhika.wordpress.com/2012/08/11/io-demystified/
 */
public class Dispatcher
{
    private Map<Integer, EventHandler> registeredHandlers = new ConcurrentHashMap<Integer, EventHandler>();
    private Selector selector;
    private static AtomicBoolean shutdownFlag;

    public Dispatcher() throws Exception
    {
        selector = Selector.open();
        shutdownFlag = new AtomicBoolean();
    }

    public Selector getSelector()
    {
        return selector;
    }

    public void registerEventHandler(int eventType, EventHandler eventHandler)
    {
        registeredHandlers.put(eventType, eventHandler);
    }

    // Used to register ServerSocketChannel with the
    // selector to accept incoming client connections
    public void registerChannel(int eventType, SelectableChannel channel) throws Exception
    {
        channel.register(selector, eventType);
    }

    public static void shutdown()
    {
        shutdownFlag.set(true);
    }

    public void run()
    {
        try
        {
            while (true)
            {
                // Waiting for events
                selector.select();

                // Get keys
                Set<SelectionKey> readyHandles = selector.selectedKeys();
                Iterator<SelectionKey> handleIterator = readyHandles.iterator();

                // For each key
                while (handleIterator.hasNext())
                {
                    SelectionKey handle = handleIterator.next();
                    if (!handle.isValid())
                    {
                        continue;
                    }

                    if (handle.isAcceptable() && !shutdownFlag.get())
                    {
                        EventHandler handler = registeredHandlers.get(SelectionKey.OP_ACCEPT);
                        handler.handleEvent(handle);
                        // Note : Don't remove this handle from selector here
                        // since we want to keep listening to new client connections
                    }

                    if (handle.isReadable())
                    {
                        EventHandler handler = registeredHandlers.get(SelectionKey.OP_READ);
                        handler.handleEvent(handle);
                        handleIterator.remove();
                    }

                    if (handle.isWritable())
                    {
                        EventHandler handler = registeredHandlers.get(SelectionKey.OP_WRITE);
                        handler.handleEvent(handle);
                        handleIterator.remove();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
