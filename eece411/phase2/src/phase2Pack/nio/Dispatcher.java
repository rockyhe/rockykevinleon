package phase2Pack.nio;

import java.nio.ByteBuffer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import phase2Pack.StringUtils;
import phase2Pack.Exceptions.SystemOverloadException;
import phase2Pack.enums.ErrorCodes;

/*
 * Dispatcher for NIO, reactor pattern
 * Reference: http://chamibuddhika.wordpress.com/2012/08/11/io-demystified/
 */
public class Dispatcher implements Runnable
{
    // Constants
    private static final int SELECT_TIMEOUT = 100;

    // Private members
    private Map<Integer, EventHandler> registeredHandlers = new ConcurrentHashMap<Integer, EventHandler>();
    private static Selector selector;
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

    // Public convenience method to send response back to client
    public static void sendBytesNIO(SelectionKey handle, byte[] src)
    {
        System.out.println("Sending response bytes to client: " + StringUtils.byteArrayToHexString(src));
        handle.interestOps(SelectionKey.OP_WRITE);
        handle.attach(ByteBuffer.wrap(src));
        selector.wakeup();
    }

    // Public convenience method to send only error code back to client
    public static void sendBytesNIO(SelectionKey handle, ErrorCodes errorCode)
    {
        System.out.println("Sending error code byte to client: " + errorCode.getValue());
        sendBytesNIO(handle, new byte[] { errorCode.toByte() } );
    }

    public static void shutdown()
    {
        shutdownFlag.set(true);
    }

    public void run()
    {
        try
        {
            Set<SelectionKey> readyHandles;
            Iterator<SelectionKey> handleIterator;
            while (true)
            {
                // Waiting for events
                selector.select(SELECT_TIMEOUT);
                // Get keys
                readyHandles = selector.selectedKeys();
                handleIterator = readyHandles.iterator();

                // For each key
                SelectionKey handle;
                while (handleIterator.hasNext())
                {
                    handle = handleIterator.next();
                    if (!handle.isValid())
                    {
                        continue;
                    }

                    if (handle.isAcceptable() && !shutdownFlag.get())
                    {
                        AcceptEventHandler handler = (AcceptEventHandler) registeredHandlers.get(SelectionKey.OP_ACCEPT);
                        handler.handleEvent(handle);
                        // Note : Don't remove this handle from selector here
                        // since we want to keep listening to new client connections
                    }

                    if (handle.isReadable())
                    {
                        ReadEventHandler handler = (ReadEventHandler) registeredHandlers.get(SelectionKey.OP_READ);
                        try {
                            handler.handleEvent(handle);
                        } catch (SystemOverloadException e) {
                            System.out.println("System Overload");
                            Dispatcher.sendBytesNIO(handle, ErrorCodes.SYSTEM_OVERLOAD);
                        }
                        handleIterator.remove();
                    }

                    if (handle.isWritable())
                    {
                        WriteEventHandler handler = (WriteEventHandler) registeredHandlers.get(SelectionKey.OP_WRITE);
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
