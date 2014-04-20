package phase2Pack;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.*;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.channels.SelectableChannel;
import java.nio.channels.Selector;
import java.nio.channels.SelectionKey;
import java.nio.charset.*;
import java.nio.CharBuffer;

public class ServerNIO {
	private static final int PORT = 5000;
	private static ConcurrentHashMap<String, byte[]> store;
	private static final long TIMEOUT = 10000;

	public static void main(String[] args)
	{
		try{
			// Create the server socket channel
			ServerSocketChannel server = ServerSocketChannel.open();
			// nonblocking I/O
			server.configureBlocking(false);
			// host-port 8000
			server.socket().bind(new java.net.InetSocketAddress(PORT));
			System.out.println("Server connected to port "+PORT);
			// Create the selector
			Selector selector = Selector.open();
			// Recording server to selector (type OP_ACCEPT)
			server.register(selector,SelectionKey.OP_ACCEPT);

			// Infinite server loop
			for(;;)
			{
				// Waiting for events
				selector.select();
				// Get keys
				Set keys = selector.selectedKeys();
				Iterator i = keys.iterator();

				// For each keys...
				while(i.hasNext())
				{
					SelectionKey key = (SelectionKey) i.next();

					// Remove the current key
					i.remove();

					// if isAccetable = true
					// then a client required a connection
					if (key.isAcceptable())
					{
						// get client socket channel
						SocketChannel client = server.accept();
						// Non Blocking I/O
						client.configureBlocking(false);
						// recording to the selector (reading)
						client.register(selector, SelectionKey.OP_READ);
						continue;
					}

					// if isReadable = true
					// then the server is ready to read
					if (key.isReadable())
					{
						SocketChannel client = (SocketChannel) key.channel();

						// Read byte coming from the client
						int BUFFER_SIZE = 33;
					    ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
                        byte[] msg = null;
                        try {
							client.read(buffer);
						    buffer.flip();
                            msg = buffer.array();
                            System.out.println(Arrays.toString(msg));
						}
						catch (Exception e) {
							// client is no longer active
							e.printStackTrace();
							continue;
						}
						// Show bytes on the console
						continue;
					}
				}
			}
		} catch (IOException e) {
            e.printStackTrace();
			System.out.print("Internal Server Error!");
		}
	}
}

