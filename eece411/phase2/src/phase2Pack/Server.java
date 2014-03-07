package phase2Pack;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.*;

public class Server {
	private static final int PORT = 5000;
	private static ConcurrentHashMap<String, byte[]> store;

	public static void main(String[] args)
	{
		try {
			//Create a server socket to accept client connection requests
			ServerSocket servSock = new ServerSocket(PORT);
			Socket clntSock;
			store = new ConcurrentHashMap<String,byte[]>();
			System.out.println("Server is ready...");

			AtomicInteger clientCount = new AtomicInteger(0);
			for (;;)
			{
				//Run forever, accepting and servicing connections
				clntSock = servSock.accept();     // Get client connection
				clientCount.getAndIncrement();
				//System.out.println("New client connection.");
				KVStore connection = new KVStore(clntSock, store, clientCount);
				//Create a new thread for each client connection
				Thread t = new Thread(connection);
				t.start();
				
				//System.out.println("# of clients: " + clientCount.get());
				//System.out.println("--------------------");
			}
		} catch(Exception e) {
			System.out.println("Internal Server Error!");
		}
	}
}

