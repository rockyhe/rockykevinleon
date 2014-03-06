package phase2Pack;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Server {
	private static final int PORT = 5000;
	private static final int CLIENT_BACKLOG_SIZE = 30;
	private static ConcurrentHashMap<String, byte[]> store;

	public static void main(String[] args)
	{
		try {
			//Create a server socket to accept client connection requests
			//Set the connection backlog size so if connection queue exceeds it, a system overload error is thrown
			ServerSocket servSock = new ServerSocket(PORT, CLIENT_BACKLOG_SIZE);
			Socket clntSock;
			store = new ConcurrentHashMap<String,byte[]>();
			int threadcnt = 0;
			System.out.println("Server is ready...");

			for (;;)
			{
				//Run forever, accepting and servicing connections
				clntSock = servSock.accept();     // Get client connection
				System.out.println("New client connection.");
				KVStore connection = new KVStore(clntSock, store);
				//Create a new thread for each client connection
				Thread t = new Thread(connection);
				t.start();
				threadcnt++;
				System.out.println("Thread #"+ threadcnt);
				System.out.println("--------------------");
			}
		} catch(Exception e) {
			System.out.println("Internal Server Error!");
		}
	}
}

