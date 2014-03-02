package phase2Pack;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Server {
	private static int port=5000;
	private static ConcurrentHashMap<String, byte[]> store;

	public static void main(String[] args)
	{
		try {
			//Create a server socket to accept client connection requests
			ServerSocket servSock = new ServerSocket(port);
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
		} catch(IOException e) {
			System.out.println("IOException on socket listen");
		}
	}
}

