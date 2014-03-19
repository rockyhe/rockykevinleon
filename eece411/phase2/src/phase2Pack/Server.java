package phase2Pack;

import java.io.*;
import java.net.*;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.*;
import java.util.concurrent.ArrayBlockingQueue;

public class Server {
	//Constants
	private static final int PORT = 5000;
	private static final int MAX_NUM_CLIENTS = 50;
	private static final int BACKLOG_SIZE = 50;
	private static final String NODE_LIST_FILE = "nodeList.txt";

	//Private members
	private static ServerSocket servSock;
	private static ConcurrentHashMap<String, byte[]> store;
	private static Queue<Socket> backlog;
	private static AtomicInteger concurrentClientCount;
	private static AtomicInteger shutdownFlag;
	private static ExecutorService threadPool;

	private static ConcurrentSkipListMap<String, KVStore.Node> nodes;

	public static void main(String[] args)
	{
		try {
			//Initialize members
			servSock = new ServerSocket(PORT);
			store = new ConcurrentHashMap<String,byte[]>();
			backlog = new ArrayBlockingQueue<Socket>(BACKLOG_SIZE);
			concurrentClientCount = new AtomicInteger(0);
			shutdownFlag = new AtomicInteger(0);

			//Create a fixed thread pool since we'll have at most MAX_NUM_CLIENTS concurrent threads
			threadPool = Executors.newFixedThreadPool(MAX_NUM_CLIENTS);

			try {
				Scanner s = new Scanner(new File(NODE_LIST_FILE));
				nodes = new ConcurrentSkipListMap<String, KVStore.Node>();
				while (s.hasNext())
				{
					String node = s.next();
					nodes.put(KVStore.getHash(node), new KVStore.Node(new InetSocketAddress(node, PORT), true));
				}
				s.close();
			} catch (FileNotFoundException e) {
				System.out.println("Cannot find node list file!");
			} catch (Exception e) {
				System.out.println("Error loading node list!");
			}

			DisplayNodeList();

			System.out.println("Server is ready...");

			//Create a new Producer thread for accepting client connections and adding them to the queue
			Thread producer = new Thread(new Producer());
			producer.start();

			//Create a new Consumer thread for servicing clients in the queue
			Thread consumer = new Thread(new Consumer());
			consumer.start();
		} catch (Exception e) {
			System.out.println("Internal Server Error!");
		}
	}

	private static void DisplayNodeList()
	{
		for (Map.Entry<String, KVStore.Node> entry : nodes.entrySet())
		{
			String key = entry.getKey();
			InetSocketAddress addr = entry.getValue().address;
			System.out.println(key + " => " + addr.toString());
		}
	}

	private static class Producer implements Runnable
	{
		public void run()
		{
			try {
				//Run forever, listening for and accepting client connections
				while (true)
				{
					Socket clntSock = servSock.accept(); // Get client connection
					//If backlog isn't full, add client to it
					if (backlog.size() < BACKLOG_SIZE)
					{
						backlog.add(clntSock);
						//System.out.println("Adding client to backlog.");
					}
					//Otherwise return system overload error
					else
					{
						OutputStream out = clntSock.getOutputStream();
						out.write(new byte[] {0x03});
						System.out.println("Backlog is full.");
					}

					//					if (backlog.size() > 0)
					//					{
					//						System.out.println("# of clients in backlog: " + backlog.size());
					//					}
				}
			} catch (Exception e) {
				System.out.println("Internal Server Error!");
			}
		}
	}

	private static class Consumer implements Runnable
	{
		public void run()
		{
			try {
				Socket clntSock;
				//Run forever, servicing client connections in queue
				while (true)
				{
					//If current number of concurrent clients hasn't reached MAX_NUM_CLIENTS
					//then service client at the head of queue
					if (shutdownFlag.get() == 0)
					{
						if (concurrentClientCount.get() < MAX_NUM_CLIENTS && (clntSock = backlog.poll()) != null)
						{
							KVStore connection = new KVStore(clntSock, store, nodes, concurrentClientCount, shutdownFlag);
							//Create a new thread for each client connection
							threadPool.execute(connection);
							//System.out.println("New client executing.");
						}
					}
					else
					{
						//FIXME proporgate it to somewhere
						System.out.println("closing down server!");
						System.exit(0);
					}

					//					if (concurrentClientCount.get() > 0)
					//					{
					//						System.out.println("# of concurrent clients: " + concurrentClientCount.get());
					//					}
				}
			} catch (Exception e) {
				System.out.println("Internal Server Error!");
			}
		}
	}
}

