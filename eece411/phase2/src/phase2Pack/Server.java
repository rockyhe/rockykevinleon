package phase2Pack;

import java.io.*;
import java.net.*;
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
	private static final int MAX_NUM_CLIENTS = 100;
	private static final int MAX_GOSSIP_MEMBERS = 3;
	private static final int BACKLOG_SIZE = 100;
	private static final String NODE_LIST_FILE = "nodeList.txt";
	private static final int CMD_SIZE = 1;
    private static final int GOSSIP_MSG = 255;
	//Make sure this value is larger than number of physical nodes
	//Since potential max nodes is 100, then use 100 * 100 = 10000
	private static final int NUM_PARTITIONS = 10000;
	//Private members
	private static ServerSocket servSock;
	private static ConcurrentHashMap<String, byte[]> store;
	private static Queue<Socket> backlog;
	private static AtomicInteger concurrentClientCount;
	private static AtomicInteger shutdownFlag;
	private static ExecutorService threadPool;

	private static List<KVStore.Node> onlineNodeList;
	//Sorted map for mapping hashed values to physical nodes
	private static ConcurrentSkipListMap<String, KVStore.Node> nodes;

	public static void main(String[] args)
	{
		try {
			//Load the list of nodes
			try {
				Scanner s = new Scanner(new File(NODE_LIST_FILE));
				onlineNodeList = new ArrayList<KVStore.Node>();
				while (s.hasNext())
				{
					String node = s.next();
					onlineNodeList.add(new KVStore.Node(new InetSocketAddress(node, PORT), true));
				}
				s.close();
			} catch (FileNotFoundException e) {
				System.out.println("Cannot find node list file!");
			} catch (Exception e) {
				System.out.println("Error loading node list!");
			}
			
			//Map the nodes to partitions
			constructNodeMap();
			//displayNodeMap();

			//Initialize members
			servSock = new ServerSocket(PORT);
			store = new ConcurrentHashMap<String,byte[]>();
			backlog = new ArrayBlockingQueue<Socket>(BACKLOG_SIZE);
			concurrentClientCount = new AtomicInteger(0);
			shutdownFlag = new AtomicInteger(0);

			//Create a fixed thread pool since we'll have at most MAX_NUM_CLIENTS concurrent threads
			threadPool = Executors.newFixedThreadPool(MAX_NUM_CLIENTS);
			System.out.println("Server is ready...");

			//Create a new Producer thread for accepting client connections and adding them to the queue
			Thread producer = new Thread(new Producer());
			producer.start();

			Thread gossiper = new Thread(new Gossiper());
			gossiper.start();
			//Create a new Consumer thread for servicing clients in the queue
			Thread consumer = new Thread(new Consumer());
			consumer.start();
		} catch (Exception e) {
			System.out.println("Internal Server Error!");
		}
	}
	
	private static void constructNodeMap()
	{
		nodes = new ConcurrentSkipListMap<String, KVStore.Node>();
		//Divide the hash space into NUM_PARTITIONS partitions
		//with each physical node responsible for (NUM_PARTITIONS / number of nodes) hash ranges
		int partitionsPerNode = NUM_PARTITIONS / onlineNodeList.size();
		for (KVStore.Node node : onlineNodeList)
		{
			for (int i=0; i < partitionsPerNode; ++i)
			{				
				nodes.put(KVStore.getHash(node.address.getHostName() + i), node);
			}
		}
	}

	private static void displayNodeMap()
	{
		for (Map.Entry<String, KVStore.Node> entry : nodes.entrySet())
		{
			String key = entry.getKey();
			InetSocketAddress addr = entry.getValue().address;
			System.out.println(key + " => " + addr.toString() + " => " + entry.getValue().online);
		}
	}

	private static class Gossiper implements Runnable {
            public void run() {
                Socket socket = null;
                while(true){
                    try{
                        Random randomGenerator = new Random();
                        int randomInt = randomGenerator.nextInt(MAX_GOSSIP_MEMBERS);
                        
                        while(onlineNodeList.get(randomInt).address.getHostName().equals(java.net.InetAddress.getLocalHost().getHostName())){
                            randomInt = randomGenerator.nextInt(MAX_GOSSIP_MEMBERS);
                        }
                         
                        System.out.println("gossiping to server: "+onlineNodeList.get(randomInt).address.getHostName());
                        socket = new Socket(onlineNodeList.get(randomInt).address.getHostName(), PORT);
                        
                        //Send the message to the server
                        OutputStream os = socket.getOutputStream();
                        byte[] gossipBuffer = new byte[CMD_SIZE];
                        gossipBuffer[0]=(byte)(GOSSIP_MSG & 0x000000FF);
                        //Send the encoded string to the server
                        os.write(gossipBuffer);
                        System.out.println("Sending request:");
                        System.out.println(StringUtils.byteArrayToHexString(gossipBuffer));
                    
                        Thread.currentThread().sleep(4000);
                    }catch (Exception e) {
                        try{
                            Thread.currentThread().sleep(4000);
                        }catch(InterruptedException ex){
                            System.out.println("Gossiping Error!");
                        }
                    }

               }
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

