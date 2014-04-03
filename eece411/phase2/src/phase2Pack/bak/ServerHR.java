//package phase2Pack;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.sql.Timestamp;
import java.util.Date;
import java.lang.Math;

public class ServerHR {
	//Constants
	private static final int PORT = 5000;
	private static final int MAX_GOSSIP_MEMBERS = 16;
	private static final int MAX_NUM_CLIENTS = 250;
	private static final int BACKLOG_SIZE = 50;
	private static final String NODE_LIST_FILE = "nodeList.txt";
    //Constants
    private static final int CMD_SIZE = 1;
    private static final int KEY_SIZE = 32;
    private static final int VALUE_SIZE = 1024;

    private static final int GOSSIP_MSG = 255;
	//Make sure this value is larger than number of physical nodes
	//Since potential max nodes is 100, then use 100 * 100 = 10000
	private static final int NUM_PARTITIONS = 10000;
    private static final int SLEEP_TIME = 1000; //4 seconds
    private static final int PROP_BUFFER = 2000;
    private static final int OFFLINE_THRES = (int)(Math.log10(MAX_GOSSIP_MEMBERS)/Math.log10(2))*SLEEP_TIME+PROP_BUFFER; //10 seconds log(N)/log(2) * SLEEP_TIME
    //Private members
	private static ServerSocket servSock;
	private static ConcurrentHashMap<String, byte[]> store;
	private static Queue<Socket> backlog;
	private static AtomicInteger concurrentClientCount;
	private static AtomicInteger shutdownFlag;
	private static ExecutorService threadPool;
    private static int partitionsPerNode;
	private static CopyOnWriteArrayList<KVStore.Node> onlineNodeList;
    //Sorted map for mapping hashed values to physical nodes
	private static ConcurrentSkipListMap<String, KVStore.Node> nodes;
    private static AtomicIntegerArray heartRateMonitor;

	public static void main(String[] args)
	{
		try {
			//Load the list of nodes
			try {
				Scanner s = new Scanner(new File(NODE_LIST_FILE));
                KVStore.Node node;
				onlineNodeList = new CopyOnWriteArrayList<KVStore.Node>();
				while (s.hasNext())
				{
					String nodeName = s.next();
                    node = new KVStore.Node(new InetSocketAddress(nodeName, PORT), true);
					onlineNodeList.add(node);
                    if(nodeName.equals(java.net.InetAddress.getLocalHost().getHostName())){
                        Global.myIndex = onlineNodeList.indexOf(node);
                        System.out.println("this host's index: "+Global.myIndex);
                    }
				}
				s.close();
			} catch (FileNotFoundException e) {
				System.out.println("Cannot find node list file!");
			} catch (Exception e) {
				System.out.println("Error loading node list!");
			}

            //Map the nodes to partitions
            partitionsPerNode = NUM_PARTITIONS / onlineNodeList.size();
			constructNodeMap();
			//displayNodeMap();
			//verifyNodeMap();
            
            //heartRates = new byte[Math.round((double)onlineNodeList.size()/8.0)];
            //System.out.println("bytes needed: "+Math.round((double)onlineNodeList.size()/8.0));
            
            //Initialize members
			servSock = new ServerSocket(PORT);
			store = new ConcurrentHashMap<String,byte[]>();
			backlog = new ArrayBlockingQueue<Socket>(BACKLOG_SIZE);
			concurrentClientCount = new AtomicInteger(0);
			shutdownFlag = new AtomicInteger(0);
            heartRateMonitor = new AtomicIntegerArray(8);//8 integers, 32*8 = 8*32
			//Create a fixed thread pool since we'll have at most MAX_NUM_CLIENTS concurrent threads
			threadPool = Executors.newFixedThreadPool(MAX_NUM_CLIENTS);
			System.out.println("Server is ready...");

			//Create a new Producer thread for accepting client connections and adding them to the queue
			Thread producer = new Thread(new Producer());
			producer.start();

            //randomly grab 2 nodes concurrently
            Thread gossiper = new Thread(new Gossiper());
			gossiper.start();
            Thread gossiper2 = new Thread(new Gossiper());
            gossiper2.start();
			//Create a new Consumer thread for servicing clients in the queue
			Thread consumer = new Thread(new Consumer());
			consumer.start();

            //check timestamp from the nodeList
            Thread timestampCheck = new Thread(new TimestampCheck());
            timestampCheck.start();

		} catch (IOException e) {
			System.out.println("Internal Server Error!");
		}
	}
	
	private static void constructNodeMap()
	{
		nodes = new ConcurrentSkipListMap<String, KVStore.Node>();
		//Divide the hash space into NUM_PARTITIONS partitions
		//with each physical node responsible for (NUM_PARTITIONS / number of nodes) hash ranges
		//int partitionsPerNode = NUM_PARTITIONS / onlineNodeList.size();
		for (KVStore.Node node : onlineNodeList)
		{
			for (int i=0; i < partitionsPerNode; ++i)
			{				
				nodes.put(KVStore.getHash(node.address.getHostName() + i), node);
			}
		}
	}

    private static void returnPartitions(int idx)
    {
        //foreach nodes in the nodeList
            //System.out.println("<<<<<<<<<<<<<<<<<<<<<<<rejoined node: "+onlineNodeList.get(idx).address.toString());
        for (KVStore.Node node : onlineNodeList)
        {

            //foreach partition in each node
            for (int i=0; i<partitionsPerNode; ++i)
            {
                //if current partition's hash key's value (node) is the rejoin node
                if(node.address.getHostName().equals(onlineNodeList.get(idx).address.getHostName()))
                {
                    //replace it with the next node, or the first node
                    //System.out.println("rejoined node: "+onlineNodeList.get(idx).address.toString());
                    //System.out.println("hash key for rejoin node: "+KVStore.getHash(node.address.getHostName() + i).toString());
                    
                    nodes.replace(KVStore.getHash(node.address.getHostName() + i),onlineNodeList.get(idx));
                }
            }
        }
    }
    
    private static void takePartitions(int idx)
    {
        //get the node that is offline now
        //foreach nodes in the nodeList
        int j;
        
        //System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>offline  node: "+onlineNodeList.get(idx).address.toString());
        for (KVStore.Node node : onlineNodeList){
            //foreach partition in each node
            for (int i=0; i<partitionsPerNode; ++i){
                j=0;
                //if current partition's hash key's value (node) is the offline node
                if(nodes.get(KVStore.getHash(node.address.getHostName() + i)).address.getHostName().equals(
                    onlineNodeList.get(idx).address.getHostName()))
                {
                    //replace it with the next node, or the first node
                   // System.out.println("hash key for offline node: "+KVStore.getHash(node.address.getHostName() + i).toString());
                    
                    if(idx < (onlineNodeList.size()-1)){
                        j=idx+1;
                    }else{
                        j=0;
                    }

                    while(true){
                        if(onlineNodeList.get(j).online){
                            nodes.replace(KVStore.getHash(node.address.getHostName() + i),onlineNodeList.get(j));
                            break;
                        }

                        if(j == (onlineNodeList.size()-1)){
                            j=0;
                        }else{
                            j++;
                        }
                    }
                }
            }
        }
    }

	private static void verifyNodeMap()
	{
		Map<KVStore.Node, Integer> distribution = new HashMap<KVStore.Node, Integer>();
		for (KVStore.Node node : onlineNodeList)
		{
			distribution.put(node, 0);
		}
		
		for (Map.Entry<String, KVStore.Node> entry : nodes.entrySet())
		{
			KVStore.Node node = entry.getValue();
			Integer count = distribution.get(node);
			distribution.put(node, count + 1);
		}
		
		for (Map.Entry<KVStore.Node, Integer> node : distribution.entrySet())
		{
			System.out.println(node.getKey().address.getHostName() + " => " + node.getValue().toString());
		}
	}

	private static void displayNodeMap()
	{
		for (Map.Entry<String, KVStore.Node> entry : nodes.entrySet())
		{
			String key = entry.getKey();
			InetSocketAddress addr = entry.getValue().address;
			//System.out.println(key + " => " + addr.toString() + " => " + entry.getValue().online);
			System.out.println(key + " => " + onlineNodeList.indexOf(entry.getValue()));
		}
	}

    private static class TimestampCheck implements Runnable {
        public void run() {
            long timeDiff = 0;
            Timestamp currentTime;
            while(true){
                //System.out.println("----------------------------");
                for (KVStore.Node node : onlineNodeList){
                    try{
                        if(!(node.address.getHostName().equals(java.net.InetAddress.getLocalHost().getHostName()))){
                            Thread.currentThread().sleep(SLEEP_TIME);
                            currentTime = new Timestamp(new Date().getTime());
                            //System.out.println("node: "+onlineNodeList.get(onlineNodeList.indexOf(node)).address.getHostName());
                            //System.out.println("last update: "+onlineNodeList.get(onlineNodeList.indexOf(node)).t.toString());
                            timeDiff=currentTime.getTime()-node.t.getTime();
                            //System.out.println("timeDiff: "+timeDiff);
                            if(timeDiff > OFFLINE_THRES){
                                node.online=false;
                                takePartitions(onlineNodeList.indexOf(node));
                            }
                        }
                    }catch(Exception e){
                        System.out.println("unrecognized node");
                    }
                }
            }
        }
    }

	private static class Gossiper implements Runnable {
            public void run() {
                Socket socket = null;
                Random randomGenerator;
                int randomInt=-1;                
              
                //int lastRandNum=-1;
                byte[] gossipBuffer = new byte[CMD_SIZE+KEY_SIZE];
                //gossipBuffer[0]=(byte)(GOSSIP_MSG & 0x000000FF);
                ByteOrder.int2leb(GOSSIP_MSG, gossipBuffer, 0);   //Command byte - 1 byte
                
                //init every node's heart rate bit to true
                //keep this heartRates local, cuz bitSet doesn't have concurrent character
			    byte[] heartRateBytes;
                BitSet heartRateBits;
                int[] heartRateInts;
                System.out.println("1");
                heartRateBits = new BitSet(onlineNodeList.size());
                System.out.println("2");
                heartRateBits.set(0,onlineNodeList.size());
                System.out.println("3");

                heartRateBytes = ByteOrder.toByteArray(heartRateBits);
                System.out.println("4");
                heartRateInts = ByteOrder.toIntArray(heartRateBytes);
                for(int i=0; i<heartRateInts.length;i++){
                    heartRateMonitor.set(i,heartRateInts[i]);
                }
                System.out.println("5");
                System.out.println("bits "+heartRateBits.cardinality());
            
                //convert heartRateBits to heartRate byte[]
                heartRateBytes = ByteOrder.toByteArray(heartRateBits);
                
                System.out.println("init heart rates: "+Arrays.toString(heartRateBytes));

                while(true){
                    try{
                        randomGenerator = new Random();
                        //Random randomGenerator = new Random();
                        //randomly select a node to gossip 
                        while(true){
                            //do{
                            randomInt = randomGenerator.nextInt(onlineNodeList.size());
                            //}while(randomInt == lastRandNum);
                            
                            if(!(onlineNodeList.get(randomInt).address.getHostName().equals(java.net.InetAddress.getLocalHost().getHostName()))){
                                if(onlineNodeList.get(randomInt).online){ 
                                    break;
                                }
                            }
                        }

                        if(onlineNodeList.get(randomInt).rejoin){
                            returnPartitions(randomInt);
                            onlineNodeList.get(randomInt).rejoin = false;
                        }
                        System.out.println("heart rates: "+Arrays.toString(heartRateInts));
                        //System.out.println("gossiping to server: "+onlineNodeList.get(randomInt).address.getHostName());
                        System.arraycopy(heartRateBytes, 0, gossipBuffer, CMD_SIZE, heartRateBytes.length); //Key bytes - 32 bytes
                        socket = new Socket(onlineNodeList.get(randomInt).address.getHostName(), PORT);
                        
                        //Send the message to the server
                        OutputStream os = socket.getOutputStream();
                        //Send the encoded string to the server
                        os.write(gossipBuffer);
                        //System.out.println("Sending request:");
                        //System.out.println(StringUtils.byteArrayToHexString(gossipBuffer));
                        
                        //lastRandNum = randomInt;
                        //sleep
                        Thread.currentThread().sleep(SLEEP_TIME);
                    }catch (IOException e) {
                        //chear offline's heart rate
                        heartRateBits.clear(randomInt);
                        heartRateBytes = ByteOrder.toByteArray(heartRateBits);

                        onlineNodeList.get(randomInt).online = false;
                        onlineNodeList.get(randomInt).t = new Timestamp(0);
                        System.out.println(onlineNodeList.get(randomInt).address.getHostName().toString()+" left");
                    }catch (InterruptedException e) {
                        System.out.println("Sleep error!");
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
					//If shutdown flag is set to true, then stop accepting any more connections
					if (shutdownFlag.get() == 0)
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
							//System.out.println("Backlog is full.");
						}
	
						//					if (backlog.size() > 0)
						//					{
						//						System.out.println("# of clients in backlog: " + backlog.size());
						//					}
					}
					else
					{
						System.out.println("Server has received shutdown command. No longer accepting connections!");
						//If shutdown flag is set to 2, then we have finished processing existing client requests and it's safe to shutdown
						if (shutdownFlag.get() == 2)
						{
							System.exit(0);
						}
					}
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
					if (concurrentClientCount.get() < MAX_NUM_CLIENTS && (clntSock = backlog.poll()) != null)
					{
						KVStore connection = new KVStore(clntSock, store, nodes, concurrentClientCount, shutdownFlag, onlineNodeList);
						//Create a new thread for each client connection
						threadPool.execute(connection);
						//System.out.println("New client executing.");
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

