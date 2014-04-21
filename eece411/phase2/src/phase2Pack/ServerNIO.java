package phase2Pack;

import phase2Pack.nio.ReactorInitiator;

public class ServerNIO
{
    // Constants
    public static final int PORT = 5000;
    public static final int GOSSIP_PORT = 5555;
    private static final int MAX_NUM_CLIENTS = 50;
    private static final int BACKLOG_SIZE = 25;
    public static final long TIMEOUT = 10000;

    // Private members
    private static KVStore ring;
    private static ServerSocket servSock;
    private static Queue<Socket> backlog;
    private static AtomicInteger concurrentClientCount;

    private static class Producer implements Runnable
    {
        public void run()
        {
            try
            {
                // Run forever, listening for and accepting client connections
                while (true)
                {
                    // If shutdown flag is set to true, then stop accepting any more connections
                    Socket clntSock = servSock.accept(); // Get client connection
                    // If backlog isn't full, add client to it
                    if (backlog.size() < BACKLOG_SIZE)
                    {
                        backlog.add(clntSock);
                        // System.out.println("Adding client to backlog.");
                    }
                }
              
            } catch (Exception e)
            {
                System.out.println("Internal Server Error!");
                e.printStackTrace();
            }
        }
    }
    
    private static class Consumer implements Runnable
    {
        public void run()
        {
            try
            {
                Socket clntSock;
                // Run forever, servicing client connections in queue
                while (true)
                {
                    // If current number of concurrent clients hasn't reached MAX_NUM_CLIENTS
                    // then service client at the head of queue
                    if (concurrentClientCount.get() < MAX_NUM_CLIENTS && (clntSock = backlog.poll()) != null)
                    {
                        GossipListener connection = new GossipListener(clntSock,concurrentClientCount, ring.getMembership());
                        // Create a new thread for each client connection
                        threadPool.execute(connection);
                        // System.out.println("New client executing.");
                    }                    
                    // if (concurrentClientCount.get() > 0)
                    // {
                    // System.out.println("# of concurrent clients: " + concurrentClientCount.get());
                    // }
                }
            } catch (Exception e)
            {
                System.out.println("Internal Server Error!");
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws Exception
    {
        try
        {
            //creating consistent hashing ring
            ring = new KVStore(PORT);

            System.out.println("Starting NIO server at port : " + PORT);
            new ReactorInitiator().initiateReactiveServer(PORT, kvStore);

            // Initialize gossip variables
            servSock = new ServerSocket(PORT);
            backlog = new ArrayBlockingQueue<Socket>(BACKLOG_SIZE);
            concurrentClientCount = new AtomicInteger(0);

            threadPool = Executors.newFixedThreadPool(MAX_NUM_CLIENTS);

            Thread producer = new Thread(new Producer());
            producer.start();
            Thread consumer = new Thread(new Consumer());
            consumer.start();
            
            // randomly grab 2 nodes concurrently
            Thread gossiper = new Thread(new Gossiper(ring.getMembership(),ring, GOSSIP_PORT));
            gossiper.start();
            Thread gossiper2 = new Thread(new Gossiper(ring.getMembership(),ring,GOSSIP_PORT));
            gossiper2.start();
            
            // check timestamp from the nodeList
            Thread timestampCheck = new Thread(new Gossiper.TimestampCheck(ring.getMembership(),ring));
            timestampCheck.start();
            
            System.out.println("Server is ready...");
        } catch (Exception e) {
            System.out.println("Internal Server Error!");
            e.printStackTrace();
        }
    }
}
