package phase2Pack;

import java.net.ServerSocket;
import java.net.Socket;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import phase2Pack.nio.ReactorInitiator;

public class Server
{
    // Constants
    public static final int PORT = 6666;
    public static final int PING_PORT = 5555;
    private static final int PING_MAX_NUM_CLIENTS = 50;
    private static final int PING_BACKLOG_SIZE = 25;

    // Private members
    private static ConsistentHashRing ring;
    private static KVStore kvStore;
    private static ServerSocket servSock;
    private static Queue<Socket> backlog;
    private static AtomicInteger concurrentClientCount;
    private static ExecutorService threadPool;

    public static void main(String[] args) throws Exception
    {
        try {
            // Create the consistent hash ring and KVStore data structure
            ring = new ConsistentHashRing(PORT);
            kvStore = new KVStore();

            System.out.println("Starting NIO server at port : " + PORT);
            new ReactorInitiator().initiateReactiveServer(PORT, ring, kvStore);

            // Initialize ping variables
            servSock = new ServerSocket(PING_PORT);
            backlog = new ArrayBlockingQueue<Socket>(PING_BACKLOG_SIZE);
            concurrentClientCount = new AtomicInteger(0);
            threadPool = Executors.newFixedThreadPool(PING_MAX_NUM_CLIENTS);

            Thread producer = new Thread(new Producer());
            producer.start();
            Thread consumer = new Thread(new Consumer());
            consumer.start();
            // randomly grab 2 nodes concurrently
            Thread pinger = new Thread(new Ping(ring, PING_PORT, 1));
            pinger.start();
            Thread pinger2 = new Thread(new Ping(ring, PING_PORT, 2));
            pinger2.start();

            // check timestamp from the nodeList
            Thread timestampCheck = new Thread(new Ping.TimestampCheck(ring));
            timestampCheck.start();

            System.out.println("Server is ready...");
        } catch (Exception e) {
            System.out.println("Server Internal Server Error!");
            e.printStackTrace();
        }
    }

    private static class Producer implements Runnable
    {
        public void run()
        {
            try {
                // Run forever, listening for and accepting client connections
                while (true)
                {
                    Socket clntSock = servSock.accept(); // Get client connection
                    // If backlog isn't full, add client to it
                    if (backlog.size() < PING_BACKLOG_SIZE)
                    {
                        backlog.add(clntSock);
                    }
                }
            } catch (Exception e) {
                System.out.println("Producer Internal Server Error!");
                e.printStackTrace();
            }
        }
    }

    private static class Consumer implements Runnable
    {
        public void run()
        {
            try {
                Socket clntSock;
                // Run forever, servicing client connections in queue
                while (true)
                {
                    // If current number of concurrent clients hasn't reached MAX_NUM_CLIENTS
                    // then service client at the head of queue
                    if (concurrentClientCount.get() < PING_MAX_NUM_CLIENTS && (clntSock = backlog.poll()) != null)
                    {
                        concurrentClientCount.getAndIncrement();
                        PingListener connection = new PingListener(clntSock, concurrentClientCount, ring.getMembership());
                        // Create a new thread for each client connection
                        threadPool.execute(connection);
                    }
                }
            } catch (Exception e) {
                System.out.println("consumer Internal Server Error!");
                e.printStackTrace();
            }
        }
    }
}
