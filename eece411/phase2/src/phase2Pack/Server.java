package phase2Pack;

import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import phase2Pack.enums.ErrorCodes;
import phase2Pack.nio.ReactorInitiator;

public class Server
{
    // Constants
    public static final int PORT = 6666;
    public static final int HEARTBEAT_PORT = 5555;
    private static final int HEARTBEAT_MAX_NUM_CLIENTS = 50;
    private static final int HEARTBEAT_BACKLOG_SIZE = 50;

    private static final int REQ_MAX_NUM_CLIENTS = 200;
    private static final int REQ_BACKLOG_SIZE = 100;

    // Private members
    private static ConsistentHashRing ring;
    private static KVStore kvStore;

    // Heartbeat server variables
    private static ServerSocket heartbeatServSock;
    private static Queue<Socket> heartbeatBacklog;
    private static AtomicInteger heartbeatClientCount;
    private static ExecutorService heartbeatThreadPool;

    // Request server variables
    private static ServerSocket reqServSock;
    private static Queue<Socket> reqBacklog;
    private static AtomicInteger reqClientCount;
    private static ExecutorService reqThreadPool;

    public static void main(String[] args) throws Exception
    {
        try {
            // Create the consistent hash ring and KVStore data structure
            ring = new ConsistentHashRing(PORT);
            kvStore = new KVStore();

            //System.out.println("Starting NIO server at port : " + PORT);
            //new ReactorInitiator().initiateReactiveServer(PORT, ring, kvStore);

            System.out.println("Starting multi-threaded server at port : " + PORT);
            reqServSock = new ServerSocket(PORT);
            reqBacklog = new ArrayBlockingQueue<Socket>(REQ_BACKLOG_SIZE);
            reqClientCount = new AtomicInteger(0);
            reqThreadPool = Executors.newFixedThreadPool(REQ_MAX_NUM_CLIENTS);

            Thread reqProducer = new Thread(new ReqProducer());
            reqProducer.start();
            Thread reqConsumer = new Thread(new ReqConsumer());
            reqConsumer.start();

            // Initialize heartbeat variables
            System.out.println("Starting heartbeat server at port : " + HEARTBEAT_PORT);
            heartbeatServSock = new ServerSocket(HEARTBEAT_PORT);
            heartbeatBacklog = new ArrayBlockingQueue<Socket>(HEARTBEAT_BACKLOG_SIZE);
            heartbeatClientCount = new AtomicInteger(0);
            heartbeatThreadPool = Executors.newFixedThreadPool(HEARTBEAT_MAX_NUM_CLIENTS);

            Thread producer = new Thread(new HeartbeatProducer());
            producer.start();
            Thread consumer = new Thread(new HeartbeatConsumer());
            consumer.start();

            // randomly grab 2 nodes concurrently
            Thread pinger = new Thread(new Heartbeat(ring, HEARTBEAT_PORT, 1));
            pinger.start();
            Thread pinger2 = new Thread(new Heartbeat(ring, HEARTBEAT_PORT, 2));
            pinger2.start();

            // check timestamp from the nodeList
            Thread timestampCheck = new Thread(new Heartbeat.TimestampCheck(ring));
            timestampCheck.start();

            System.out.println("Server is ready...");
        } catch (Exception e) {
            System.out.println("Server Internal Server Error!");
            e.printStackTrace();
        }
    }

    private static class HeartbeatProducer implements Runnable
    {
        public void run()
        {
            try {
                // Run forever, listening for and accepting client connections
                while (true)
                {
                    Socket clntSock = heartbeatServSock.accept(); // Get client connection
                    // If backlog isn't full, add client to it
                    if (heartbeatBacklog.size() < HEARTBEAT_BACKLOG_SIZE)
                    {
                        heartbeatBacklog.add(clntSock);
                    }
                    else
                    {
                        clntSock.close();
                    }
                }
            } catch (Exception e) {
                System.out.println("Heartbeat Producer Internal Server Error!");
                e.printStackTrace();
            }
        }
    }

    private static class HeartbeatConsumer implements Runnable
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
                    if (heartbeatClientCount.get() < HEARTBEAT_MAX_NUM_CLIENTS && (clntSock = heartbeatBacklog.poll()) != null)
                    {
                        heartbeatClientCount.getAndIncrement();
                        HeartbeatListener connection = new HeartbeatListener(clntSock, heartbeatClientCount, ring);
                        // Create a new thread for each client connection
                        heartbeatThreadPool.execute(connection);
                    }
                }
            } catch (Exception e) {
                System.out.println("Heartbeat Consumer Internal Server Error!");
                e.printStackTrace();
            }
        }
    }


    private static class ReqProducer implements Runnable
    {
        public void run()
        {
            try {
                // Run forever, listening for and accepting client connections
                while (true)
                {
                    Socket clntSock = reqServSock.accept(); // Get client connection
                    // If backlog isn't full, add client to it
                    if (reqBacklog.size() < REQ_BACKLOG_SIZE)
                    {
                        reqBacklog.add(clntSock);
                    }
                    // Otherwise return system overload error
                    else
                    {
                        OutputStream out = clntSock.getOutputStream();
                        out.write(new byte[] { ErrorCodes.SYSTEM_OVERLOAD.toByte() });
                    }
                }
            } catch (Exception e) {
                System.out.println("Request Producer Internal Server Error!");
                e.printStackTrace();
            }
        }
    }

    private static class ReqConsumer implements Runnable
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
                    if (reqClientCount.get() < REQ_MAX_NUM_CLIENTS && (clntSock = reqBacklog.poll()) != null)
                    {
                        reqClientCount.getAndIncrement();
                        ProcessRequest connection = new ProcessRequest(clntSock, ring, kvStore, reqClientCount);
                        // Create a new thread for each client connection
                        reqThreadPool.execute(connection);
                    }
                }
            } catch (Exception e) {
                System.out.println("Request Consumer Internal Server Error!");
                e.printStackTrace();
            }
        }
    }

}
