package phase2Pack;

import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class Server
{
    // Constants
    private static final int PORT = 5000;
    private static final int MAX_GOSSIP_MEMBERS = 16;
    private static final int MAX_NUM_CLIENTS = 250;
    private static final int BACKLOG_SIZE = 50;
    private static final String NODE_LIST_FILE = "nodeList.txt";
    private static final int CMD_SIZE = 1;
    private static final int GOSSIP_MSG = 255;
    // Make sure this value is larger than number of physical nodes
    // Since potential max nodes is 100, then use 100 * 100 = 10000
    private static final int NUM_PARTITIONS = 10000;
    private static final int SLEEP_TIME = 1000; // 4 seconds
    private static final int PROP_BUFFER = 2000;
    private static final int OFFLINE_THRES = (int) (Math.log10(MAX_GOSSIP_MEMBERS) / Math.log10(2)) * SLEEP_TIME + PROP_BUFFER; // 10 seconds log(N)/log(2) * SLEEP_TIME
    private static final int REPLICATION_FACTOR = 3;

    // Private members
    private static ServerSocket servSock;
    private static Queue<Socket> backlog;
    private static AtomicInteger concurrentClientCount;
    private static AtomicInteger shutdownFlag;

    public static void main(String[] args)
    {
        try
        {
            // Initialize members
            servSock = new ServerSocket(PORT);
            backlog = new ArrayBlockingQueue<Socket>(BACKLOG_SIZE);
            concurrentClientCount = new AtomicInteger(0);
            shutdownFlag = new AtomicInteger(0);

            System.out.println("Server is ready...");

            // Create a new Producer thread for accepting client connections and adding them to the queue
            Thread producer = new Thread(new Producer());
            producer.start();

            // randomly grab 2 nodes concurrently
            Thread gossiper = new Thread(new Gossiper());
            gossiper.start();
            Thread gossiper2 = new Thread(new Gossiper());
            gossiper2.start();
            // Create a new Consumer thread for servicing clients in the queue
            Thread consumer = new Thread(new Consumer());
            consumer.start();

            // check timestamp from the nodeList
            Thread timestampCheck = new Thread(new TimestampCheck());
            timestampCheck.start();

        } catch (Exception e)
        {
            System.out.println("Internal Server Error!");
            e.printStackTrace();
        }
    }

    private static class TimestampCheck implements Runnable
    {
        public void run()
        {
            long timeDiff = 0;
            Timestamp currentTime;
            while (true)
            {
                // System.out.println("----------------------------");
                for (Node node : membership)
                {
                    try
                    {
                        if (!(node.Equals(java.net.InetAddress.getLocalHost())))
                        {
                            Thread.currentThread().sleep(SLEEP_TIME);
                            currentTime = new Timestamp(new Date().getTime());
                            // System.out.println("node: "+onlineNodeList.get(onlineNodeList.indexOf(node)).address.getHostName());
                            // System.out.println("last update: "+onlineNodeList.get(onlineNodeList.indexOf(node)).t.toString());
                            timeDiff = currentTime.getTime() - node.t.getTime();
                            // System.out.println("timeDiff: "+timeDiff);
                            if (timeDiff > OFFLINE_THRES)
                            {
                                node.online = false;
                                takePartitions(membership.indexOf(node));
                            }
                        }
                    } catch (Exception e)
                    {
                        System.out.println("unrecognized node");
                    }
                }
            }
        }
    }

    private static class Gossiper implements Runnable
    {
        public void run()
        {
            Socket socket = null;
            Random randomGenerator;
            int randomInt = 0;
            int lastRandNum = -1;
            byte[] gossipBuffer = new byte[CMD_SIZE];
            gossipBuffer[0] = (byte) (GOSSIP_MSG & 0x000000FF);

            while (true)
            {
                try
                {
                    randomGenerator = new Random();
                    // Random randomGenerator = new Random();
                    // randomly select a node to gossip
                    while (true)
                    {
                        // do{
                        randomInt = randomGenerator.nextInt(membership.size());
                        // }while(randomInt == lastRandNum);

                        if (!(membership.get(randomInt).Equals(java.net.InetAddress.getLocalHost())))
                        {
                            if (membership.get(randomInt).online)
                            {
                                break;
                            }
                        }
                    }

                    if (membership.get(randomInt).rejoin)
                    {
                        returnPartitions(randomInt);
                        membership.get(randomInt).rejoin = false;
                    }

                    // System.out.println("gossiping to server: "+onlineNodeList.get(randomInt).address.getHostName());
                    socket = new Socket(membership.get(randomInt).address.getHostName(), PORT);

                    // Send the message to the server
                    OutputStream os = socket.getOutputStream();
                    // Send the encoded string to the server
                    os.write(gossipBuffer);
                    // System.out.println("Sending request:");
                    // System.out.println(StringUtils.byteArrayToHexString(gossipBuffer));

                    // lastRandNum = randomInt;
                    // sleep
                    Thread.currentThread().sleep(SLEEP_TIME);
                } catch (Exception e)
                {
                    membership.get(randomInt).online = false;
                    membership.get(randomInt).t = new Timestamp(0);
                    // System.out.println(membership.get(randomInt).address.getHostName().toString() + " left");
                }

            }
        }
    }

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
                    if (shutdownFlag.get() == 0)
                    {
                        Socket clntSock = servSock.accept(); // Get client connection
                        // If backlog isn't full, add client to it
                        if (backlog.size() < BACKLOG_SIZE)
                        {
                            backlog.add(clntSock);
                            // System.out.println("Adding client to backlog.");
                        }
                        // Otherwise return system overload error
                        else
                        {
                            OutputStream out = clntSock.getOutputStream();
                            out.write(new byte[] { 0x03 });
                            // System.out.println("Backlog is full.");
                        }

                        // if (backlog.size() > 0)
                        // {
                        // System.out.println("# of clients in backlog: " + backlog.size());
                        // }
                    }
                    else
                    {
                        System.out.println("Server has received shutdown command. No longer accepting connections!");
                        // If shutdown flag is set to 2, then we have finished processing existing client requests and it's safe to shutdown
                        if (shutdownFlag.get() == 2)
                        {
                            System.exit(0);
                        }
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
}
