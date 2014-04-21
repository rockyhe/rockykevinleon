package phase2Pack;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class Server
{
    // Constants
    private static final int PORT = 5000;
    private static final int MAX_NUM_CLIENTS = 250;
    private static final int BACKLOG_SIZE = 50;
    private static final String NODE_LIST_FILE = "nodeList.txt";
    private static final int CMD_SIZE = 1;
    // Make sure this value is larger than number of physical nodes
    // Since potential max nodes is 100, then use 100 * 100 = 10000
    private static final int NUM_PARTITIONS = 10000;
    private static final int REPLICATION_FACTOR = 3;

    // Private members
    private static ServerSocket servSock;
    private static ConcurrentHashMap<String, byte[]> store;
    private static Queue<Socket> backlog;
    private static AtomicInteger concurrentClientCount;
    private static AtomicInteger shutdownFlag;
    private static ExecutorService threadPool;
    private static int partitionsPerNode;
    private static CopyOnWriteArrayList<Node> membership;
    // Sorted map for mapping hashed values to physical nodes
    private static ConcurrentSkipListMap<String, Node> nodeMap;
    // List that specifies the successor partitions (since we may skip partitions to ensure only unique physical nodes)
    private static ConcurrentSkipListMap<String, ArrayList<String>> successorListMap;

    public static void main(String[] args)
    {
        try
        {
            // Load the list of nodes
            try
            {
                Scanner s = new Scanner(new File(NODE_LIST_FILE));
                Node node;
                membership = new CopyOnWriteArrayList<Node>();
                while (s.hasNext())
                {
                    String nodeName = s.next();
                    node = new Node(new InetSocketAddress(nodeName, PORT), true);
                    membership.add(node);
                }
                s.close();
            } catch (FileNotFoundException e)
            {
                System.out.println("Cannot find node list file!");
            } catch (Exception e)
            {
                System.out.println("Error loading node list!");
            }

            // Map the nodes to partitions
            partitionsPerNode = NUM_PARTITIONS / membership.size();
            constructNodeMap();
            // displayNodeMap();
            // verifyNodeMap();
            // displaySuccessorListMap();

            // Initialize members
            servSock = new ServerSocket(PORT);
            store = new ConcurrentHashMap<String, byte[]>();
            backlog = new ArrayBlockingQueue<Socket>(BACKLOG_SIZE);
            concurrentClientCount = new AtomicInteger(0);
            shutdownFlag = new AtomicInteger(0);

            // Create a fixed thread pool since we'll have at most MAX_NUM_CLIENTS concurrent threads
            threadPool = Executors.newFixedThreadPool(MAX_NUM_CLIENTS);
            System.out.println("Server is ready...");

            // Create a new Producer thread for accepting client connections and adding them to the queue
            Thread producer = new Thread(new Producer());
            producer.start();

            // randomly grab 2 nodes concurrently
            Thread gossiper = new Thread(new Gossiper(membership,ring, PORT));
            gossiper.start();
            Thread gossiper2 = new Thread(new Gossiper(membership,ring,PORT));
            gossiper2.start();
            // Create a new Consumer thread for servicing clients in the queue
            Thread consumer = new Thread(new Consumer());
            consumer.start();

            // check timestamp from the nodeList
            Thread timestampCheck = new Thread(new Gossiper.TimestampCheck(membership,ring));
            timestampCheck.start();

        } catch (Exception e)
        {
            System.out.println("Internal Server Error!");
            e.printStackTrace();
        }
    }

    private static void constructNodeMap()
    {
        nodeMap = new ConcurrentSkipListMap<String, Node>();
        // Divide the hash space into NUM_PARTITIONS partitions
        // with each physical node responsible for (NUM_PARTITIONS / number of nodes) hash ranges
        // int partitionsPerNode = NUM_PARTITIONS / onlineNodeList.size();
        for (Node node : membership)
        {
            for (int i = 0; i < partitionsPerNode; ++i)
            {
                nodeMap.put(KVStore.getHash(node.address.getHostName() + i), node);
            }
        }

        // Construct the successor list for each partition
        constructSuccessorLists();
    }

    private static void constructSuccessorLists()
    {
        successorListMap = new ConcurrentSkipListMap<String, ArrayList<String>>();
        int numSuccessors = REPLICATION_FACTOR - 1;
        if (membership.size() <= numSuccessors)
        {
            // If the number of participating nodes is not larger than the backups desired,
            // then just set to number of backups to the number of participating nodes
            numSuccessors = membership.size() - 1;
        }

        // For each partition, construct its successor list
        // Ensure there are no duplicate physical nodes in the successor list
        HashMap<String, Node> successors;
        Iterator<Map.Entry<String, Node>> partitionIterator = nodeMap.entrySet().iterator();
        Map.Entry<String, Node> sourcePartition;

        while (partitionIterator.hasNext())
        {
            sourcePartition = partitionIterator.next();
            successors = new HashMap<String, Node>();

            Map.Entry<String, Node> lastSuccessor = sourcePartition;
            while (successors.size() < numSuccessors)
            {
                // Keep looking for the next successor if we already have a successor partition owned by the same physical node
                // or if the successor is owned by the same physical node that owns the current partition that we're generating successors for
                // This guarantees that the successors (and therefore the replicas) will be different physical nodes
                do
                {
                    lastSuccessor = nodeMap.higherEntry(lastSuccessor.getKey());
                    // If there are no high entries, then we're at the end of the ring, so wrap around
                    if (lastSuccessor == null)
                    {
                        lastSuccessor = nodeMap.firstEntry();
                    }

                } while (successors.values().contains(lastSuccessor.getValue()) || lastSuccessor.getValue().Equals(sourcePartition.getValue()));

                successors.put(lastSuccessor.getKey(), lastSuccessor.getValue());
            }

            successorListMap.put(sourcePartition.getKey(), new ArrayList<String>(successors.keySet()));
        }
    }

    private static void returnPartitions(int idx)
    {
        // foreach nodes in the nodeList
        // System.out.println("<<<<<<<<<<<<<<<<<<<<<<<rejoined node: "+onlineNodeList.get(idx).address.toString());
        for (Node node : membership)
        {

            // foreach partition in each node
            for (int i = 0; i < partitionsPerNode; ++i)
            {
                // if current partition's hash key's value (node) is the rejoin node
                if (node.Equals(membership.get(idx)))
                {
                    // replace it with the next node, or the first node
                    // System.out.println("rejoined node: "+onlineNodeList.get(idx).address.toString());
                    // System.out.println("hash key for rejoin node: "+KVStore.getHash(node.address.getHostName() + i).toString());

                    nodeMap.replace(KVStore.getHash(node.address.getHostName() + i), membership.get(idx));
                }
            }
        }
    }

    private static void takePartitions(int idx)
    {
        // get the node that is offline now
        // foreach nodes in the nodeList
        int j;

        // System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>offline  node: "+onlineNodeList.get(idx).address.toString());
        for (Node node : membership)
        {
            // foreach partition in each node
            for (int i = 0; i < partitionsPerNode; ++i)
            {
                j = 0;
                // if current partition's hash key's value (node) is the offline node
                if (nodeMap.get(KVStore.getHash(node.address.getHostName() + i)).Equals(membership.get(idx)))
                {
                    // replace it with the next node, or the first node
                    // System.out.println("hash key for offline node: "+KVStore.getHash(node.address.getHostName() + i).toString());

                    if (idx < (membership.size() - 1))
                    {
                        j = idx + 1;
                    }
                    else
                    {
                        j = 0;
                    }

                    while (true)
                    {
                        if (membership.get(j).online)
                        {
                            nodeMap.replace(KVStore.getHash(node.address.getHostName() + i), membership.get(j));
                            break;
                        }

                        if (j == (membership.size() - 1))
                        {
                            j = 0;
                        }
                        else
                        {
                            j++;
                        }
                    }
                }
            }
        }
    }

    private static void verifyNodeMap()
    {
        Map<Node, Integer> distribution = new HashMap<Node, Integer>();
        for (Node node : membership)
        {
            distribution.put(node, 0);
        }

        for (Map.Entry<String, Node> entry : nodeMap.entrySet())
        {
            Node node = entry.getValue();
            Integer count = distribution.get(node);
            distribution.put(node, count + 1);
        }

        for (Map.Entry<Node, Integer> node : distribution.entrySet())
        {
            System.out.println(node.getKey().address.getHostName() + " => " + node.getValue().toString());
        }
    }

    private static void displayNodeMap()
    {
        for (Map.Entry<String, Node> entry : nodeMap.entrySet())
        {
            String key = entry.getKey();
            InetSocketAddress addr = entry.getValue().address;
            // System.out.println(key + " => " + addr.toString() + " => " + entry.getValue().online);
            System.out.println(key + " => " + membership.indexOf(entry.getValue()));
        }
    }

    private static void displaySuccessorListMap()
    {
        for (Map.Entry<String, ArrayList<String>> entry : successorListMap.entrySet())
        {
            String key = entry.getKey();
            ArrayList<String> successors = entry.getValue();
            System.out.println(key + " => Successors:");
            for (String successor : successors)
            {
                System.out.println("\t" + successor + " => " + nodeMap.get(successor).address.toString());
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
                    // If current number of concurrent clients hasn't reached MAX_NUM_CLIENTS
                    // then service client at the head of queue
                    if (concurrentClientCount.get() < MAX_NUM_CLIENTS && (clntSock = backlog.poll()) != null)
                    {
                        KVStore connection = new KVStore(clntSock, store, nodeMap, concurrentClientCount, shutdownFlag, membership, successorListMap);
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
}
