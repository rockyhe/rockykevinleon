package phase2Pack;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class ConsistentHashRing
{
    // Constants
    private static final String NODE_LIST_FILE = "nodeList.txt";
    // Make sure this value is larger than number of physical nodes
    // Since potential max nodes is 100, then use 100 * 100 = 10000
    private static final int NUM_PARTITIONS = 10000;
    private static final int REPLICATION_FACTOR = 3;

    public String localHost;

    // Private members
    private int partitionsPerNode;
    private CopyOnWriteArrayList<Node> membership;
    private ConcurrentSkipListMap<String, Node> partitionMap; // Sorted map for mapping partitions to physical nodes
    private ConcurrentSkipListMap<String, ArrayList<String>> successorListMap; // Sorted map for mapping each partition to its successor partitions

    public ConsistentHashRing(int port)
    {
        // Load the list of participating nodes and construct the membership list
        try
        {
            // Store the local host name for convenient access later
            localHost = InetAddress.getLocalHost().getHostName();

            membership = new CopyOnWriteArrayList<Node>();
            Scanner s = new Scanner(new File(NODE_LIST_FILE));
            Node node;
            String nodeName;
            while (s.hasNext())
            {
                nodeName = s.next();
                node = new Node(nodeName, true);
                membership.add(node);
            }
            s.close();
        } catch (FileNotFoundException e) {
            System.out.println("Cannot find node list file!");
        } catch (UnknownHostException e) {
            System.out.println("Couldn't determine IP of local host!");
        } catch (Exception e) {
            System.out.println("Error loading node list!");
        }

        // Construct the initial ring by mapping the nodes to partitions
        constructRing();
        // displayRing();
        // verifyRing();
        // displaySuccessorListMap();
    }

    public CopyOnWriteArrayList<Node> getMembership()
    {
        return membership;
    }

    public Node getNodeForPartition(String key)
    {
        return partitionMap.get(key);
    }

    public Map.Entry<String, Node> getPrimary(String hashedKey)
    {
        // Get the node responsible for the partition with first hashed value that is greater than or equal to the key (i.e. clockwise on the ring)
        // System.out.println("Hashed key string: " + hashedKey);
        Map.Entry<String, Node> entry = partitionMap.ceilingEntry(hashedKey);
        // If ceiling entry is null, then we've wrapped around the entire node ring, so set to first node
        if (entry == null)
        {
            // System.out.println("Setting entry to first entry");
            entry = partitionMap.firstEntry();
        }
        // System.out.println("Entry hash: " + entry.getKey());
        return entry;
    }

    public ArrayList<String> getSuccessors(String primaryKey)
    {
        return successorListMap.get(primaryKey);
    }

    public boolean isSuccessor(String primaryKey, String hostname)
    {
        for (String successor : getSuccessors(primaryKey))
        {
            if (partitionMap.get(successor).Equals(hostname))
            {
                return true;
            }
        }
        return false;
    }

    public int membershipIndexOf(String hostname)
    {
        int idx = -1;
        for (Node node : membership)
        {
            if (node.Equals(hostname))
            {
                idx = membership.indexOf(node);
                break;
            }
        }
        return idx;
    }

    public static String getHash(String msg)
    {
        String result = null;
        try
        {
            // Hash the id using SHA-256 to get a 32 byte hash
            // since the ring space is from 0 to (2^256)-1
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] hash = md.digest(msg.getBytes("UTF-8"));
            result = StringUtils.byteArrayToHexString(hash);
        } catch (Exception e)
        {
            System.out.println("Error trying to get hash of string: " + msg);
        }
        return result;
    }

    private void constructRing()
    {
        // Divide the hash space into NUM_PARTITIONS partitions (ideally, but may be less if membership size doesn't divide nicely)
        // with each physical node responsible for (NUM_PARTITIONS / number of nodes) hash ranges
        // int partitionsPerNode = NUM_PARTITIONS / onlineNodeList.size();
        partitionMap = new ConcurrentSkipListMap<String, Node>();
        partitionsPerNode = NUM_PARTITIONS / membership.size();
        for (Node node : membership)
        {
            for (int i = 0; i < partitionsPerNode; ++i)
            {
                // Distribution of partitions will be random based on hash value of hostname + i
                partitionMap.put(getHash(node.hostname + i), node);
            }
        }

        // Construct the successor list for each partition
        constructSuccessorLists();
    }

    // When constructing successor list, we may skip partitions to ensure only unique physical nodes
    private void constructSuccessorLists()
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
        Iterator<Map.Entry<String, Node>> partitionIterator = partitionMap.entrySet().iterator();
        Map.Entry<String, Node> sourcePartition;

        while (partitionIterator.hasNext())
        {
            sourcePartition = partitionIterator.next();
            successors = new HashMap<String, Node>();

            Map.Entry<String, Node> lastSuccessor = sourcePartition;
            while (successors.size() < numSuccessors)
            {
                // Keep looking for the next successor if we already have a successor partition owned by the same physical node
                // or if the successor is owned by the same physical node that owns the current partition that we're generating successors for (i.e. source partition)
                // This guarantees that the successors (and therefore the replicas) will be different physical nodes
                do
                {
                    lastSuccessor = partitionMap.higherEntry(lastSuccessor.getKey());
                    // If there are no high entries, then we're at the end of the ring, so wrap around
                    if (lastSuccessor == null)
                    {
                        lastSuccessor = partitionMap.firstEntry();
                    }

                } while (successors.values().contains(lastSuccessor.getValue()) || lastSuccessor.getValue().Equals(sourcePartition.getValue()));

                successors.put(lastSuccessor.getKey(), lastSuccessor.getValue());
            }

            successorListMap.put(sourcePartition.getKey(), new ArrayList<String>(successors.keySet()));
        }
    }

    public void returnPartitions(Node rejoinedNode)
    {
        // find the partitions originally owned by the rejoined node and reassigned them
        for (int i = 0; i < partitionsPerNode; ++i)
        {
            partitionMap.replace(getHash(rejoinedNode.hostname + i), rejoinedNode);
        }
    }

    public void takePartitions(Node offlineNode)
    {
        // get the index of the offline node
        int idx = membership.indexOf(offlineNode);

        // default new owner to the next node in the membership list. if at the end of the list, set to the beginning
        int idxOfNewOwner = idx + 1;
        if (idx == (membership.size() - 1))
        {
            idxOfNewOwner = 0;
        }

        // find the partitions currently owned by the offline node
        for (int i = 0; i < partitionsPerNode; ++i)
        {
            String partitionHash = getHash(offlineNode.hostname + i);
            // if the current partition is owned by the offline node
            if (partitionMap.get(partitionHash).Equals(offlineNode))
            {
                // replace it with the next node, or the first node, in the membership list
                Node newOwner;
                while (true)
                {
                    newOwner = membership.get(idxOfNewOwner);
                    // IMPORTANT: ensure that the owner-to-be isn't already a node owning one of the partition's successors
                    if (newOwner.online && !isSuccessor(partitionHash, newOwner.hostname))
                    {
                        partitionMap.replace(partitionHash, newOwner);
                        break;
                    }

                    if (idxOfNewOwner == (membership.size() - 1))
                    {
                        idxOfNewOwner = 0;
                    }
                    else
                    {
                        idxOfNewOwner++;
                    }
                }
            }
        }
    }

    /*
     * For debugging purposes: print the number of partitions assigned to each physical node
     */
    private void verifyRing()
    {
        Map<Node, Integer> distribution = new HashMap<Node, Integer>();
        for (Node node : membership)
        {
            distribution.put(node, 0);
        }

        for (Map.Entry<String, Node> entry : partitionMap.entrySet())
        {
            Node node = entry.getValue();
            Integer count = distribution.get(node);
            distribution.put(node, count + 1);
        }

        for (Map.Entry<Node, Integer> node : distribution.entrySet())
        {
            System.out.println(node.getKey().hostname + " => " + node.getValue().toString());
        }
    }

    /*
     * For debugging purposes: print the mapping of hash value to physical node, for each partition
     */
    private void displayRing()
    {
        for (Map.Entry<String, Node> entry : partitionMap.entrySet())
        {
            String key = entry.getKey();
            String hostname = entry.getValue().hostname;
            // System.out.println(key + " => " + hostname + " => " + entry.getValue().online);
            System.out.println(key + " => " + membership.indexOf(entry.getValue()));
        }
    }

    /*
     * For debugging purposes: print the successor list mapping of hash value to physical node, for each partition
     */
    private void displaySuccessorListMap()
    {
        for (Map.Entry<String, ArrayList<String>> entry : successorListMap.entrySet())
        {
            String key = entry.getKey();
            ArrayList<String> successors = entry.getValue();
            System.out.println(key + " => " + partitionMap.get(key).hostname + " => Successors:");
            for (String successor : successors)
            {
                System.out.println("\t" + successor + " => " + partitionMap.get(successor).hostname);
            }
        }
    }

}
