package phase2Pack;

import java.sql.Timestamp;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;

import phase2Pack.nio.Dispatcher;

public class KVStore
{
    // Constants
    private static final int KVSTORE_SIZE = 40000;
    private static final int CMD_SIZE = 1;
    private static final int KEY_SIZE = 32;
    private static final int VALUE_SIZE = 1024;
    private static final int ERR_SIZE = 1;

    // Private members
    private ConsistentHashRing ring;
    private ConcurrentHashMap<String, byte[]> store;

    public KVStore(ConsistentHashRing ring)
    {
        this.ring = ring;
        this.store = new ConcurrentHashMap<String, byte[]>();
    }

    public void put(String hashedKey, byte[] value) throws OutOfSpaceException
    {
        if (store.size() < KVSTORE_SIZE)
        {
            store.put(hashedKey, value);
        }
        else
        {
            throw new OutOfSpaceException();
        }
    }

    public byte[] get(String hashedKey) throws InexistedKeyException
    {
        // If key doesn't exist on this node's local store
        if (!store.containsKey(hashedKey))
        {
            throw new InexistedKeyException();
        }
        return store.get(hashedKey);
    }

    public void remove(String hashedKey) throws InexistedKeyException
    {
        if (!store.containsKey(hashedKey))
        {
            throw new InexistedKeyException();
        }
        else
        {
            store.remove(hashedKey);
        }
    }

    public void shutdown()
    {
        // Increment the shutdown flag to no longer accept incoming connections
        Dispatcher.shutdown();

        // Update online status to false and timestamp to 0 of self node, so it propagates faster
        int index = ring.getMembership().indexOf(ring.localHost);
        if (index >= 0)
        {
            Node self = ring.getMembership().get(index);
            self.online = false;
            self.t = new Timestamp(0);
        }
    }

    public void gossip()
    {
        for (Node node : ring.getMembership())
        {
            // System.out.println("client sock: "+clntSock.getInetAddress().getHostName().toString());
            if (node.Equals(ring.localHost))
            {
                if (!node.online)
                {
                    node.rejoin = true;
                }
                node.online = true;
                node.t = new Timestamp(new Date().getTime());
                // System.out.println("timestamp: "+onlineNodeList.get(onlineNodeList.indexOf(node)).t.toString());
                break;
            }
        }
    }

    public void putToReplica(byte[] key, byte[] value)
    {
        System.out.println("replicating: " + StringUtils.byteArrayToHexString(key));
        // Convert key bytes to string
        String keyStr = StringUtils.byteArrayToHexString(key);
        // Re-hash the key using our hash function so it's consistent
        String rehashedKeyStr = ConsistentHashRing.getHash(keyStr);
        store.put(rehashedKeyStr, value);
    }

    public void removeFromReplica(byte[] key)
    {
        // Convert key bytes to string
        String keyStr = StringUtils.byteArrayToHexString(key);
        // Re-hash the key using our hash function so it's consistent
        String rehashedKeyStr = ConsistentHashRing.getHash(keyStr);
        store.remove(rehashedKeyStr);
    }
}
