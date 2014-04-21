package phase2Pack;

import java.util.concurrent.ConcurrentHashMap;

import phase2Pack.Exceptions.InexistedKeyException;
import phase2Pack.Exceptions.OutOfSpaceException;

public class KVStore
{
    // Constants
    private static final int KVSTORE_SIZE = 40000;

    // Private members
    private ConcurrentHashMap<String, byte[]> store;

    public KVStore()
    {
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
}
