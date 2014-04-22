package phase2Pack;

import java.util.concurrent.ConcurrentHashMap;

import phase2Pack.Exceptions.InexistentKeyException;
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

    public byte[] get(String hashedKey) throws InexistentKeyException
    {
        // If key doesn't exist on this node's local store
        if (!store.containsKey(hashedKey))
        {
            throw new InexistentKeyException();
        }
        return store.get(hashedKey);
    }

    public void remove(String hashedKey) throws InexistentKeyException
    {
        if (!store.containsKey(hashedKey))
        {
            throw new InexistentKeyException();
        }
        store.remove(hashedKey);
    }
}
