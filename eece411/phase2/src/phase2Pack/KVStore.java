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

    /**
     * Puts the given value into the store, mapped to the given key.
     * If there is already a value corresponding to the key, then the value is overwritten.
     * If the number of key-value pairs is KVSTORE_SIZE, the store returns out of space error.
     */
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

    /**
     * Returns the value associated with the given key.
     * If there is no such key in the store, the store returns key not found error.
     */
    public byte[] get(String hashedKey) throws InexistentKeyException
    {
        // If key doesn't exist on this node's local store
        if (!store.containsKey(hashedKey))
        {
            throw new InexistentKeyException();
        }
        return store.get(hashedKey);
    }

    /**
     * Removes the value associated with the given key.
     * If there is no such key in the store, the store returns key not found error.
     */
    public void remove(String hashedKey) throws InexistentKeyException
    {
        if (!store.containsKey(hashedKey))
        {
            throw new InexistentKeyException();
        }
        store.remove(hashedKey);
    }
}
