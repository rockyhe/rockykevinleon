package phase2Pack.enums;

import phase2Pack.ByteOrder;

public enum Commands
{
    PUT(1),
    GET(2),
    REMOVE(3),
    SHUTDOWN(4),
    PUT_TO_REPLICA(101),
    REMOVE_FROM_REPLICA(103),
    GOSSIP(255);

    private static final int CMD_SIZE = 1;
    private int value;

    private Commands(int value)
    {
        this.value = value;
    }

    public int getValue()
    {
        return this.value;
    }

    public static Commands fromInt(int i)
    {
        switch (i)
        {
        case 1:
            return PUT;
        case 2:
            return GET;
        case 3:
            return REMOVE;
        case 4:
            return SHUTDOWN;
        case 101:
            return PUT_TO_REPLICA;
        case 103:
            return REMOVE_FROM_REPLICA;
        case 255:
            return GOSSIP;
        default:
            System.out.println("Error trying to cast int of " + i + " to Commands enum. Returning null!");
            return null;
        }
    }

    public byte toByte()
    {
        byte[] buffer = new byte[CMD_SIZE];
        ByteOrder.int2leb(this.value, buffer, 0);
        return buffer[0];
    }
}
