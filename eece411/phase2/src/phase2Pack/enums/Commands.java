package phase2Pack.enums;

public enum Commands
{
    PUT(1),
    GET(2),
    REMOVE(3),
    SHUTDOWN(4),
    PUT_TO_REPLICA(101),
    REMOVE_FROM_REPLICA(103),
    GOSSIP(255);

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
            return null;
        }
    }

    public static byte toByte(Commands code)
    {
        return ((Integer)(code.value)).byteValue();
    }

    public byte toByte()
    {
        return ((Integer)(this.value)).byteValue();
    }
}
