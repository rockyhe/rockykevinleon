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

    private final static Commands[] values = Commands.values();
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
        return values[i];
    }

    public static Commands fromByte(byte b)
    {
        return values[b];
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
