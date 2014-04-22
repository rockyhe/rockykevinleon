package phase2Pack.enums;

public enum ErrorCodes
{
    SUCCESS(0),
    INEXISTENT_KEY(1),
    OUT_OF_SPACE(2),
    SYSTEM_OVERLOAD(3),
    INTERNAL_KVSTORE(4),
    UNRECOGNIZED_COMMAND(5);

    private final static ErrorCodes[] values = ErrorCodes.values();
    private int value;

    private ErrorCodes(int value)
    {
        this.value = value;
    }

    public int getValue()
    {
        return this.value;
    }

    public static ErrorCodes fromInt(int i)
    {
        return values[i];
    }

    public static ErrorCodes fromByte(byte b)
    {
        return values[b];
    }

    public static byte toByte(ErrorCodes code)
    {
        return ((Integer)(code.value)).byteValue();
    }

    public byte toByte()
    {
        return ((Integer)(this.value)).byteValue();
    }
}
