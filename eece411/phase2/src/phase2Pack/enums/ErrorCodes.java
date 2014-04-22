package phase2Pack.enums;

public enum ErrorCodes
{
    SUCCESS(0),
    INEXISTENT_KEY(1),
    OUT_OF_SPACE(2),
    SYSTEM_OVERLOAD(3),
    INTERNAL_KVSTORE(4),
    UNRECOGNIZED_COMMAND(5);

    private int value;

    private ErrorCodes(int value)
    {
        this.value = value;
    }

    public int getValue()
    {
        return this.value;
    }

    public static ErrorCodes fromByte(byte b)
    {
        switch (b)
        {
        case 0:
            return SUCCESS;
        case 1:
            return INEXISTENT_KEY;
        case 2:
            return OUT_OF_SPACE;
        case 3:
            return SYSTEM_OVERLOAD;
        case 4:
            return INTERNAL_KVSTORE;
        case 5:
            return UNRECOGNIZED_COMMAND;
        default:
            System.out.println("Error trying to cast byte of " + b + " to ErrorCodes enum. Returning null!");
            return null;
        }
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
