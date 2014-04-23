package phase2Pack.enums;

import phase2Pack.ByteOrder;

public enum ErrorCodes
{
    SUCCESS(0),
    INEXISTENT_KEY(1),
    OUT_OF_SPACE(2),
    SYSTEM_OVERLOAD(3),
    INTERNAL_KVSTORE(4),
    UNRECOGNIZED_COMMAND(5);

    private static final int ERR_SIZE = 1;
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
        switch (i)
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
            System.out.println("Error trying to cast int of " + i + " to ErrorCodes enum. Returning null!");
            return null;
        }
    }

    public byte toByte()
    {
        byte[] buffer = new byte[ERR_SIZE];
        buffer[0]=(byte)(this.value & 0x000000FF);
        return buffer[0];
    }
}
