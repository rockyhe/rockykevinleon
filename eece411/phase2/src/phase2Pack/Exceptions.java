package phase2Pack;

import java.io.OutputStream;
import java.net.Socket;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;

public class InexistedKeyException extends Exception
{
}

public class OutOfSpaceException extends Exception
{
}

public class SystemOverloadException extends Exception
{
}

public class InternalKVStoreException extends Exception
{
}

public class UnrecognizedCmdException extends Exception
{
}
