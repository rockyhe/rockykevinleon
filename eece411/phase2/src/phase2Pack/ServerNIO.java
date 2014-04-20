package phase2Pack;

import java.util.concurrent.ConcurrentHashMap;

import phase2Pack.nio.ReactorInitiator;

public class ServerNIO
{
    private static final int PORT = 5000;
    private static ConcurrentHashMap<String, byte[]> store;
    private static final long TIMEOUT = 10000;

    public static void main(String[] args) throws Exception
    {
        System.out.println("Starting NIO server at port : " + PORT);
        new ReactorInitiator().initiateReactiveServer(PORT);
    }
}
