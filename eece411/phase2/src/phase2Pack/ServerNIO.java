package phase2Pack;

import phase2Pack.nio.ReactorInitiator;

public class ServerNIO
{
    // Constants
    public static final int PORT = 5000;
    public static final long TIMEOUT = 10000;

    // Private members
    private static KVStore kvStore;

    public static void main(String[] args) throws Exception
    {
        try
        {
            kvStore = new KVStore(PORT);

            System.out.println("Starting NIO server at port : " + PORT);
            new ReactorInitiator().initiateReactiveServer(PORT, kvStore);

            System.out.println("Server is ready...");
        } catch (Exception e) {
            System.out.println("Internal Server Error!");
            e.printStackTrace();
        }
    }
}
