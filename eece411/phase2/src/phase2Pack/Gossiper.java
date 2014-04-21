package phase2Pack;

import java.io.OutputStream;
import java.net.Socket;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;

public class Gossiper implements Runnable
{
    private static final int CMD_SIZE = 1;
    private static final int MAX_GOSSIP_MEMBERS = 16;
    private static final int GOSSIP_MSG = 255;
    private static final int SLEEP_TIME = 1000; // 4 seconds
    private static final int PROP_BUFFER = 2000;
    private static final int OFFLINE_THRES = (int) (Math.log10(MAX_GOSSIP_MEMBERS) / Math.log10(2)) * SLEEP_TIME + PROP_BUFFER; // 10 seconds log(N)/log(2) * SLEEP_TIME

    private CopyOnWriteArrayList<Node> membership;
    private KVStore kvStore;
    private int PORT;


    Gossiper(KVStore kvStore, int serverPORT)
    {
        this.kvStore = kvStore;
        this.membership = kvStore.getMembership();
        this.PORT = serverPORT;
    }

    public void run()
    {
        Socket socket = null;
        Random randomGenerator;
        int randomInt = 0;
        byte[] gossipBuffer = new byte[CMD_SIZE];
        gossipBuffer[0] = (byte) (GOSSIP_MSG & 0x000000FF);

        while (true)
        {
            try {
                randomGenerator = new Random();
                // Random randomGenerator = new Random();
                // randomly select a node to gossip
                while (true)
                {
                    randomInt = randomGenerator.nextInt(membership.size());

                    if (!(membership.get(randomInt).Equals(java.net.InetAddress.getLocalHost())))
                    {
                        if (membership.get(randomInt).online)
                        {
                            break;
                        }
                    }
                }

                if (membership.get(randomInt).rejoin)
                {
                    kvStore.returnPartitions(randomInt);
                    membership.get(randomInt).rejoin = false;
                }

                // System.out.println("gossiping to server: "+onlineNodeList.get(randomInt).address.getHostName());
                socket = new Socket(membership.get(randomInt).address.getHostName(), PORT);

                // Send the message to the server
                OutputStream os = socket.getOutputStream();
                // Send the encoded string to the server
                os.write(gossipBuffer);
                // System.out.println("Sending request:");
                // System.out.println(StringUtils.byteArrayToHexString(gossipBuffer));

                // lastRandNum = randomInt;
                // sleep
                Thread.currentThread().sleep(SLEEP_TIME);
            } catch (Exception e) {
                membership.get(randomInt).online = false;
                membership.get(randomInt).t = new Timestamp(0);
                // System.out.println(membership.get(randomInt).address.getHostName().toString() + " left");
            }

        }
    }

    static class TimestampCheck implements Runnable
    {
        private CopyOnWriteArrayList<Node> membership;
        private KVStore kvStore;

        TimestampCheck(CopyOnWriteArrayList<Node> gossipMembers, KVStore kvStore)
        {
            this.membership = gossipMembers;
            this.kvStore = kvStore;
        }

        public void run()
        {
            long timeDiff = 0;
            Timestamp currentTime;
            while (true)
            {
                // System.out.println("----------------------------");
                for (Node node : membership)
                {
                    try {
                        if (!(node.Equals(java.net.InetAddress.getLocalHost())))
                        {
                            Thread.currentThread().sleep(SLEEP_TIME);
                            currentTime = new Timestamp(new Date().getTime());
                            // System.out.println("node: "+onlineNodeList.get(onlineNodeList.indexOf(node)).address.getHostName());
                            // System.out.println("last update: "+onlineNodeList.get(onlineNodeList.indexOf(node)).t.toString());
                            timeDiff = currentTime.getTime() - node.t.getTime();
                            // System.out.println("timeDiff: "+timeDiff);
                            if (timeDiff > OFFLINE_THRES)
                            {
                                node.online = false;
                                kvStore.takePartitions(membership.indexOf(node));
                            }
                        }
                    } catch (Exception e) {
                        System.out.println("unrecognized node");
                    }
                }
            }
        }
    }
}
