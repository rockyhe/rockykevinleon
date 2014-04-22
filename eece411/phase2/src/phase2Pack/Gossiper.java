package phase2Pack;

import java.io.OutputStream;
import java.net.Socket;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Random;

public class Gossiper implements Runnable
{
    private static final int CMD_SIZE = 1;
    private static final int MAX_GOSSIP_MEMBERS = 16;
    private static final int GOSSIP_MSG = 255;
    private static final int SLEEP_TIME = 1000; // 4 seconds
    private static final int PROP_BUFFER = 2000;
    private static final int OFFLINE_THRES = (int) (Math.log10(MAX_GOSSIP_MEMBERS) / Math.log10(2)) * SLEEP_TIME + PROP_BUFFER; // 10 seconds log(N)/log(2) * SLEEP_TIME

    private ConsistentHashRing ring;
    private int gossipPort;

    Gossiper(ConsistentHashRing ring, int gossipPort)
    {
        this.ring = ring;
        this.gossipPort = gossipPort;
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
                    randomInt = randomGenerator.nextInt(ring.getMembership().size());

                    if (!(ring.getMembership().get(randomInt).Equals(java.net.InetAddress.getLocalHost())))
                    {
                        if (ring.getMembership().get(randomInt).online)
                        {
                            break;
                        }
                    }
                }

                if (ring.getMembership().get(randomInt).rejoin)
                {
                    System.out.println(ring.getMembership().get(randomInt).hostname+" rejoined");
                    ring.returnPartitions(randomInt);
                    ring.getMembership().get(randomInt).rejoin = false;
                }

                socket = new Socket(ring.getMembership().get(randomInt).hostname, gossipPort);

                // Send the message to the server
                OutputStream os = socket.getOutputStream();
                // Send the encoded string to the server
                os.write(gossipBuffer);
                //System.out.println("gossiping to:"+ring.getMembership().get(randomInt).hostname);

                // sleep
                Thread.currentThread().sleep(SLEEP_TIME);
            } catch (Exception e) {
                ring.getMembership().get(randomInt).online = false;
                ring.getMembership().get(randomInt).t = new Timestamp(0);
                System.out.println(ring.getMembership().get(randomInt).hostname + " left");
            }

        }
    }

    static class TimestampCheck implements Runnable
    {
        private ConsistentHashRing ring;

        TimestampCheck(ConsistentHashRing ring)
        {
            this.ring = ring;
        }

        public void run()
        {
            long timeDiff = 0;
            Timestamp currentTime;
            while (true)
            {
                // System.out.println("----------------------------");
                for (Node node : ring.getMembership())
                {
                    try {
                        if (!(node.Equals(java.net.InetAddress.getLocalHost())))
                        {
                            Thread.currentThread().sleep(SLEEP_TIME);
                            currentTime = new Timestamp(new Date().getTime());
                            // System.out.println("node: "+onlineNodeList.get(onlineNodeList.indexOf(node)).hostname);
                            // System.out.println("last update: "+onlineNodeList.get(onlineNodeList.indexOf(node)).t.toString());
                            timeDiff = currentTime.getTime() - node.t.getTime();
                            // System.out.println("timeDiff: "+timeDiff);
                            if (timeDiff > OFFLINE_THRES)
                            {
                                node.online = false;
                                ring.takePartitions(ring.getMembership().indexOf(node));
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
