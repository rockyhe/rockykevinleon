package phase2Pack;

import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.sql.Timestamp;
import java.util.Date;
import phase2Pack.enums.Commands;

public class Heartbeat implements Runnable
{
    private static final int CMD_SIZE = 1;
    private static final int MAX_HEARTBEAT_MEMBERS = 4;
    private static final int SLEEP_TIME = 300; // 4 seconds
    private static final int PROP_BUFFER = 2000;
    private static final int OFFLINE_THRES = (int)SLEEP_TIME*MAX_HEARTBEAT_MEMBERS/2 + PROP_BUFFER; // 10 seconds log(N)/log(2) * SLEEP_TIME

    public String localHost;

    private ConsistentHashRing ring;
    private int port;
    private int threadId;

    Heartbeat(ConsistentHashRing ring, int port, int id)
    {
        this.ring = ring;
        this.port = port;
        this.threadId = id;

        // Store the local host name for convenient access later
        try {
            localHost = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            System.out.println("Couldn't determine IP of local host!");
        }
    }

    public void run()
    {
        Socket socket = null;
        //Random randomGenerator;
        int randomInt = 0;
        int rangeHigh = 0;
        int rangeLow = 0;
        byte[] buffer = new byte[CMD_SIZE];
        buffer[0] = (byte) (Commands.HEARTBEAT.getValue() & 0x000000FF);

        if (threadId == 1)
        {
            rangeLow = 0;
            rangeHigh = ring.getMembership().size()/2;
        }
        else
        {
            rangeLow = ring.getMembership().size()/2;
            rangeHigh = ring.getMembership().size();
        }

        //randomGenerator = new Random();
        if(ring.membershipIndexOf(localHost) == 0) {
            randomInt = ring.membershipIndexOf(localHost);
        } else {
            randomInt = ring.membershipIndexOf(localHost)-1;
        }

        Node target;
        while (true)
        {
            try {
                // Random randomGenerator = new Random();
                // randomly select a node to ping
                //while (true)
                //{
                //randomInt = randomGenerator.nextInt(rangeHigh-rangeLow)+rangeLow;

                target = ring.getMembership().get(randomInt);
                if (target.Equals(localHost))
                {
                    if (randomInt >= rangeHigh-1)
                    {
                        randomInt = rangeLow;
                    }
                    else
                    {
                        randomInt++;
                    }
                    continue;
                }
                //}


                if (target.rejoin)
                {
                    ring.returnPartitions(target);
                    target.rejoin = false;
                }

                socket = new Socket(target.hostname, port);

                // Send the message to the server
                OutputStream os = socket.getOutputStream();
                // Send the encoded string to the server
                os.write(buffer);
                //System.out.println("pinging: " + target.hostname);

                // sleep
                if(randomInt >= rangeHigh-1) {
                    randomInt = rangeLow;
                } else {
                    randomInt++;
                }

                Thread.currentThread().sleep(SLEEP_TIME);
            } catch (Exception e) {
                target = ring.getMembership().get(randomInt);
                target.online = false;
                target.t = new Timestamp(0);
                //System.out.println(target.hostname + " offline");
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
                                ring.takePartitions(node);
                                System.out.println("I took partition of "+node.hostname);
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
