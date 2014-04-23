package phase2Pack;

import java.io.OutputStream;
import java.net.Socket;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Random;

import phase2Pack.enums.Commands;

public class Ping implements Runnable
{
    private static final int CMD_SIZE = 1;
    private static final int MAX_PING_MEMBERS = 100;
    private static final int SLEEP_TIME = 250; // 4 seconds
    private static final int PROP_BUFFER = 2000;
    private static final int OFFLINE_THRES = (int) (Math.log10(MAX_PING_MEMBERS) / Math.log10(2)) * SLEEP_TIME + PROP_BUFFER; // 10 seconds log(N)/log(2) * SLEEP_TIME

    private ConsistentHashRing ring;
    private int pingPort;
    private int pingerNum;

    Ping(ConsistentHashRing ring, int port, int num)
    {
        this.ring = ring;
        this.pingPort = port;
        this.pingerNum = num;
    }

    public void run()
    {
        Socket socket = null;
        Random randomGenerator;
        int randomInt = 0;
        int rangeHigh = 0;
        int rangeLow = 0;
        byte[] pingBuffer = new byte[CMD_SIZE];
        pingBuffer[0] = (byte) (Commands.PING.getValue() & 0x000000FF);

        if (pingerNum == 1)
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
        randomInt = rangeLow;
        while (true)
        {
            try {
                // Random randomGenerator = new Random();
                // randomly select a node to ping
                //while (true)
                //{
                //randomInt = randomGenerator.nextInt(rangeHigh-rangeLow)+rangeLow;

                if(ring.getMembership().get(randomInt).Equals(java.net.InetAddress.getLocalHost()))
                {
                    if(randomInt == rangeHigh-1) {
                        randomInt = rangeLow;
                    } else {
                        randomInt++;
                    }
                    continue;
                }
                //}


                if (ring.getMembership().get(randomInt).rejoin)
                {
                    ring.returnPartitions(randomInt);
                    ring.getMembership().get(randomInt).rejoin = false;
                }

                socket = new Socket(ring.getMembership().get(randomInt).hostname, pingPort);

                // Send the message to the server
                OutputStream os = socket.getOutputStream();
                // Send the encoded string to the server
                os.write(pingBuffer);
                //System.out.println("pinging:"+ring.getMembership().get(randomInt).hostname);

                // sleep
                if(randomInt == rangeHigh-1) {
                    randomInt = rangeLow;
                } else {
                    randomInt++;
                }

                Thread.currentThread().sleep(SLEEP_TIME);
            } catch (Exception e) {
                ring.getMembership().get(randomInt).online = false;
                ring.getMembership().get(randomInt).t = new Timestamp(0);
                //System.out.println(ring.getMembership().get(randomInt).hostname + " offline");
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
                                //System.out.println("I took partition of "+node.hostname);
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
