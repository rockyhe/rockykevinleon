package phase2Pack;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.sql.Timestamp;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

import phase2Pack.enums.Commands;

public class PingListener implements Runnable
{
    // Constants
    private static final int CMD_SIZE = 1;

    // Private members
    private Socket clntSock;
    private ConsistentHashRing ring;
    private AtomicInteger concurrentClientCount;

    // Constructor
    PingListener(Socket clientSocket, AtomicInteger clientCount, ConsistentHashRing ring)
    {
        this.clntSock = clientSocket;
        this.concurrentClientCount = clientCount;
        this.ring = ring;
    }

    private void receiveBytes(Socket srcSock, byte[] dest) throws IOException
    {
        InputStream in = srcSock.getInputStream();
        int totalBytesRcvd = 0;
        int bytesRcvd = 0;
        while (totalBytesRcvd < dest.length)
        {
            if ((bytesRcvd = in.read(dest, totalBytesRcvd, dest.length - totalBytesRcvd)) != -1)
            {
                totalBytesRcvd += bytesRcvd;
            }
        }
    }

    private void updateStatus()
    {
        String clientHostname = clntSock.getInetAddress().getHostName();
        int idx = ring.membershipIndexOf(clientHostname);
        if (idx >= 0)
        {
            Node clientNode = ring.getMembership().get(idx);
            if (!clientNode.online)
            {
                //System.out.println(clientNode.hostname + " rejoined");
                clientNode.rejoin = true;
            }
            clientNode.online = true;
            clientNode.t = new Timestamp(new Date().getTime());
            // System.out.println("timestamp: "+onlineNodeList.get(onlineNodeList.indexOf(node)).t.toString());
        }
    }

    public void run()
    {
        try {
            byte[] command = new byte[CMD_SIZE];
            receiveBytes(clntSock, command);
            Commands cmd = Commands.fromInt(ByteOrder.leb2int(command, 0, CMD_SIZE));
            //System.out.println("cmd: " + cmd);
            if (cmd == Commands.PING)
            {
                updateStatus();
            }

        } catch (Exception e) {
            System.out.println("internal Server Error");
        } finally {
            concurrentClientCount.getAndDecrement();
        }
    }

}
