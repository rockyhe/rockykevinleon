package phase2Pack;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.sql.Timestamp;
import java.util.Date;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class GossipListener implements Runnable
{
    // Constants
    private static final int CMD_SIZE = 1;

    // Private members
    private Socket clntSock;
    private CopyOnWriteArrayList<Node> membership;
    private AtomicInteger concurrentClientCount;

    // Constructor
    GossipListener(Socket clientSocket, AtomicInteger clientCount, CopyOnWriteArrayList<Node> members)
    {
        this.clntSock = clientSocket;
        this.concurrentClientCount = clientCount;
        this.membership = members;
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

    private void gossip()
    {
        for (Node node : membership)
        {
            //System.out.println("client sock: "+clntSock.getInetAddress().getHostName().toString());
            //System.out.println("Clnt Sock: "+clntSock.getInetAddress().getHostName());
            //System.out.println("member: "+node.hostname);
            if (node.Equals(clntSock.getInetAddress()))
            {
                if (!node.online)
                {
                    System.out.println(node.hostname + " rejoined");
                    node.rejoin = true;
                }
                node.online = true;
                node.t = new Timestamp(new Date().getTime());
                // System.out.println("timestamp: "+onlineNodeList.get(onlineNodeList.indexOf(node)).t.toString());
                break;
            }
        }
    }

    public void run()
    {
        try {
            byte[] command = new byte[CMD_SIZE];
            receiveBytes(clntSock, command);
            int cmd = ByteOrder.leb2int(command, 0, CMD_SIZE);

            //System.out.println("cmd: " + cmd);
            if(cmd == 255)
            {
                gossip();
            }
        } catch (Exception e) {
            System.out.println("internal Server Error");
        } finally {
            concurrentClientCount.getAndDecrement();
        }
    }

}
