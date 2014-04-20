package phase2Pack;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.sql.Timestamp;
import java.util.Date;

//Class for node info
public class Node
{
    public InetSocketAddress address;
    public boolean online;
    public Timestamp t = new Timestamp(new Date().getTime());
    public boolean rejoin = false;

    Node(InetSocketAddress addr, boolean alive)
    {
        this.address = addr;
        this.online = alive;
    }

    public boolean Equals(InetAddress addr)
    {
        if (addr != null)
        {
            if (addr.getHostName().equals(this.address.getHostName()))
            {
                return true;
            }
        }
        return false;
    }

    public boolean Equals(Node node)
    {
        if (node != null)
        {
            if (this == node)
            {
                return true;
            }

            if (node.address.getHostName().equals(this.address.getHostName()))
            {
                return true;
            }
        }
        return false;
    }
}
