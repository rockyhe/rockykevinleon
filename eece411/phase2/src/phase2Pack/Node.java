package phase2Pack;

import java.net.InetAddress;
import java.sql.Timestamp;
import java.util.Date;

//Class for node info
public class Node
{
    public String hostname;
    public boolean online;
    public Timestamp t = new Timestamp(new Date().getTime());
    public boolean rejoin = false;

    Node(String hostname, boolean alive)
    {
        this.hostname = hostname;
        this.online = alive;
    }

    public boolean Equals(String hostname)
    {
        if (hostname != null)
        {
            if (hostname.equals(this.hostname))
            {
                return true;
            }
        }
        return false;
    }

    public boolean Equals(InetAddress addr)
    {
        if (addr != null)
        {
            if (addr.getHostName().equals(this.hostname))
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

            if (node.hostname.equals(this.hostname))
            {
                return true;
            }
        }
        return false;
    }
}
