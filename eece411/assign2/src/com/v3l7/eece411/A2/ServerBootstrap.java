package com.v3l7.eece411.A2;
import java.rmi.*;                                    
import java.rmi.server.*;
import java.rmi.registry.*;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TimerTask;
import java.util.Timer;
import java.util.List;
import java.util.ArrayList;

public class ServerBootstrap
{
	//Maintain a hashmap of client key-value pairs using client id and their callback object
	private static ConcurrentHashMap<String, Callback> clients;
	//Maintain a list of clients that have sent a keep alive signal each flush
	private static List<String> clientKeepAlives;
	private static ChatImpl chatroom;
	
	private static final int FLUSH_RATE = 10000;
	
	//Main method to instantiate server and bind an ChatImpl object
	public static void main(String args[]) 
	{
		//Get the chatroom name command line argument
		if (args.length != 1)
		{
			System.out.println("USAGE: Server " + "<chatroomName>");
			System.exit(0);
		}
		String chatroomName = (args.length == 1) ? args[0] : "ChatServer";
		
		//Instantiate the hashmap to store client id and callback object key-value pairs
		clients = new ConcurrentHashMap<String, Callback>();
		clientKeepAlives = new ArrayList<String>();
		
		try {
			//Instantiate a ChatImpl object and pass reference to the hashmap
			chatroom = new ChatImpl(clients, clientKeepAlives); 
			//Bind this object instance to the specified chatroom name in the rmiregistry
			//Use createRegistry in case rmiregistry isn't running, using default port number
			Registry registry = LocateRegistry.createRegistry(1099);
			registry.rebind(chatroomName, chatroom);
			
			//Instantiate a new FlushClients and schedule it to run periodically
			FlushClients flushTask = new FlushClients();
			Timer t = new Timer();
			t.scheduleAtFixedRate(flushTask, 0, FLUSH_RATE);
			
			System.out.println("Server is up and running...\n");
		} catch (Exception e) { 
			System.out.println("Server err: " + e.getMessage()); 
			e.printStackTrace();
		}
	}
	
	//Nested timer class to check for clients that haven't sent a keep alive recently
	//See reference here: http://stackoverflow.com/questions/4985343/java-rmi-timeouts-crashes
	//Also see: http://www.teamliquid.net/blogs/viewblog.php?topic_id=192763
	private static class FlushClients extends TimerTask
	{
		public void run()
		{
			System.out.println("Running FlushClients...");
			//If a client id is not in the keep alive list, assume the client has disconnected and remove it from the client list
			for (Iterator<Entry<String, Callback>> it = clients.entrySet().iterator(); it.hasNext(); )
			{
				Entry<String, Callback> entry = it.next();
				String id = entry.getKey();
				
				if (!clientKeepAlives.contains(id))
				{
					//Remove the entry from the hashmap
					it.remove();
					//Broadcast the disconnection to all other clients
					//Since the server is invoking this, assume we don't need to do retries
					String msg = entry.getKey() + " has disconnected from the chatroom!";
					try {
						chatroom.broadcast(msg);
						System.out.println(msg);
					} catch (Exception e) {
						System.out.println("Server exception: " + e.getMessage());
						e.printStackTrace();
					}
				}
			}
			
			//Clear the clientKeepAlive list once flush is complete
			clientKeepAlives.clear();
			//System.out.println("Finished FlushClients!");
		}
	}
}

 
      
