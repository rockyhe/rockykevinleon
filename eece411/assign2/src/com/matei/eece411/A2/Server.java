package com.matei.eece411.A2;
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

public class Server
{
	//Maintain a hashmap of client key-value pairs using client id and their callback object
	private static ConcurrentHashMap<String, Callback> clients;
	private static ChatImpl chatroom;
	
	private static final int PING_RATE = 5000;
	
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
		
		try {
			//Instantiate a ChatImpl object and pass reference to the hashmap
			chatroom = new ChatImpl(clients); 
			//Bind this object instance to the specified chatroom name in the rmiregistry
			//Use createRegistry in case rmiregistry isn't running, using default port number
			Registry registry = LocateRegistry.createRegistry(1099);
			registry.rebind(chatroomName, chatroom);
			
			//Instantiate a new PingClients and schedule it to run every 5s
			PingClients pingTask = new PingClients();
			Timer t = new Timer();
			t.scheduleAtFixedRate(pingTask, 0, PING_RATE);
			
			System.out.println("Server is up and running...\n");
		} catch (Exception e) { 
			System.out.println("Server err: " + e.getMessage()); 
			e.printStackTrace();
		}
	}
	
	//Nested timer class to ping clients to check for connectivity
	//See reference here: http://stackoverflow.com/questions/4985343/java-rmi-timeouts-crashes
	private static class PingClients extends TimerTask
	{
		public void run()
		{
			for (Iterator<Entry<String, Callback>> it = clients.entrySet().iterator(); it.hasNext(); )
			{
				Entry<String, Callback> entry = it.next();				
				Callback client = entry.getValue();
				//Ping each client up to 3 times. 
				//If ping fails to get reponse after 3 tries, assume client has disconnected so remove it from the list of clients.
				for (int i=0; i < 3; i++)
				{
					try {
						client.ping();
						//Quit trying if we succeed (i.e. no exception)
						break;
					} catch (RemoteException re) {
						if (i < 2)
						{
							System.out.println("Attempt " + (i+1) + ": No ping reply from client " + entry.getKey() + ". Trying again.");
						}
						else
						{
							//Remove the entry from the hashmap
							it.remove();
							System.out.println("Attempt " + (i+1) + ": No ping reply from client " + entry.getKey() + ". Assumming disconnected!");
							//Broadcast the disconnection to all other clients
							//Since the server is invoking this, assume we don't need to do retries
							try {
								chatroom.broadcast(entry.getKey() + " has disconnected from the chatroom!");
							} catch (Exception e) {
								System.out.println("Server exception: " + e.getMessage());
								e.printStackTrace();
							}
						}
					}
				}
			}
		}
	}
}

 
      
