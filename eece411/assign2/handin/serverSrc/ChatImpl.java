package com.v3l7.eece411.A2;
import java.rmi.*;                                    
import java.rmi.server.*;
import java.rmi.registry.*;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.List;
import java.util.ArrayList;

public class ChatImpl extends UnicastRemoteObject implements Chat
{
	private ConcurrentHashMap<String, Callback> clients;
	private List<String> clientKeepAlives;

	//Constructor for ChatImpl object using reference to hashmap of clients
	public ChatImpl(ConcurrentHashMap<String, Callback> clientList, List<String> clientKeepAliveList) throws RemoteException
	{
		clients = clientList;
		clientKeepAlives = clientKeepAliveList;
	}

	//Method to register a client to the server by passing a reference to the client's Callback stub
	public boolean register(String id, Callback clientCallback) throws RemoteException
	{
		//System.out.println("Invoked register method! Client name: " + id);

		//Check if there is already an existing client with the same ID
		//If there is a duplicate, display an error message and return false so the client can act accordingly
		if (!clients.containsKey(id))
		{
			//Send a message to existing clients that a new client has joined before adding the new client to the list
			String msg = id + " has joined the chatroom!";
			broadcast(msg);
			clients.put(id, clientCallback);
			
			//Also update the client keep alive list with the newly connected client
			//Should never have id conflict here, but put debug message in case
			if (!clientKeepAlives.contains(id))
			{
				clientKeepAlives.add(id);
			}
			else
			{
				System.out.println("ChatImpl err: client id " + id + " already exists in client keep alive list.");
			}
			
			System.out.println(msg);
			return true;
		}
		else
		{
			System.out.println ("ChatImpl err: duplicate client id: " + id + ". Unable to register!");
			return false;
		}
	}

	//Method to broadcast a message to all existing clients
	public void broadcast(String txt) throws RemoteException
	{
		//System.out.println("Invoked broadcast method! Message: " + txt);
		System.out.println("Broadcasting message: " + txt);		
		
		//List to maintain clients that fail to receive the message
		List<String> failedClients = new ArrayList<String>();
		
		//Broadcast the message to all existing clients by iterating over the hashmap
		for (Iterator<Entry<String, Callback>> it = clients.entrySet().iterator(); it.hasNext(); )
		{
			Entry<String, Callback> entry = it.next();
			String id = entry.getKey();
			Callback client = entry.getValue();
			
			//Send the message to the client.
			try {
				client.receive(txt);
			} catch (RemoteException e) {
				System.out.println("Failed to send message to " + id + ".");
				failedClients.add(id);
			}
		}
		
		//This code is for retrying to send messages to clients that failed to receive the first time
		//However it sacrifices responsiveness for other users.
		if (!failedClients.isEmpty())
		{		
			//Try up to 3 times to send message to each client that failed to receive the first time.
			for (int i=0; i < 3; i++)
			{
				for (Iterator<String> it = failedClients.iterator(); it.hasNext(); )
				{
					//Lookup the client's callback interface from the hashmap using the client name key
					//If null, assume server has flushed the client (i.e. determined client has disconnected), so don't bother 
					String id = it.next();
					Callback client = clients.get(id);
					
					if (client != null)
					{
						try {
							client.receive(txt);
							//Remove the client from the failedClients list if we succeed
							it.remove();
						} catch (RemoteException e) {
							if (i < 2)
							{
								System.out.println("Attempt " + (i+1) + ": Failed to send message to client " + id);
							}
							else
							{
								System.out.println("Attempt " + (i+1) + ": Failed to send message to client " + id + ". Giving up!");
							}
						}
					}
				}				
				//If there are no more clients that need to receive the message
				//then we don't need to retry sending the message to clients that failed to receive
				if (failedClients.isEmpty())
				{
					break;
				}
			}
		}
	}
	
	//Method to inform server that client is alive
	public void keepAlive(String id) throws RemoteException
	{
		//System.out.println("Invoked keepAlive method!");
		//Only add to the list if the client hasn't already sent a keep alive signal since the last flush
		if (!clientKeepAlives.contains(id))
		{
			clientKeepAlives.add(id);
		}
	}
}

 
      
