package com.matei.eece411.A2;
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
				System.out.println("ChatImpl err: client id " + id + " already exists in client keep alive list. Something went wrong!");
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

		//Broadcast the message to all existing clients by iterating over the hashmap
		for (Iterator<Entry<String, Callback>> it = clients.entrySet().iterator(); it.hasNext(); )
		{
			Entry<String, Callback> entry = it.next();			
			//Get the client's callback interface
			Callback client = entry.getValue();
			
			//Send the message to the client
			//Try up to 3 times. Don't remove the entry here, let the server handle detection of disconnection/updating the list.
			for (int i=0; i < 3; i++)
			{				
				try {
					client.receive(txt);
					//Quit trying if we succeed (i.e. no exception)
					break;
				} catch (ConnectException e) {
					if (i < 2)
					{
						System.out.println("Attempt " + (i+1) + ": Failed to send message to client " + entry.getKey() + ". Trying again...");
					}
					else
					{
						System.out.println("Attempt " + (i+1) + ": Failed to send message to client " + entry.getKey() + ". Giving up!");
					}
				} catch (Exception e) {
					System.out.println("ChatImpl err: " + e.getMessage()); 
					e.printStackTrace(); 
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

 
      
