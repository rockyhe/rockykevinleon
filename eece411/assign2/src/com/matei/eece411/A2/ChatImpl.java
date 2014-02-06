package com.matei.eece411.A2;
import java.rmi.*;                                    
import java.rmi.server.*;
import java.rmi.registry.*;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

public class ChatImpl extends UnicastRemoteObject implements Chat
{
	 //Maintain a hashmap of client key-value pairs using client id and their callback object
	 private HashMap<String, Callback> clients = new HashMap<String, Callback>();
	
	 //Constructor for ChatImpl object
	 public ChatImpl() throws RemoteException{}
	 
	 //Method to register a client to the server by passing a reference to the client's Callback stub
	 public boolean register(Callback clientCallback) throws RemoteException
	 {
		//System.out.println ("Invoked register method! Client name: " + clientCallback.getClientId());
		
		//Check if there is already an existing client with the same ID.
		if (!clients.containsKey(clientCallback.getClientId()))
		{
			//Send a message to existing clients that a new client has joined before adding the new client to the list		
			broadcast(clientCallback.getClientId() + " has joined the chatroom!");
			clients.put(clientCallback.getClientId(), clientCallback);
			System.out.println ("Added client " + clientCallback.getClientId() + " to hashmap.");
			return true;
		}
		else
		{
			System.out.println ("ChatImpl err: duplicate client id: " + clientCallback.getClientId() + ". Unable to register!");
			return false;
		}
	}

	 //Method to broadcast a message to all existing clients
 	 public void broadcast(String txt) throws RemoteException
	 {
		//System.out.println ("Invoked broadcast method! Message: " + txt);
		
		//Broadcast the message to all existing clients 
		for (Iterator<Entry<String, Callback>> it = clients.entrySet().iterator(); it.hasNext(); )
		{
			Entry<String, Callback> entry = it.next();			
			//Send the message to the client
			//If ConnectException occurs, assume client has disconnected and remove it from hashmap
			try {
				Callback client = entry.getValue();			
				client.receive(txt);
			} catch (ConnectException e) {
				it.remove();
				broadcast(entry.getKey() + " has disconnected from the chatroom!");
			} catch (Exception e) {
				System.out.println("ChatImpl err: " + e.getMessage()); 
				e.printStackTrace(); 
			}
		}
 	 }
	
	//Main method to instantiate and bind an ChatImpl object
	public static void main(String args[]) 
	{
		//Get the chatroom name command line argument
		if (args.length != 1)
		{
			System.out.println("USAGE: ChatImpl " + "<chatroomName>");
			System.exit(0);
		}
		String chatroomName = (args.length == 1) ? args[0] : "ChatServer";
		
		try {
			//Instantiate a ChatImpl object
			ChatImpl obj = new ChatImpl(); 
			//Bind this object instance to the specified chatroom name in the rmiregistry
			//Use createRegistry in case rmiregistry isn't running, using default port number
			Registry registry = LocateRegistry.createRegistry(1099);
			registry.rebind(chatroomName, obj);
			System.out.println("Server is up and running...\n"); 
		} catch (Exception e) { 
			System.out.println("ChatImpl err: " + e.getMessage()); 
			e.printStackTrace();
		}
	}   
}

 
      
