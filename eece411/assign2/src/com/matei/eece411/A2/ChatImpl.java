package com.matei.eece411.A2;
import java.rmi.*;                                    
import java.rmi.server.*;
import java.rmi.registry.*;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

public class ChatImpl extends UnicastRemoteObject implements Chat
{
	 //Maintain a hashmap of client key-value pairs using client id and their callback object
	 private HashMap<String, Callback> clients = new HashMap<String, Callback>();
	
	 //Constructor for ChatImpl object
	 public ChatImpl() throws RemoteException{}
	 
	 //Method to register a client to the server by passing a reference to the client's Callback stub
	 public boolean register(Callback clientCallback) throws RemoteException
	 {
		System.out.println ("Invoked register method! Client name: " + clientCallback.getClientId());
		
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
		System.out.println ("Invoked broadcast method! Message: " + txt);
		
		//Temp array of client keys that need to be removed
		List<String> disconnectedClients = new ArrayList<String>();
		
		//Broadcast the message to all existing clients 
		for (String client : clients.keySet())
		{
			//Send the message to the client
			//If ConnectException occurs, assume client has disconnected
			try {
				clients.get(client).receive(txt);
			} catch (ConnectException e) {
				disconnectedClients.add(client);
			} catch (Exception e) {
				System.out.println("CallbackImpl err: " + e.getMessage()); 
				e.printStackTrace(); 
			}
		}
		
		removeClients(disconnectedClients);
 	 }
	 
	 //Helper method to remove disconnected clients from hashmap
	 private void removeClients(List<String> clientsToRemove) throws RemoteException
	 {
		for (String client : clientsToRemove)
		{
			clients.remove(client);
			broadcast(client + " has disconnected from the chatroom!");
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

 
      
