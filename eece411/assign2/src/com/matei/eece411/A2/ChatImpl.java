package com.matei.eece411.A2;
import java.rmi.*;                                    
import java.rmi.server.*;
import java.rmi.registry.*;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.List;
import java.util.ArrayList;

public class ChatImpl extends UnicastRemoteObject implements Chat
{
	 //Maintain a list of clients based on references to their callback interfaces
	 private List<Callback> clients = new ArrayList<Callback>();
	
	 //Constructor for ChatImpl object
	 public ChatImpl() throws RemoteException{}
	 
	 //Method to register a client to the server by passing a reference to the client's Callback stub
	 public void register(Callback clientCallback) throws RemoteException
	 {
		if (clientCallback != null)
		{
			System.out.println ("Invoked register method! Client name: " + clientCallback.getClientId());
			//Send a message to existing clients that a new client has joined
			//Easier to do this with the foreach loop before actually adding the reference to the list
			for (Callback client : clients)
			{
				client.receive("Client " + clientCallback.getClientId() + " has joined the chatroom!");
			}
			clients.add(clientCallback);
		}
		else
		{
			System.out.println ("ChatImpl err: client reference is null!");
		}
	}

	 //Method to broadcast a message to all existing clients
 	 public void broadcast(String txt) throws RemoteException
	 {
		System.out.println ("Invoked broadcast method! Message: " + txt);
		//Broadcast the message to all existing clients 
		for (Callback client : clients)
		{
			try {
				client.receive(txt);
			} catch (Exception e) {
				System.out.println("CallbackImpl err: " + e.getMessage()); 
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
		} catch (Exception e) { 
			System.out.println("ChatImpl err: " + e.getMessage()); 
			e.printStackTrace();
		}
	}   
}

 
      
