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
	 private List<Callback> clients = new ArrayList<Callback>();
   
	 public ChatImpl() throws RemoteException{}
	 
	 public void register(Callback clientCallback) throws RemoteException
	 {
		if (clientCallback != null)
		{
			System.out.println ("Client invoked register method! Client name: " + clientCallback.getClientId());
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
	 
	 public String sayHello() throws RemoteException
	 {
		System.out.println ("Client invoked sayHello method!");
		return "Hello world!";
	 }

 	 public void broadcast(String txt) throws RemoteException {
		System.out.println ("Client invoked broadcast method! Message: " + txt);
		for (Callback client : clients)
		{
			client.receive(txt);
		}
 	 }
	 
	public static void main(String args[]) 
	{ 
		try { 
		    ChatImpl obj = new ChatImpl(); 
		    // Bind this object instance to the name "ChatServer" in the rmiregistry
			Registry registry = LocateRegistry.createRegistry(1099);
			registry.rebind("ChatServer", obj);
		} catch (Exception e) { 
		    System.out.println("Chat err: " + e.getMessage()); 
		    e.printStackTrace(); 
		}
	}   
}

 
      
