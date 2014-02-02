package com.matei.eece411.A2;
//package com;
import java.rmi.*;                                    
import java.rmi.server.*;                             
//import ChatInterface;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class ChatImpl extends UnicastRemoteObject implements Chat {
	  //private int clientId;
  /* Constructor for a remote object
   * Throws a RemoteException if the object handle   
   * cannot be constructed 
   */
	 public ChatImpl() throws RemoteException{}
	 
	 public String sayHello() { return "Hello world!"; }

 /* Implementation of the remotely invocable method*/
//	 public String Connect(int id) throws RemoteException {
//		clientId = id;
//	 }
 	 public void broadcast(String txt) throws RemoteException {
 	 }

	 public void testing() throws RemoteException{
	 }
	
	    //server implemenation starts here	
	    public static void main(String args[]) 
	    { 
		try 
		{ 
		    ChatImpl obj = new ChatImpl(); 
		    // Bind this object instance to the name "HelloServer" 
		    Naming.rebind("ChatServer", obj); 
		} 
		catch (Exception e) 
		{ 
		    System.out.println("Chat err: " + e.getMessage()); 
		    e.printStackTrace(); 
		} 
	    } 
	
}

 
      
