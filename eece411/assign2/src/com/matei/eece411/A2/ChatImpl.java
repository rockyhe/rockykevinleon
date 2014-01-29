package com.matei.eece411.A2;

import java.rmi.*;                                    
import java.rmi.server.*;                             

public class ChatImpl extends UnicastRemoteObject implements ChatInterface {
	  private String message;
	  //private int clientId;
  /* Constructor for a remote object
   * Throws a RemoteException if the object handle   
   * cannot be constructed 
   */
	  public ChatImpl(String msg) throws RemoteException{
    		message = msg;
	  }

 /* Implementation of the remotely invocable method*/
//	 public String Connect(int id) throws RemoteException {
//		clientId = id;
//	 }

 	 public String Broadcast(String txt) throws RemoteException {
   		 return message;
 	 }
	
}

 
      
