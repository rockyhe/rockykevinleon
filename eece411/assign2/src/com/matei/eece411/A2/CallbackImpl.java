package com.matei.eece411.A2;
import java.rmi.*;                                    
import java.rmi.server.*;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class CallbackImpl extends UnicastRemoteObject implements Callback
{
	 private String clientId;
	 private GUI clientGUI;
	 
	 public CallbackImpl(String id, GUI gui) throws RemoteException
	 {
		//Construct a new CallbackImpl object with client id and GUI references
		clientId = id;
		clientGUI = gui;
	 }
	 
	 public String getClientId() throws RemoteException
	 {
		//System.out.println ("Invoked getClientId method!");
		return clientId;
	 }
	 
	 public void receive(String txt) throws RemoteException
	 {
		//System.out.println ("Invoked receive method! Message: " + txt);
		
		// update the client's GUI with the message received
		if (clientGUI != null)
		{
			clientGUI.addToTextArea(txt);
		}
		else
		{
			System.out.println ("CallbackImpl err: clientGUI reference is null!");
		}
	}
}

 
      
