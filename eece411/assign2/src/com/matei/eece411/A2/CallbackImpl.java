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
		clientId = id;
		clientGUI = gui;
	 }
	 
	 public String getClientId() throws RemoteException
	 {
		return clientId;
	 }
	 
	 public void receive(String txt) throws RemoteException
	 {
		System.out.println ("Client invoked receive method!");
		// update the GUI with the message entered by the user
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

 
      
