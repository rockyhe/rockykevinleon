package com.matei.eece411.A2;
import java.rmi.*;                                    
import java.rmi.server.*;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class CallbackImpl extends UnicastRemoteObject implements Callback
{
	private String clientId;
	private GUI clientGUI;
	
	//Constructor for CallbackImpl using client id and GUI references
	public CallbackImpl(String id, GUI gui) throws RemoteException
	{		
		clientId = id;
		clientGUI = gui;
	}
	 
	//Getter method for client id
	public String getClientId() throws RemoteException
	{
		//System.out.println("Invoked getClientId method!");
		return clientId;
	}

	//Method to receive a message and display it on the client gui
	public void receive(String txt) throws RemoteException
	{
		//System.out.println("Invoked receive method! Message: " + txt);

		// update the client's GUI with the message received
		clientGUI.addToTextArea(txt);
	}
	
	//Method to ping client to check for connectivity
	//Can display a message for debugging
	public void ping() throws RemoteException
	{
		//System.out.println("Invoked ping method!");
	}
}

 
      
