package com.matei.eece411.A2;
import java.rmi.*;                                    
import java.rmi.server.*;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class CallbackImpl extends UnicastRemoteObject implements Callback
{
	private GUI clientGUI;
	
	//Constructor for CallbackImpl using client GUI references
	public CallbackImpl(GUI gui) throws RemoteException
	{
		clientGUI = gui;
	}

	//Method to receive a message and display it on the client gui
	public void receive(String txt) throws RemoteException
	{
		//System.out.println("Invoked receive method! Message: " + txt);

		// update the client's GUI with the message received
		clientGUI.addToTextArea(txt);
	}
}

 
      
