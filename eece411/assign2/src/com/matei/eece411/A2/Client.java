package com.matei.eece411.A2;
//package com;
import java.io.*;
import java.rmi.*;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.RMISecurityManager;
import java.rmi.Naming;
import java.rmi.RemoteException;

public class Client
{    
    private static GUI gui;
    private static MessageQueue _queue;
	private static String hostname = "Rocky-PC";//"dhcp-128-189-216-231.ubcsecure.wireless.ubc.ca";
    
    public static void main(String[] args)
	{
        //Get the chatroom name and client name command line arguments
		if (args.length != 2)
		{
			System.out.println("USAGE: Client " + "<chatroomName> <clientName>");
			System.exit(0);
		}		
		String chatroomName = (args.length == 2) ? args[0] : "ChatServer";
		String clientName = (args.length == 2) ? args[1] : "Unknown Client";
		
		// create a shared buffer where the GUI add the messages thet need to 
        // be sent out by the main thread.  The main thread stays in a loop 
        // and when a new message shows up in the buffer it sends it out 
        // to the chat server (using RMI)
        _queue = new MessageQueue();
        
        // instantiate the GUI - in a new thread
        javax.swing.SwingUtilities.invokeLater(new Runnable() {
            public void run() {
                gui = GUI.createAndShowGUI(_queue);  
            }
        });        
        
        // hack make sure the GUI instantioation is completed by the GUI thread 
        // before the next call
        while (gui == null)
            Thread.currentThread().yield();       
        
        // The code below serves as an example to show how the shares message 
        // between the GUI and the main thread.
        // You will probably want to replace the code below with code that sits in a loop,  
        // waits for new messages to be entered by the user, and sends them to the 
        // chat server (using an RMI call)
        // 
        // In addition you may want to add code that
        //   * connects to the chat server and provides an object for callbacks (so 
        //     that the server has a way to send messages generated by other users)
        //   * implement the callback object which is called by the server remotely 
        //     and, in turn, updates the local GUI

        // I download server's stubs ==> must set a SecurityManager 
        System.setSecurityManager(new RMISecurityManager());

		Chat chatStub = null;
		Callback callbackStub = null;
		
		//Lookup the chat server, create a callback obj and try registering the client to it
        try {
			//Look for the stub in the rmi registry with the specified chatroom name 
            chatStub = (Chat) Naming.lookup( "//" + hostname + "/" + chatroomName);
			callbackStub = new CallbackImpl(clientName, gui);			
		    chatStub.register(callbackStub);
        
			while (true)
			{
				String s;
				try {
					//Wait until the user enters a new chat message
					s = _queue.dequeue();
					//Clear the text field after the user enters a message
					gui.clearTextField();
				} catch (InterruptedException ie) {
					break;
				}
				
				//Print it to System.out (or send it to the RMI server)
				System.out.println ("User entered: " + s + " -- now sending it to chat server");
				
				//Broadcast the message entered by the user
				try {
					//Prefix the message with client id
					chatStub.broadcast(clientName + ":> " + s);
				} catch (Exception e) {
				   System.out.println("Client exception: " + e.getMessage());
				   e.printStackTrace();
				}
			}
        } catch (Exception e) {
           System.out.println("Client exception: " + e.getMessage());
           e.printStackTrace();
        }
    }
   
}
