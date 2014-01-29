package com.matei.eece411.A2;

import java.io.*;
import java.rmi.*;

public class Server
{	
	public static void main(String[] args)
	{
		try {
			//Bind the name to new remote Chat object
			Naming.rebind("Chat", new ChatImpl("Chat room"));
			System.out.println("Server is ready");			
		} catch (Exception e) {
			System.out.println("Server failed: " + e);
		}
	}
}
