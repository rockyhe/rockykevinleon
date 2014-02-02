package com.matei.eece411.A2;
//package com;
import java.rmi.*;                                     

public interface Chat extends Remote {       
  /*
   * Remotely invocable method,
   * returns a message from the remote object, 
   * throws a RemoteException 
   *      if the remote invocation fails
   */
	public void broadcast(String txt) throws RemoteException;          
	public String sayHello() throws RemoteException;
	public void testing() throws RemoteException;	
}

