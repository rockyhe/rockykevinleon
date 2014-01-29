package com.matei.eece411.A2;

import java.rmi.*;                                     

public interface HelloInterface extends Remote {       
  /*
   * Remotely invocable method,
   * returns a message from the remote object, 
   * throws a RemoteException 
   *      if the remote invocation fails
   */
  public String Broadcast() throws RemoteException;          
}

