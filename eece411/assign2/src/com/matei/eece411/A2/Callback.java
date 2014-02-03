package com.matei.eece411.A2;
import java.rmi.*;                                     

public interface Callback extends Remote
{
	public String getClientId() throws RemoteException;
    public void receive(String txt) throws RemoteException;
}

