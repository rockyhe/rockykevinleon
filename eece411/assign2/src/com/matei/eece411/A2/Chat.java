package com.matei.eece411.A2;
import java.rmi.*;                                     

public interface Chat extends Remote
{       
    public boolean register(String id, Callback clientCallback) throws RemoteException;
	public void broadcast(String txt) throws RemoteException;
    public void keepAlive(String id) throws RemoteException;
}

