package com.matei.eece411.A2;
import java.rmi.*;                                     

public interface Chat extends Remote
{       
    public boolean register(Callback clientCallback) throws RemoteException;
	public void broadcast(String txt) throws RemoteException;
}

