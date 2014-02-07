package com.v3l7.eece411.A2;
import java.rmi.*;                                     

public interface Callback extends Remote
{
    public void receive(String txt) throws RemoteException;
}

