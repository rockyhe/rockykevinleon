Group members:
Rocky He 	74963091
Leon Lixing Yu 	75683094
Hui Tan  52648094

WHEN WE RUN SERVER IN COMPUTER A, CLIENT IN COMPUTER B. THE CHAT ROOM IS FULLY FUNCTIONAL. WHEN TWO COMPUTER ARE IN THE SAME NETWORK (UBC WIRELESS)	

1. Pull vs. push design
we used push design, detailed remote interface see 2.

2. A description of the remote interfaces
We used a push design, where the server binds the remote object ChatImpl, which implements the Chat remote interface, to the rmiregistry using the passed in "chatroomName" command line argument. The client has a CallbackImpl remote object, which implements the Callback remote interface and extends UnicastRemoteObject (so that the server can remotely invoke the object). Each client gets the reference to the chatroom remote object by looking it up in the rmiregistry, and then invokes the register method (invoked from server interface)  exports its Callback object to the server. The server then maintains a list of each When a client invokes the broadcast method via the Chat interface.

3. message format
the communication between server and client are done by sending a (key, value) pair each time a client checks in with the server, where key is the client ID, and value is the client's remote object 'CallbackImpl' with interface 'Callback'. in order to keep tracking the existing client, we store each (client ID, callbackObj) pair into a HashMap type. With such data structure, it is very easy to track the connection of each client.

When client wants to send a message, we concatenate client's ID and message into one string (ClientID + ">:" + message), and invokes boardcast method from server's remote interface (chatStub). the method will board cast the entire string to each existing client using HashMap (find the Keys, get the client remote object (callbackObj), invoke the GUI print method in the callbackObj). 

4. Client Failure
We have a Pin method that testify the connection. 
When client goes offline, we have a exception check before invoking the remote client object (callbackObj), meaning that we do (connectionException e) while iterating thru the (client ID , callbackObj) HashMap. if connectionException happens, we remote the (client ID , callbackObj) pair from the HaspMap. and thus server continues to work. 

5. To handle server offline issue, we keep pinning checkIn/boardcast method from server remote object (Chat), if we have consecutive 3 connectionExceptions, we assume that server is offline, and terminate the client.

6. Since we may want to run server in different hosts from time to time, we decided to make hostname a parameter instead of hard-coded string. Same idea applies to client name and rmi registry name. We choose to use push protocol to ease the client load, and reduce the traffic over the network. 
