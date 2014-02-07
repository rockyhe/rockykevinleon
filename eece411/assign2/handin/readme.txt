Group members:
Rocky He 	74963091
Leon Lixing Yu 	75683094
Hui Tan  	52648094

WHEN WE RUN SERVER IN COMPUTER A and CLIENT IN COMPUTER B (e.g. two separate computers),  THE CHAT ROOM IS FULLY FUNCTIONAL, AS LONG AS THE TWO COMPUTERS ARE IN THE SAME NETWORK (e.g. UBC SECURE WIRELESS). We are also able to run multiple client instances on the same machine. 

IF YOU ARE USING JAVA 1.6 OR ABOVE PLEASE USE allAccess.policy file to run client. see HowTo.txt for detailed instructions.

1. Pull vs. push design
we used push design (i.e. server pushes messages to clients), detailed remote interface see 2.

2. A description of the remote interfaces
We have two remote interfaces: 
 - Chat (for server-side chatroom). Implementation is ChatImpl
 - Callback (for client-side callback). Implementation is CallbackImpl

We used a push design, where the server binds the remote object ChatImpl, which implements the Chat remote interface, to the rmiregistry using the passed in "chatroomName" command line argument. The client has a CallbackImpl remote object, which implements the Callback remote interface and extends UnicastRemoteObject (so that the server can remotely invoke the object). Each client gets the reference to the ChatImpl remote object by looking it up in the rmiregistry, and then invokes the register method (invoked from server interface)  exports its Callback object to the server. The server then maintains a list of each of the client's callback, and when a client invokes the broadcast method via the Chat interface, then the server subsequently invokes the receive method via the Callback interface to send the message (i.e. push) to all clients.

3. Message Format
The communication between server and client is done by sending a (key, value) pair when a client registers with the server (i.e. joins the chatroom), where key is the client ID, and value is the client's remote object 'CallbackImpl' with interface 'Callback'. In order to keep tracking the existing clients, we store each (clientID, callbackObj) pair into a HashMap type. With such data structure, it is very easy to track the connection of each client.

When client wants to send a message, we concatenate client's ID and message into one string (ClientID + ">:" + message), and invokes the broadcast method via the server's Chat remote interface (on the ChatImpl remote objcet). The broadcast method sends the entire string to each existing client by iterating over the HashMap of (clientID, callbackObj) pairs to get each client remote object (callbackObj)), and then invoke the receive method via the Callback interface. The receive method subsequently invoke the GUI print method in the callbackObj). 

4. Client Failure
We have a keepAlive() method on the Chat interface, which each client will periodically invoke (by running a fixed Timer task client-side on a separate thread) to ping/inform the server that the client is alive. The server maintains a list of clients who have invoked the keepAlive method. The server also runs a FlushClient fixed Timer task that periodically checks which clients have invoked the keepAlive method since the last time the task was run. Therefore, if a client fails, it will stop pinging the server, and when the server runs the FlushClient task it will detect the client disconnection and remove the client from HashMap. If a client fails while other clients are sending in messages (i.e. it will fail to receive the message), then the ChatImpl broadcast method will try up to 3 times to resend the message to the client. The broadcast method will loop through all clients and try to send the message once, before attempting to resend to any clients that failed to receive. After failing to resend 3 times, the server will simply not send the message to that offline client (and then the FlushClient task will eventually remove the client). When a client disconnects, the server also broadcasts a message about that client's disconnection to all of the other clients (e.g. "User has disconnected from the chatroom!"). 

5. Server Failure
To handle server offline issue, in each client's timer task for keepAlive, we try to invoke the keepAlive method on the server's ChatImpl remote object (as mentioned in #4) up to 3 times, and if we don't succeed in connecting to the server (e.g. we get a RemoteException when calling keepAlive) after 3 attempts, then we assume that server is offline and terminate the client. We also check whether the server is up when starting a new client, in which case if the server is not running when you start a client, then we simply terminate the client. In all cases we display some informative messages.

6. One thing we added was since we may want to run server in different hosts from time to time, we decided to make hostname a command line parameter instead of a hard-coded string. Same idea applies to client name and rmi registry name for the chatroom (i.e. the ChatImpl object). We choose to use push protocol to ease the client load, and reduce the traffic over the network. There are also small additional things we do such as checking for duplicate client IDs (e.g. we don't allow two clients to join a chatroom with the same username), and broadcasting user joining/leaving the chatroom to other users in the GUI. 
