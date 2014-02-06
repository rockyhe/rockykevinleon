Group members:
Rocky He 		74963091
Leon Lixing Yu 	75683094
Kevin Tan		

1. Pull vs. push design
We used a push design, where the server binds the remote object ChatImpl, which implements the Chat remote interface, to the rmiregistry using the passed in "chatroomName" command line argument. The client has a CallbackImpl remote object, which implements the Callback remote interface and extends UnicastRemoteObject (so that the server can remotely invoke the object). Each client gets the reference to the chatroom remote object by looking it up in the rmiregistry, and then invokes the register method and passes its Callback object to the server. The server then maintains a list of each When a client invokes the broadcast method via the Chat interface, the server  

2. 