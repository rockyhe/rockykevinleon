Phase 2

Group members:
Rocky He 	74963091
Leon Lixing Yu 	75683094
Hui Tan		52648094

Server is on nodes:
planetlab1.cs.ubc.ca:5000
cs-planetlab4.cs.surrey.sfu.ca:5000
cs-planetlab3.cs.surrey.sfu.ca:5000
------------------------------------
Implementations
2 Classes - KVStore.java and Server.java (which holds an instance of KVStore)
Assumption: As stated by Matei in class, we work with the assumption that the client provides an already hashed key. 

Client-Server connection:
	Used: socket programming
	We use socket programming, similar to assignment 1, for communication between the client and server on port 5000. 

Multitreaded server:
	Class: Thread
	We use Java's Thread class to implement a multithreaded server, where a new thread is created for each client connection and the thread terminates once the client disconnects from the server. The server keeps a KVStore instance, and each thread has a reference to the server's KVStore and performs operations on it.

Separation of receive buffer:
	The receive buffer when processing the client request in the KVStore class is separated into a command+key buffer and a value buffer for faster and efficient data parsing, i.e. we only read the value buffer bytes if the command is put.

Concurrent put/get/delete:
	Class: ConcurrentHashMap
	The KVStore class is implemented using a ConcurrentHashMap. Since there are at most 50 clients that will be operating get/put/delete at the same time, we use ConncurrentHashMap to deal with concurrency issues.

General Error Handling:
	By default we set the error code variable to 0x00, so that we can assume operations complete successfully unless the error code value is explicitly changed by error handling code.

Inexistent Key Error:
	We check if the KVStore contains the key for the get/remove operations in the get/remove methods using a simple conditional statement, and thrown error 0x01 if the key doesn't exist. 

Unrecognized Command Error:
	We use a switch statement on the command value to easily handle unrecognized commands in the "default" case and throw error 0x05 (i.e. Unrecognized command).
	
Tracking & limiting total number of clients connected:
	Class: AtomicInteger
	We use Java's AtomicInteger class to create a client count variable, which we pass to each client thread to increment (when connected) and decrement (once operation is done and socket is closed). We use it because it is thread safe and can be incremented/decremented concurrently and be referenced from multiple threads. We set the client limit to 50 as per the project requirements, and if the value of the AtomicInteger exceeds the limit, we throw error 0x03 (i.e. System Overload).

KVStore Internal Error:
	Class: general Java Exception handler
	We use a general exception handler (i.e. catch Exception) to throw error code 0x04 (i.e. Internal KVStore error), since this shows that as long as there is an unhandled issue that occurs in the KVStore, it throws the 0x04 error.

KVStore Size:
	we can only do the put operation if the store size is less than 40000 as per the project requirements. Otherwise, we throw error 0x02 (i.e. Out of space).
