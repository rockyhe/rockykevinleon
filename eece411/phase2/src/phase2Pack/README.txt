Client Server connection:
	Used: socket programming
	We use socket programming used in assignment 1 for the communication at port 5000. 

multitreaded server:
	Class: Thread
	We use java class Thread to implement the multithreading server, each thread is created for a client connection, the thread terminates once the client disconnects from the server. While server keeps a KVStore, each thread references to the server KVStore and do operations in the store. 

Concurrent put/get/delete:
	Class: ConcurrentHashMap
	KVStore is created using ConcurrentHashMap. Since there are at most 50 clients will be operating get/put/delete at the same time, we use ConncurrentHashMap to deal with concurrency issue.

Tracking total number of Client connected:
	Class: atomicInteger
	We use this class to create a client count variable. This is an int for which we can pass around by its reference. This class also features mutual exclusion ability, meaning the concurrent increament/decrement can be handled by the class. Therefore, it is worry free and can be refrenced from multiple threads. 

 
