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
Client Server connection:
	Used: socket programming
	We use socket programming used in assignment 1 for the communication at port 5000. 

multitreaded server:
	Class: Thread
	We use java class Thread to implement the multithreading server, each thread is created for a client connection, the thread terminates once the client disconnects from the server. While server keeps a KVStore, each thread references to the server KVStore and do operations in the store. 

Separation of RecvBuffer
	The receive buffer in Server is separated into value buffer and key+command buffer for faster and efficient data parsing. Ex: we dont  read the value buffer if the command is get or delete. 

Concurrent put/get/delete:
	Class: ConcurrentHashMap
	KVStore is created using ConcurrentHashMap. Since there are at most 50 clients will be operating get/put/delete at the same time, we use ConncurrentHashMap to deal with concurrency issue.

Tracking & limiting total number of Client connected:
	Class: atomicInteger
	We use this class to create a client count variable. This is an int for which we can pass around by its reference. This class also features mutual exclusion ability, meaning the concurrent increament/decrement can be handled by the class. Therefore, it is worry free and can be refrenced from multiple threads, we set client limit to 50 as per requirement, if exceedingthe limit, we throw error 0x03.  

KVStore Internal Error:
	Class: general Java Exception handler
	We use a general expcetion handler to throw error code 0x04, this shows that as long as there is an issue with any of the KVStore get/put/delete operation, it throws 0x04 error, can all of these is catagorized as KVStore internal error

KVStore Size:
	we can only do put operation if the store size is less than 40000 as per request. otherwise, we throw a error 0x02. 
