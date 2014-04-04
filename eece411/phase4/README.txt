Phase 4

Group members:
Rocky He 	74963091
Leon Lixing Yu 	75683094
Hui Tan		52648094

Server is on nodes All is on port 5000
see attached nodeList.txt file


------------------------------------
Implementations
    Socket Connection Backlog Queue, Multi-threaded Server, and Threadpool
Previously in phase 2, we used a simple AtomicInteger to track the number of concurrent clients and then deny incoming connections after that integer reached a certain value. We now added a backlog queue (size of which is set to 50) for incoming socket connections (i.e. Queue<Socket>) and created a Producer thread and a Consumer thread for the server. The Producer thread simply performs socket.accept() and continuously listens for incoming connections and puts them into the queue, as long as the queue isn't full (otherwise we throw the system overload error). The Consumer thread polls the queue and processes incoming client requests in the queue by creating a new thread instance of KVStore using a threadpool (which we instantiate on bootstrap using newFixedThreadPool(MAX_NUM_CLIENTS) where MAX_NUM_CLIENTS is the maximum number of concurrent clients specified as 250 for this phase).

Replica: Primary-based Remote-Write
	Replicas are placed on the physical nodes that are currently assigned to the successive partitions, and we may skip partitions to make sure there's no duplicated physical nodes

Gossip protocol
    Major class used: CopyOnWriteList, Timestamp
    CopyOnWriteList is a java array list that is thread independent. We use this class to create a node list that consists all running nodes. Each talks to two randomly selected nodes simultaneously by sending a gossip message (byte 0xff). The gossiping interval is 1 second, referenced from Amazon's dynamo paper. The recv side of the gossip, another alive node, treat the gossip message as a cmd just like put, get, or delete. Once it receives 0xff, it processes the message by 1. matching the client socket's node with the nodes in the node list, 2. marking the client socket's node online 3. updating its last-modified-since timestamp to latest.
    The gossip protocol holds its own always on threads. Since we gossip with two other nodes simultaneously, we have two always-on threads. Nodes will not gossip to themselves or to a known offline nodes. If the randomly selected node is itself or a known offline node, we will keep getting another node till the node is not offline and not itself. Algorithm on how to determine the node is either forced offline or unannounced offline is presented in the next couple sections of this doc. 

Timestamp Check
    Major class used: CopyOnWriteList, Timestamp
    We dedicate another thread to check the timestamp of each node in the node list, meaning we always check the last-modified-since time for each node. Instead of using event based clock, we uses time based clock for faster processing (no need to read, then increment, then write). Inside the timestamp check thread, we compares each node's last-modified-since time with the current time, if the differences is larger than log(NUMBER OF NODES)/log(2)*GOSSIP_INTERVAL + BUFFER_TIME. we know that the node's time stamp haven't been updated for a while, and we consider it an offline node. And gossip protocol uses this information the decide who to gossip to. 

Consistent Hashing and Routing
    To implement consistent hashing, we used the SHA-256 hash function (which generates a 32 byte result, since the hash space is 32 bytes, i.e. the size of the key) to hash the keys and the hostnames of our physical nodes. The ring is constructed by dividing the hash space into a set number of equal sized partitions (we assigned a NUM_PARTITIONS constant set to 10000, since we assume eventually we'll have 100 total physical nodes, and as long as number of partitions is much larger than the number of physical nodes, we're fine). Each physical node is then assigned a total of NUM_PARTITIONS/num_nodes partitions (i.e. number of partitions divided by number of physical nodes), so that every physical node has an equal number of partitions assigned to it, in order to provide uniform load distribution. This strategy is similar to that used by Amazon's Dynamo (covered in Section 6.2 of their paper). Since we work with the assumption that at startup each node knows the identities of all the other nodes participating in the system (via a textfile called nodeList.txt that we load upon bootstrap), then each node is able to construct the same ring at startup. Operations are then either executed locally or routed to the corresponding node by getting the node with the smallest hash that is greater than or equal to the hashed key (i.e. the next node on the ring in the counterclockwise direction). Forwarded requests are done by opening a socket connection to the server and passing the same data sent by the client; essentially, the server that is routing the command will appear as a client to the server that is receiving the routed command.

Node leave and rejoin
    Major class used: ConcurrentSkipList, CopyOnWriteList
    When a node leaves unannounced, we 1.match the partition's hash key's value (node) with the offline node's. Once we get all the partitions for the offline node, we will see if the offline node is the last in the hash ring or not. If it is not the last in the the hash ring, we re-map the hash keys (belonged to this offline node) to the next online node following the offline one. If none of the next nodes are online, we re-start iteration at 1st node in the ring, until we find the online node. 
    When a node rejoins from failure, we know already which partitions it should take (given the nodes is not new to our node list), but the hash key is not mapped to the rejoined node rejoined nodes. All we need to do is replace the mapping with rejoined node, such that the rejoined node gets the partitions it supposed to get. 

Shutdown
    To process the shutdown command, we just process the 0x04 command in our switch statement. We added an AtomicInteger member to Server.java called shutdownFlag, which is initialized to 0 on bootstrap, so that when we receive a shutdown command, we increment it to 1 to tell the server to stop receiving any more incoming connections. Then we wait in a while loop until all existing connections/requests have been completed (i.e. by checking that the current concurrent client count is 1, since the only client that should still be connected is the one issuing the shutdown command). Once that happens, we break out of the while loop and send a success reply to the client, and increment the shutdownFlag to 2, so that the server knows it is finally safe to shutdown (i.e. using System.exit). Between incrementing the shutdownFlag to 1 and actually shutting down, we also set the timestamp of the server to be 0, so that we can propagate the status of this node earlier to the other nodes (i.e. the gossiping thread will still be running until shutdown, and by setting timestamp to 0, other servers will detect that the node is offline; this saves us time compared to waiting for the node's offline status to be implicitly detected by 

