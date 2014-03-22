Phase 3

Group members:
Rocky He 	74963091
Leon Lixing Yu 	75683094
Hui Tan		52648094

Server is on nodes:
planetlab1.cs.ubc.ca:5000


------------------------------------
Implementations

Socket Connection Backlog Queue, Multi-threaded Server, and Threadpool
Previously in phase 2, we used a simple AtomicInteger to track the number of concurrent clients and then deny incoming connections after that integer reached a certain value. We now added a backlog queue (size of which is set to 50) for incoming socket connections (i.e. Queue<Socket>) and created a Producer thread and a Consumer thread for the server. The Producer thread simply performs socket.accept() and continuously listens for incoming connections and puts them into the queue, as long as the queue isn't full (otherwise we throw the system overload error). The Consumer thread polls the queue and processes incoming client requests in the queue by creating a new thread instance of KVStore using a threadpool (which we instantiate on bootstrap using newFixedThreadPool(MAX_NUM_CLIENTS) where MAX_NUM_CLIENTS is the maximum number of concurrent clients specified as 250 for this phase).

Consistent Hashing and Routing
To implement consistent hashing, we used the SHA-256 hash function (which generates a 32 byte result, since the hash space is 32 bytes, i.e. the size of the key) to hash the keys and the hostnames of our physical nodes. The ring is constructed by dividing the hash space into a set number of equal sized partitions (we assigned a NUM_PARTITIONS constant set to 10000, since we assume eventually we'll have 100 total physical nodes, and as long as number of partitions is much larger than the number of physical nodes, we're fine). Each physical node is then assigned a total of NUM_PARTITIONS/num_nodes partitions (i.e. number of partitions divided by number of physical nodes), so that every physical node has an equal number of partitions assigned to it, in order to provide uniform load distribution. This strategy is similar to that used by Amazon's Dynamo (covered in Section 6.2 of their paper). Since we work with the assumption that at startup each node knows the identities of all the other nodes participating in the system (via a textfile called nodeList.txt that we load upon bootstrap), then each node is able to construct the same ring at startup. Operations are then either executed locally or routed to the corresponding node by getting the node with the smallest hash that is greater than or equal to the hashed key (i.e. the next node on the ring in the counterclockwise direction). Forwarded requests are done by opening a socket connection to the server and passing the same data sent by the client; essentially, the server that is routing the command will appear as a client to the server that is receiving the routed command.

Shutdown
To process the shutdown command, we just process the 0x04 command in our switch statement. We added an AtomicInteger member to Server.java called shutdownFlag, which is initialized to 0 on bootstrap, so that when we receive a shutdown command, we increment it to 1 to tell the server to stop receiving any more incoming connections. Then we wait in a while loop until all existing connections/requests have been completed (i.e. by checking that the current concurrent client count is 1, since the only client that should still be connected is the one issuing the shutdown command). Once that happens, we break out of the while loop and send a success reply to the client, and increment the shutdownFlag to 2, so that the server knows it is finally safe to shutdown (i.e. using System.exit). Between incrementing the shutdownFlag to 1 and actually shutting down, we also set the timestamp of the server to be 0, so that we can propagate the status of this node earlier to the other nodes (i.e. the gossiping thread will still be running until shutdown, and by setting timestamp to 0, other servers will detect that the node is offline; this saves us time compared to waiting for the node's offline status to be implicitly detected by gossiping).