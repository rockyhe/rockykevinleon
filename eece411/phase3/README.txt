Phase 3

Group members:
Rocky He    74963091
Leon Lixing Yu  75683094
Hui Tan     52648094


Gossip protocol
    Major class used: CopyOnWriteList, Timestamp
    CopyOnWriteList is a java array list that is thread independent. We use this class to create a node list that consists all running nodes. Each talks to two randomly selected nodes simultaneously by sending a gossip message (byte 0xff). The gossiping interval is 1 second, referenced from Amazon's dynamo paper. The recv side of the gossip, another alive node, treat the gossip message as a cmd just like put, get, or delete. Once it receives 0xff, it processes the message by 1. matching the client socket's node with the nodes in the node list, 2. marking the client socket's node online 3. updating its last-modified-since timestamp to latest.
    The gossip protocol holds its own always on threads. Since we gossip with two other nodes simultaneously, we have two always-on threads. Nodes will not gossip to themselves or to a known offline nodes. If the randomly selected node is itself or a known offline node, we will keep getting another node till the node is not offline and not itself. Algorithm on how to determine the node is either forced offline or unannounced offline is presented in the next couple sections of this doc. 

Timestamp Check
    Major class used: CopyOnWriteList, Timestamp
    We dedicate another thread to check the timestamp of each node in the node list, meaning we always check the last-modified-since time for each node. Instead of using event based clock, we uses time based clock for faster processing (no need to read, then increment, then write). Inside the timestamp check thread, we compares each node's last-modified-since time with the current time, if the differences is larger than log(NUMBER OF NODES)/log(2)*GOSSIP_INTERVAL + BUFFER_TIME. we know that the node's time stamp haven't been updated for a while, and we consider it an offline node. And gossip protocol uses this information the decide who to gossip to. 
 
Node leave and rejoin
    Major class used: ConcurrentSkipList, CopyOnWriteList
    When a node leaves unannounced, we 1.match the partition's hash key's value (node) with the offline node's. Once we get all the partitions for the offline node, we will see if the offline node is the last in the hash ring or not. If it is not the last in the the hash ring, we re-map the hash keys (belonged to this offline node) to the next online node following the offline one. If none of the next nodes are online, we re-start iteration at 1st node in the ring, until we find the online node. 
    When a node rejoins from failure, we know already which partitions it should take (given the nodes is not new to our node list), but the hash key is not mapped to the rejoined node rejoined nodes. All we need to do is replace the mapping with rejoined node, such that the rejoined node gets the partitions it supposed to get. 