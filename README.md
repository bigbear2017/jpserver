## JPServer

#### what is jpServer
jpServer is Java implementation parameter server.

#### Design

##### Depend on ZooKeeper
When a node starts, it will register itself to zookeeper.
Then this node become a server. When a client wants to find
a server, it will connect to zookeeper and get a node from there.
Then a client can connect to the server it gets. The client can
pull and push to server.

##### Double Buffer
Each server will store all values into two caches and a data map.
When a client send data to a server, the server will store all values
into the cache. Once the update counter is large or the update time is
old, the cache will be updated to data map. When syncing with primary 
server, the final value will be stored at another cache first. Then, 
if need or update time is old, we update the data map.

##### Synchronization
Each server will receive data from clients. It will collect the data
and keep it locally. Internally, Every 0.1 second, all nodes will
synchronize with each other. This only increase 10 QPS for each node.
Hopefully, it should be fine.

#### Tree Structure
Should we use a tree structure ? Then the root node broadcast the data
to every node. Or should we just broadcast its own data to every node.
I guess this really depends. In reality, if we only have less than 5
servers, I guess broadcasting is enough. But if we have more nodes, a
tree structure could be better.

#### Simple Interface
At first, I think the interface should be simple that it only supports
vectors. But then I want to use some new feature from JAVA 8. So that,
we can define the new functions to support new features.

### Some common packages to Implement
1. hadoop utils to read data
2. zoo keeper to use [finished]
3. 
