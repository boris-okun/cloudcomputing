/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"

const string delimiter = "@@";
const long timeout = 10;

/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet * emulNet, Log * log, Address * address) {
	this->memberNode = memberNode;
	this->par = par;
	this->emulNet = emulNet;
	this->log = log;
	ht = new HashTable();
	this->memberNode->addr = *address;
}

/**
 * Destructor
 */
MP2Node::~MP2Node() {
	delete ht;
	delete memberNode;
}

/**
 * FUNCTION NAME: updateRing
 *
 * DESCRIPTION: This function does the following:
 * 				1) Gets the current membership list from the Membership Protocol (MP1Node)
 * 				   The membership list is returned as a vector of Nodes. See Node class in Node.h
 * 				2) Constructs the ring based on the membership list
 * 				3) Calls the Stabilization Protocol
 */
void MP2Node::updateRing() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	vector<Node> curMemList;
	bool change = false;

	/*
	 *  Step 1. Get the current membership list from Membership Protocol / MP1
	 */
	curMemList = getMembershipList();
	curMemList.push_back(Node(memberNode->addr));

	/*
	 * Step 2: Construct the ring
	 */
	// Sort the list based on the hashCode
	sort(curMemList.begin(), curMemList.end());

	set<size_t> failedNodes;
	for (auto it1 = ring.begin(), it2 = curMemList.begin(); it1 != ring.end() || it2 != curMemList.end();) {
        if (it1 == ring.end()) {
            ++it2;
            continue;
        }
        if (it2 == curMemList.end() || it1->nodeHashCode < it2->nodeHashCode) {
            failedNodes.insert(it1->nodeHashCode);
            ++it1;
        } else if (it1->nodeHashCode < it2->nodeHashCode) {
            ++it2;
        } else {
            ++it1;
            ++it2;
        }
	}

    size_t myHash = Node(memberNode->addr).nodeHashCode;
	vector<Node>::const_iterator myit;
	for (auto it = curMemList.begin(); it != curMemList.end(); ++it) {
        if (it->nodeHashCode == myHash) {
            myit = it;
            break;
        }
    }

    vector<Node> newHasMyreplicas;
    vector<Node> hasMyreplicasDiff; //new hasMyreplicas nodes
    auto nextIt = ++myit;
    for (int i = 0; i < 2; ++i, ++nextIt) {
        if (nextIt != curMemList.end())
            newHasMyreplicas.push_back(*nextIt);
        else {
            nextIt = curMemList.begin();
            newHasMyreplicas.push_back(*nextIt);
        }
        if (!hasMyReplicasHashes.count(nextIt->nodeHashCode))
            hasMyreplicasDiff.push_back(*nextIt);
    }

    vector<Node> newHaveReplicasOf;
    vector<Node> haveReplicasOfdiff; //failed replicas
    auto prevIt = myit;
    for (int i = 0; i < 2; ++i) {
        if (prevIt != curMemList.begin())
            newHaveReplicasOf.push_back(*(--prevIt));
        else {
            prevIt = curMemList.end();
            newHaveReplicasOf.push_back(*(--prevIt));
        }
    }

    if (failedNodes.count(haveReplicasOf[1].nodeHashCode)) {
        haveReplicasOfdiff.push_back(haveReplicasOf[1]);
        if (failedNodes.count(haveReplicasOf[0].nodeHashCode))
            haveReplicasOfdiff.push_back(haveReplicasOf[0]);
    }

    vector<Node> oldRing = ring;
    ring = curMemList;
    hasMyReplicas = newHasMyreplicas;
    hasMyReplicasHashes.clear();
    for (auto node : hasMyReplicas)
        hasMyReplicasHashes.insert(node.nodeHashCode);

    haveReplicasOf = newHaveReplicasOf;
    haveReplicasOfHashes.clear();
    for (auto node : haveReplicasOf)
        haveReplicasOfHashes.insert(node.nodeHashCode);


    bool needStab = (!hasMyreplicasDiff.empty() || !haveReplicasOfdiff.empty()) && !ht->isEmpty();
    if (needStab)
        stabilizationProtocol(oldRing, hasMyreplicasDiff, haveReplicasOfdiff);
	/*
	 * Step 3: Run the stabilization protocol IF REQUIRED
	 */
	// Run stabilization protocol if the hash table size is greater than zero and if there has been a changed in the ring
}

/**
 * FUNCTION NAME: getMemberhipList
 *
 * DESCRIPTION: This function goes through the membership list from the Membership protocol/MP1 and
 * 				i) generates the hash code for each member
 * 				ii) populates the ring member in MP2Node class
 * 				It returns a vector of Nodes. Each element in the vector contain the following fields:
 * 				a) Address of the node
 * 				b) Hash code obtained by consistent hashing of the Address
 */
vector<Node> MP2Node::getMembershipList() {
	unsigned int i;
	vector<Node> curMemList;
	for ( i = 0 ; i < this->memberNode->memberList.size(); i++ ) {
		Address addressOfThisMember;
		int id = this->memberNode->memberList.at(i).getid();
		short port = this->memberNode->memberList.at(i).getport();
		memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
		memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
		curMemList.emplace_back(Node(addressOfThisMember));
	}
	return curMemList;
}

/**
 * FUNCTION NAME: hashFunction
 *
 * DESCRIPTION: This functions hashes the key and returns the position on the ring
 * 				HASH FUNCTION USED FOR CONSISTENT HASHING
 *
 * RETURNS:
 * size_t position on the ring
 */
size_t MP2Node::hashFunction(string key) {
	std::hash<string> hashFunc;
	size_t ret = hashFunc(key);
	return ret%RING_SIZE;
}

/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientCreate(string key, string value) {
	Message createMessage(g_transID, memberNode->addr, CREATE, key, value);
	vector<Node> nodes = findNodes(key);
    for (auto& node : nodes) {
        emulNet->ENsend(&memberNode->addr, node.getAddress(), createMessage.toString());
    }
	WaitList.insert(make_pair(g_transID, TransData(g_transID, par->getcurrtime(), CREATE, key, value)));
	++g_transID;
}

/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientRead(string key){
	Message createMessage(g_transID, memberNode->addr, READ, key);
	vector<Node> nodes = findNodes(key);
    for (auto& node : nodes) {
        emulNet->ENsend(&memberNode->addr, node.getAddress(), createMessage.toString());
    }
	WaitList.insert(make_pair(g_transID, TransData(g_transID, par->getcurrtime(), READ, key)));
	++g_transID;
}

/**
 * FUNCTION NAME: clientUpdate
 *
 * DESCRIPTION: client side UPDATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientUpdate(string key, string value){
	Message createMessage(g_transID, memberNode->addr, UPDATE, key, value);
	vector<Node> nodes = findNodes(key);
    for (auto& node : nodes) {
        emulNet->ENsend(&memberNode->addr, node.getAddress(), createMessage.toString());
    }
	WaitList.insert(make_pair(g_transID, TransData(g_transID, par->getcurrtime(), UPDATE, key, value)));
	++g_transID;
}

/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: client side DELETE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientDelete(string key){
	Message createMessage(g_transID, memberNode->addr, DELETE, key);
	vector<Node> nodes = findNodes(key);
    for (auto& node : nodes) {
        emulNet->ENsend(&memberNode->addr, node.getAddress(), createMessage.toString());
    }
	WaitList.insert(make_pair(g_transID, TransData(g_transID, par->getcurrtime(), DELETE, key)));
	++g_transID;
}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(string key, string value, int transId/*, ReplicaType replica*/) {
    string idVal = to_string(transId) + delimiter + value;
	return ht->create(key, idVal);
}

/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 * 			    This function does the following:
 * 			    1) Read key from local hash table
 * 			    2) Return value
 */
string MP2Node::readKey(string key) {
	return ht->read(key);
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value/*, ReplicaType replica*/) {
	return ht->update(key, value);
}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::deleteKey(string key) {
	return ht->deleteKey(key);
}


void MP2Node::HandleReplies(Message reply) {
    auto it = WaitList.find(reply.transID);
    if (it != WaitList.end()) {
        TransData& data = it->second;
        switch (data.type) {
            case (CREATE) :
                {
                    ++data.replyNumber;
                    if (data.replyNumber >= 2) { //quorum
                        log->logCreateSuccess(&memberNode->addr, true, reply.transID, data.key, data.value);
                        WaitList.erase(it);
                    }
                }
                break;
            case (DELETE) :
                {
                    if (reply.success) {
                        ++data.replyNumber;
                        if (data.replyNumber == 3) { //all replicas
                            log->logDeleteSuccess(&memberNode->addr, true, reply.transID, data.key);
                            WaitList.erase(it);
                        }
                    } else {
                        log->logDeleteFail(&memberNode->addr, true, reply.transID, data.key);
                        WaitList.erase(it);
                    }
                }
                break;
            case (READ) :
                {
                    string& idVal = reply.value;
                    if (!idVal.empty()) {
                        ++data.replyNumber;

                        size_t pos = idVal.find(delimiter);
                        int transId = stoi(idVal.substr(0, pos));
                        string value = idVal.substr(pos + 2);
                        if (transId > data.bestValue.first)
                            data.bestValue = make_pair(transId, value);
                        if (data.replyNumber >= 2) {
                            log->logReadSuccess(&memberNode->addr, true, reply.transID, data.key, data.bestValue.second);
                            WaitList.erase(it);
                        }
                    } else {
                        ++data.failedNumber;
                        if (data.failedNumber > 1) {
                            log->logReadFail(&memberNode->addr, true, reply.transID, data.key);
                            WaitList.erase(it);
                        }
                    }
                }
                break;
            case (UPDATE) :
                {
                    if (reply.success) {
                        ++data.replyNumber;
                        if (data.replyNumber >= 2) {
                            log->logUpdateSuccess(&memberNode->addr, true, reply.transID, data.key, data.value);
                            WaitList.erase(it);
                        }
                    } else {
                        ++data.failedNumber;
                        if (data.failedNumber > 1) {
                            log->logUpdateFail(&memberNode->addr, true, reply.transID, data.key, data.value);
                            WaitList.erase(it);
                        }
                    }
                }
                break;
        }
    }
}

void MP2Node::checkTimeouts() {
    long curTimestamp = par->getcurrtime();
    for (auto it = WaitList.begin(); it != WaitList.end(); ++it) {
        TransData& data = it->second;
        if (curTimestamp - data.timestamp <= timeout)
            continue;
        switch (data.type) {
            case (CREATE) :
                log->logCreateFail(&memberNode->addr, true, data.transId, data.key, data.value);
                break;
            case (DELETE) :
                log->logDeleteFail(&memberNode->addr, true, data.transId, data.key);
                break;
            case (READ) :
                log->logReadFail(&memberNode->addr, true, data.transId, data.key);
                break;
            case (UPDATE) :
                log->logUpdateFail(&memberNode->addr, true, data.transId, data.key, data.value);
                break;
        }
        WaitList.erase(it);
    }
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 * 				This function does the following:
 * 				1) Pops messages from the queue
 * 				2) Handles the messages according to message types
 */
void MP2Node::checkMessages() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	char * data;
	int size;

	/*
	 * Declare your local variables here
	 */

	// dequeue all messages and handle them
	while ( !memberNode->mp2q.empty() ) {
		/*
		 * Pop a message from the queue
		 */
		data = (char *)memberNode->mp2q.front().elt;
		size = memberNode->mp2q.front().size;
		memberNode->mp2q.pop();

		string message(data, data + size);
		Message msg(message);

		switch (msg.type) {
            case (CREATE) :
                {
                    bool success = createKeyValue(msg.key, msg.value, msg.transID);
                    Message reply(msg.transID, memberNode->addr, REPLY, success);
                    emulNet->ENsend(&memberNode->addr, &msg.fromAddr, reply.toString());
                    if (success)
                        log->logCreateSuccess(&memberNode->addr, false, msg.transID, msg.key, msg.value);
                    else
                        log->logCreateFail(&memberNode->addr, false, msg.transID, msg.key, msg.value);
                }
                break;
            case (DELETE) :
                {
                    bool success = deleteKey(msg.key);
                    Message reply(msg.transID, memberNode->addr, REPLY, success);
                    emulNet->ENsend(&memberNode->addr, &msg.fromAddr, reply.toString());
                    if (success)
                        log->logDeleteSuccess(&memberNode->addr, false, msg.transID, msg.key);
                    else
                        log->logDeleteFail(&memberNode->addr, false, msg.transID, msg.key);
                }
                break;
            case (READ) :
                {
                    string idVal = readKey(msg.key);
                    Message reply(msg.transID, memberNode->addr, idVal);
                    emulNet->ENsend(&memberNode->addr, &msg.fromAddr, reply.toString());
                    if (!idVal.empty()) {
                        size_t pos = idVal.find(delimiter);
                        string value = idVal.substr(pos + 2);
                        log->logReadSuccess(&memberNode->addr, false, msg.transID, msg.key, value);
                    } else
                        log->logReadFail(&memberNode->addr, false, msg.transID, msg.key);
                }
                break;
            case (UPDATE) :
                {
                    bool success = updateKeyValue(msg.key, msg.value);
                    Message reply(msg.transID, memberNode->addr, REPLY, success);
                    emulNet->ENsend(&memberNode->addr, &msg.fromAddr, reply.toString());
                    if (success)
                        log->logUpdateSuccess(&memberNode->addr, false, msg.transID, msg.key, msg.value);
                    else
                        log->logUpdateFail(&memberNode->addr, false, msg.transID, msg.key, msg.value);
                }
                break;
            case (REPLY) :
                HandleReplies(msg);
                break;
            case (READREPLY) :
                HandleReplies(msg);
                break;
		}
	}
	checkTimeouts();

	/*
	 * This function should also ensure all READ and UPDATE operation
	 * get QUORUM replies
	 */
}

/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 * 				This function is responsible for finding the replicas of a key
 */
vector<Node> MP2Node::findNodes(string key) {
	return findNodes(key, ring);
}

vector<Node> MP2Node::findNodes(string key, vector<Node>& newRing) {
	size_t pos = hashFunction(key);
	vector<Node> addr_vec;
	if (ring.size() >= 3) {
		// if pos <= min || pos > max, the leader is the min
		if (pos <= newRing.at(0).getHashCode() || pos > newRing.at(ring.size()-1).getHashCode()) {
			addr_vec.emplace_back(newRing.at(0));
			addr_vec.emplace_back(newRing.at(1));
			addr_vec.emplace_back(newRing.at(2));
		}
		else {
			// go through the ring until pos <= node
			for (int i=1; i<newRing.size(); i++){
				Node addr = newRing.at(i);
				if (pos <= addr.getHashCode()) {
					addr_vec.emplace_back(addr);
					addr_vec.emplace_back(newRing.at((i+1)%ring.size()));
					addr_vec.emplace_back(newRing.at((i+2)%ring.size()));
					break;
				}
			}
		}
	}
	return addr_vec;
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: Receive messages from EmulNet and push into the queue (mp2q)
 */
bool MP2Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}
/**
 * FUNCTION NAME: stabilizationProtocol
 *
 * DESCRIPTION: This runs the stabilization protocol in case of Node joins and leaves
 * 				It ensures that there always 3 copies of all keys in the DHT at all times
 * 				The function does the following:
 *				1) Ensures that there are three "CORRECT" replicas of all the keys in spite of failures and joins
 *				Note:- "CORRECT" replicas implies that every key is replicated in its two neighboring nodes in the ring
 */
void MP2Node::stabilizationProtocol(vector<Node>& oldRing, vector<Node>& hasMyReplicasDiff, vector<Node>& haveReplicasOfDiff) {
    size_t myHash = Node(memberNode->addr).nodeHashCode;
    for (auto item : ht->hashTable) {
        if (findNodes(item.first).front().nodeHashCode == myHash) {
            Message createMessage(g_transID, memberNode->addr, CREATE, item.first, item.second);
            for (auto node: hasMyReplicasDiff) {
                emulNet->ENsend(&memberNode->addr, node.getAddress(), createMessage.toString());
            }
        }
    }
    for (auto item : ht->hashTable) {
        for (auto node : haveReplicasOfDiff) {
            if (findNodes(item.first, oldRing).front().nodeHashCode == node.nodeHashCode) {
                Message createMessage(g_transID, memberNode->addr, CREATE, item.first, item.second);
                for (auto node: hasMyReplicas) {
                    emulNet->ENsend(&memberNode->addr, node.getAddress(), createMessage.toString());
                }
            }
        }
    }

}
