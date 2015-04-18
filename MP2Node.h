/**********************************
 * FILE NAME: MP2Node.h
 *
 * DESCRIPTION: MP2Node class header file
 **********************************/

#ifndef MP2NODE_H_
#define MP2NODE_H_

/**
 * Header files
 */
#include "stdincludes.h"
#include "EmulNet.h"
#include "Node.h"
#include "HashTable.h"
#include "Log.h"
#include "Params.h"
#include "Message.h"
#include "Queue.h"

#include <map>
using namespace std;

struct TransData {
    int transId;
    MessageType type;
    string key;
    string value;
    size_t replyNumber;
    vector<pair<int, string>> replies; //replies for read <TransId, value>

    TransData(int id, MessageType t, string k) :
        transId(id),
        type(t),
        key(k),
        replyNumber(0)
    {}

    TransData(int id, MessageType t, string k, string v) :
        transId(id),
        type(t),
        key(k),
        value(v),
        replyNumber(0)
    {}
};

typedef map<int, TransData> TransMap;

/**
 * CLASS NAME: MP2Node
 *
 * DESCRIPTION: This class encapsulates all the key-value store functionality
 * 				including:
 * 				1) Ring
 * 				2) Stabilization Protocol
 * 				3) Server side CRUD APIs
 * 				4) Client side CRUD APIs
 */
class MP2Node {
private:
	// Vector holding the next two neighbors in the ring who have my replicas
	vector<Node> hasMyReplicas;
	// Vector holding the previous two neighbors in the ring whose replicas I have
	vector<Node> haveReplicasOf;
	// Ring
	vector<Node> ring;
	// Hash Table
	HashTable * ht;
	// Member representing this member
	Member *memberNode;
	// Params object
	Params *par;
	// Object of EmulNet
	EmulNet * emulNet;
	// Object of Log
	Log * log;

	int TransId;

	TransMap WaitList;

public:
	MP2Node(Member *memberNode, Params *par, EmulNet *emulNet, Log *log, Address *addressOfMember);
	Member * getMemberNode() {
		return this->memberNode;
	}

	// ring functionalities
	void updateRing();
	vector<Node> getMembershipList();
	size_t hashFunction(string key);
	void findNeighbors();

	// client side CRUD APIs
	void clientCreate(string key, string value);
	void clientRead(string key);
	void clientUpdate(string key, string value);
	void clientDelete(string key);

	// receive messages from Emulnet
	bool recvLoop();
	static int enqueueWrapper(void *env, char *buff, int size);

	// handle messages from receiving queue
	void checkMessages();

	void HandleReplies(Message reply);

	// coordinator dispatches messages to corresponding nodes
	void dispatchMessages(Message message);

	// find the addresses of nodes that are responsible for a key
	vector<Node> findNodes(string key);

	// server
	bool createKeyValue(string key, string value, int transId/*, ReplicaType replica*/);
	string readKey(string key);
	bool updateKeyValue(string key, string value/*, ReplicaType replica*/);
	bool deleteKey(string key);

	// stabilization protocol - handle multiple failures
	void stabilizationProtocol();

	~MP2Node();
};

#endif /* MP2NODE_H_ */
