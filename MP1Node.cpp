/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

 namespace {
    pair<int, short> getIdPort(Address& addr) {
        int id = 0;
		short port;
		char* a = addr.addr;
		memcpy(&id, &a[0], sizeof(int));
		memcpy(&port, &a[4], sizeof(short));
		return make_pair(id, port);
    }

    Address getAddress(int id, short port) {
        Address addr;
        char* a = addr.addr;
        memcpy(&a[0], &id, sizeof(int));
        memcpy(&a[4], &port, sizeof(short));
        return addr;
    }
 }

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */
	int id = *(int*)(&memberNode->addr.addr);
	int port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode);

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    }
    else {
#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        sendMessage(*joinaddr, JOINREQ, false);
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
   /*
    * Your code goes here
    */
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
	long timestamp = par->getcurrtime();
	if (!data) {
#ifdef DEBUGLOG
            log->LOG(&memberNode->addr, "Empty message recieved");
#endif
        return false;
    }
	try {
        Message msg(data, size);
        pair<int, short> idPort = getIdPort(msg.addr);
        MemberListEntry new_entry(idPort.first, idPort.second, msg.heartbeat, timestamp);
        switch (msg.message_type) {
            case (JOINREQ) :
                {
                    addMember(new_entry);
                    sendMessage(msg.addr, JOINREP, true);
                }
                break;
            case (JOINREP) :
                {
                    addMember(new_entry);
                    mergeMembers(msg.members, timestamp);
                    memberNode->inGroup = true;
                }
                break;
            case (PINGREQ) :
                {
                    addMember(new_entry);
                    mergeMembers(msg.members, timestamp);
                    sendMessage(msg.addr, PINGREP, true);
                }
                break;
            case (PINGREP) :
                {
                    addMember(new_entry);
                    mergeMembers(msg.members, timestamp);
                }
                break;
            //case (LEAVEREQ) :
                //process
            //    break;
            //case (LEAVEREP) :
                //process
            //    break;
            }
        }
    catch(...) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Failed to unpack message");
#endif
    }
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {
    size_t randomId = rand() % memberNode->memberList.size();
    MemberListEntry& randomMember = memberNode->memberList[randomId];
    Address addr = getAddress(randomMember.getid(), randomMember.getport());
    sendMessage(addr, PINGREQ, true);
#ifdef DEBUGLOG
//                log->LOG(&memberNode->addr, "ping, %d", addr.addr[0]);
#endif
    long timestamp = par->getcurrtime();
    for (auto it = memberNode->memberList.begin(); it != memberNode->memberList.end();) {
        if (timestamp - it->timestamp > 40) {
            failedItems.push_back(*it);
            Address addr = getAddress(it->id, it->port);
            log->logNodeRemove(&memberNode->addr, &addr);
            it = memberNode->memberList.erase(it);
        }
        else
            ++it;
    }

    for (auto it = failedItems.begin(); it != failedItems.end();) {
        if (timestamp - it->timestamp > 80)
            it = failedItems.erase(it);
        else
            ++it;
    }

	/*
	 * Your code goes here
	 */

    return;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;
}

void MP1Node::addMember(const MemberListEntry& new_entry, long timestamp) {
    Address new_addr = getAddress(new_entry.id, new_entry.port);
    if (new_addr == memberNode->addr)
        return;
    for (auto &entry : memberNode->memberList) {
        if (entry.id == new_entry.id && entry.port == new_entry.port) {
            if (entry.heartbeat < new_entry.heartbeat) {
#ifdef DEBUGLOG
//                log->LOG(&memberNode->addr, "Update, %d", new_entry.id);
#endif
                entry.setheartbeat(new_entry.heartbeat);
                if (timestamp)
                    entry.settimestamp(timestamp);
            }
            return;
        }
    }
    if (isFailed(new_entry))
        return;
    memberNode->memberList.push_back(new_entry);
    if (timestamp)
        memberNode->memberList.back().settimestamp(timestamp);
    log->logNodeAdd(&memberNode->addr, &new_addr);
}

bool MP1Node::isFailed(const MemberListEntry& new_entry) {
    for (auto it = failedItems.begin(); it != failedItems.end(); ++it) {
        if (it->id == new_entry.id && it->port == new_entry.port) {
            if (it->heartbeat < new_entry.heartbeat) {
                failedItems.erase(it);
                return false;
            }
            else
                return true;
        }
    }
    return false;
}


void MP1Node::mergeMembers(const vector<MemberListEntry>& members, long timestamp) {
    for (auto &entry : members) {
        addMember(entry, timestamp);
    }
}

void MP1Node::sendMessage(Address& joinaddr, MsgTypes type, bool pack_data) {
    Message msg(type, memberNode->addr, memberNode->heartbeat, memberNode->memberList);
    pair<char*, size_t> data = msg.Pack(pack_data);
    if (!!data.first) {
        emulNet->ENsend(&memberNode->addr, &joinaddr, data.first, data.second);
        free(data.first);
        ++memberNode->heartbeat;
    }
    else {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Failed to pack message");
#endif
    }
}

Message::Message() {}

Message::Message(MsgTypes t, Address a, long hb, vector<MemberListEntry> m) :
    message_type(t)
    , addr(a)
    , heartbeat(hb)
    , members(m) {}

//Unpack packed message
Message::Message(char* packed_message, size_t message_size) {
    size_t min_size = sizeof(message_type) + sizeof(addr) + sizeof(heartbeat);
    if (message_size < min_size){
        message_type = FAILEDMESSAGE;
        return;
    }
    char* cur = packed_message;
    memcpy(&message_type, cur, sizeof(message_type));
    cur += sizeof(message_type);
    memcpy(&addr, cur, sizeof(addr));
    cur += sizeof(addr);
    memcpy(&heartbeat, cur, sizeof(heartbeat));
    cur += sizeof(heartbeat);
    if (message_size >= min_size + sizeof(size_t)) {
        size_t members_size;
        memcpy(&members_size, cur, sizeof(size_t));
        cur += sizeof(size_t);
        if (members_size > 0) {
            if (message_size >= min_size + sizeof(size_t) + members_size * sizeof(MemberListEntry)) {
                members = vector<MemberListEntry>((MemberListEntry*)cur, (MemberListEntry*)cur + members_size);
            }
            else {
                message_type = FAILEDMESSAGE;
                return;
            }
        }
    }
}

pair<char*, size_t> Message::Pack(bool pack_data) {
    size_t msgsize = sizeof(MsgTypes) + sizeof(Address) + sizeof(long);
    if (pack_data) {
        msgsize += sizeof(size_t) + members.size() * sizeof(MemberListEntry);
    }
    char* msg = (char*) malloc(msgsize * sizeof(char));
        if (!msg) {
        return make_pair<char*, size_t>(nullptr, 0);
    }
    char* cur = msg;
    memcpy(cur, &message_type, sizeof(message_type));
    cur += sizeof(message_type);
    memcpy(cur, &addr, sizeof(addr));
    cur += sizeof(addr);
    memcpy(cur, &heartbeat, sizeof(heartbeat));
    cur += sizeof(heartbeat);
    if (pack_data) {
        size_t sizeoflist = members.size();
        memcpy(cur, &sizeoflist, sizeof(size_t));
        cur += sizeof(size_t);
        if (sizeoflist > 0) {
            memcpy(cur, &members.front(), sizeoflist * sizeof(MemberListEntry));
        }
    }
    return make_pair(msg, msgsize);
}

