/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Header file of MP1Node class.
 **********************************/

#ifndef _MP1NODE_H_
#define _MP1NODE_H_

#include "stdincludes.h"
#include "Log.h"
#include "Params.h"
#include "Member.h"
#include "EmulNet.h"
#include "Queue.h"

/**
 * Macros
 */
#define TREMOVE 20
#define TFAIL 5

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Message Types
 */
enum MsgTypes{
    JOINREQ,
    JOINREP,
    PINGREQ,
    PINGREP,
    LEAVEREQ,
    LEAVEREP,
    FAILEDMESSAGE,
    DUMMYLASTMSGTYPE
};

/**
 * STRUCT NAME: MessageHdr
 *
 * DESCRIPTION: Header and content of a message
 */
typedef struct MessageHdr {
	enum MsgTypes msgType;
}MessageHdr;

/**
 * CLASS NAME: MP1Node
 *
 * DESCRIPTION: Class implementing Membership protocol functionalities for failure detection
 */
class MP1Node {
private:
	EmulNet *emulNet;
	Log *log;
	Params *par;
	Member *memberNode;
	char NULLADDR[6];
	vector<MemberListEntry> failedItems;

public:
	MP1Node(Member *, Params *, EmulNet *, Log *, Address *);
	Member * getMemberNode() {
		return memberNode;
	}
	int recvLoop();
	static int enqueueWrapper(void *env, char *buff, int size);
	void nodeStart(char *servaddrstr, short serverport);
	int initThisNode(Address *joinaddr);
	int introduceSelfToGroup(Address *joinAddress);
	int finishUpThisNode();
	void nodeLoop();
	void checkMessages();
	bool recvCallBack(void *env, char *data, int size);
	void nodeLoopOps();
	int isNullAddress(Address *addr);
	Address getJoinAddress();
	void initMemberListTable(Member *memberNode);
	void printAddress(Address *addr);
	char* packMessage(MsgTypes msgtype, bool pack_data, size_t& msgsize);
	void addMember(const MemberListEntry& new_entry, long timestamp = 0);
	void sendMessage(Address& joinaddr, MsgTypes type, bool pack_data);
	void mergeMembers(const vector<MemberListEntry>& members, long timestamp);
	bool isFailed(const MemberListEntry& new_entry);
	virtual ~MP1Node();
};

struct MessageMP1 {
    MsgTypes message_type;
    Address addr;
    long heartbeat;
    vector<MemberListEntry> members;
    MessageMP1();
    MessageMP1(MsgTypes t, Address a, long hb, vector<MemberListEntry> m);
    //Unpack packed message
    MessageMP1(char* packed_message, size_t message_size);
    pair<char*, size_t> Pack(bool pack_data);
};

#endif /* _MP1NODE_H_ */
