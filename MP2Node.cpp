/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"

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
	map<int, transaction*>::iterator it = transMap.begin();
	while(it != transMap.end()) {
		delete it->second;
		it++;
	}
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
	Node myself(this->memberNode->addr);
	curMemList.push_back(myself);

	/*
	 * Step 2: Construct the ring
	 */
	// Sort the list based on the hashCode
	
	sort(curMemList.begin(), curMemList.end());
	
	
	if(ring.size() != curMemList.size()){
		change = true;
	}else if(ring.size() != 0){
		for(int i = 0; i < ring.size(); i++){
			if(curMemList[i].getHashCode() != ring[i].getHashCode()){
				change = true;
				break;
			}
		}
	}
	
	/*
	 * Step 3: Run the stabilization protocol IF REQUIRED
	 */
	
	// Run stabilization protocol if the hash table size is greater than zero and if there has been a changed in the ring

	ring = curMemList;
	if(change){
		stabilizationProtocol();
	}	
}


/**
 * FUNCTION NAME: getMembershipList	
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
	vector<Node> replicas = findNodes(key);
	for (int i =0; i < replicas.size(); i++) {
		Message msg = constructMsg(MessageType::CREATE, key, value);
		// cout << "client create trans_id :" << msg.transID << " ; address : "<< memberNode->addr.getAddress() << endl;
		string data = msg.toString();
		emulNet->ENsend(&memberNode->addr, replicas[i].getAddress(), data);
	}
	g_transID ++;
	
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
	vector<Node> replicas = findNodes(key);
	for (int i =0; i < replicas.size(); i++) {
		Message msg = constructMsg(MessageType::READ, key);
		string data = msg.toString();
		emulNet->ENsend(&memberNode->addr, replicas[i].getAddress(), data);
	}
	g_transID ++;
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
	vector<Node> replicas = findNodes(key);
	for (int i =0; i < replicas.size(); i++) {
		Message msg = constructMsg(MessageType::UPDATE, key, value);
		string data = msg.toString();
		emulNet->ENsend(&memberNode->addr, replicas[i].getAddress(), data);
	}
	g_transID ++;
	
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
	vector<Node> replicas = findNodes(key);
	for (int i =0; i < replicas.size(); i++) {
		Message msg = constructMsg(MessageType::DELETE, key);
		string data = msg.toString();
		emulNet->ENsend(&memberNode->addr, replicas[i].getAddress(), data);
	}
	g_transID ++;
}

Message MP2Node::constructMsg(MessageType mType, string key, string value, bool success){
	int trans_id = g_transID;
	createTransaction(trans_id, mType, key, value);
	if(mType == CREATE || mType == UPDATE){
		Message msg(trans_id, this->memberNode->addr, mType, key, value);
		return msg;
	}else if(mType == READ || mType == DELETE){
		Message msg(trans_id, this->memberNode->addr, mType, key);
		return msg;
	}else{
		assert(1!=1); // for debug
	}
}

void MP2Node::createTransaction(int trans_id, MessageType mType, string key, string value){
	int timestamp = this->par->getcurrtime();
	transaction* t = new transaction(trans_id, timestamp, mType, key, value);
	this->transMap.emplace(trans_id, t);
}

// Constructor of transaction
transaction::transaction(int trans_id, int timestamp, MessageType mType, string key, string value){
	this->id = trans_id;
	this->timestamp = timestamp;
	this->replyCount = 0;
	this->successCount = 0;
	this->mType = mType;
	this->key = key;
	this->value = value;
}
/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(string key, string value, ReplicaType replica, int transID) {
	bool success = false;
	if(transID != STABLE){
		success = this->ht->create(key, value);
		if(success)
			log->logCreateSuccess(&memberNode->addr, false, transID, key, value);
		else 
			log->logCreateFail(&memberNode->addr, false, transID, key, value);	
	}else{
		string content = this->ht->read(key);
		bool exist = (content != "");
		if(!exist){
			success = this->ht->create(key, value);	
		}
	}
	return success;
	// Insert key, value, replicaType into the hash table
}

/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 * 			    This function does the following:
 * 			    1) Read key from local hash table
 * 			    2) Return value
 */
string MP2Node::readKey(string key, int transID) {
	string content = this->ht->read(key);
	bool success = (content!="");
	if (success) {
		log->logReadSuccess(&memberNode->addr, false, transID, key, content);
	}else {
		log->logReadFail(&memberNode->addr, false, transID, key);
	}

	return content;
	// Read key from local hash table and return value
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica, int transID) {
	bool success = this->ht->update(key,value);
	if (success) {
		log->logUpdateSuccess(&memberNode->addr, false, transID, key, value);
	} else {
		log->logUpdateFail(&memberNode->addr, false, transID, key, value);
	}
	return success;
	// Update key in local hash table and return true or false
}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::deletekey(string key, int transID) {
	bool success = this->ht->deleteKey(key);
	if(transID != STABLE){
		if (success) {
			log->logDeleteSuccess(&memberNode->addr, false, transID, key);
		} else {
			log->logDeleteFail(&memberNode->addr, false, transID, key);
		}
	}
	return success;
	// Delete the key from the local hash table
}

void MP2Node::sendreply(string key, MessageType mType, bool success, Address* fromaddr, int transID, string content) {
	MessageType replyType = (mType == MessageType::READ)? MessageType::READREPLY: MessageType::REPLY;
	
	if(replyType == MessageType::READREPLY){
		Message msg(transID, this->memberNode->addr, content);
		string data = msg.toString();
		emulNet->ENsend(&memberNode->addr, fromaddr, data);	
	}else{
		// MessageType::REPLY
		Message msg(transID, this->memberNode->addr, replyType, success);
		string data = msg.toString();
		emulNet->ENsend(&memberNode->addr, fromaddr, data);
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


		switch(msg.type){
			case MessageType::CREATE:{
				bool success = createKeyValue(msg.key, msg.value, msg.replica, msg.transID);
				if (msg.transID != STABLE) {
					sendreply(msg.key, msg.type, success, &msg.fromAddr, msg.transID);
				}
				break;
			}
			case MessageType::DELETE:{
				bool success = deletekey(msg.key, msg.transID);
				if (msg.transID != STABLE){
					sendreply(msg.key, msg.type, success, &msg.fromAddr, msg.transID);
				}
				break;
			}
			case MessageType::READ:{
				string content = readKey(msg.key, msg.transID);
				bool success = !content.empty();
				sendreply(msg.key, msg.type, success, &msg.fromAddr, msg.transID, content);
				break;
			}
			case MessageType::UPDATE:{
				bool success = updateKeyValue(msg.key, msg.value, msg.replica, msg.transID);
				sendreply(msg.key, msg.type, success, &msg.fromAddr, msg.transID);
				break;
			}
			case MessageType::READREPLY:{
				map<int, transaction*>::iterator it = transMap.find(msg.transID);
				if(it == transMap.end())
					break;
				transaction* t = transMap[msg.transID];
				t->replyCount ++;
				t->value = msg.value; // content 
				bool success = (msg.value != "");
				
				if(success) {
					t->successCount ++;
				}	
				break;
			}
			case MessageType::REPLY:{
				map<int, transaction*>::iterator it = transMap.find(msg.transID);
				if(it == transMap.end()){
					break;
				}
				
				transaction* t = transMap[msg.transID];
				t->replyCount ++;
				if(msg.success)
					t->successCount ++;
				break;
			}
		}
		/*
		 * Handle the message types here
		 */
		checkTransMap();


	}

	/*
	 * This function should also ensure all READ and UPDATE operation
	 * get QUORUM replies
	 */
}

void MP2Node::checkTransMap(){
	map<int, transaction*>::iterator it = transMap.begin();
	while (it != transMap.end()){
		if(it->second->replyCount == 3) {
			if(it->second->successCount >= 2) {
				logOperation(it->second, true, true, it->first);
			}else{
				logOperation(it->second, true, false, it->first); 
			}
			delete it->second;
			it = transMap.erase(it);
			continue;
		}else {
			if(it->second->successCount == 2) {
				logOperation(it->second, true, true, it->first);
				transComplete.emplace(it->first, true);
				delete it->second;
				it = transMap.erase(it);
				continue;
			}
			
			if(it->second->replyCount - it->second->successCount == 2) {
				logOperation(it->second, true, false, it->first);
				transComplete.emplace(it->first, false);
				delete it->second;
				it = transMap.erase(it);
				continue;
			}
		}
		
		// time limit 
		if(this->par->getcurrtime() - it->second->getTime() > 10) {
				logOperation(it->second, true, false, it->first);
				transComplete.emplace(it->first, false);
				delete it->second;
				it = transMap.erase(it);
				continue;
		}

		it++;
	}	
}

void MP2Node::logOperation(transaction* t, bool isCoordinator, bool success, int transID) {
	switch (t->mType) {
		case CREATE: {
			if (success) {
				log->logCreateSuccess(&memberNode->addr, isCoordinator, transID, t->key, t->value);
			} else {
				log->logCreateFail(&memberNode->addr, isCoordinator, transID, t->key, t->value);
			}
			break;
		}
			
		case READ: {
			if (success) {
				log->logReadSuccess(&memberNode->addr, isCoordinator, transID, t->key, t->value);
			} else {
				log->logReadFail(&memberNode->addr, isCoordinator, transID, t->key);
			}
			break;
		}
			
		case UPDATE: {
			if (success) {
				log->logUpdateSuccess(&memberNode->addr, isCoordinator, transID, t->key, t->value);
			} else {
				log->logUpdateFail(&memberNode->addr, isCoordinator, transID, t->key, t->value);
			}
			break;
		}
			
		case DELETE: {
			if (success) {
				log->logDeleteSuccess(&memberNode->addr, isCoordinator, transID, t->key);
			} else {
				log->logDeleteFail(&memberNode->addr, isCoordinator, transID, t->key);
			}
			break;
		}
	}
}

/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 * 				This function is responsible for finding the replicas of a key
 */
vector<Node> MP2Node::findNodes(string key) {
	size_t pos = hashFunction(key);
	vector<Node> addr_vec;
	if (ring.size() >= 3) {
		// if pos <= min || pos > max, the leader is the min
		if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size()-1).getHashCode()) {
			addr_vec.emplace_back(ring.at(0));
			addr_vec.emplace_back(ring.at(1));
			addr_vec.emplace_back(ring.at(2));
		}
		else {
			// go through the ring until pos <= node
			for (int i=1; i<ring.size(); i++){
				Node addr = ring.at(i);
				if (pos <= addr.getHashCode()) {
					addr_vec.emplace_back(addr);
					addr_vec.emplace_back(ring.at((i+1)%ring.size()));
					addr_vec.emplace_back(ring.at((i+2)%ring.size()));
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
void MP2Node::stabilizationProtocol() {
	map<string, string>::iterator it;
	for(it = this->ht->hashTable.begin(); it != this->ht->hashTable.end(); it++) {
		string key = it->first;
		string value = it->second;
		vector<Node> replicas = findNodes(key);
		for (int i = 0; i < replicas.size(); i++) {
			// create
			Message createMsg(STABLE, this->memberNode->addr, MessageType::CREATE, key, value);
			string createData = createMsg.toString();
			emulNet->ENsend(&memberNode->addr, replicas[i].getAddress(), createData);
		}
	}
}
