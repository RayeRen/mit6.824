package raftkv

import "labrpc"
import "crypto/rand"
import (
	"math/big"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lastLeader  int
	operationId int
	clientId    int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.operationId = 0
	ck.clientId = nrand()
	//DPrintf("make new clerk %s", ck)
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Key:         key,
		OperationId: ck.operationId,
		ClientId:    ck.clientId,
	}
	ck.operationId++
	var reply GetReply
	for {
		reply = GetReply{}
		DPrintf("client call %s", "Get")
		ok := ck.servers[ck.lastLeader].Call("RaftKV.Get", &args, &reply)

		if ok && !reply.WrongLeader {
			DPrintf("client call %s successful. The value is %s", "Get", reply.Value)
			break
		}
		ck.lastLeader = (ck.lastLeader + 1) % len(ck.servers)
	}

	// You will have to modify this function.
	return reply.Value
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{
		Key:         key,
		Value:       value,
		Op:          op,
		OperationId: ck.operationId,
		ClientId:    ck.clientId,
	}
	ck.operationId++
	for {
		reply := PutAppendReply{}
		DPrintf("client call %s", op)
		ok := ck.servers[ck.lastLeader].Call("RaftKV.PutAppend", &args, &reply)

		if ok && !reply.WrongLeader {
			DPrintf("client call  %s successful. The value is %s", op, args.Value)
			break
		}
		ck.lastLeader = (ck.lastLeader + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
