package raftkv

import (
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
	"encoding/gob"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation   string
	Key         string
	Value       string
	OperationId int
	ClientId    int64
}

const (
	Get    string = "Get"
	Put           = "Put"
	Append        = "Append"
)

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvStorage map[string]string
	result    map[int]chan Result
	opHistory map[int64]int
}

type Result struct {
	OperationId int
	ClientId    int64
	Value       string
	Error       Err
}

func (kv *RaftKV) CheckDup(clientId int64, opId int) bool {
	hisOpId, ok := kv.opHistory[clientId]
	if !ok || hisOpId < opId {
		return false
	}
	DPrintf("Detect duplicate --- his: %d, new: %d", opId, hisOpId)

	return true
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{Operation: "Get", Key: args.Key, OperationId: args.OperationId, ClientId: args.ClientId}
	DPrintf("Raftkv start to %s, key: %s", "Get",op.Key)
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	kv.mu.Lock()
	ch, ok := kv.result[index]
	if !ok {
		//ch = make(chan Result, 1)
		//kv.result[index] = ch
		kv.result[index] = make(chan Result,1)
		ch = kv.result[index]
	}
	kv.mu.Unlock()

	select {
	case result := <-ch:
		if result.OperationId != args.OperationId || result.ClientId != args.ClientId {
			reply.WrongLeader = true
			return
		}
		DPrintf("Get the result, client:%s", args.ClientId)

		reply.Value = result.Value
		reply.Err = result.Error
		reply.WrongLeader = false
	case <-time.After(1000 * time.Millisecond):
		reply.WrongLeader = true
		return
	}
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs,
	reply *PutAppendReply) {
	// Your code here.
	op := Op{
		Operation:   args.Op,
		Key:         args.Key,
		Value:       args.Value,
		OperationId: args.OperationId,
		ClientId:    args.ClientId,
	}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	//DPrintf("start %s operation#%d-%d, key: %s", args.Op, args.ClientId, args.OperationId, args.Key)
	kv.mu.Lock()
	ch, ok := kv.result[index]
	if !ok {
		kv.result[index] = make(chan Result,1)
		ch = kv.result[index]
	}
	kv.mu.Unlock()
	//DPrintf("end %s operation#%d-%d, key: %s", args.Op, args.ClientId, args.OperationId, args.Key)

	select {
	case result := <-ch:
		DPrintf("get result %s operation#%d-%d, key: %s", args.Op, args.ClientId, args.OperationId, args.Key)
		if result.OperationId != args.OperationId || result.ClientId != args.ClientId {
			reply.WrongLeader = true
			return
		}
		//DPrintf("Return %s result, operationId %d, clientId %d, value %s", args.Op, args.OperationId, args.ClientId, result.Value)

		reply.Err = result.Error
		reply.WrongLeader = false
		return
	case <-time.After(1000 * time.Millisecond):
		reply.WrongLeader = true
		return
	}
}

func (kv *RaftKV) runServer() {
	for true {
		applied := <-kv.applyCh
		log := applied.Command.(Op)
		result := Result{OperationId: log.OperationId, ClientId: log.ClientId}
		kv.mu.Lock()
		switch log.Operation {
		case "Get":
			value, ok := kv.kvStorage[log.Key]
			if ok {
				result.Error = OK
				result.Value = value
			} else {
				result.Error = ErrNoKey
				result.Value = ""
			}
		case "Put":
			if !kv.CheckDup(result.ClientId, result.OperationId) {
				kv.kvStorage[log.Key] = log.Value
			}
		case "Append":
			if !kv.CheckDup(result.ClientId, result.OperationId) {
				_, ok := kv.kvStorage[log.Key]
				if ok {
					kv.kvStorage[log.Key] += log.Value
				} else {
					kv.kvStorage[log.Key] = log.Value
				}
			}
		}

		if !kv.CheckDup(result.ClientId, result.OperationId) {
			DPrintf("Apply run %s", log.Operation)
			kv.opHistory[result.ClientId] = result.OperationId
		}

		ch, ok := kv.result[applied.Index]
		if ok {
			// clean the ch
			select {
			case <-ch:
			default:
			}
			ch <- result
		} else {
			kv.result[applied.Index] = make(chan Result, 1)
		}

		kv.mu.Unlock()
	}
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	kv.kvStorage = make(map[string]string)
	kv.result = make(map[int]chan Result)
	kv.opHistory = make(map[int64]int)

	go kv.runServer()

	return kv
}
