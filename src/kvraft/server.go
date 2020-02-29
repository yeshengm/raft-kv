package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"log"
	"sync"
	"sync/atomic"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const (
	PUT = iota
	APPEND
	GET
)

type OpType int

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	RpcId  int
	OpType OpType
	Key    string
	Value  string
}

type KVServer struct {
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db map[string]string

	rpcId    int
	// TODO any better approach?
	getChans map[int]chan string
	putChans map[int]chan struct{}
	mu       sync.Mutex
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	kv.rpcId++
	resChan := make(chan string)
	kv.getChans[kv.rpcId] = resChan
	op := Op{
		RpcId:  kv.rpcId,
		OpType: GET,
		Key:    args.Key,
		Value:  "",
	}
	kv.mu.Unlock()

	_, _, ok := kv.rf.Start(op)
	if !ok {
		kv.mu.Lock()
		delete(kv.getChans, op.RpcId)
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		return
	}

	reply.Err = OK
	reply.Value = <-resChan
	kv.mu.Lock()
	delete(kv.getChans, op.RpcId)
	kv.mu.Unlock()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	kv.rpcId++
	resChan := make(chan struct{})
	kv.putChans[kv.rpcId] = resChan
	op := Op{
		RpcId: kv.rpcId,
		Key:   args.Key,
		Value: args.Value,
	}
	if args.Op == "Put" {
		op.OpType = PUT
	} else if args.Op == "Append" {
		op.OpType = APPEND
	} else {
		panic("unexpected")
	}
	kv.mu.Unlock()

	_, _, ok := kv.rf.Start(op)
	if !ok {
		kv.mu.Lock()
		delete(kv.putChans, op.RpcId)
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		return
	}

	reply.Err = OK
	<-resChan
	kv.mu.Lock()
	delete(kv.putChans, op.RpcId)
	kv.mu.Unlock()
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.db = make(map[string]string)
	kv.getChans = make(map[int]chan string)
	kv.putChans = make(map[int]chan struct {})
	// You may need initialization code here.

	// dispatch applyCh messages to RPC handlers
	go func() {
		for !kv.killed() {
			applyMsg := <-kv.applyCh
			if !applyMsg.CommandValid {
				panic("all cmds should be valid by now")
			}
			op := applyMsg.Command.(Op)
			switch op.OpType {
			case GET:
				resChan := kv.getChans[op.RpcId]
				resChan <- kv.db[op.Key]
			case PUT:
				resChan := kv.putChans[op.RpcId]
				kv.db[op.Key] = op.Value
				resChan <- struct{}{}
			case APPEND:
				resChan := kv.putChans[op.RpcId]
				kv.db[op.Key] = kv.db[op.Key] + op.Value
				resChan <- struct {}{}
			}
		}
	}()

	return kv
}
