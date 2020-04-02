package kvraft

import (
	"bytes"
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"sync/atomic"
	"time"
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
	ClerkId int
	SeqNum  int
	OpType  OpType
	Key     string
	Value   string // also serves as Get's response
}

type KVServer struct {
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	persister *raft.Persister

	// state machine for the kv store
	db           map[string]string
	resps        map[int]Op
	lastApplied int

	// a mapping from index to RPC completion channel, so that
	// a client request can be notified of the log replicated
	// on that index.
	completionCh map[int]chan Op
	mu           sync.Mutex
}

// synchronously call into Raft.Start()
func (kv *KVServer) syncRaftStart(op Op) (Err, Op) {
	index, _, ok := kv.rf.Start(op)

	// not leader
	if !ok {
		return ErrWrongLeader, op
	}

	// wait for response on this channel
	// NB: it is possible that a major GC is triggered here and
	// this channel can never receive the response.
	kv.mu.Lock()
	ch, ok := kv.completionCh[index]
	if !ok {
		ch = make(chan Op)
		kv.completionCh[index] = ch
	}
	kv.mu.Unlock()

	var err Err
	var committedOp Op
	select {
	case committedOp = <-ch:
		// successful commit at this index
		if op.ClerkId == committedOp.ClerkId && op.SeqNum == committedOp.SeqNum {
			err = OK
		} else {
			err = ErrWrongLeader
		}
	case <-time.After(1000 * time.Millisecond):
		// timeout, but not necessarily failed commit. simply retry.
		err = ErrWrongLeader
	}

	kv.mu.Lock()
	delete(kv.completionCh, index)
	kv.mu.Unlock()
	return err, committedOp
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	op := Op{
		ClerkId: args.ClerkId,
		SeqNum:  args.SeqNum,
		OpType:  GET,
		Key:     args.Key,
		Value:   "",
	}

	// check for duplicate
	kv.mu.Lock()
	lastOp, ok := kv.resps[args.ClerkId]
	if ok && lastOp.SeqNum == args.SeqNum {
		reply.Err = OK
		reply.Value = lastOp.Value
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	err, committedOp := kv.syncRaftStart(op)
	reply.Err = err
	reply.Value = committedOp.Value
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	var opType OpType
	switch args.Op {
	case "Put":
		opType = PUT
	case "Append":
		opType = APPEND
	default:
		panic("unexpected")
	}
	op := Op{
		ClerkId: args.ClerkId,
		SeqNum:  args.SeqNum,
		OpType:  opType,
		Key:     args.Key,
		Value:   args.Value,
	}

	// check for duplicate
	kv.mu.Lock()
	lastOp, ok := kv.resps[args.ClerkId]
	if ok && lastOp.SeqNum == args.SeqNum {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	err, _ := kv.syncRaftStart(op)
	reply.Err = err
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
// restore previously persisted snapshot.
//
func (kv *KVServer) readPersist(data []byte) {
	// bootstrap without any state
	if data == nil || len(data) < 1 {
		kv.db = make(map[string]string)
		kv.resps = make(map[int]Op)
		kv.lastApplied = 0
		return
	}
	kv.lastApplied = 0
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&kv.db) != nil {
		panic("error decoding db")
	}
	if d.Decode(&kv.resps) != nil {
		panic("error decoding resps")
	}
	if d.Decode(&kv.lastApplied) != nil {
		panic("error decoding lastApplied")
	}
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
	kv.persister = persister
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.readPersist(persister.ReadSnapshot())
	kv.completionCh = make(map[int]chan Op)

	// dispatch applyCh messages to RPC handlers
	go func() {
		for !kv.killed() {
			applyMsg := <-kv.applyCh
			kv.mu.Lock()
			// handle InstallSnapshot RPC
			if !applyMsg.CommandValid {
				kv.readPersist(applyMsg.Data)
				// garbage collect unused channels in completionCh
				for index := range kv.completionCh {
					if index <= applyMsg.LastIncludedIndex {
						delete(kv.completionCh, index)
					}
				}
				kv.mu.Unlock()
				continue
			}
			op := applyMsg.Command.(Op)
			index := applyMsg.CommandIndex

			DPrintf("[RSM][%v] receive log %v", kv.me, index)
			if index != kv.lastApplied + 1 {
				DPrintf("[RSM][%v] log index %v, last applied %v", kv.me, index, kv.lastApplied)
				kv.mu.Unlock()
				continue
			}

			lastOp, ok := kv.resps[op.ClerkId]
			if !ok || op.SeqNum > lastOp.SeqNum {
				switch op.OpType {
				case GET:
					op.Value = kv.db[op.Key]
					DPrintf("[KV][%v] Get(%v)@%v", kv.me, op.Key, index)
				case PUT:
					kv.db[op.Key] = op.Value
					DPrintf("[KV][%v] Put(%v, %v)@%v", kv.me, op.Key, op.Value, index)
				case APPEND:
					kv.db[op.Key] = kv.db[op.Key] + op.Value
					DPrintf("[KV][%v] Append(%v, %v)@%v", kv.me, op.Key, op.Value, index)
				}
				kv.resps[op.ClerkId] = op
				// notify the completion channel
				ch, ok := kv.completionCh[index]
				kv.lastApplied += 1

				kv.mu.Unlock()
				if ok {
					// TODO can have deadlock here...
					ch <- op
				}
			} else {
				switch op.OpType {
				case GET:
					DPrintf("[RSM][%v] duplicate log %v, Get(%v), %v", kv.me, index, op.Key, op)
				case PUT:
					DPrintf("[RSM][%v] duplicate log %v, Put(%v, %v), %v", kv.me, index, op.Key, op.Value, op)
				case APPEND:
					DPrintf("[RSM][%v] duplicate log %v, Append(%v, %v), %v", kv.me, index, op.Key, op.Value, op)
				}
				kv.lastApplied += 1
				kv.mu.Unlock()
			}

			// check if a log compaction is necessary
			if kv.maxraftstate == -1 {
				continue
			}
			if kv.persister.RaftStateSize() >= kv.maxraftstate {
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				_ = e.Encode(kv.db)
				_ = e.Encode(kv.resps)
				_ = e.Encode(kv.lastApplied)
				snapshot := w.Bytes()
				go kv.rf.Snapshot(snapshot, index)
			}
		}
	}()

	return kv
}
