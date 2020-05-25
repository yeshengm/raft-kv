package shardkv

import (
	"bytes"
	"labgob"
	"labrpc"
	"log"
	"raft"
	"shardmaster"
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

type ShardKV struct {
	mu   sync.Mutex
	me   int
	dead int32 // set by Kill()

	// shardmaster status
	make_end func(string) *labrpc.ClientEnd
	gid      int
	masters  []*labrpc.ClientEnd
	mck      *shardmaster.Clerk
	config   *shardmaster.Config

	// snapshot & persistence
	applyCh      chan raft.ApplyMsg
	rf           *raft.Raft
	maxraftstate int // snapshot if log grows this big
	persister    *raft.Persister

	// state machine for the shard kv
	db          map[string]string
	resps       map[int]Op
	lastApplied int

	// notification channels for client requests
	completionCh map[int]chan Op
}

// synchronously call into Raft.Start()
func (kv *ShardKV) syncRaftStart(op Op) (Err, Op) {
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

func (kv *ShardKV) key2gid(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardmaster.NShards
	if kv.config == nil {
		return -1
	}
	return kv.config.Shards[shard]
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	if kv.key2gid(args.Key) != kv.gid {
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return
	}
	kv.mu.Unlock()

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

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	if kv.key2gid(args.Key) != kv.gid {
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return
	}
	kv.mu.Unlock()

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
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// restore previously persisted snapshot.
//
func (kv *ShardKV) readPersist(data []byte) {
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
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	// initialize ShardKV server
	kv := new(ShardKV)
	kv.me = me

	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters
	kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.maxraftstate = maxraftstate
	kv.persister = persister
	kv.readPersist(persister.ReadSnapshot())
	kv.completionCh = make(map[int]chan Op)

	// keep polling shardmaster for newest configuration
	go func() {
		for !kv.killed() {
			config := kv.mck.Query(-1)
			kv.mu.Lock()
			kv.config = &config
			kv.mu.Unlock()
			time.Sleep(100 * time.Millisecond)
		}
	}()

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
			if index != kv.lastApplied+1 {
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
				kv.lastApplied++

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
