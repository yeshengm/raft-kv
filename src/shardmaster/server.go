package shardmaster

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sort"
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

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	resps       map[int]Op
	configs     []Config // indexed by config num

	completionCh map[int]chan Op
}

const (
	JOIN = iota
	LEAVE
	MOVE
	QUERY
)

type OpType int

// I really should have implemented something like tagged union here...
type Op struct {
	// Your data here.
	ClerkId int
	SeqNum  int
	OpType  OpType
	// Join RPC
	Servers map[int][]string
	// Leave RPC
	GIDs []int
	// Move RPC
	Shard int
	GID   int
	// Query RPC
	Num    int
	Config Config
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	op := Op{
		ClerkId: args.ClerkId,
		SeqNum:  args.SeqNum,
		OpType:  JOIN,
		Servers: args.Servers,
	}

	// check for duplicate
	sm.mu.Lock()
	lastOp, ok := sm.resps[args.ClerkId]
	if ok && lastOp.SeqNum == args.SeqNum {
		reply.Err = OK
		sm.mu.Unlock()
		return
	}
	sm.mu.Unlock()

	err, _ := sm.syncRaftStart(op)
	reply.Err = err
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	op := Op{
		ClerkId: args.ClerkId,
		SeqNum:  args.SeqNum,
		OpType:  LEAVE,
		GIDs:    args.GIDs,
	}

	// check for duplicate
	sm.mu.Lock()
	lastOp, ok := sm.resps[args.ClerkId]
	if ok && lastOp.SeqNum == args.SeqNum {
		reply.Err = OK
		sm.mu.Unlock()
		return
	}
	sm.mu.Unlock()

	err, _ := sm.syncRaftStart(op)
	reply.Err = err
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	op := Op{
		ClerkId: args.ClerkId,
		SeqNum:  args.SeqNum,
		OpType:  MOVE,
		Shard:   args.Shard,
		GID:     args.GID,
	}

	// check for duplicate
	sm.mu.Lock()
	lastOp, ok := sm.resps[args.ClerkId]
	if ok && lastOp.SeqNum == args.SeqNum {
		reply.Err = OK
		sm.mu.Unlock()
		return
	}
	sm.mu.Unlock()

	err, _ := sm.syncRaftStart(op)
	reply.Err = err
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	op := Op{
		ClerkId: args.ClerkId,
		SeqNum:  args.SeqNum,
		OpType:  QUERY,
		Num:     args.Num,
	}

	// check for duplicate
	sm.mu.Lock()
	lastOp, ok := sm.resps[args.ClerkId]
	if ok && lastOp.SeqNum == args.SeqNum {
		reply.Err = OK
		reply.Config = lastOp.Config
		sm.mu.Unlock()
		return
	}
	sm.mu.Unlock()

	err, committedOp := sm.syncRaftStart(op)
	reply.Err = err
	reply.Config = committedOp.Config
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	sm.rf.Kill()
	// Your code here, if desired.
}

func (sm *ShardMaster) killed() bool {
	z := atomic.LoadInt32(&sm.dead)
	return z == 1
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

func (sm *ShardMaster) syncRaftStart(op Op) (Err, Op) {
	index, _, ok := sm.rf.Start(op)

	// not leader
	if !ok {
		return ErrWrongLeader, op
	}

	sm.mu.Lock()
	ch, ok := sm.completionCh[index]
	if !ok {
		ch = make(chan Op)
		sm.completionCh[index] = ch
	}
	sm.mu.Unlock()

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

	sm.mu.Lock()
	delete(sm.completionCh, index)
	sm.mu.Unlock()

	return err, committedOp
}

func (sm *ShardMaster) copyLastConfig() Config {
	lastConfig := sm.configs[len(sm.configs)-1]
	var config Config
	config.Num = lastConfig.Num + 1
	copy(config.Shards[:], lastConfig.Shards[:])
	config.Groups = map[int][]string{}
	for k, v := range lastConfig.Groups {
		config.Groups[k] = append([]string{}, v...)
	}
	return config
}

type GroupShardCnt struct {
	gid      int
	shardCnt int
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	labgob.Register(Op{})

	sm := new(ShardMaster)
	sm.me = me
	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)
	sm.resps = make(map[int]Op)
	sm.completionCh = make(map[int]chan Op)

	go func() {
		for !sm.killed() {
			applyMsg := <-sm.applyCh
			op := applyMsg.Command.(Op)
			index := applyMsg.CommandIndex

			sm.mu.Lock()
			lastOp, ok := sm.resps[op.ClerkId]
			if !ok || op.SeqNum > lastOp.SeqNum {
				// step state machine here
				switch op.OpType {
				case JOIN:
					config := sm.copyLastConfig()
					// add new groups
					for gid, servers := range op.Servers {
						if _, ok := config.Groups[gid]; ok {
							panic("joining an existing group")
						}
						config.Groups[gid] = servers
					}
					// calculate new shard load
					shardCnt := make(map[int]int)
					for gid := range config.Groups {
						shardCnt[gid] = 0
					}
					for _, gid := range config.Shards {
						if gid != 0 {
							shardCnt[gid]++
						}
					}
					shardCntArr := make([]GroupShardCnt, 0)
					for gid, cnt := range shardCnt {
						shardCntArr = append(shardCntArr, GroupShardCnt{
							gid:      gid,
							shardCnt: cnt,
						})
					}
					sort.Slice(shardCntArr, func(i, j int) bool {
						return shardCntArr[i].shardCnt > shardCntArr[j].shardCnt
					})
					for i := range shardCntArr {
						shardCntArr[i].shardCnt = len(config.Shards) / len(config.Groups)
					}
					for i := 0; i < len(config.Shards)%len(config.Groups); i++ {
						shardCntArr[i].shardCnt++
					}
					shardLoad := make(map[int]int)
					for _, it := range shardCntArr {
						shardLoad[it.gid] = it.shardCnt
					}
					DPrintf("%v", shardLoad)
					// re-balance load
					for i, gid := range config.Shards {
						load, ok := shardLoad[gid]
						if ok {
							config.Shards[i] = gid
							if load == 1 {
								delete(shardLoad, gid)
							} else {
								shardLoad[gid]--
							}
						} else {
							for gid, load := range shardLoad {
								config.Shards[i] = gid
								if load == 1 {
									delete(shardLoad, gid)
								} else {
									shardLoad[gid]--
								}
								break
							}
						}
					}
					//DPrintf("[%v][%v][JOIN] %v", sm.me, index, config)
					sm.configs = append(sm.configs, config)
				case LEAVE:
					config := sm.copyLastConfig()
					// delete groups, and mark related shards as orphaned
					for _, gid := range op.GIDs {
						if _, ok := config.Groups[gid]; !ok {
							panic("deleting a non-existing group")
						}
						delete(config.Groups, gid)
					}
					deletedGIDs := make(map[int]struct{})
					for _, gid := range op.GIDs {
						deletedGIDs[gid] = struct{}{}
					}
					for i, gid := range config.Shards {
						if _, ok := deletedGIDs[gid]; ok {
							config.Shards[i] = 0
						}
					}

					if len(config.Groups) == 0 {
						for i := range config.Shards {
							config.Shards[i] = 0
						}
					} else {
						// calculate new shard load
						shardCnt := make(map[int]int)
						for gid := range config.Groups {
							shardCnt[gid] = 0
						}
						for _, gid := range config.Shards {
							if gid != 0 {
								shardCnt[gid]++
							}
						}
						shardCntArr := make([]GroupShardCnt, 0)
						for gid, cnt := range shardCnt {
							shardCntArr = append(shardCntArr, GroupShardCnt{
								gid:      gid,
								shardCnt: cnt,
							})
						}
						sort.Slice(shardCntArr, func(i, j int) bool {
							return shardCntArr[i].shardCnt > shardCntArr[j].shardCnt
						})
						for i := range shardCntArr {
							shardCntArr[i].shardCnt = len(config.Shards) / len(config.Groups)
						}
						for i := 0; i < len(config.Shards)%len(config.Groups); i++ {
							shardCntArr[i].shardCnt++
						}
						shardLoad := make(map[int]int)
						for _, it := range shardCntArr {
							shardLoad[it.gid] = it.shardCnt
						}
						DPrintf("%v", shardLoad)
						// re-balance load
						for i, gid := range config.Shards {
							if gid != 0 {
								load, ok := shardLoad[gid]
								if ok {
									config.Shards[i] = gid
									if load == 1 {
										delete(shardLoad, gid)
									} else {
										shardLoad[gid]--
									}
								} else {
									for gid, load := range shardLoad {
										config.Shards[i] = gid
										if load == 1 {
											delete(shardLoad, gid)
										} else {
											shardLoad[gid]--
										}
										break
									}
								}
							}
						}
						for i, gid := range config.Shards {
							if gid == 0 {
								load, ok := shardLoad[gid]
								if ok {
									config.Shards[i] = gid
									if load == 1 {
										delete(shardLoad, gid)
									} else {
										shardLoad[gid]--
									}
								} else {
									for gid, load := range shardLoad {
										config.Shards[i] = gid
										if load == 1 {
											delete(shardLoad, gid)
										} else {
											shardLoad[gid]--
										}
										break
									}
								}
							}
						}
					}
					DPrintf("[%v][%v][LEAVE] %v", sm.me, index, config)
					sm.configs = append(sm.configs, config)
				case MOVE:
					config := sm.copyLastConfig()
					if _, ok := config.Groups[op.GID]; !ok {
						panic("moving a shard to a non-existing group")
					}
					config.Shards[op.Shard] = op.GID
					sm.configs = append(sm.configs, config)
				case QUERY:
					if op.Num == -1 || op.Num >= len(sm.configs) {
						op.Config = sm.configs[len(sm.configs)-1]
					} else {
						op.Config = sm.configs[op.Num]
					}
					DPrintf("[%v][%v][QUERY] %v", sm.me, index, op.Config)
				}

				sm.resps[op.ClerkId] = op
				ch, ok := sm.completionCh[index]
				sm.mu.Unlock()
				if ok {
					ch <- op
				}
			} else {
				sm.mu.Unlock()
			}
		}
	}()

	return sm
}
