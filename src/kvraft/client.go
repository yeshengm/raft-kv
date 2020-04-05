package kvraft

import "labrpc"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers  []*labrpc.ClientEnd
	leaderId int
	clerkId  int
	seqNum   int
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
	ck.leaderId = int(nrand()) % len(ck.servers)
	ck.clerkId = int(nrand())
	ck.seqNum = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
func (ck *Clerk) Get(key string) string {
	ck.seqNum++
	DPrintf("[CLIENT][%v][%v] Get(%v)", ck.clerkId, ck.seqNum, key)
	args := GetArgs{
		ClerkId: ck.clerkId,
		SeqNum:  ck.seqNum,
		Key:     key,
	}
	// retry until success
	for {
		reply := GetReply{}
		ok := ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply)
		if !ok {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		} else {
			switch reply.Err {
			case ErrWrongLeader:
				ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			case ErrNoKey:
				return ""
			case OK:
				return reply.Value
			}
		}
	}
}

//
// shared by Put and Append.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.seqNum++
	DPrintf("[CLIENT][%v][%v] %v(%v, %v)", ck.clerkId, ck.seqNum, op, key, value)
	args := PutAppendArgs{
		ClerkId: ck.clerkId,
		SeqNum:  ck.seqNum,
		Key:     key,
		Value:   value,
		Op:      op,
	}
	// retry until success
	for true {
		reply := PutAppendReply{}
		ok := ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply)
		if !ok {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			DPrintf("[CLIENT][%v][%v] %v(%v, %v), retry Raft %v", ck.clerkId, ck.seqNum, op, key, value, ck.leaderId)
		} else {
			switch reply.Err {
			case ErrWrongLeader:
				ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
				DPrintf("[CLIENT][%v][%v] %v(%v, %v), retry Raft %v", ck.clerkId, ck.seqNum, op, key, value, ck.leaderId)
			case ErrNoKey:
				panic("unreachable code")
			case OK:
				DPrintf("[CLIENT][%v][%v] %v(%v, %v) committed", ck.clerkId, ck.seqNum, op, key, value)
				return
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
