package kvraft

import (
	"../labrpc"
	"sync"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers  []*labrpc.ClientEnd
	leaderId int
	mu       sync.Mutex
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	// You'll have to add code here.
	ck := new(Clerk)
	ck.servers = servers
	ck.leaderId = int(nrand()) % len(ck.servers)
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	args := GetArgs{Key: key}
	ck.mu.Lock()
	leaderId := ck.leaderId
	ck.mu.Unlock()
	// retry until success
	for true {
		reply := GetReply{}
		ok := ck.servers[leaderId].Call("KVServer.Get", &args, &reply)
		if !ok {
			leaderId = (leaderId + 1) % len(ck.servers)
		} else {
			switch reply.Err {
			case ErrWrongLeader:
				leaderId = (leaderId + 1) % len(ck.servers)
			case ErrNoKey:
				ck.mu.Lock()
				ck.leaderId = leaderId
				ck.mu.Unlock()
				return ""
			case OK:
				ck.mu.Lock()
				ck.leaderId = leaderId
				ck.mu.Unlock()
				return reply.Value
			}
		}
	}
	return ""
}

//
// shared by Put and Append.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
	}
	ck.mu.Lock()
	leaderId := ck.leaderId
	ck.mu.Unlock()
	// retry until success
	for true {
		reply := PutAppendReply{}
		ok := ck.servers[leaderId].Call("KVServer.PutAppend", &args, &reply)
		if !ok {
			leaderId = (leaderId + 1) % len(ck.servers)
		} else {
			switch reply.Err {
			case ErrWrongLeader:
				DPrintf("[KV] (%v, %v, %v) reached wrong leader %v", op, key, value, leaderId)
				leaderId = (leaderId + 1) % len(ck.servers)
			case ErrNoKey:
				panic("unreachable code")
			case OK:
				ck.mu.Lock()
				ck.leaderId = leaderId
				ck.mu.Unlock()
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
