package shardmaster

//
// Shardmaster clerk.
//

import "labrpc"
import "time"
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

func (ck *Clerk) Query(num int) Config {
	ck.seqNum++
	args := &QueryArgs{
		ClerkId: ck.clerkId,
		SeqNum:  ck.seqNum,
		Num:     num,
	}

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardMaster.Query", args, &reply)
			if ok && reply.Err == OK {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	ck.seqNum++
	args := &JoinArgs{
		ClerkId: ck.clerkId,
		SeqNum:  ck.seqNum,
		Servers: servers,
	}

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardMaster.Join", args, &reply)
			if ok && reply.Err == OK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	ck.seqNum++
	args := &LeaveArgs{
		ClerkId: ck.clerkId,
		SeqNum:  ck.seqNum,
		GIDs:    gids,
	}

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardMaster.Leave", args, &reply)
			if ok && reply.Err == OK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.seqNum++
	args := &MoveArgs{
		ClerkId: ck.clerkId,
		SeqNum:  ck.seqNum,
		Shard:   shard,
		GID:     gid,
	}

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardMaster.Move", args, &reply)
			if ok && reply.Err == OK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
