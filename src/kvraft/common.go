package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	ClerkId int
	SeqNum  int

	Key   string
	Value string
	Op    string // "Put" or "Append"
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	ClerkId int
	SeqNum  int
	Key     string
}

type GetReply struct {
	Err   Err
	Value string
}
