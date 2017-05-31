package raft

// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"simrpc"
	"math/rand"
	"sync"
	"time"
	"bytes"
	"encoding/gob"
)

type LogEntry struct {
	Index   int
	Term    int
	Command interface{} // generic command for later override
}

// RPC args & reps: fields must start with CAPITAL letters to be exported
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term         int
	Success      bool
	PeerId       int
	LastLogIndex int
	Inconsistent bool
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool
	Snapshot    []byte
}

// raft status
const (
	Follower  = iota
	Candidate
	Leader
)

// timeout thresholds
const (
	ElectionBaseTimeout = 500
	ElectionRandTimeout = 300
	HeartbeatTimeout    = 100
)

// generate a random timeout to prevent deadlock
func randomTimeoutGen() time.Duration {
	baseTime := time.Duration(ElectionBaseTimeout * time.Millisecond)
	randTime := time.Duration(rand.Int63n(ElectionRandTimeout)) * time.Millisecond
	return baseTime + randTime
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*simrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// persistent fields
	status      int
	currentTerm int
	votedFor    int
	log         []LogEntry
	// volatile fields
	commitIndex int
	lastApplied int
	lastAppend  int
	nextIndex   []int
	matchIndex  []int

	// vote counter
	voteCnt int
	// utilities
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
	// channels
	electionTimeout    chan struct{}
	heartbeatTimeout   chan struct{}
	requestVoteReply   chan RequestVoteReply
	appendEntriesReply chan AppendEntriesReply
	applyCh            chan ApplyMsg // interface to outside service
}

// timer methods
func (rf *Raft) createElectionTimer() {
	rf.electionTimer = time.NewTimer(randomTimeoutGen())
	go func() {
		for {
			select {
			case <-rf.electionTimer.C:
				rf.electionTimeout <- struct{}{}
			}
		}
	}()
}

func (rf *Raft) createHeartbeatTimer() {
	rf.heartbeatTimer = time.NewTimer(HeartbeatTimeout * time.Millisecond)
	go func() {
		for {
			select {
			case <-rf.heartbeatTimer.C:
				rf.heartbeatTimeout <- struct{}{}
			}
		}
	}()
}

func (rf *Raft) resetElectionTimer() {
	rf.electionTimer.Reset(randomTimeoutGen())
}

func (rf *Raft) resetHeartbeatTimer() {
	rf.heartbeatTimer.Reset(HeartbeatTimeout * time.Millisecond)
}

// listening to channels and ensure exclusive access to Raft
func (rf *Raft) StartWorking() {
	rf.createElectionTimer()
	rf.createHeartbeatTimer()
	// raft works according to incoming msgs in a sequential manner
	for {
		select {
		case rep := <-rf.requestVoteReply:
			rf.requestVoteReplyHandler(rep)
		case rep := <-rf.appendEntriesReply:
			rf.appendEntriesReplyHandler(rep)
		case <-rf.electionTimeout:
			rf.electionTimeoutHandler()
		case <-rf.heartbeatTimeout:
			rf.heartbeatTimeoutHandler()
		}
		rf.persist()
	}
}

/////////////////////////////////////////////////
//    Truly interesting stuff begins here      //
/////////////////////////////////////////////////

func (rf *Raft) requestVoteReplyHandler(rep RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.commitIndex > rf.lastApplied {
		msg := ApplyMsg{}
		msg.Index = rf.lastApplied + 1
		msg.Command = rf.log[msg.Index].Command
		rf.applyCh <- msg
		rf.lastApplied++
	}

	if rf.status == Candidate && rf.currentTerm == rep.Term && rep.VoteGranted {
		rf.voteCnt++
		// the moment wins the election, starts first empty heartbeat
		if rf.voteCnt == len(rf.peers)/2+1 {
			// fmt.Println("Node", rf.me, " elected leader")
			rf.status = Leader
			// when switch to leader, initialize volatile states on leader
			for i := range rf.peers {
				rf.nextIndex[i] = len(rf.log)
				rf.matchIndex[i] = 0
			}
			for i := range rf.peers {
				// reinitialize fields after election
				if i != rf.me {
					args := AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: -1,
						PrevLogTerm:  -1,
						LeaderCommit: rf.commitIndex,
					}
					rep := AppendEntriesReply{
						PeerId: i,
					}
					ii := i // take care! copy loop var here
					go func() {
						rf.sendAppendEntries(ii, &args, &rep)
						rf.appendEntriesReply <- rep
					}()
				}
			}
			rf.resetHeartbeatTimer()
		}
	} else if rep.Term > rf.currentTerm {
		rf.status = Follower
		rf.voteCnt = 0
		rf.currentTerm = rep.Term
		rf.resetElectionTimer()
	}
}

func (rf *Raft) appendEntriesReplyHandler(rep AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.commitIndex > rf.lastApplied {
		msg := ApplyMsg{}
		msg.Index = rf.lastApplied + 1
		msg.Command = rf.log[msg.Index].Command
		rf.applyCh <- msg
		rf.lastApplied++
	}

	if rep.Term > rf.currentTerm {
		rf.status = Follower
		rf.currentTerm = rep.Term
		rf.resetElectionTimer()
		return
	}
	// if all info match, roll forward log
	if rf.status == Leader && rf.currentTerm == rep.Term {
		if rep.Success {
			rf.nextIndex[rep.PeerId] = rep.LastLogIndex + 1
			rf.matchIndex[rep.PeerId] = rep.LastLogIndex
			for i := rep.LastLogIndex; i > rf.commitIndex; i-- {
				if i < len(rf.log) && rf.log[i].Term == rf.currentTerm {
					cnt := 0
					for j := range rf.peers {
						if rf.matchIndex[j] >= i {
							cnt++
						}
					}
					// win majority
					if cnt >= len(rf.peers)/2+1 {
						rf.commitIndex = i
						break
					}
				}
			}
		} else if rf.nextIndex[rep.PeerId] != 0 && rep.Inconsistent {
			rf.nextIndex[rep.PeerId] -= 1
		}

	}
}

func (rf *Raft) electionTimeoutHandler() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.status != Leader {
		rf.status = Candidate
		// start a new term
		rf.currentTerm++
		rf.voteCnt = 1
		for i := range rf.peers {
			if i != rf.me {
				args := RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateId:  rf.me,
					LastLogIndex: rf.log[len(rf.log)-1].Index,
					LastLogTerm:  rf.log[len(rf.log)-1].Term,
				}
				rep := RequestVoteReply{}
				ii := i
				go func() {
					rf.sendRequestVote(ii, &args, &rep)
					rf.requestVoteReply <- rep
				}()
			}
		}
	}
	rf.resetElectionTimer()
}

func (rf *Raft) heartbeatTimeoutHandler() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.status == Leader {
		rf.resetHeartbeatTimer()
		for i := range rf.peers {
			if i != rf.me {
				// setup replicate log args
				args := AppendEntriesArgs{}
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.PrevLogIndex = rf.nextIndex[i] - 1
				args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
				if rf.nextIndex[i] < len(rf.log) {
					args.Entries = rf.log[rf.nextIndex[i]:]
				} else {
					args.Entries = []LogEntry{}
				}
				args.LeaderCommit = rf.commitIndex
				// replicate log reply
				rep := AppendEntriesReply{}
				ii := i
				go func() {
					rep.PeerId = ii
					rf.sendAppendEntries(ii, &args, &rep)
					rf.appendEntriesReply <- rep
				}()
			}
		}
		rf.persist()
	}
}

// RPC calls
//
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The simrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.commitIndex > rf.lastApplied {
		msg := ApplyMsg{}
		msg.Index = rf.lastApplied + 1
		msg.Command = rf.log[msg.Index].Command
		rf.applyCh <- msg
		rf.lastApplied++
	}
	if args.Term > rf.currentTerm {
		// revert to follower if received more up-to-date term & log entries
		rf.status = Follower
		rf.voteCnt = 0
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		// grant vote if log is more up-to-date
		lastLog := rf.log[len(rf.log)-1]
		if args.LastLogTerm > lastLog.Term ||
			args.LastLogTerm == lastLog.Term && args.LastLogIndex >= lastLog.Index {
			reply.VoteGranted = true
			rf.resetElectionTimer()
		}
	} else {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.commitIndex > rf.lastApplied {
		msg := ApplyMsg{}
		msg.Index = rf.lastApplied + 1
		msg.Command = rf.log[msg.Index].Command
		rf.applyCh <- msg
		rf.lastApplied++
	}
	// out-dated append request, just return
	if rf.currentTerm > args.Term || args.PrevLogIndex < 0 {
		reply.Term = rf.currentTerm
		return
	}
	// convert to follower if term is out-dated
	if args.Term > rf.currentTerm {
		rf.status = Follower
		rf.voteCnt = 0
	} else if args.Term == rf.currentTerm && rf.status == Candidate {
		rf.status = Follower
		rf.voteCnt = 0
	}
	reply.Term = rf.currentTerm
	// same term now
	prevLogIndex := args.PrevLogIndex
	prevLogTerm := args.PrevLogTerm
	rf.resetElectionTimer()

	// fmt.Println("peer: ", rf.me, " ", prevLogIndex, " ", rf.log)
	if prevLogIndex == 0 {
		// log empty
		if len(args.Entries) != 0 {
			rf.log = append(rf.log[:1], args.Entries...)
			reply.LastLogIndex = len(args.Entries)
			reply.Success = true
			rf.lastAppend = reply.LastLogIndex
		} else {
			reply.LastLogIndex = 0
			reply.Success = false
			return
		}
	} else if prevLogIndex < len(rf.log) {
		rf.log = rf.log[:prevLogIndex+1]
		// prevLogIndex in log
		if rf.log[prevLogIndex].Term != prevLogTerm {
			// log mismatch
			rf.log = rf.log[:prevLogIndex]
			reply.LastLogIndex = prevLogIndex - 1
			reply.Inconsistent = true
			reply.Success = false
			return
		} else {
			// log match
			reply.LastLogIndex = prevLogIndex
			// append to this log
			rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
			reply.LastLogIndex = len(rf.log) - 1
			rf.lastAppend = reply.LastLogIndex
			if len(args.Entries) != 0 {
				reply.Success = true
			}
		}
	} else if prevLogIndex >= len(rf.log) {
		reply.LastLogIndex = len(rf.log) - 1
		reply.Inconsistent = true
		reply.Success = false
		return
	}
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit < rf.lastAppend {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = rf.lastAppend
		}
	}
	rf.persist()
}

// RPC helper functions
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// return currentTerm and whether this server believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.status == Leader
}

// simulates disk io
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)

	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index, term := -1, -1
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.status == Leader {
		index, term = len(rf.log), rf.currentTerm
		logEntry := LogEntry{index, term, command}
		rf.log = append(rf.log, logEntry)
		rf.nextIndex[rf.me] = len(rf.log)
		rf.matchIndex[rf.me] = len(rf.log) - 1
		return index, term, true
	} else {
		return index, term, false
	}
}

// de-allocates resources
func (rf *Raft) Kill() {
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	//close(rf.electionTimeout)
	//close(rf.heartbeatTimeout)
	//close(rf.requestVoteReply)
	//close(rf.appendEntriesReply)
}

// factory method to create a Raft peer
func Make(peers []*simrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// initialize fields
	rf.currentTerm = 1
	rf.status = Follower // a peer should be follower at the first place
	rf.log = []LogEntry{{0, 0, nil}}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// initialize channels
	rf.electionTimeout = make(chan struct{}, 1)
	rf.heartbeatTimeout = make(chan struct{}, 1)
	rf.requestVoteReply = make(chan RequestVoteReply, 1)
	rf.appendEntriesReply = make(chan AppendEntriesReply, 1)
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.StartWorking()

	return rf
}
