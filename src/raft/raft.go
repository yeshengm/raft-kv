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
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

type LogEntry struct {
	Index   int
	Term    int
	Command interface{} // generic command for later override
}

// RPC args & reps: fields must start with Cap letters to be exported
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
	Entry        LogEntry
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
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

// raft status
const (
	Follower  = iota
	Candidate
	Leader
)

// timeout thresholds
const (
	ElectionBaseTimeout = 400
	ElectionRandTimeout = 400
	HeartbeatTimeout    = 150
)

// generate a random timeout to prevent deadlock
func randomTimeoutGen() time.Duration {
	baseTime := time.Duration(ElectionBaseTimeout * time.Millisecond)
	randTime := time.Duration(rand.Int63n(ElectionRandTimeout)) * time.Millisecond
	return baseTime + randTime
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// persistent fields
	status      int
	currentTerm int
	votedFor    int
	log         []LogEntry
	// volatile fields
	commitIndex int
	lastApplied int
	lastAppend int
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
	applyCh            chan ApplyMsg // interface to outside server
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
	}
}

// TODO handler functions for incoming messages
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
		// the moment wins the election, starts heartbeat
		if rf.voteCnt == len(rf.peers)/2+1 {
			rf.status = Leader
			DPrintf("%v elected leader", rf.me)
			// when switch to leader, initialize volatile states on leader
			for i := range rf.peers {
				// reinitialize after election
				rf.nextIndex[i] = len(rf.log)
				rf.matchIndex[i] = 0
				if i != rf.me {
					// heartbeat args
					args := AppendEntriesArgs{}
					args.Term = rf.currentTerm
					args.LeaderId = rf.me
					args.PrevLogIndex = rf.nextIndex[i] - 1
					args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
					if rf.nextIndex[i] < len(rf.log) {
						args.Entry = rf.log[rf.nextIndex[i]]
					} else {
						args.Entry = LogEntry{}
					}
					args.LeaderCommit = rf.commitIndex
					// heartbeat reply
					rep := AppendEntriesReply{}
					ii := i
					go func() {
						rep.PeerId = ii
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
		DPrintf("log at leader %v, commitIndex %v: %v", rf.me, rf.commitIndex, rf.log[1:])
		if rep.Success {
			rf.nextIndex[rep.PeerId] = rep.LastLogIndex + 1
			rf.matchIndex[rep.PeerId] = rep.LastLogIndex
			// possible commitIndex update TODO
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
			rf.nextIndex[rep.PeerId] = rep.LastLogIndex
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
		for i := range rf.peers {
			if i != rf.me {
				// setup replicate log args
				args := AppendEntriesArgs{}
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.PrevLogIndex = rf.nextIndex[i] - 1
				args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
				if rf.nextIndex[i] < len(rf.log) {
					args.Entry = rf.log[rf.nextIndex[i]]
				} else {
					args.Entry = LogEntry{}
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
		rf.resetHeartbeatTimer()
	}
}

// return currentTerm and whether this server believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.status == Leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

// RPC calls
//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
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
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
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
	DPrintf("log at follow %v, commitIndex %v: %v", rf.me, rf.commitIndex, rf.log[1:])
	if rf.commitIndex > rf.lastApplied {
		msg := ApplyMsg{}
		msg.Index = rf.lastApplied + 1
		msg.Command = rf.log[msg.Index].Command
		rf.applyCh <- msg
		rf.lastApplied++
	}
	// out-dated append request, just return
	if rf.currentTerm > args.Term {
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

	if prevLogIndex == 0 {
		// log empty
		if args.Entry.Index != 0 {
			rf.log = append(rf.log, args.Entry)
			reply.LastLogIndex = 1
			reply.Success = true
			rf.lastAppend = reply.LastLogIndex
		} else {
			reply.LastLogIndex = 0
			reply.Success = false
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
		} else {
			// log match
			reply.LastLogIndex = prevLogIndex
			if args.Entry.Index != 0 {
				// append to this log
				rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entry)
				reply.LastLogIndex++
				rf.lastAppend = reply.LastLogIndex
			}
			reply.Success = true
		}
	} else if prevLogIndex >= len(rf.log) {
		reply.LastLogIndex = prevLogIndex
		reply.Inconsistent = true
		reply.Success = false
	}
	if args.LeaderCommit > rf.commitIndex && !reply.Inconsistent {
		if args.LeaderCommit < rf.lastAppend {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = rf.lastAppend
		}
	}
	rf.resetElectionTimer()
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

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.status == Leader {
		index = len(rf.log)
		term = rf.currentTerm
		logEntry := LogEntry{index, term, command}
		rf.log = append(rf.log, logEntry)
		rf.nextIndex[rf.me] = len(rf.log)
		rf.matchIndex[rf.me] = len(rf.log) - 1

		return index, term, true
	} else {
		return index, term, false
	}
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	// initialize fields
	rf.currentTerm = 1
	rf.status = Follower // a peer should be follower at the first place
	rf.log = []LogEntry{LogEntry{0, 0, nil}}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// initialize channels
	rf.electionTimeout = make(chan struct{}, 1) // timeout channel
	rf.heartbeatTimeout = make(chan struct{}, 1)
	rf.requestVoteReply = make(chan RequestVoteReply, 1)     // requestVote's reply channel
	rf.appendEntriesReply = make(chan AppendEntriesReply, 1) // appendEntries's reply channel
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.StartWorking()

	return rf
}
