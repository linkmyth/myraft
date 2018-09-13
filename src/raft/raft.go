package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
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
	"bytes"
	"labgob"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

const (
	leader = iota
	candidate
	follower
)

// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Command interface{}
	Term    int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	log         []LogEntry

	commitIndex int
	lastApplied int

	role int

	votedNum int

	nextIndex  []int
	matchIndex []int

	chanApply     chan ApplyMsg
	chanGrantVote chan bool
	chanWinElect  chan bool
	chanHeartbeat chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	if rf.role == leader {
		isleader = true
	} else {
		isleader = false
	}
	return term, isleader
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
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil {
		panic("error read persist")
	} else {

		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

func (rf *Raft) getLastIndex() int {
	return len(rf.log) - 1
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()

	grantVote := false

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		rf.mu.Unlock()
		return
	}

	if args.Term > rf.currentTerm {
		rf.role = follower
		rf.currentTerm = args.Term
		rf.votedFor = -1

	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if ((rf.votedFor == -1) || (rf.votedFor == args.CandidateId)) && (args.LastLogTerm > rf.log[rf.getLastIndex()].Term || (args.LastLogTerm == rf.log[rf.getLastIndex()].Term && args.LastLogIndex >= rf.getLastIndex())) {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		grantVote = true
	}
	rf.persist()

	rf.mu.Unlock()

	if grantVote {
		rf.chanGrantVote <- true
	}
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()

	if !ok {
		rf.mu.Unlock()
		return ok
	}

	winElect := false

	if rf.role != candidate || rf.currentTerm != args.Term {
		rf.mu.Unlock()
		return ok
	}
	if rf.currentTerm < reply.Term {
		rf.role = follower
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.persist()
		rf.mu.Unlock()
		return ok
	}
	if reply.VoteGranted {
		rf.votedNum++
		DPrintf("server %d get vote form server %d, current term %d\n", rf.me, server, rf.currentTerm)
		if rf.votedNum > len(rf.peers)/2 {
			rf.role = leader
			rf.nextIndex = make([]int, len(rf.peers))
			rf.matchIndex = make([]int, len(rf.peers))
			nextIndex := len(rf.log)
			for i := range rf.nextIndex {
				rf.nextIndex[i] = nextIndex
			}

			winElect = true
		}
	}

	rf.mu.Unlock()
	if winElect {
		rf.chanWinElect <- true
	}
	return ok
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
	NextTryIndex int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()

	reply.Success = false

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm

		DPrintf("server %d receiving invalid appendentries from server %d. my current term: %d\n", rf.me, args.LeaderId, rf.currentTerm)
		rf.mu.Unlock()
		return
	}

	DPrintf("server %d receiving valid appendentries from server %d. my current term: %d args prevlogindex %d prelogterm %d entries length %d\n", rf.me, args.LeaderId, rf.currentTerm, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries))

	if args.Term > rf.currentTerm {
		rf.role = follower
		rf.currentTerm = args.Term
		rf.votedFor = -1

	}

	reply.Term = rf.currentTerm

	DPrintf("server %d current logs before accepting appendentries from server %d %v\n", rf.me, args.LeaderId, rf.log)

	if rf.getLastIndex() < args.PrevLogIndex {
		reply.Success = false
		reply.NextTryIndex = rf.getLastIndex() + 1
		rf.mu.Unlock()
		rf.chanHeartbeat <- true
		return
	}

	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		i := args.PrevLogIndex
		for i = args.PrevLogIndex; rf.log[i].Term == rf.log[args.PrevLogIndex].Term; i-- {

		}
		reply.NextTryIndex = i + 1
		DPrintf("server num %d reject append entries\n", rf.me)
		rf.mu.Unlock()
		rf.chanHeartbeat <- true
		return
	}
	DPrintf("server num %d accept append entries\n", rf.me)

	reply.Success = true

	startIndex := args.PrevLogIndex + 1
	for i, e := range args.Entries {
		if len(rf.log)-1 >= startIndex+i {
			// may have bug here
			if rf.log[startIndex+i].Term != e.Term {
				rf.log = rf.log[:startIndex+i]
				rf.log = append(rf.log, e)
			}
		} else {
			rf.log = append(rf.log, e)
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		if rf.getLastIndex() < args.LeaderCommit {
			rf.commitIndex = rf.getLastIndex()
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		go rf.applyLog()
	}
	rf.persist()
	rf.mu.Unlock()
	rf.chanHeartbeat <- true

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	rf.mu.Lock()

	DPrintf("server %d begin sending append entries to server %d, leader(maybe) current term is %d\n", rf.me, server, rf.currentTerm)
	rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !ok || rf.role != leader || args.Term != rf.currentTerm {
		return ok
	}

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.role = follower
		rf.votedFor = -1
		rf.persist()
		return ok
	}

	if reply.Success {
		rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
		rf.matchIndex[server] = rf.nextIndex[server] - 1
		DPrintf("server %d receving success reply from server %d, leader(maybe) current term is %d\n", rf.me, server, rf.currentTerm)
	} else {
		// rpc may fail because of network err (not just logically)
		// if rf.nextIndex[server] > 1 {
		// 	rf.nextIndex[server] = rf.nextIndex[server] - 1
		// }

		if reply.NextTryIndex > rf.matchIndex[server] {
			rf.nextIndex[server] = reply.NextTryIndex
		} else {
			rf.nextIndex[server] = rf.matchIndex[server] + 1
		}

		DPrintf("server %d receving false reply from server %d, leader(maybe) current term is %d, nextTryIndex is %d\n", rf.me, server, rf.currentTerm, reply.NextTryIndex)
	}

	for N := rf.getLastIndex(); N > rf.commitIndex && rf.log[N].Term == rf.currentTerm; N-- {
		count := 1
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= N {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			DPrintf("leader %d commit log with index %d term %d\n", rf.me, N, rf.currentTerm)
			rf.commitIndex = N
			go rf.applyLog()
			break
		}
	}

	return ok
}

func (rf *Raft) applyLog() {
	rf.mu.Lock()
	start := rf.lastApplied + 1
	end := rf.commitIndex
	commands := make([]interface{}, end-start+1)
	for i := start; i <= end; i++ {
		commands[i-start] = rf.log[i].Command
	}
	rf.lastApplied = rf.commitIndex
	rf.mu.Unlock()
	for i := start; i <= end; i++ {
		msg := ApplyMsg{}
		msg.CommandIndex = i
		msg.CommandValid = true
		msg.Command = commands[i-start]
		rf.chanApply <- msg
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != leader {
		return index, term, false
	}

	index = rf.getLastIndex() + 1
	term = rf.currentTerm
	rf.log = append(rf.log, LogEntry{Term: term, Command: command})
	rf.persist()

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.mu.Lock()
	rf.role = follower
	rf.mu.Unlock()
}

func (rf *Raft) broadcastRequestVote() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	args := &RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = rf.getLastIndex()
	args.LastLogTerm = rf.log[rf.getLastIndex()].Term

	for server := range rf.peers {
		if server != rf.me && rf.role == candidate {
			go rf.sendRequestVote(server, args, &RequestVoteReply{})
		}
	}
}

func (rf *Raft) broadcastHeartbeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("server %d begin broadcast heartbeat \n", rf.me)

	for server := range rf.peers {
		if server != rf.me && rf.role == leader {
			args := &AppendEntriesArgs{}
			args.Term = rf.currentTerm
			args.LeaderId = rf.me
			args.PrevLogIndex = rf.nextIndex[server] - 1
			// args.PrevLogIndex must be larger than 0
			DPrintf("server number %d\n", server)

			DPrintf("args.PrevLogIndex: %d\n", args.PrevLogIndex)
			args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
			args.Entries = rf.log[rf.nextIndex[server]:]
			args.LeaderCommit = rf.commitIndex

			go rf.sendAppendEntries(server, args, &AppendEntriesReply{})
		}
	}
}

func (rf *Raft) Run() {
	for {
		rf.mu.Lock()
		role := rf.role
		rf.mu.Unlock()
		switch role {
		case follower:
			select {
			case <-rf.chanGrantVote:
			case <-rf.chanHeartbeat:
			case <-time.After(time.Millisecond * time.Duration(rand.Intn(600)+400)):
				rf.mu.Lock()
				rf.role = candidate
				rf.mu.Unlock()
			}
		case candidate:
			rf.mu.Lock()
			rf.currentTerm++
			rf.votedFor = rf.me
			rf.votedNum = 1
			rf.persist()
			rf.mu.Unlock()
			go rf.broadcastRequestVote()

			select {
			case <-rf.chanHeartbeat:
				rf.mu.Lock()
				rf.role = follower
				rf.mu.Unlock()
			case <-rf.chanWinElect:
				rf.mu.Lock()
				DPrintf("server %d become new leader for term %d\n", rf.me, rf.currentTerm)
				rf.mu.Unlock()
			case <-time.After(time.Millisecond * time.Duration(rand.Intn(600)+400)):
			}
		case leader:
			go rf.broadcastHeartbeat()
			time.Sleep(time.Millisecond * 100)
		}
	}
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.role = follower
	rf.votedNum = 0

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = append(rf.log, LogEntry{Term: 0})

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.chanApply = applyCh

	rf.chanGrantVote = make(chan bool)
	rf.chanWinElect = make(chan bool)
	rf.chanHeartbeat = make(chan bool)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.Run()

	return rf
}
