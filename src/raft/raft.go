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
	"encoding/gob"
	"fmt"
	"labrpc"
	"math/rand"
	//	"sort"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool // ignore for lab2; only used in lab3
}

type LogEntry struct {
	Term    int
	Command interface{}
}

type VoteState int

const (
	Agree   VoteState = iota
	Reject            = iota
	Timeout           = iota
	Stop              = iota
)

type State int

const (
	Leader    State = iota
	Candidate       = iota
	Follower        = iota
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Persistent state on all servers:
	CurrentTerm int        // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	VotedFor    int        // candidateId that received vote in current term (or null if none)
	Log         []LogEntry // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	timer         *time.Timer
	applyCh       chan ApplyMsg
	applySignalCh chan bool

	// Volatile state on all servers:
	state       State
	commitIndex int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	// Volatile state on leaders:
	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// var term int
	var isleader bool
	// // Your code here (2A).
	// return term, isleader
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == Leader {
		isleader = true
	} else {
		isleader = false
	}
	return rf.CurrentTerm, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Log)
	data := w.Bytes()
	DPrintf(fmt.Sprintf("persist: me = %v, term = %v, voteFor = %v, log = %v", rf.me, rf.CurrentTerm, rf.VotedFor, rf.Log))
	rf.persister.SaveRaftState(data)
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
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.CurrentTerm)
	d.Decode(&rf.VotedFor)
	d.Decode(&rf.Log)
	DPrintf(fmt.Sprintf("readPersist: me = %v, term = %v, voteFor = %v, log = %v", rf.me, rf.CurrentTerm, rf.VotedFor, rf.Log))
}

type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader's commitIndex
}

type AppendEntriesReply struct {
	Term          int  // currentTerm, for leader to update itself
	Success       bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	ConflictTerm  int
	ConflictIndex int
}

func selectTopK(array []int, k int) (bool, int) {
	left := 0
	right := len(array) - 1
	for left <= right {
		start := left
		end := right
		x := array[start]
		for start < end {
			for start < end && x >= array[end] {
				end--
			}
			array[start] = array[end]
			for start < end && x <= array[start] {
				start++
			}
			array[end] = array[start]
		}
		array[start] = x
		if start+1 == k {
			return true, x
		} else if start+1 > k {
			right = start - 1
		} else {
			left = start + 1
		}
	}
	DPrintf(fmt.Sprintf("selectTopK FAIL with k = %v, array = %v", k, array))
	return false, -1
}

func (rf *Raft) UpdateCommitIndex() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	matchList := make([]int, len(rf.matchIndex))
	copy(matchList, rf.matchIndex)
	matchList[rf.me] = len(rf.Log) - 1
	//sort.Ints(matchList)
	//n := matchList[len(rf.peers)/2]
	_, n := selectTopK(matchList, 1+len(rf.peers)/2)
	DPrintf(fmt.Sprintf("UpdateCommitIndex: me = %v, commitIndex = %v, currentTerm = %v, cl = %v, matchIndex = %v", rf.me, rf.commitIndex, rf.CurrentTerm, matchList, rf.matchIndex))
	if n > rf.commitIndex && rf.Log[n].Term == rf.CurrentTerm {
		rf.commitIndex = n
		go func() { rf.applySignalCh <- true }()
	}
}

func (rf *Raft) ApplyCommitedMsg() {
	// just block a little time
	for {
		select {
		case <-rf.applySignalCh:
			rf.mu.Lock()
			DPrintf(fmt.Sprintf("Apply: me = %v, from %v to %v, log = %v", rf.me, rf.lastApplied+1, rf.commitIndex, rf.Log))
			enties := rf.Log[rf.lastApplied+1 : rf.commitIndex+1]
			start := rf.lastApplied + 1
			if rf.commitIndex > rf.lastApplied {
				rf.lastApplied = rf.commitIndex
			}
			rf.mu.Unlock()
			for i := 0; i < len(enties); i++ {
				msg := ApplyMsg{Index: i + start, Command: enties[i].Command}
				rf.applyCh <- msg
			}
		}
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.CurrentTerm
	DPrintf(fmt.Sprintf("AppendEntries: me = %v, currentTerm = %v, len(rf.Log) = %v, rf.Log = %v",
		rf.me, rf.CurrentTerm, len(rf.Log), rf.Log))
	DPrintf(fmt.Sprintf("rf.me = %v, leaderId = %v, leader's term = %v, entries = %v, PrevLogIndex = %v, PrevLogTerm = %v",
		rf.me, args.LeaderId, args.Term, args.Entries, args.PrevLogIndex, args.PrevLogTerm))

	if args.Term < reply.Term {
		reply.Success = false
	} else {
		rf.ResetTimer(Follower)
		rf.state = Follower
		if rf.CurrentTerm < args.Term {
			rf.VotedFor = -1
			rf.CurrentTerm = args.Term
			rf.persist()
		}
		logLen := len(rf.Log)
		if args.PrevLogIndex >= logLen {
			reply.ConflictTerm = -1
			reply.ConflictIndex = logLen
			DPrintf(fmt.Sprintf("me = %v, PrevLogIndx %v >= len(rf.log) %v, ConflictTerm = %v, ConflictIndex = %v", rf.me, args.PrevLogIndex, len(rf.Log), reply.ConflictTerm, reply.ConflictIndex))
			reply.Success = false
		} else if args.PrevLogTerm != rf.Log[args.PrevLogIndex].Term {
			// prev not match
			reply.ConflictTerm = rf.Log[args.PrevLogIndex].Term
			reply.ConflictIndex = args.PrevLogIndex
			for reply.ConflictIndex-1 > 0 {
				if rf.Log[reply.ConflictIndex-1].Term != reply.ConflictTerm {
					break
				}
				reply.ConflictIndex--
			}
			DPrintf(fmt.Sprintf("PrevLog not match: me = %v, leaderId = %v, me's prevLogTerm = %v, leader's prevLogTerm = %v, ConflictTerm = %v, ConflictIndex = %v", rf.me, args.LeaderId, rf.Log[args.PrevLogIndex].Term, args.Term, reply.ConflictTerm, reply.ConflictIndex))
			reply.Success = false
		} else {
			reply.Success = true
			entryLen := len(args.Entries)
			if entryLen > 0 {
				start := 0 // start >= entryLen means entries already exsit
				for i := args.PrevLogIndex + 1; i < len(rf.Log) && start < entryLen; i++ {
					if rf.Log[i].Term != args.Entries[start].Term {
						// delete conflict
						DPrintf(fmt.Sprintf("Delete conflict: me = %v, leaderId = %v, index = %v, me's term = %v, leader's term = %v", rf.me, args.LeaderId, i, rf.Log[i].Term, args.Entries[i-1-args.PrevLogIndex].Term))
						rf.Log = rf.Log[:i]
						break
					} else {
						start++
					}
				}
				for i := start; i < entryLen; i++ {
					rf.Log = append(rf.Log, args.Entries[i])
				}
				if start < entryLen {
					rf.persist()
					DPrintf(fmt.Sprintf("Replic success: me = %v, commitIndex = %v, leaderId = %v, leader's commitIndex = %v, rf.Log = %v", rf.me, rf.commitIndex, args.LeaderId, args.LeaderCommit, rf.Log))
				}
			}
			oldCommitIndex := rf.commitIndex
			if args.LeaderCommit > rf.commitIndex {
				lastNewEntryIndex := len(rf.Log) - 1
				if args.LeaderCommit < lastNewEntryIndex {
					rf.commitIndex = args.LeaderCommit
				} else {
					rf.commitIndex = lastNewEntryIndex
				}
			}
			if oldCommitIndex < rf.commitIndex {
				go func() { rf.applySignalCh <- true }()
			}
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

func LogUpToDate(termA, lastIndexA, termB, lastIndexB int) bool {
	if termA > termB {
		return true
	}
	if termA == termB && lastIndexA >= lastIndexB {
		return true
	}
	return false
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// 1. avoid current vote
	// 2. avoid TA voting and TB eleting, conflict with term write or compare and decide
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.CurrentTerm
	DPrintf(fmt.Sprintf("Vote: me = %v, currentTerm = %v, CandidateId = %v, c'term = %v", rf.me, reply.Term, args.CandidateId, args.Term))
	if reply.Term > args.Term {
		reply.VoteGranted = false
	} else {
		if args.Term > reply.Term {
			// update self
			rf.CurrentTerm = args.Term
			rf.VotedFor = -1
		}
		lastLogTerm := 0
		lastLogIndex := len(rf.Log) - 1
		lastLogTerm = rf.Log[lastLogIndex].Term
		if (rf.VotedFor < 0 || rf.VotedFor == args.CandidateId) &&
			LogUpToDate(args.LastLogTerm, args.LastLogIndex,
				lastLogTerm, lastLogIndex) {
			reply.VoteGranted = true
			rf.VotedFor = args.CandidateId
			rf.ResetTimer(Follower)
		} else {
			reply.VoteGranted = false
		}
		if reply.VoteGranted || args.Term > reply.Term {
			rf.state = Follower
			rf.persist()
		}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader := false
	index := len(rf.Log)
	term := rf.CurrentTerm
	if rf.state == Leader {
		isLeader = true
		rf.Log = append(rf.Log, LogEntry{rf.CurrentTerm, command})
		rf.persist()
		rf.ResetTimer(Leader)
		go rf.BroadcastHeartbeats()
	}

	DPrintf(fmt.Sprintf("Start: me = %v, index = %v, term = %v, isLeader = %v", rf.me, index, term, isLeader))
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
	defer rf.mu.Unlock()
	rf.persist()
}

func RandomTimeOutDelta(state State) time.Duration {
	const electionTimeoutLowerBound = 300
	const electionTimeoutUpperBound = 600
	const heartbeatDelta = 100
	if state == Leader {
		return time.Duration(heartbeatDelta) * time.Millisecond
	} else {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		delta := r.Intn(electionTimeoutUpperBound-electionTimeoutLowerBound) +
			electionTimeoutLowerBound
		return time.Duration(delta) * time.Millisecond
	}
}

func (rf *Raft) Elect() {
	DPrintf(fmt.Sprintf("Elect start: me = %v", rf.me))

	rf.mu.Lock()
	if rf.state == Leader {
		rf.mu.Unlock()
		return
	}
	nums := len(rf.peers)
	lastLogTerm := 0
	lastLogIndex := len(rf.Log) - 1
	lastLogTerm = rf.Log[lastLogIndex].Term
	// increase term and reset votedFor
	rf.CurrentTerm += 1
	rf.VotedFor = rf.me
	rf.persist()

	currentTerm := rf.CurrentTerm
	me := rf.me
	rf.mu.Unlock()

	votes := make(chan VoteState, len(rf.peers)-1)

	for i := 0; i < nums; i++ {
		if i == me {
			continue
		}
		serverIndex := i
		args := RequestVoteArgs{
			Term:         currentTerm,
			CandidateId:  me,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastLogTerm}
		go func() {
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(serverIndex, &args, &reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.CurrentTerm != args.Term {
				votes <- Stop // outdate response, stop elect
			} else {
				if ok {
					if reply.VoteGranted {
						votes <- Agree
					} else {
						if rf.CurrentTerm < reply.Term {
							rf.CurrentTerm = reply.Term
							rf.state = Follower
							rf.ResetTimer(Follower)
							rf.persist()
							votes <- Stop // stop elect or reset elect timeout
						} else {
							votes <- Reject
						}
					}
				} else {
					votes <- Timeout // maybe loss connections
					// FIXME: retry?
				}
			}
		}()
	}

	agreeCount := 1
	rejectCount := 0
	finish := false
	for !finish {
		select {
		case vote := <-votes:
			rf.mu.Lock()
			if currentTerm == rf.CurrentTerm {
				if vote == Agree {
					agreeCount++
					DPrintf(fmt.Sprintf("Elect: me = %v, agreeCount = %v, total = %v", me, agreeCount, nums))
					if agreeCount >= 1+nums/2 {
						rf.state = Leader
						nextIndex := len(rf.Log)
						for i := 0; i < len(rf.nextIndex); i++ {
							rf.nextIndex[i] = nextIndex
							rf.matchIndex[i] = 0
						}
						go rf.BroadcastHeartbeats()
						rf.ResetTimer(Leader)
						finish = true
					}
				} else if vote == Reject || vote == Timeout {
					rejectCount++
					DPrintf(fmt.Sprintf("Elect: me = %v, rejectCount = %v, total = %v", me, rejectCount, nums))
					if rejectCount >= 1+nums/2 {
						rf.state = Follower
						rf.ResetTimer(Follower)
						finish = true
					}
				} else {
					DPrintf(fmt.Sprintf("Elect: me = %v stoped!"))
					finish = true
				}
			} else {
				DPrintf(fmt.Sprintf("Elect: me = %v outdateTerm = %v, currentTerm = %v", currentTerm, rf.CurrentTerm))
				finish = true
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) Heartbeat(index int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	ok := rf.sendAppendEntries(index, args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.CurrentTerm == args.Term && ok {
		if reply.Success {
			DPrintf(fmt.Sprintf("HB reply success me = %v, index = %v", rf.me, index))
			newNextIndex := args.PrevLogIndex + len(args.Entries) + 1
			newMatchIndex := newNextIndex - 1
			if newMatchIndex > rf.matchIndex[index] {
				rf.nextIndex[index] = newNextIndex
				rf.matchIndex[index] = newMatchIndex
				if newMatchIndex > rf.commitIndex {
					go rf.UpdateCommitIndex()
				}
			}
		} else {
			if rf.CurrentTerm >= reply.Term {
				DPrintf(fmt.Sprintf("Miss match casue resend HB: me = %v, index = %v, oldPrevLogIndex = %v, ConflictTerm = %v, ConflictIndex = %v", rf.me, index, args.PrevLogIndex, reply.ConflictTerm, reply.ConflictIndex))
				for i := args.PrevLogIndex - 1; i >= 0; i-- {
					if rf.Log[i].Term == reply.ConflictTerm {
						args.PrevLogIndex = i
						break
					}
				}
				if rf.Log[args.PrevLogIndex].Term != reply.ConflictTerm {
					args.PrevLogIndex = reply.ConflictIndex - 1
				}
				args.Entries = rf.Log[args.PrevLogIndex+1 : len(rf.Log)]
				args.PrevLogTerm = rf.Log[args.PrevLogIndex].Term
				// args.Entries = rf.Log[args.PrevLogIndex:len(rf.Log)]
				// args.PrevLogIndex = args.PrevLogIndex - 1 // always >= 0
				// args.PrevLogTerm = rf.Log[args.PrevLogIndex].Term

				go rf.Heartbeat(index, args, reply)
			} else {
				rf.state = Follower
				rf.CurrentTerm = reply.Term
				rf.VotedFor = -1
				rf.persist()
			}
		}
	}
}

func (rf *Raft) BroadcastHeartbeats() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return
	}
	nums := len(rf.peers)
	me := rf.me
	currentTerm := rf.CurrentTerm
	lastLogIndex := len(rf.Log) - 1

	DPrintf(fmt.Sprintf("HB Leader's log: me = %v, currentTerm = %v, commitIndex = %v, rf.Log = %v", rf.me, rf.CurrentTerm, rf.commitIndex, rf.Log))

	for i := 0; i < nums; i++ {
		if i == me {
			continue
		}
		DPrintf(fmt.Sprintf("HB entries: me = %v, index = %v, from %v to %v", rf.me, i, rf.nextIndex[i], lastLogIndex))
		prevLogIndex := rf.nextIndex[i] - 1 // always >= 0
		prevLogTerm := rf.Log[prevLogIndex].Term
		args := AppendEntriesArgs{
			Term:         currentTerm,
			LeaderId:     me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      rf.Log[rf.nextIndex[i] : lastLogIndex+1],
			LeaderCommit: rf.commitIndex}

		reply := AppendEntriesReply{}
		go rf.Heartbeat(i, &args, &reply)
	}
}

// call ResetTimer with rf.mu.Lock() please
func (rf *Raft) ResetTimer(state State) {
	if !rf.timer.Stop() {
		select {
		case <-rf.timer.C:
		default:
		}
	}
	delta := RandomTimeOutDelta(state)
	rf.timer.Reset(delta)
}

func (rf *Raft) BackGroundTimer() {
	for {
		select {
		case <-rf.timer.C:
			rf.mu.Lock()
			DPrintf(fmt.Sprintf("Timer: me = %v, state = %v", rf.me, rf.state))
			if Leader == rf.state {
				go rf.BroadcastHeartbeats()
			} else {
				go rf.Elect()
			}
			rf.timer.Reset(RandomTimeOutDelta(rf.state))
			rf.mu.Unlock()
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
	DPrintf(fmt.Sprintf("Init: me = %v, len(peers) = %v", me, len(peers)))
	rf.state = Follower
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.matchIndex = make([]int, len(peers))
	rf.applyCh = applyCh
	rf.applySignalCh = make(chan bool)

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.CurrentTerm = 0
	rf.VotedFor = -1
	rf.Log = make([]LogEntry, 1)
	rf.readPersist(persister.ReadRaftState())
	rf.nextIndex = make([]int, len(peers))
	nextIndex := len(rf.Log)
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = nextIndex
	}
	rf.timer = time.NewTimer(RandomTimeOutDelta(Follower))
	go rf.BackGroundTimer()
	go rf.ApplyCommitedMsg()

	return rf
}
