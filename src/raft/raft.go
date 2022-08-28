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
	//	"bytes"

	"math/rand"
	"sync"
	"sync/atomic"

	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

const FOLLOWER int = 0
const CANDIDATE int = 1
const LEADER int = 2

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	ch chan ApplyMsg

	// State
	state              int
	currentTerm        int
	votedFor           int
	lastBeatFromLeader time.Time

	nextIndex   []int
	log         []LogEntry
	commitIndex int
	lastApplied int

	broadcasting []bool
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
	isleader = rf.state == LEADER

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
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
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// Append entries RPC

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm

	// 1) Reply false if term < currentTerm
	if rf.currentTerm > args.Term {
		DPrintf("[%d] Received old term %d (mine is %d) from [%d].", rf.me, args.Term, rf.currentTerm, args.LeaderId)
		reply.Success = false
		return
	}

	// 2) Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm
	if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		DPrintf("[%d] Log doesn't contain at index %d term %d from [%d]", rf.me, args.PrevLogIndex, args.PrevLogTerm, args.LeaderId)
		reply.Success = false
		rf.lastBeatFromLeader = time.Now()
		return
	}

	// If we made it till here, we must be follower
	if rf.state != FOLLOWER {
		DPrintf("[%d] Received term %d from [%d], mine is %d. Converting to a follower...", rf.me, args.Term, args.LeaderId, rf.currentTerm)
		rf.state = FOLLOWER
		rf.votedFor = -1
	}

	rf.currentTerm = args.Term
	rf.lastBeatFromLeader = time.Now()
	reply.Success = true

	// 3) If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it
	// 4) Append any new entries not already in the log

	for _, entry := range args.Entries {
		if entry.Index < len(rf.log) {
			// Overwrite entries
			if rf.log[entry.Index] != entry {

				DPrintf("[%d] Received entry, overwriting since index %d", rf.me, entry.Index)
				rf.log = rf.log[:entry.Index]
				rf.log = append(rf.log, entry)
			}
		} else {
			DPrintf("[%d] Received entry %d", rf.me, entry.Index)
			rf.log = append(rf.log, entry)
		}
	}

	// 5) If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		startIndex := rf.commitIndex
		endIndex := min(args.LeaderCommit, rf.log[len(rf.log)-1].Index)

		DPrintf("[%d] Setting commit index to %d", rf.me, endIndex)
		rf.commitIndex = endIndex

		DPrintf("[%d] Sending entries [%d->%d] to client", rf.me, startIndex+1, endIndex)
		for _, entry := range rf.log[startIndex+1 : endIndex+1] {
			rf.sendToClient(entry)
		}
	}
}

func (rf *Raft) sendToClient(entry LogEntry) {
	msg := ApplyMsg{}
	msg.Command = entry.Command
	msg.CommandIndex = entry.Index
	msg.CommandValid = true

	rf.ch <- msg
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs) (bool, AppendEntriesReply) {
	reply := AppendEntriesReply{}

	ok := rf.peers[server].Call("Raft.AppendEntries", args, &reply)
	return ok, reply
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateId int

	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	lastEntry := rf.log[len(rf.log)-1]

	leaderIsUpToDate := args.LastLogTerm > lastEntry.Term || (args.LastLogTerm == lastEntry.Term && args.LastLogIndex >= lastEntry.Index)

	if args.Term < rf.currentTerm {
		DPrintf("[%d] Denying request from %d, because our term is %d and his is %d", rf.me, args.CandidateId, rf.currentTerm, args.Term)
		reply.VoteGranted = false
	} else if !leaderIsUpToDate {
		DPrintf("[%d] Denying request from %d, because his log (%d, %d) is worse than ours (%d, %d)", rf.me, args.CandidateId, args.LastLogTerm, args.LastLogIndex, lastEntry.Term, lastEntry.Index)
		reply.VoteGranted = false
	} else { // or candidate log >=
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	}
}

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
func (rf *Raft) sendRequestVote(server int, candidateId int, term int, lastLogIndex int, lastLogTerm int) bool {

	args := RequestVoteArgs{}
	args.CandidateId = candidateId
	args.Term = term
	args.LastLogIndex = lastLogIndex
	args.LastLogTerm = lastLogTerm

	reply := RequestVoteReply{}
	ok := rf.peers[server].Call("Raft.RequestVote", &args, &reply)
	return ok && reply.VoteGranted
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := 0
	isLeader := rf.state == LEADER
	if isLeader {
		index = len(rf.log)
		term = rf.currentTerm
		rf.log = append(rf.log, LogEntry{rf.currentTerm, index, command})
		rf.nextIndex[rf.me] = index
	}

	// Your code here (2B).
	if isLeader {
		DPrintf("[%d] Start broadcast entry with index %d", rf.me, index)

		for peer, _ := range rf.peers {
			go rf.broadcastToPeer(peer)
		}
	}

	return index, term, isLeader
}

func (rf *Raft) broadcastToPeer(peer int) {
	for !rf.killed() {

		rf.mu.Lock()

		if rf.broadcasting[peer] {
			DPrintf("[%d] Already broadcasting skipping", rf.me)
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		shouldStop := func() bool {

			rf.mu.Lock()
			if peer == rf.me {
				rf.mu.Unlock()
				return true
			}

			if rf.state != LEADER {
				rf.mu.Unlock()
				return true
			}

			indx := rf.nextIndex[peer] + 1
			entries := rf.log[indx:]

			args := AppendEntriesArgs{}
			args.LeaderId = rf.me
			args.Term = rf.currentTerm

			lastEntry := rf.log[indx-1]

			args.PrevLogIndex = lastEntry.Index
			args.PrevLogTerm = lastEntry.Term
			args.Entries = entries
			args.LeaderCommit = rf.commitIndex

			rf.mu.Unlock()

			ok, response := rf.sendAppendEntries(peer, &args)

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.state != LEADER {
				return true
			}

			if ok && response.Success {
				// Store last success peer index
				if len(args.Entries) > 0 {
					rf.nextIndex[peer] = max(rf.nextIndex[peer], args.Entries[len(args.Entries)-1].Index)
				}
				return true
			} else if ok && !response.Success {
				if response.Term > rf.currentTerm {
					DPrintf("[%d] Downgrade to follower", rf.me)
					rf.state = FOLLOWER
					rf.lastBeatFromLeader = time.Now()
				} else {
					rf.nextIndex[peer] = max(rf.nextIndex[peer]-1, 0)
					DPrintf("[%d] Decreasing index %d for [%d]", rf.me, rf.nextIndex[peer], peer)
				}
			} else {
				rf.nextIndex[peer] = max(rf.nextIndex[peer]-1, 0)
				DPrintf("[%d] Decreasing index %d for [%d]", rf.me, rf.nextIndex[peer], peer)
			}
			return false
		}()

		rf.mu.Lock()
		rf.broadcasting[peer] = false
		rf.mu.Unlock()

		if shouldStop {
			return
		}

		time.Sleep(50 * time.Millisecond)
	}
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) isLeader() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state == LEADER
}

func (rf *Raft) startPing() {

	const LEADER_PING_INTERVAL_MS int = 150

	for !rf.killed() {

		rf.mu.Lock()
		state := rf.state

		if state != LEADER {
			rf.mu.Unlock()
			return
		}

		// Move commit index if necessary
		replicatedCommit := getMajority(rf.nextIndex)

		if replicatedCommit > rf.commitIndex {
			startIndex := rf.commitIndex
			rf.commitIndex = replicatedCommit
			DPrintf("[%d] Commiting entry with index %d", rf.me, rf.commitIndex)

			for _, entry := range rf.log[startIndex+1 : replicatedCommit+1] {
				rf.sendToClient(entry)
				DPrintf("[%d] Sending entry with index %d", rf.me, entry.Index)
			}
		}

		rf.mu.Unlock()

		for other, _ := range rf.peers {
			go rf.broadcastToPeer(other)
		}

		time.Sleep(time.Millisecond * time.Duration(LEADER_PING_INTERVAL_MS))
	}
}

func (rf *Raft) startVote() {

	const VOTE_TIMEOUT_MS int = 300

	rf.mu.Lock()
	DPrintf("[%d] Starting the election at term %d", rf.me, rf.currentTerm)
	rf.currentTerm += 1
	rf.state = CANDIDATE
	rf.votedFor = rf.me

	lastLogEntry := rf.log[len(rf.log)-1]

	lastLogIndex := lastLogEntry.Index
	lastLogTerm := lastLogEntry.Term

	term := rf.currentTerm
	candidateId := rf.me
	required := len(rf.peers) / 2
	rf.mu.Unlock()

	var electionLock sync.Mutex
	var granted int
	var finished int

	cond := sync.NewCond(&electionLock)

	started := time.Now()

	go func() {
		time.Sleep(time.Duration(VOTE_TIMEOUT_MS) * time.Millisecond)
		electionLock.Lock()
		defer electionLock.Unlock()
		cond.Broadcast()
	}()

	for other, _ := range rf.peers {
		if other == rf.me {
			continue
		}

		go func(other int) {
			success := rf.sendRequestVote(other, candidateId, term, lastLogIndex, lastLogTerm)

			electionLock.Lock()
			defer electionLock.Unlock()
			finished += 1
			if success {
				granted += 1
			}
			cond.Broadcast()
		}(other)
	}

	electionLock.Lock()
	for granted < required && finished < required && time.Since(started) < time.Duration(VOTE_TIMEOUT_MS)*time.Millisecond {
		cond.Wait()
	}
	elected := granted >= required
	electionLock.Unlock()

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if elected {

		if rf.state == CANDIDATE {
			DPrintf("[%d] Elected at term %d", rf.me, rf.currentTerm)
			rf.state = LEADER

			// Clean nextIndex
			for i := range rf.nextIndex {
				rf.nextIndex[i] = rf.log[len(rf.log)-1].Index
			}

			// Leader ping
			go rf.startPing()
		} else {
			DPrintf("[%d] Elected but turned to a follower during the process", rf.me)
		}
	} else {
		DPrintf("[%d] Wasn't elected at all", rf.me)
		rf.currentTerm -= 1
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	const TICKER_MIN_MS int64 = 400
	const TICKER_MAX_MS int64 = 500

	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		interval := TICKER_MIN_MS + rand.Int63n(TICKER_MAX_MS-TICKER_MIN_MS)
		time.Sleep(time.Duration(interval) * time.Millisecond)

		rf.mu.Lock()
		elapsed := time.Since(rf.lastBeatFromLeader)
		startElection := elapsed.Milliseconds() >= interval && rf.state != LEADER
		rf.mu.Unlock()

		if startElection {
			// Start the election
			rf.startVote()
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.ch = applyCh

	// State
	rf.state = FOLLOWER
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.lastBeatFromLeader = time.Now()

	rf.nextIndex = make([]int, len(rf.peers))
	rf.log = make([]LogEntry, 1)

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.broadcasting = make([]bool, len(rf.peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
