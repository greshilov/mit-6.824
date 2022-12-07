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
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

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

// ====== My types ======

type RaftState int32

const (
	INITIAL   RaftState = -1
	FOLLOWER  RaftState = 0
	CANDIDATE RaftState = 1
	LEADER    RaftState = 2
)

type PeerNotify bool

const (
	EXIT          PeerNotify = true
	NEW_LOG_ENTRY PeerNotify = false
)

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

	state            int32
	stateCh          chan RaftState
	lastBeatFromPeer time.Time
	currentTerm      int
	votedFor         int
	votedTerm        int
	log              []LogEntry
	nextIndex        []int
	commitIndex      int
	lastApplied      int
	peerCh           []chan PeerNotify

	clientCh chan struct{}
	applyCh  chan ApplyMsg

	lastIncludedIndex int
	lastIncludedTerm  int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.getRaftState() == LEADER
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

	rf.persister.SaveRaftState(rf.getStateData())
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var log []LogEntry
	var lastIncludedTerm int
	var lastIncludedIndex int
	var lastApplied int

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludedTerm) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastApplied) != nil {
		rf.DPrintf("Error during read state")
	} else {
		rf.mu.Lock()
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		_, lastEntry := rf.getLogEntry(-1)

		for i := range rf.peers {
			rf.nextIndex[i] = lastEntry.Index
		}

		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.lastApplied = lastIncludedIndex

		rf.DPrintf("Recovered with logFirstItem %d, lastItem %d, lastIncludedIndex %d (lastApplied %d)", rf.log[0].Index, rf.log[len(rf.log)-1].Index, rf.lastIncludedIndex, lastApplied)
		rf.mu.Unlock()
	}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Don't prune lower than current commit
	index = min(index, rf.commitIndex)

	newSlice := rf.getLogSlice(index, -1)

	DPrintf("[%d] Prunning log to index %d, log size %d -> %d", rf.me, index, len(rf.log), len(newSlice))

	_, entry := rf.getLogEntry(index)
	rf.log = newSlice

	rf.lastIncludedIndex = entry.Index
	rf.lastIncludedTerm = entry.Term

	rf.persister.SaveStateAndSnapshot(rf.getStateData(), snapshot)
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
	reply.VoteGranted = false
	_, lastEntry := rf.getLogEntry(-1)
	leaderIsUpToDate := args.LastLogTerm > lastEntry.Term || (args.LastLogTerm == lastEntry.Term && args.LastLogIndex >= lastEntry.Index)

	if args.Term < rf.currentTerm {
		rf.DPrintf("Denying request from [%d], because our term is %d and his is %d", args.CandidateId, rf.currentTerm, args.Term)
	} else if !leaderIsUpToDate {
		rf.DPrintf("Denying request from [%d], because his log (%d, %d) is worse than ours (%d, %d)", args.CandidateId, args.LastLogTerm, args.LastLogIndex, lastEntry.Term, lastEntry.Index)
	} else {
		rf.lastBeatFromPeer = time.Now()
		if rf.votedTerm == args.Term && rf.votedFor == args.CandidateId || rf.votedTerm != args.Term {
			rf.DPrintf("Vote granted for [%d] in term %d", args.CandidateId, args.Term)
			rf.votedFor = args.CandidateId
			rf.votedTerm = args.Term
			reply.VoteGranted = true
		}
		rf.persist()
	}

	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		if rf.getRaftState() != FOLLOWER {
			rf.DPrintf("Downgrading to FOLLOWER %d > %d", args.Term, rf.currentTerm)
			rf.setRaftState(FOLLOWER)
		}
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
func (rf *Raft) sendRequestVote(server int, candidateId int, term int, lastLogIndex int, lastLogTerm int) (bool, int) {

	args := RequestVoteArgs{}
	args.CandidateId = candidateId
	args.Term = term
	args.LastLogIndex = lastLogIndex
	args.LastLogTerm = lastLogTerm

	reply := RequestVoteReply{}
	ok := rf.peers[server].Call("Raft.RequestVote", &args, &reply)
	return ok && reply.VoteGranted, reply.Term
}

// ====== AppendEntries ======
type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term        int
	Success     bool
	FirstTermId int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	persist := false
	reply.Term = rf.currentTerm

	// 1) Reply false if term < currentTerm
	if rf.currentTerm > args.Term {
		rf.DPrintf("Received old term %d (mine is %d) from [%d].", args.Term, rf.currentTerm, args.LeaderId)
		reply.Success = false
		return
	}

	if rf.currentTerm <= args.Term {
		rf.currentTerm = args.Term

		if rf.getRaftState() != FOLLOWER {
			rf.DPrintf("Downgrading to FOLLOWER %d > %d", args.Term, rf.currentTerm)
			rf.setRaftState(FOLLOWER)
		}
	}

	rf.lastBeatFromPeer = time.Now()

	// 2) Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm
	logSize := rf.getLogSize()
	exist, prevLogEntry := rf.getLogEntry(args.PrevLogIndex)

	if !exist || prevLogEntry.Term != args.PrevLogTerm {
		rf.DPrintf("Log doesn't contain at index %d term %d from [%d]", args.PrevLogIndex, args.PrevLogTerm, args.LeaderId)
		reply.Success = false

		// Find first index in current term
		// optimization that backs up nextIndex by more than one entry at a time
		startIndex := min(args.PrevLogIndex, logSize-1)
		_, startEntry := rf.getLogEntry(startIndex)
		failedTerm := startEntry.Term
		for i := startIndex; i >= 0; i-- {
			_, entry := rf.getLogEntry(i)
			if entry.Term != failedTerm {
				reply.FirstTermId = i + 1
				return
			}
		}
		return
	}

	reply.Success = true

	// 3) If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it
	// 4) Append any new entries not already in the log

	for _, entry := range args.Entries {
		if entry.Index < rf.getLogSize() {
			// Overwrite entries
			_, logEntry := rf.getLogEntry(entry.Index)
			if logEntry != entry {

				rf.DPrintf("Received entry, overwriting since index %d from [%d]", entry.Index, args.LeaderId)
				rf.log = rf.getLogSlice(-1, entry.Index)
				rf.log = append(rf.log, entry)
				persist = true
			}
		} else {
			rf.DPrintf("Received entry %d from [%d]", entry.Index, args.LeaderId)
			rf.log = append(rf.log, entry)
			persist = true
		}
	}

	// 5) If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		_, lastEntry := rf.getLogEntry(-1)
		endIndex := min(args.LeaderCommit, lastEntry.Index)

		rf.DPrintf("Setting commit index to %d", endIndex)
		rf.commitIndex = endIndex

		go rf.notifyClient()
		persist = true
	}

	if persist {
		rf.persist()
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs) (bool, bool, int, int) {
	const APPEND_TIMEOUT_MS = 50
	var ok bool
	var success bool
	var term int
	var firstTermId int

	reply := AppendEntriesReply{}
	rpcChannel := make(chan bool)

	go func() {
		ok := rf.peers[server].Call("Raft.AppendEntries", args, &reply)
		rpcChannel <- ok
	}()

	select {
	case ok = <-rpcChannel:
		success = reply.Success
		term = reply.Term
		firstTermId = reply.FirstTermId
	case <-time.After(time.Duration(APPEND_TIMEOUT_MS) * time.Millisecond):
		ok = false
		success = false
		term = -1
		firstTermId = -1
	}
	return ok, success, term, firstTermId
}

// ====== InstallSnapshot ======

type InstallSnapshotArgs struct {
	Term     int
	LeaderId int

	LastIncludedIndex int
	LastIncludedTerm  int

	Data []byte
}

type InstallSnapshotReply struct {
	Success bool
	Term    int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		DPrintf("[%d] Discard install snapshot from %d (%d < %d)", rf.me, args.LeaderId, args.Term, rf.currentTerm)
		reply.Success = false
	} else {
		DPrintf("[%d] Installing snapshot from %d", rf.me, args.LeaderId)
		reply.Success = true

		rf.log = make([]LogEntry, 1)
		rf.log[0].Term = args.LastIncludedTerm
		rf.log[0].Index = args.LastIncludedIndex

		msg := ApplyMsg{}
		msg.CommandValid = false
		msg.SnapshotValid = true
		msg.Snapshot = args.Data
		msg.SnapshotTerm = args.LastIncludedTerm
		msg.SnapshotIndex = args.LastIncludedIndex

		rf.applyCh <- msg

		rf.lastIncludedIndex = args.LastIncludedIndex
		rf.lastIncludedTerm = args.LastIncludedTerm
		rf.lastApplied = args.LastIncludedIndex
		DPrintf("[%d] Setting lastApplied to %d", rf.me, args.LastIncludedIndex)
		rf.persister.SaveStateAndSnapshot(rf.getStateData(), args.Data)
	}
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs) (bool, bool, int) {
	const INSTALL_SNAPSHOT_TIMEOUT_MS = 50
	var ok bool
	var success bool
	var term int

	reply := InstallSnapshotReply{}
	rpcChannel := make(chan bool)

	go func() {
		ok := rf.peers[server].Call("Raft.InstallSnapshot", args, &reply)
		rpcChannel <- ok
	}()

	select {
	case ok = <-rpcChannel:
		success = reply.Success
		term = reply.Term
	case <-time.After(time.Duration(INSTALL_SNAPSHOT_TIMEOUT_MS) * time.Millisecond):
		ok = false
		success = false
		term = -1
	}
	return ok, success, term
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
	index := -1
	term := 0
	isLeader := rf.getRaftState() == LEADER

	// Your code here (2B).
	if isLeader {
		rf.mu.Lock()
		_, lastEntry := rf.getLogEntry(-1)
		index = lastEntry.Index + 1
		term = rf.currentTerm
		rf.log = append(rf.log, LogEntry{rf.currentTerm, index, command})
		rf.nextIndex[rf.me] = index
		rf.persist()

		rf.DPrintf("New LogEntry with index %d", index)
		for peer := range rf.peers {
			if peer != rf.me {
				go rf.notifyPeer(peer, NEW_LOG_ENTRY)
			}
		}
		rf.mu.Unlock()
	}

	return index, term, isLeader
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

// ====== My methods ======

func (state RaftState) toString() string {
	if state == INITIAL {
		return "INITIAL"
	} else if state == FOLLOWER {
		return "FOLLOWER"
	} else if state == CANDIDATE {
		return "CANDIDATE"
	} else if state == LEADER {
		return "LEADER"
	} else {
		return "UNKNOWN"
	}
}

func (rf *Raft) DPrintf(format string, a ...interface{}) {

	if Debug {
		state := rf.getRaftState()

		str := fmt.Sprintf("[%d][%s] %s", rf.me, state.toString(), format)
		DPrintf(str, a...)
	}
}

func (rf *Raft) getStateData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedTerm)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastApplied)
	return w.Bytes()
}

func (rf *Raft) getRaftState() RaftState {
	z := atomic.LoadInt32(&rf.state)
	return RaftState(z)
}

func (rf *Raft) setRaftState(state RaftState) {
	// rf.DPrintf(" -> [%s]", state.toString())
	atomic.StoreInt32(&rf.state, int32(state))
	rf.stateCh <- state
}

func (rf *Raft) getLogEntry(index int) (bool, LogEntry) {
	if index < 0 {
		return true, rf.log[len(rf.log)-1]
	}

	startIndex := rf.log[0].Index
	if index >= startIndex && index-startIndex < len(rf.log) {
		return true, rf.log[index-startIndex]
	} else {
		return false, LogEntry{}
	}
}

func (rf *Raft) getLogSlice(start int, end int) []LogEntry {
	startIndex := rf.log[0].Index

	if start < 0 {
		start = startIndex
	}

	if end > 0 {
		return rf.log[start-startIndex : end-startIndex]
	} else {
		return rf.log[start-startIndex:]
	}
}

func (rf *Raft) getLogSize() int {
	_, entry := rf.getLogEntry(-1)
	return entry.Index + 1
}

func (rf *Raft) notifyPeer(peer int, command PeerNotify) {
	// See peerSender()
	if peer != rf.me {
		rf.peerCh[peer] <- command
	}
}

func (rf *Raft) notifyClient() {
	// See clientSender()
	rf.clientCh <- struct{}{}
}

func (rf *Raft) runFollower() {
	const TICKER_MIN_MS int64 = 450
	const TICKER_MAX_MS int64 = 650

	rf.mu.Lock()
	rf.lastBeatFromPeer = time.Now()
	rf.mu.Unlock()

	for !rf.killed() && rf.getRaftState() == FOLLOWER {

		interval := TICKER_MIN_MS + rand.Int63n(TICKER_MAX_MS-TICKER_MIN_MS)
		time.Sleep(time.Duration(interval) * time.Millisecond)

		rf.mu.Lock()
		elapsed := time.Since(rf.lastBeatFromPeer)
		startElection := elapsed.Milliseconds() >= interval

		if startElection {
			// Become candidate
			rf.DPrintf("Starting the election")
			rf.setRaftState(CANDIDATE)
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) runCandidate() {
	const ELECTION_TIMEOUT_MS int = 400

	rf.mu.Lock()
	initialTerm := rf.currentTerm
	initialVotedFor := rf.votedFor
	initialVotedTerm := rf.votedTerm

	rf.currentTerm += 1
	rf.DPrintf("Starting the election at term %d", rf.currentTerm)

	rf.lastBeatFromPeer = time.Now()
	rf.votedFor = rf.me
	rf.votedTerm = rf.currentTerm

	_, lastLogEntry := rf.getLogEntry(-1)

	lastLogIndex := lastLogEntry.Index
	lastLogTerm := lastLogEntry.Term

	term := rf.currentTerm
	candidateId := rf.me
	required := len(rf.peers) / 2
	all := len(rf.peers) - 1
	rf.persist()
	rf.mu.Unlock()

	var electionLock sync.Mutex
	var granted int
	var finished int

	cond := sync.NewCond(&electionLock)

	started := time.Now()

	go func() {
		time.Sleep(time.Duration(ELECTION_TIMEOUT_MS) * time.Millisecond)
		electionLock.Lock()
		defer electionLock.Unlock()
		cond.Broadcast()
	}()

	for other := range rf.peers {
		if other == rf.me {
			continue
		}

		go func(other int) {
			success, term := rf.sendRequestVote(other, candidateId, term, lastLogIndex, lastLogTerm)

			electionLock.Lock()
			defer electionLock.Unlock()
			finished += 1
			if success {
				granted += 1

				rf.mu.Lock()
				if rf.currentTerm < term {
					rf.currentTerm = term

					rf.DPrintf("Received term greater then ours during election %d > %d", term, rf.currentTerm)
					if rf.getRaftState() != FOLLOWER {
						rf.DPrintf("Downgrading to FOLLOWER %d > %d", term, rf.currentTerm)
						rf.setRaftState(FOLLOWER)
					}
				}
				rf.mu.Unlock()
			}
			cond.Broadcast()
		}(other)
	}

	electionLock.Lock()
	for (granted < required || finished < all) && time.Since(started) < time.Duration(ELECTION_TIMEOUT_MS)*time.Millisecond {
		cond.Wait()
	}
	elected := granted >= required
	_finished := finished
	electionLock.Unlock()

	rf.mu.Lock()
	if rf.getRaftState() == CANDIDATE && elected {
		rf.setRaftState(LEADER)
		rf.DPrintf("Elected at term %d", rf.currentTerm)
	} else {
		rf.DPrintf("Wasn't elected at term %d, finished %d, granted %d, required %d", rf.currentTerm, _finished, granted, required)
		if _finished < required {
			rf.DPrintf("Cluster is split, abort the election %d", rf.currentTerm)
			rf.currentTerm = initialTerm
			rf.votedFor = initialVotedFor
			rf.votedTerm = initialVotedTerm
		}
		rf.setRaftState(FOLLOWER)
	}
	rf.persist()
	rf.mu.Unlock()
}

func (rf *Raft) peerSender(peer int) {
	for !rf.killed() && rf.getRaftState() == LEADER {
		command := <-rf.peerCh[peer]
		if command == EXIT {
			rf.DPrintf("Received exit for peer [%d]", peer)
			return
		}

		rf.mu.Lock()
		indx := rf.nextIndex[peer] + 1
		exist, previousLogEntry := rf.getLogEntry(indx - 1)

		// Send snapshot
		if !exist {
			rf.DPrintf("Sending snapshot to %d, indx %d, lastSnapshotIndx %d", peer, indx-1, rf.lastIncludedIndex)
			args := InstallSnapshotArgs{}

			args.Term = rf.currentTerm
			args.LeaderId = rf.me
			args.LastIncludedIndex = rf.lastIncludedIndex
			args.LastIncludedTerm = rf.lastIncludedTerm

			args.Data = rf.persister.ReadSnapshot()
			rf.mu.Unlock()

			ok, success, term := rf.sendInstallSnapshot(peer, &args)

			rf.mu.Lock()
			if ok && success {
				rf.DPrintf("Snapshot is sent succesfully, setting index %d for [%d]", rf.lastIncludedIndex, peer)
				rf.nextIndex[peer] = rf.lastIncludedIndex
			} else if ok && !success {
				rf.DPrintf("Snapshot installation rejected")

				if rf.currentTerm < term {
					rf.DPrintf("Received term greater then ours during installSnapshot %d > %d", term, rf.currentTerm)
					rf.currentTerm = term
					if rf.getRaftState() != FOLLOWER {
						rf.DPrintf("Downgrading to FOLLOWER %d > %d", term, rf.currentTerm)
						rf.setRaftState(FOLLOWER)
					}
				}

			} else {
				rf.DPrintf("Snapshot installation failed")
			}
			rf.mu.Unlock()
			continue
		}

		// Send all messages from log since nextIndex[peer]
		var entries []LogEntry
		entries = append(entries, rf.getLogSlice(indx, -1)...)

		args := AppendEntriesArgs{}
		args.LeaderId = rf.me
		args.Term = rf.currentTerm

		args.PrevLogIndex = previousLogEntry.Index
		args.PrevLogTerm = previousLogEntry.Term
		args.Entries = entries
		args.LeaderCommit = rf.commitIndex
		rf.mu.Unlock()

		ok, success, term, firstTermId := rf.sendAppendEntries(peer, &args)

		rf.mu.Lock()
		if ok {
			// Resseting last beat
			rf.lastBeatFromPeer = time.Now()

			if !success && rf.currentTerm >= term {
				newIndx := max(firstTermId-1, 0)
				rf.DPrintf("[%d] Decreasing index %d -> %d for [%d]", rf.me, rf.nextIndex[peer], newIndx, peer)
				rf.nextIndex[peer] = newIndx
				rf.persist()
			}

			if !success && rf.currentTerm < term {
				rf.DPrintf("Received term greater then ours during appendEntries %d > %d", term, rf.currentTerm)
				rf.currentTerm = term
				if rf.getRaftState() != FOLLOWER {
					rf.DPrintf("Downgrading to FOLLOWER %d > %d", term, rf.currentTerm)
					rf.setRaftState(FOLLOWER)
				}
			}

			// Try to move nextIndex and update commitIndex
			if success && len(args.Entries) > 0 {
				rf.nextIndex[peer] = max(rf.nextIndex[peer], args.Entries[len(args.Entries)-1].Index)
				rf.DPrintf("Setting peer [%d] index to %d", peer, rf.nextIndex[peer])

				replicatedCommit := getMajority(rf.nextIndex)
				found, replicatedEntry := rf.getLogEntry(replicatedCommit)

				if replicatedCommit > rf.commitIndex && found && replicatedEntry.Term == rf.currentTerm {
					rf.DPrintf("Commiting entry with index %d", replicatedCommit)
					rf.commitIndex = replicatedCommit
					rf.persist()
					go rf.notifyClient()
				}
			}
		} else {
			rf.DPrintf("Failed to send AppendEntires to [%d]", peer)
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) runLeader() {
	const LEADER_PING_INTERVAL_MS int = 100

	rf.DPrintf("Starting peer senders...")

	rf.mu.Lock()
	rf.lastBeatFromPeer = time.Now()

	// Clean nextIndex
	for i := range rf.nextIndex {
		_, entry := rf.getLogEntry(-1)
		rf.nextIndex[i] = entry.Index
	}

	for peer := range rf.peers {
		if peer != rf.me {
			go rf.peerSender(peer)
		}
	}
	rf.mu.Unlock()

	rf.DPrintf("Starting ping...")
	for !rf.killed() && rf.getRaftState() == LEADER {

		// Check that we are alone
		rf.mu.Lock()
		elapsed := time.Since(rf.lastBeatFromPeer)
		rf.mu.Unlock()

		if elapsed > 500*time.Millisecond {
			rf.DPrintf("Haven't heard from another peer downgrading to follower")
			rf.setRaftState(FOLLOWER)
			break
		}

		// Ping messages
		for peer := range rf.peers {
			go rf.notifyPeer(peer, NEW_LOG_ENTRY)
		}
		time.Sleep(time.Duration(LEADER_PING_INTERVAL_MS) * time.Millisecond)
	}

	rf.DPrintf("Shutdown runLeader()")
	// TODO: Use WaitGroup to properly shutdown all peerSenders

	for peer := range rf.peers {
		go rf.notifyPeer(peer, EXIT)
	}
}

func (rf *Raft) clientSender() {
	// This routine sends new messages to raft client
	for !rf.killed() {
		<-rf.clientCh

		rf.mu.Lock()
		lastApplied := rf.lastApplied
		commitIndex := rf.commitIndex

		exist, _ := rf.getLogEntry(commitIndex)

		if !exist {
			rf.DPrintf("Waiting from leader to receive elements (%d is not in log)", commitIndex)
			rf.mu.Unlock()
			continue
		}

		var entries []LogEntry
		if lastApplied < commitIndex {
			rf.DPrintf("Sending entries %d->%d to a client", lastApplied+1, commitIndex)
			entries = append(entries, rf.getLogSlice(lastApplied+1, commitIndex+1)...)
		}
		rf.mu.Unlock()

		for _, entry := range entries {
			msg := ApplyMsg{}
			msg.Command = entry.Command
			msg.CommandIndex = entry.Index
			msg.CommandValid = true

			rf.applyCh <- msg
		}

		rf.mu.Lock()
		// Sometimes lastApplied can be set during the snapshot installation
		// in this case we mustn't overwrite the greater value
		if commitIndex > rf.lastApplied {
			rf.DPrintf("Setting last applied to %d", commitIndex)
			rf.lastApplied = commitIndex
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) main() {
	for !rf.killed() {
		state := <-rf.stateCh

		switch state {
		//
		case LEADER:
			go rf.runLeader()
		//
		case CANDIDATE:
			go rf.runCandidate()
		//
		case FOLLOWER:
			go rf.runFollower()
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
	rf.state = int32(INITIAL)
	rf.stateCh = make(chan RaftState)
	rf.lastBeatFromPeer = time.Now()
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.votedTerm = -1
	rf.log = make([]LogEntry, 1)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.peerCh = make([]chan PeerNotify, len(rf.peers))
	for peer := range peers {
		rf.peerCh[peer] = make(chan PeerNotify)
	}

	rf.clientCh = make(chan struct{})
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.DPrintf("----- Start -----")
	go rf.main()
	go rf.clientSender()

	rf.setRaftState(FOLLOWER)
	return rf
}
