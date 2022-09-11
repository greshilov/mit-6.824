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

	"math/rand"
	"sync"
	"sync/atomic"

	"time"

	"6.824/labgob"
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

	lastIncludedIndex int
	lastIncludedTerm  int
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

func (rf *Raft) getStateData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	return data
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

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		DPrintf("[%d] Error during read state", rf.me)
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log

		for i := range rf.peers {
			_, entry := rf.getLogEntry(-1)
			rf.nextIndex[i] = entry.Index
		}

		_, lastEntry := rf.getLogEntry(-1)

		rf.lastIncludedIndex = lastEntry.Index
		rf.lastIncludedTerm = lastEntry.Term
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

	newSlice := rf.getLogSlice(index, -1)

	rf.persister.SaveStateAndSnapshot(rf.getStateData(), snapshot)

	DPrintf("[%d] Prunning log to index %d, log size %d -> %d", rf.me, index, len(rf.log), len(newSlice))

	_, entry := rf.getLogEntry(index)
	rf.log = newSlice

	rf.lastIncludedIndex = entry.Index
	rf.lastIncludedTerm = entry.Term
	rf.persist()
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
		DPrintf("[%d] Received old term %d (mine is %d) from [%d].", rf.me, args.Term, rf.currentTerm, args.LeaderId)
		reply.Success = false
		return
	}

	// 2) Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm

	logSize := rf.getLogSize()
	exist, prevLogEntry := rf.getLogEntry(args.PrevLogIndex)

	if !exist || prevLogEntry.Term != args.PrevLogTerm {
		DPrintf("[%d] Log doesn't contain at index %d term %d from [%d]", rf.me, args.PrevLogIndex, args.PrevLogTerm, args.LeaderId)
		reply.Success = false
		rf.lastBeatFromLeader = time.Now()

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

	// If we made it till here, we must be follower
	if rf.currentTerm < args.Term || rf.state != FOLLOWER {
		DPrintf("[%d] Received term %d from [%d], mine is %d. Converting to a follower...", rf.me, args.Term, args.LeaderId, rf.currentTerm)
		rf.becomeFollower(args.Term)
		persist = true
	}

	rf.lastBeatFromLeader = time.Now()
	reply.Success = true

	// 3) If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it
	// 4) Append any new entries not already in the log

	for _, entry := range args.Entries {
		if entry.Index < rf.getLogSize() {
			// Overwrite entries
			_, logEntry := rf.getLogEntry(entry.Index)
			if logEntry != entry {

				DPrintf("[%d] Received entry, overwriting since index %d from [%d]", rf.me, entry.Index, args.LeaderId)
				rf.log = rf.getLogSlice(-1, entry.Index)
				rf.log = append(rf.log, entry)
				persist = true
			}
		} else {
			DPrintf("[%d] Received entry %d from [%d]", rf.me, entry.Index, args.LeaderId)
			rf.log = append(rf.log, entry)
			persist = true
		}
	}

	// 5) If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		_, lastEntry := rf.getLogEntry(-1)
		endIndex := min(args.LeaderCommit, lastEntry.Index)

		DPrintf("[%d] Setting commit index to %d", rf.me, endIndex)
		rf.commitIndex = endIndex
		persist = true
	}

	if persist {
		rf.persist()
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs) (bool, AppendEntriesReply) {
	reply := AppendEntriesReply{}

	ok := rf.peers[server].Call("Raft.AppendEntries", args, &reply)
	return ok, reply
}

// InstallSnapshot RPC

type InstallSnapshotArgs struct {
	Term     int
	LeaderId int

	LastIncludedIndex int
	LastIncludedTerm  int

	Data []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	_, lastEntry := rf.getLogEntry(-1)

	if args.Term < rf.currentTerm {
		DPrintf("[%d] Discard install snapshot from %d (%d < %d)", rf.me, args.LeaderId, args.Term, rf.currentTerm)
	} else if args.LastIncludedIndex < lastEntry.Index {
		DPrintf("[%d] Delete old entries from log %d < %d", rf.me, args.LastIncludedIndex, lastEntry.Index)
		rf.log = rf.getLogSlice(args.LastIncludedIndex, -1)
	} else {
		DPrintf("[%d] Installing snapshot from %d", rf.me, args.LeaderId)
		rf.log = make([]LogEntry, 0)
		rf.log = append(rf.log, LogEntry{args.LastIncludedTerm, args.LastIncludedIndex, Empty{}})

		msg := ApplyMsg{}
		msg.CommandValid = false
		msg.SnapshotValid = true
		msg.Snapshot = args.Data
		msg.SnapshotTerm = args.LastIncludedTerm
		msg.SnapshotIndex = args.LastIncludedIndex

		rf.ch <- msg
		rf.lastApplied = args.LastIncludedIndex
		DPrintf("[%d] Setting lastApplied to %d", rf.me, args.LastIncludedIndex)
	}
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs) bool {

	reply := InstallSnapshotReply{}
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, &reply)
	return ok
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

	persist := false

	if rf.currentTerm < args.Term {
		rf.becomeFollower(args.Term)
		persist = true
	}

	reply.Term = rf.currentTerm
	_, lastEntry := rf.getLogEntry(-1)

	leaderIsUpToDate := args.LastLogTerm > lastEntry.Term || (args.LastLogTerm == lastEntry.Term && args.LastLogIndex >= lastEntry.Index)

	if args.Term < rf.currentTerm {
		DPrintf("[%d] Denying request from %d, because our term is %d and his is %d", rf.me, args.CandidateId, rf.currentTerm, args.Term)
		reply.VoteGranted = false
	} else if !leaderIsUpToDate {
		DPrintf("[%d] Denying request from %d, because his log (%d, %d) is worse than ours (%d, %d)", rf.me, args.CandidateId, args.LastLogTerm, args.LastLogIndex, lastEntry.Term, lastEntry.Index)
		reply.VoteGranted = false
	} else if rf.votedFor == -1 {
		DPrintf("[%d] Vote granted for [%d]", rf.me, args.CandidateId)
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		persist = true
	} else {
		DPrintf("[%d] Already voted during current term", rf.me)
		reply.VoteGranted = false
	}

	if persist {
		rf.persist()
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

	index := -1
	term := 0
	isLeader := rf.state == LEADER
	if isLeader {
		_, lastEntry := rf.getLogEntry(-1)
		index = lastEntry.Index + 1
		term = rf.currentTerm
		rf.log = append(rf.log, LogEntry{rf.currentTerm, index, command})
		rf.nextIndex[rf.me] = index
		rf.persist()
	}
	rf.mu.Unlock()

	// Your code here (2B).
	if isLeader {
		DPrintf("[%d] Start broadcast entry with index %d", rf.me, index)

		for peer, _ := range rf.peers {
			go rf.broadcastToPeer(peer)
		}
	}

	return index, term, isLeader
}

func (rf *Raft) becomeFollower(term int) {
	DPrintf("[%d] Downgrade to follower", rf.me)
	rf.state = FOLLOWER
	rf.currentTerm = term
	rf.votedFor = -1
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

			exist, lastEntry := rf.getLogEntry(indx - 1)

			// Send snapshot
			if !exist {
				DPrintf("[%d] Sending snapshot to %d, indx %d, lastSnapshotIndx %d\n%d", rf.me, peer, indx-1, rf.lastIncludedIndex, rf.log[0].Index)
				args := InstallSnapshotArgs{}

				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.LastIncludedIndex = rf.lastIncludedIndex
				args.LastIncludedTerm = rf.lastIncludedTerm

				args.Data = rf.persister.ReadSnapshot()
				rf.mu.Unlock()

				ok := rf.sendInstallSnapshot(peer, &args)

				if ok {
					rf.mu.Lock()
					rf.nextIndex[peer] = rf.lastIncludedIndex
					rf.mu.Unlock()
				}
				return ok
			}

			var entries []LogEntry
			entries = append(entries, rf.getLogSlice(indx, -1)...)

			args := AppendEntriesArgs{}
			args.LeaderId = rf.me
			args.Term = rf.currentTerm

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
				rf.persist()
				return true
			} else if ok && !response.Success {
				if response.Term > rf.currentTerm {
					rf.becomeFollower(response.Term)
				} else {

					newIndx := max(response.FirstTermId-1, 0)
					DPrintf("[%d] Decreasing index %d -> %d for [%d]", rf.me, rf.nextIndex[peer], newIndx, peer)
					rf.nextIndex[peer] = newIndx
				}
			} else {
				DPrintf("[%d] Missing RPC response [%d]", rf.me, peer)
				return false
			}
			rf.persist()
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
		found, replicatedEntry := rf.getLogEntry(replicatedCommit)

		if replicatedCommit > rf.commitIndex && found && replicatedEntry.Term == rf.currentTerm {
			DPrintf("[%d] Commiting entry with index %d", rf.me, rf.commitIndex)
			rf.commitIndex = replicatedCommit
			rf.persist()
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

	_, lastLogEntry := rf.getLogEntry(-1)

	lastLogIndex := lastLogEntry.Index
	lastLogTerm := lastLogEntry.Term

	term := rf.currentTerm
	candidateId := rf.me
	required := len(rf.peers) / 2
	rf.persist()
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
				_, entry := rf.getLogEntry(-1)
				rf.nextIndex[i] = entry.Index
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
	rf.persist()
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

func (rf *Raft) clientSender() {
	const SENDER_TICK_MS int64 = 100

	for !rf.killed() {

		rf.mu.Lock()
		lastApplied := max(rf.lastApplied, rf.log[0].Index)
		commitIndex := rf.commitIndex

		var entries []LogEntry

		if lastApplied < commitIndex {
			DPrintf("[%d] Sending entries %d->%d to a client", rf.me, lastApplied+1, commitIndex)
			entries = append(entries, rf.getLogSlice(lastApplied+1, commitIndex+1)...)
		}
		rf.mu.Unlock()

		for _, entry := range entries {
			msg := ApplyMsg{}
			msg.Command = entry.Command
			msg.CommandIndex = entry.Index
			msg.CommandValid = true

			rf.ch <- msg
		}

		rf.mu.Lock()
		// Sometimes lastApplied can be set during the snapshot installation
		// in this case we mustn't overwrite the greater value
		if commitIndex > rf.lastApplied {
			rf.lastApplied = commitIndex
		}
		rf.mu.Unlock()

		time.Sleep(time.Millisecond * time.Duration(SENDER_TICK_MS))
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
	DPrintf("[%d] --- START --- ", me)
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
	go rf.clientSender()

	return rf
}
