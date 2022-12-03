package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.SetFlags(log.LstdFlags | log.Lmicroseconds)
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key   string
	Value string
	Type  string
	OpId  int64
	CId   int64 // ClerkId
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	persister *raft.Persister
	termId    int
	observer  map[int64]chan struct{}
	storage   map[string]string

	lastAppliedOp    map[int64]int64 // [client id] -> op id
	lastAppliedIndex int
}

func (kv *KVServer) handleCommand(key string, value string, op string, opId int64, cId int64) Err {
	index, termId, isLeader := kv.rf.Start(Op{key, value, op, opId, cId})

	if !isLeader {
		DPrintf("[kv][server][%d] WRONG LEADER", kv.me)
		return ErrWrongLeader
	} else {
		DPrintf("[kv][server][%d] Leader received %s index %d", kv.me, op, index)

		kv.mu.Lock()

		prevOpId, ok := kv.lastAppliedOp[cId]

		if ok && prevOpId == opId {
			DPrintf("[kv][server][%d] Duplicate operation id %d", kv.me, opId)
			kv.mu.Unlock()
			return OK
		}

		waiter := make(chan struct{})
		kv.observer[opId] = waiter
		kv.mu.Unlock()

		DPrintf("[kv][server][%d] index %d passed lock", kv.me, index)

		partitionHappend := make(chan struct{})
		done := make(chan struct{})

		exit := func(opId int64, done chan struct{}) {

			DPrintf("[kv][server][%d] Stops waiting %s %d", kv.me, op, index)

			kv.mu.Lock()
			delete(kv.observer, opId)
			kv.mu.Unlock()

			go func() {
				done <- struct{}{}
			}()
		}
		defer exit(opId, done)

		go func(termId int, ch chan struct{}, done chan struct{}) {
			for {

				select {
				case <-done:
					return
				default:
					newTermId, _ := kv.rf.GetState()
					if termId != newTermId {
						ch <- struct{}{}
					}
					time.Sleep(time.Duration(50) * time.Millisecond)
				}
			}
		}(termId, partitionHappend, done) // Partition waiter

		DPrintf("[kv][server][%d] Starts waiting %s %d", kv.me, op, index)
		select {
		case <-waiter:
			DPrintf("[kv][server][%d] Committed %s %d", kv.me, op, index)
			return OK
		case <-partitionHappend:
			DPrintf("[kv][server][%d] Partition happend %s %d", kv.me, op, index)
			return ErrPartitioned
		}
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	err := kv.handleCommand(args.Key, "", GET, args.OpId, args.CId)
	reply.Err = err

	if err == OK {
		kv.mu.Lock()
		reply.Value = kv.storage[args.Key]
		kv.mu.Unlock()
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	err := kv.handleCommand(args.Key, args.Value, args.Op, args.OpId, args.CId)
	reply.Err = err
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) loadFromSnapshot(snapshot []byte) {
	DPrintf("[kv][server][%d] Loading snapshot", kv.me)

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var storage map[string]string
	var lastAppliedIndex int
	var lastAppliedOp map[int64]int64

	if d.Decode(&storage) != nil || d.Decode(&lastAppliedIndex) != nil || d.Decode(&lastAppliedOp) != nil {
		DPrintf("[%d] Error during read state", kv.me)
	} else {
		kv.mu.Lock()

		kv.storage = storage
		kv.lastAppliedIndex = lastAppliedIndex
		kv.lastAppliedOp = lastAppliedOp
		kv.termId = -1

		kv.mu.Unlock()
	}
}

func (kv *KVServer) applier() {

	// start := time.Now()

	for !kv.killed() {

		for msg := range kv.applyCh {
			// log.Printf("[%d] applyCh wait: %d", kv.me, time.Since(start).Milliseconds())
			// start = time.Now()

			DPrintf("[kv][server][%d] Start processing command %d", kv.me, msg.CommandIndex)

			if msg.CommandValid {
				// Single command

				op := msg.Command.(Op)

				kv.mu.Lock()

				var lastOpId, exists = kv.lastAppliedOp[op.CId]

				if !exists || lastOpId != op.OpId {
					DPrintf("[kv][server][%d] Applying storage[%s] = %s (type: %s, cmdindex: %d, opid: %d)", kv.me, op.Key, op.Value, op.Type, msg.CommandIndex, op.OpId)
					if op.Type == PUT {
						kv.storage[op.Key] = op.Value
					} else if op.Type == APPEND {
						kv.storage[op.Key] = kv.storage[op.Key] + op.Value
					}
					kv.lastAppliedOp[op.CId] = op.OpId
				} else {
					DPrintf("[kv][server][%d] Duplicate op %d", kv.me, op.OpId)
				}

				waiter, ok := kv.observer[op.OpId]
				if ok {
					DPrintf("[kv][server][%d] Locked on notification %d", kv.me, op.OpId)
					waiter <- struct{}{}
					delete(kv.observer, op.OpId)
					DPrintf("[kv][server][%d] Notification sent %d", kv.me, op.OpId)
				} else {
					DPrintf("[kv][server][%d] No notification found for %d", kv.me, op.OpId)
				}

				kv.lastAppliedIndex = msg.CommandIndex
				kv.mu.Unlock()
			} else {
				// Snapshot installation
				DPrintf("[kv][server][%d] Installing snapshot snapshot index %d", kv.me, msg.SnapshotIndex)
				kv.loadFromSnapshot(msg.Snapshot)
			}

			DPrintf("[kv][server][%d] Finish processing command %d", kv.me, msg.CommandIndex)
			//log.Printf("[%d] cycle wait: %d", kv.me, time.Since(start).Milliseconds())
			//start = time.Now()
		}
	}
}

func (kv *KVServer) persisterMonitor() {

	PERSISTER_PERIOD := 100

	for !kv.killed() {

		kv.mu.Lock()
		// now := time.Now().Unix()

		if kv.persister.RaftStateSize() > kv.maxraftstate && kv.maxraftstate > 0 {

			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			e.Encode(kv.storage)
			e.Encode(kv.lastAppliedIndex)
			e.Encode(kv.lastAppliedOp)

			DPrintf("[kv][server][%d] Raft state reached max, persisting %d bytes, index %d", kv.me, w.Len(), kv.lastAppliedIndex)

			kv.rf.Snapshot(kv.lastAppliedIndex, w.Bytes())
		}
		kv.mu.Unlock()
		time.Sleep(time.Duration(PERSISTER_PERIOD) * time.Millisecond)
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.persister = persister
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.termId = -1
	kv.observer = make(map[int64]chan struct{})
	kv.storage = make(map[string]string)
	kv.lastAppliedOp = make(map[int64]int64)

	// You may need initialization code here.
	kv.loadFromSnapshot(persister.ReadSnapshot())

	go kv.applier()
	go kv.persisterMonitor()

	return kv
}
