package kvraft

import (
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
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	termId   int
	observer map[int64]chan struct{}
	storage  map[string]string

	opsMu sync.Mutex
	ops   map[int64]int64
}

func (kv *KVServer) opExist(opId int64) bool {
	kv.opsMu.Lock()
	defer kv.opsMu.Unlock()

	var _, exists = kv.ops[opId]
	return exists
}

func (kv *KVServer) opStore(opId int64) {
	kv.opsMu.Lock()
	defer kv.opsMu.Unlock()
	kv.ops[opId] = time.Now().Unix()
}

func (kv *KVServer) handleCommand(key string, value string, op string, opId int64) Err {
	index, termId, isLeader := kv.rf.Start(Op{key, value, op, opId})

	if !isLeader {
		DPrintf("[kv][server][%d] WRONG LEADER", kv.me)
		return ErrWrongLeader
	} else {
		DPrintf("[kv][server][%d] Leader received %s index %d", kv.me, op, index)
		waiter := make(chan struct{})

		kv.mu.Lock()
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
	err := kv.handleCommand(args.Key, "", "Get", args.OpId)

	reply.Err = err

	if err == OK {
		kv.mu.Lock()
		reply.Value = kv.storage[args.Key]
		kv.mu.Unlock()
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	if !kv.opExist(args.OpId) {
		err := kv.handleCommand(args.Key, args.Value, args.Op, args.OpId)
		reply.Err = err
		if err == OK {
			kv.opStore(args.OpId)
		}
	} else {
		reply.Err = OK
	}
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

func (kv *KVServer) applier() {

	//start := time.Now()

	for !kv.killed() {

		for msg := range kv.applyCh {
			//log.Printf("[%d] applyCh wait: %d", kv.me, time.Since(start).Milliseconds())
			//start = time.Now()

			if msg.CommandValid {
				op := msg.Command.(Op)

				kv.mu.Lock()

				if !kv.opExist(op.OpId) {
					DPrintf("[kv][server][%d] Applying storage[%s] = %s (%s, %d)", kv.me, op.Key, op.Value, op.Type, msg.CommandIndex)
					if op.Type == "Put" {
						kv.storage[op.Key] = op.Value
					} else if op.Type == "Append" { // PutAppend
						kv.storage[op.Key] = kv.storage[op.Key] + op.Value
					}
					kv.opStore(op.OpId)
				} else {
					DPrintf("[kv][server][%d] Duplicate op %d", kv.me, op.OpId)
				}
				waiter, ok := kv.observer[op.OpId]
				if ok {
					DPrintf("[kv][server][%d] Locked on notification %d", kv.me, op.OpId)
					waiter <- struct{}{}
					delete(kv.observer, op.OpId)
					DPrintf("[kv][server][%d] Notification sent %d", kv.me, op.OpId)
				}
				kv.mu.Unlock()
			}
			//log.Printf("[%d] cycle wait: %d", kv.me, time.Since(start).Milliseconds())
			//start = time.Now()
		}
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

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.termId = -1
	kv.observer = make(map[int64]chan struct{})
	kv.storage = make(map[string]string)
	kv.ops = make(map[int64]int64)

	// You may need initialization code here.

	go kv.applier()

	return kv
}
