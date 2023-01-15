package shardkv

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

const Debug = true

type KVCommand string
type ServerState string

const (
	Get          KVCommand = "GET"
	Put          KVCommand = "PUT"
	Append       KVCommand = "APPEND"
	ConfigChange KVCommand = "CONFIG_CHANGE"
)

const (
	Operational    ServerState = "OPERATIONAL"
	ShardMigration ServerState = "SHARD_MIGRATION"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	Cmd     KVCommand
	CmdArgs []byte
	OpId    int64
	CId     int64 // ClerkId
	ShardId int
}

func (op *Op) ToString() string {
	return fmt.Sprintf("{typ: %s, opId: %d, cId: %d, shardId: %d}", op.Cmd, op.OpId, op.CId, op.ShardId)
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dead             int32
	storage          map[string]string
	termId           int
	observer         map[int64]chan Err
	lastAppliedOp    map[int64]int64 // [client id] -> op id
	lastAppliedIndex int
	trimCh           chan struct{}
	persister        *raft.Persister
	mck              *shardctrler.Clerk
	config           shardctrler.Config
	state            ServerState
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(args)

	op := Op{
		Get,
		w.Bytes(),
		args.OpId,
		args.CId,
		key2shard(args.Key),
	}

	err := kv.handleCommand(op)
	reply.Err = err

	if err == OK {
		kv.mu.Lock()
		reply.Value = kv.storage[args.Key]
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(args)

	var cmd KVCommand

	if args.Op == "Put" {
		cmd = Put
	} else {
		cmd = Append
	}

	op := Op{
		cmd,
		w.Bytes(),
		args.OpId,
		args.CId,
		key2shard(args.Key),
	}

	err := kv.handleCommand(op)
	reply.Err = err
}

func (kv *ShardKV) requestMigrateShard(gid int, shard int) map[string]string {
	kv.mu.Lock()
	currentConfig := kv.config.Num
	servers, ok := kv.config.Groups[gid]
	kv.mu.Unlock()

	kv.DPrintf("Requesting shard %d from %d (servers: %v)", shard, gid, servers)

	for !kv.killed() {
		if ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {

				select {
				case <-time.After(500 * time.Millisecond):
					kv.DPrintf("RPC Timeout")
					continue
				default:
					srv := kv.make_end(servers[si])

					args := MigrateShardArgs{currentConfig, shard}

					var reply MigrateShardReply
					ok := srv.Call("ShardKV.MigrateShard", &args, &reply)
					if ok && reply.Err == OK {
						return reply.Data
					}

					kv.DPrintf("Unsuccessful shard transmission %d (%b, %s)", gid, ok, reply.Err)
					// ... not ok, or ErrWrongLeader
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}

	return nil
}

func (kv *ShardKV) MigrateShard(args *MigrateShardArgs, reply *MigrateShardReply) {
	_, isLeader := kv.rf.GetState()

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	// requester config must be the same or -1
	if (kv.config.Num == args.ConfigNum && kv.state == ShardMigration) || kv.config.Num > args.ConfigNum {
		response := make(map[string]string)

		for k, v := range kv.storage {
			shardId := key2shard(k)

			if shardId == args.Shard {
				response[k] = v
			}
		}
		reply.Err = OK
		reply.Data = response
	} else {
		kv.DPrintf("Wrong config number state: %s, my config: %d, requester config: %d", kv.state, kv.config.Num, args.ConfigNum)
		reply.Err = ErrWrongConfig
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&kv.dead, 1)
}

// === My methods ===

func (kv *ShardKV) DPrintf(format string, a ...interface{}) {
	if Debug {

		_, isLeader := kv.rf.GetState()
		var leader string

		if isLeader {
			leader = "[LEADER]"
		} else {
			leader = ""
		}
		str := fmt.Sprintf("[shardkv][%d][%d][c: %d]%s %s", kv.gid, kv.me, kv.config.Num, leader, format)

		log.SetFlags(log.LstdFlags | log.Lmicroseconds)
		log.Printf(str, a...)
	}
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) isMyShard(op Op) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.config.Num != 0 && kv.config.Shards[op.ShardId] == kv.gid
}

func (kv *ShardKV) applier() {
	for !kv.killed() {
		for msg := range kv.applyCh {
			kv.DPrintf("Start processing command %d", msg.CommandIndex)

			if msg.CommandValid {
				op := msg.Command.(Op)

				res := kv.dispatchMsg(msg, op)

				kv.mu.Lock()

				waiter, ok := kv.observer[op.OpId]
				if ok {
					kv.DPrintf("Sending notification %d", op.OpId)
					go func(waiter chan Err, res Err) {
						waiter <- res
					}(waiter, res)

					delete(kv.observer, op.OpId)
				} else {
					kv.DPrintf("No notification found for %d", op.OpId)
				}

				go func() {
					kv.trimCh <- struct{}{}
				}()

				kv.mu.Unlock()
			} else {
				kv.DPrintf("Installing snapshot (snapshot index %d)", msg.SnapshotIndex)
				kv.loadFromSnapshot(msg.Snapshot)
			}
			kv.DPrintf("Finish processing command %d", msg.CommandIndex)
		}
	}
}

func (kv *ShardKV) dispatchMsg(msg raft.ApplyMsg, op Op) Err {
	if op.Cmd != ConfigChange && !kv.isMyShard(op) {
		return ErrWrongGroup
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if msg.CommandIndex < kv.lastAppliedIndex {
		return ErrOldMsg
	}

	var lastOpId, exists = kv.lastAppliedOp[op.CId]
	kv.DPrintf("Exist: %b, lastOpId: %d, op: %s", exists, lastOpId, op.ToString())
	if !exists || lastOpId != op.OpId {

		switch op.Cmd {
		case Get:
			r := bytes.NewBuffer(op.CmdArgs)
			d := labgob.NewDecoder(r)

			var getArgs GetArgs

			if d.Decode(&getArgs) != nil {
				kv.DPrintf("Invalid GetArgs bytes, skipping")
				return ErrInvalidMsgBody
			} else {
				kv.DPrintf("Processing Get command: %s", op.ToString())
			}
		case Put:
			r := bytes.NewBuffer(op.CmdArgs)
			d := labgob.NewDecoder(r)

			var putArgs PutAppendArgs

			if d.Decode(&putArgs) != nil {
				kv.DPrintf("Invalid PutArgs bytes, skipping %s", op.ToString())
				return ErrInvalidMsgBody
			} else {
				kv.DPrintf("PUT %s -> %s", putArgs.Key, putArgs.Value)
				kv.storage[putArgs.Key] = putArgs.Value
			}
		case Append:
			r := bytes.NewBuffer(op.CmdArgs)
			d := labgob.NewDecoder(r)

			var appendArgs PutAppendArgs

			if d.Decode(&appendArgs) != nil {
				kv.DPrintf("Invalid AppendArgs bytes, skipping %s", op.ToString())
				return ErrInvalidMsgBody
			} else {
				kv.DPrintf("APPEND %s -> %s", appendArgs.Key, appendArgs.Value)
				kv.storage[appendArgs.Key] = kv.storage[appendArgs.Key] + appendArgs.Value
			}
		case ConfigChange:
			r := bytes.NewBuffer(op.CmdArgs)
			d := labgob.NewDecoder(r)

			var config shardctrler.Config
			if d.Decode(&config) != nil {
				kv.DPrintf("Invalid ConfigChange bytes, skipping %s", op.ToString())
				return ErrInvalidMsgBody
			} else {
				kv.state = ShardMigration
				kv.DPrintf("Config change %d -> %d", kv.config.Num, config.Num)
				for shard, gid := range config.Shards {
					// Detect we have to request our shard from another replica group
					if gid == kv.gid && kv.config.Num > 0 && kv.config.Shards[shard] != gid {
						kv.mu.Unlock()
						data := kv.requestMigrateShard(kv.config.Shards[shard], shard)
						kv.mu.Lock()

						kv.DPrintf("Adding %d keys from %d", len(data), kv.config.Shards[shard])
						for k, v := range data {
							kv.storage[k] = v
						}
					}
				}

				kv.DPrintf("Comitting config %d", config.Num)
				kv.config = config
				kv.state = Operational
			}
		default:
			kv.DPrintf("Unknown command type: %s", op.Cmd)
			return ErrUnknownCmd
		}

		kv.DPrintf("Setting lastApplied [%d] -> %d", op.CId, op.OpId)
		kv.lastAppliedOp[op.CId] = op.OpId
		kv.lastAppliedIndex = msg.CommandIndex
	} else {
		kv.DPrintf("Duplicate op %d", op.OpId)
	}

	return OK
}

func (kv *ShardKV) configWatcher() {
	for !kv.killed() {

		_, isLeader := kv.rf.GetState()

		kv.mu.Lock()
		state := kv.state
		kv.mu.Unlock()

		if isLeader && state == Operational {

			kv.mu.Lock()
			nextNum := kv.config.Num + 1
			kv.mu.Unlock()

			config := kv.mck.Query(nextNum)

			if config.Num == nextNum {

				kv.DPrintf("New config observed: %s", config.ToString())

				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				e.Encode(config)

				op := Op{
					ConfigChange,
					w.Bytes(),
					int64(config.Num), // Probably not very wise OpId choice
					-1,
					-1,
				}

				kv.state = ShardMigration
				res := kv.handleCommand(op)

				if res != OK {
					kv.DPrintf("Migration OP result is %s, rolling back", res)
					kv.state = Operational
				}
			}
		}
		time.Sleep(time.Duration(100) * time.Millisecond)
	}
}

func (kv *ShardKV) persisterMonitor() {
	for !kv.killed() && kv.maxraftstate > 0 {
		<-kv.trimCh

		kv.mu.Lock()

		if kv.persister.RaftStateSize() > kv.maxraftstate && kv.state == Operational {

			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			e.Encode(kv.storage)
			e.Encode(kv.lastAppliedIndex)
			e.Encode(kv.lastAppliedOp)
			e.Encode(kv.config)

			kv.DPrintf("Raft state reached max, persisting %d bytes, index %d", w.Len(), kv.lastAppliedIndex)

			kv.rf.Snapshot(kv.lastAppliedIndex, w.Bytes())
		}
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) loadFromSnapshot(snapshot []byte) {
	kv.DPrintf("Loading snapshot")

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var storage map[string]string
	var lastAppliedIndex int
	var lastAppliedOp map[int64]int64
	var config shardctrler.Config

	if d.Decode(&storage) != nil || d.Decode(&lastAppliedIndex) != nil || d.Decode(&lastAppliedOp) != nil || d.Decode(&config) != nil {
		kv.DPrintf("Error during read state")
	} else {
		kv.mu.Lock()
		kv.DPrintf("Applying snapshot lastAppliedIndex %d, lastAppliedOp %v, config %s", lastAppliedIndex, lastAppliedOp, config.ToString())

		kv.storage = storage
		kv.lastAppliedIndex = lastAppliedIndex
		kv.lastAppliedOp = lastAppliedOp
		kv.config = config

		kv.mu.Unlock()
	}
}

func (kv *ShardKV) handleCommand(op Op) Err {
	if op.Cmd != ConfigChange && !kv.isMyShard(op) {
		return ErrWrongGroup
	}

	index, termId, isLeader := kv.rf.Start(op)

	if !isLeader {
		return ErrWrongLeader
	} else {
		kv.DPrintf("Leader received %s index %d", op.ToString(), index)

		kv.mu.Lock()

		prevOpId, ok := kv.lastAppliedOp[op.CId]

		if ok && prevOpId == op.OpId {
			kv.DPrintf("Duplicate operation id %d", op.OpId)
			kv.mu.Unlock()
			return OK
		}

		waiter := make(chan Err)
		kv.observer[op.OpId] = waiter
		kv.mu.Unlock()

		partitionHappend := make(chan struct{})
		done := make(chan struct{})

		exit := func(opId int64, done chan struct{}) {
			kv.mu.Lock()
			delete(kv.observer, opId)
			kv.mu.Unlock()

			go func() {
				done <- struct{}{}
			}()
		}
		defer exit(op.OpId, done)

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

		select {
		case res := <-waiter:
			kv.DPrintf("%s %s %d", res, op.ToString(), index)
			return res
		case <-partitionHappend:
			kv.DPrintf("Partition happend %s %d", op.ToString(), index)
			return ErrPartitioned
		}
	}
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	labgob.Register(GetArgs{})
	labgob.Register(PutAppendArgs{})
	labgob.Register(Op{})

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.config = shardctrler.Config{}
	kv.config.Num = 0

	kv.persister = persister
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.termId = -1
	kv.observer = make(map[int64]chan Err)
	kv.lastAppliedOp = make(map[int64]int64)
	kv.trimCh = make(chan struct{})
	kv.storage = make(map[string]string)
	kv.state = Operational

	kv.loadFromSnapshot(persister.ReadSnapshot())

	go kv.applier()
	go kv.configWatcher()
	go kv.persisterMonitor()

	kv.DPrintf("Server started")

	return kv
}
