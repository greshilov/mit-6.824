package shardctrler

import (
	"bytes"
	"fmt"
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	dead          int32
	configs       []Config // indexed by config num
	termId        int
	observer      map[int64]chan Config
	lastAppliedOp map[int64]int64 // [client id] -> op id
}

type CtrlCommand string

const (
	Join  CtrlCommand = "JOIN"
	Leave CtrlCommand = "LEAVE"
	Move  CtrlCommand = "MOVE"
	Query CtrlCommand = "QUERY"
)

type Op struct {
	// Your data here.
	Cmd     CtrlCommand
	CmdArgs []byte
	OpId    int64
	CId     int64 // ClerkId
}

func (op *Op) ToString() string {
	return fmt.Sprintf("{typ: %s, opId: %d, cId: %d}", op.Cmd, op.OpId, op.CId)
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(args)

	op := Op{Join, w.Bytes(), args.OpId, int64(args.CId)}
	err, _ := sc.handleCommand(op)
	reply.Err = err
	reply.WrongLeader = (err == ErrWrongLeader)
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(args)

	op := Op{Leave, w.Bytes(), args.OpId, int64(args.CId)}
	err, _ := sc.handleCommand(op)
	reply.Err = err
	reply.WrongLeader = (err == ErrWrongLeader)
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(args)

	op := Op{Move, w.Bytes(), args.OpId, int64(args.CId)}
	err, _ := sc.handleCommand(op)
	reply.Err = err
	reply.WrongLeader = (err == ErrWrongLeader)
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(args)

	op := Op{Query, w.Bytes(), args.OpId, int64(args.CId)}
	err, conf := sc.handleCommand(op)
	if err == OK {
		sc.DPrintf("Replying %s with conf: %s", err, conf.ToString())
	}
	reply.Config = conf
	reply.Err = err
	reply.WrongLeader = (err == ErrWrongLeader)
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// === My methods ===

func (sc *ShardCtrler) DPrintf(format string, a ...interface{}) {

	if Debug {
		str := fmt.Sprintf("[shardctrler][%d] %s", sc.me, format)

		log.SetFlags(log.LstdFlags | log.Lmicroseconds)
		log.Printf(str, a...)
	}
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

func (sc *ShardCtrler) handleCommand(op Op) (Err, Config) {
	index, termId, isLeader := sc.rf.Start(op)

	if !isLeader {
		return ErrWrongLeader, Config{}
	} else {
		sc.DPrintf("Leader received %s index %d", op.ToString(), index)

		sc.mu.Lock()

		prevOpId, ok := sc.lastAppliedOp[op.CId]

		if ok && prevOpId == op.OpId {
			sc.DPrintf("Duplicate operation id %d", op.OpId)
			sc.mu.Unlock()
			return OK, Config{}
		}

		waiter := make(chan Config)
		sc.observer[op.OpId] = waiter
		sc.mu.Unlock()

		partitionHappend := make(chan struct{})
		done := make(chan struct{})

		exit := func(opId int64, done chan struct{}) {

			sc.DPrintf("Stops waiting %s %d", op.ToString(), index)

			sc.mu.Lock()
			delete(sc.observer, opId)
			sc.mu.Unlock()

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
					newTermId, _ := sc.rf.GetState()
					if termId != newTermId {
						ch <- struct{}{}
					}
					time.Sleep(time.Duration(50) * time.Millisecond)
				}
			}
		}(termId, partitionHappend, done) // Partition waiter

		sc.DPrintf("Starts waiting %s %d", op.ToString(), index)
		select {
		case config := <-waiter:
			sc.DPrintf("Committed %s %d", op.ToString(), index)
			return OK, config
		case <-partitionHappend:
			sc.DPrintf("Partition happend %s %d", op.ToString(), index)
			return ErrPartitioned, Config{}
		}
	}
}

func (sc *ShardCtrler) applier() {
	for !sc.killed() {
		for msg := range sc.applyCh {
			sc.DPrintf("Start processing command %d", msg.CommandIndex)

			if msg.CommandValid {
				op := msg.Command.(Op)

				sc.mu.Lock()
				var lastOpId, exists = sc.lastAppliedOp[op.CId]
				var config Config

				if !exists || lastOpId != op.OpId {
					sc.lastAppliedOp[op.CId] = op.OpId

					switch op.Cmd {
					case Join:
						r := bytes.NewBuffer(op.CmdArgs)
						d := labgob.NewDecoder(r)

						var joinArgs JoinArgs

						if d.Decode(&joinArgs) != nil {
							sc.DPrintf("Invalid JoinArgs bytes, skipping")
						} else {
							sc.DPrintf("Adding gIds: %v", joinArgs.Servers)
							config = sc.getCopyOfConfig(-1)
							config.Num += 1

							for gId, servers := range joinArgs.Servers {
								// TODO: Check if gId exists?
								config.Groups[gId] = servers
							}
							sc.rebalance(&config)
							sc.configs = append(sc.configs, config)
						}
					case Query:
						r := bytes.NewBuffer(op.CmdArgs)
						d := labgob.NewDecoder(r)

						var queryArgs QueryArgs

						if d.Decode(&queryArgs) != nil {
							sc.DPrintf("Invalid QueryArgs bytes, skipping %s")
						} else {
							if queryArgs.Num == -1 || queryArgs.Num >= len(sc.configs) {
								config = sc.getCopyOfConfig(-1)
							} else {
								config = sc.getCopyOfConfig(queryArgs.Num)
							}
						}
					case Leave:
						r := bytes.NewBuffer(op.CmdArgs)
						d := labgob.NewDecoder(r)

						var leaveArgs LeaveArgs

						if d.Decode(&leaveArgs) != nil {
							sc.DPrintf("Invalid LeaveArgs bytes, skipping %s")
						} else {
							sc.DPrintf("Deleting gIds: %v", leaveArgs.GIDs)
							config = sc.getCopyOfConfig(-1)
							config.Num += 1

							for i := range leaveArgs.GIDs {
								// TODO: Check if gId exists?
								delete(config.Groups, leaveArgs.GIDs[i])
							}
							sc.rebalance(&config)
							sc.configs = append(sc.configs, config)
						}
					case Move:
						r := bytes.NewBuffer(op.CmdArgs)
						d := labgob.NewDecoder(r)

						var moveArgs MoveArgs

						if d.Decode(&moveArgs) != nil {
							sc.DPrintf("Invalid MoveArgs bytes, skipping %s")
						} else {
							sc.DPrintf("Moving gId to sId: %d -> %d", moveArgs.GID, moveArgs.Shard)
							config = sc.getCopyOfConfig(-1)
							config.Num += 1
							config.Shards[moveArgs.Shard] = moveArgs.GID
							sc.configs = append(sc.configs, config)
						}
					default:
						sc.DPrintf("Unknown command type: %s", op.Cmd)
					}
				} else {
					sc.DPrintf("Duplicate op %d", op.OpId)
				}

				waiter, ok := sc.observer[op.OpId]
				if ok {
					sc.DPrintf("Sending notification %d", op.OpId)
					go func(waiter chan Config, conf Config) {
						waiter <- conf
					}(waiter, config)

					delete(sc.observer, op.OpId)
				} else {
					sc.DPrintf("No notification found for %d", op.OpId)
				}

				sc.mu.Unlock()
			} else {
				sc.DPrintf("Installing snapshot message, ignoring (snapshot index %d)", msg.SnapshotIndex)
			}
			sc.DPrintf("Finish processing command %d", msg.CommandIndex)
		}
	}
}

func (sc *ShardCtrler) getCopyOfConfig(i int) Config {

	var conf Config

	if i < 0 {
		conf = sc.configs[len(sc.configs)-1]
	} else {
		conf = sc.configs[i]
	}

	newConf := Config{}
	newConf.Groups = make(map[int][]string)
	newConf.Num = conf.Num
	newConf.Shards = conf.Shards
	for k, v := range conf.Groups {
		newConf.Groups[k] = v
	}
	return newConf
}

func (sc *ShardCtrler) rebalance(conf *Config) {
	sc.DPrintf("Rebalancing with groups: %v", conf.Groups)
	// Delete existing groups
	for sId := range conf.Shards {
		conf.Shards[sId] = 0
	}

	if len(conf.Groups) == 0 {
		// Nothing to balance, lol
		return
	}

	var gIds []int
	for gId := range conf.Groups {
		gIds = append(gIds, gId)
	}
	sort.Ints(gIds)

	i := 0
	for sId, gId := range conf.Shards {
		if gId == 0 {
			conf.Shards[sId] = gIds[i]
			i++
			i %= len(gIds)
		}
	}
	sc.DPrintf("Balanced shards: %v", conf.Shards)
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(QueryArgs{})
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.

	sc.termId = -1
	sc.observer = make(map[int64]chan Config)
	sc.lastAppliedOp = make(map[int64]int64)

	go sc.applier()

	return sc
}
