package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK                = "OK"
	ErrNoKey          = "ErrNoKey"
	ErrInvalidMsgBody = "ErrInvalidMsgBody"
	ErrOldMsg         = "ErrOldMsg"
	ErrUnknownCmd     = "ErrUnknownCmd"
	ErrWrongState     = "ErrWrongState"
	ErrWrongGroup     = "ErrWrongGroup"
	ErrWrongLeader    = "ErrWrongLeader"
	ErrPartitioned    = "ErrPartitioned"
	ErrWrongConfig    = "ErrWrongConfig"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpId int64
	CId  int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	OpId int64
	CId  int64
}

type GetReply struct {
	Err   Err
	Value string
}

type MigrateShardArgs struct {
	ConfigNum int
	Shard     int
}

type MigrateShardReply struct {
	Data map[string]string
	Err  Err
}
