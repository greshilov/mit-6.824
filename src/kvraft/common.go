package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrPartitioned = "ErrPartitioned"
	ErrTimeout     = "ErrTimeout"

	GET    = "Get"
	PUT    = "Put"
	APPEND = "Append"
)

type Err string

type Reply interface {
	isSuccess() bool
}

// Put or Append
type PutAppendArgs struct {
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

func (r PutAppendReply) isSuccess() bool {
	return r.Err == OK
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

func (r GetReply) isSuccess() bool {
	return r.Err == OK
}
