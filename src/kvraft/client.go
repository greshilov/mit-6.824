package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	mu      sync.Mutex
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.leader = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args := GetArgs{}
	args.Key = key
	args.OpId = nrand()

	reply := GetReply{}

	ck.callRPC("KVServer.Get", &args, &reply)
	DPrintf("[kv][client][%d] RPC GET success", ck.leader)
	return reply.Value
}

func (ck *Clerk) callRPC(rpc string, args interface{}, reply Reply) {

	ck.mu.Lock()
	leader := ck.leader
	n := len(ck.servers)
	ck.mu.Unlock()

	for {
		for i := leader; i < n; i++ {

			select {
			case <-time.After(150 * time.Millisecond):
				DPrintf("[kv][client][%d] Timeout", i)
				continue
			default:
				DPrintf("[kv][client][%d] Making call %s", i, rpc)
				ok := ck.servers[i].Call(rpc, args, reply)

				if !ok || !reply.isSuccess() {
					DPrintf("[kv][client][%d] No success ok: %t reply %s", i, ok, reply)
					continue
				} else {

					if leader != i {
						ck.mu.Lock()
						DPrintf("[kv][client][%d] Set leader", i)
						ck.leader = i
						ck.mu.Unlock()
					}
					return
				}
			}
		}
		leader = 0
		time.Sleep(time.Duration(50) * time.Millisecond)
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op
	args.OpId = nrand()

	reply := PutAppendReply{}

	ck.callRPC("KVServer.PutAppend", &args, &reply)
	DPrintf("[kv][client][%d] RPC PUTAPPEND success", ck.leader)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
