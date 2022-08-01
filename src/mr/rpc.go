package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

const (
	TEMPTY = -1
	TMAP = 0
	TREDUCE = 1
	TWAIT = 2
)

type Empty struct {

}

type Task struct {
	TaskId int
	TaskType int
	NReduce int
	Files []string
}

type SubmitTaskArgs struct {
	TaskId int
	BucketFiles map[int][]string
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
