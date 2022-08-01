package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

//import "os"

//import "time"

type Coordinator struct {
	Queue         []Task
	NReduce       int
	ReduceBuckets map[int][]string
	InProgress    map[int]Task
	StartedAt     map[int]int64
	State         int // TMAP, TREDUCE

	mutex     sync.Mutex
	idcounter int
}

func (c *Coordinator) acquireTask() *Task {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if len(c.Queue) == 0 {
		return nil
	} else {
		task := c.Queue[len(c.Queue)-1]
		c.Queue = c.Queue[:len(c.Queue)-1]
		c.InProgress[task.TaskId] = task
		c.StartedAt[task.TaskId] = time.Now().Unix()
		return &task
	}
}

func (c *Coordinator) submitTask(taskArgs *SubmitTaskArgs) bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	task, ok := c.InProgress[taskArgs.TaskId]
	fmt.Println(task)

	if ok {
		switch task.TaskType {
		case TMAP:
			{
				log.Printf("Appending %d buckets to reduce task", len(taskArgs.BucketFiles))
				for bucket, files := range taskArgs.BucketFiles {
					c.ReduceBuckets[bucket] = append(c.ReduceBuckets[bucket], files...)
				}
			}
		case TREDUCE:
			{
				log.Println("Reduce task %d received", task.TaskId)
			}
		}

		delete(c.InProgress, task.TaskId)
		delete(c.StartedAt, task.TaskId)
		return true

	} else {
		return false
	}
}

// Your code here -- RPC handlers for the worker to call.

// Get job
func (c *Coordinator) AcquireTask(args *Empty, reply *Task) error {
	log.Println("Acquiring a task")

	task := c.acquireTask()

	if task != nil {
		reply.TaskId = task.TaskId
		reply.TaskType = task.TaskType
		reply.Files = task.Files
		reply.NReduce = task.NReduce
		log.Printf("Task reply %s", reply)
	} else {
		reply.TaskType = TWAIT
		log.Println("No task")
	}

	return nil
}

func (c *Coordinator) SubmitTask(args *SubmitTaskArgs, reply *Empty) error {

	log.Printf("Submiting a task with args %s", args)
	c.submitTask(args)
	return nil
}

func (c *Coordinator) createTask(state int, nreduce int, files []string) Task {
	task := Task{c.idcounter, state, nreduce, files}
	c.Queue = append(c.Queue, task)
	c.idcounter++
	return task
}

func min(times map[int]int64) (int, int64) {
	var minTime int64
	var minId int

	for minId, minTime = range times {
		break
	}
	for tId, t := range times {
		if t < minTime {
			minTime = t
			minId = tId
		}
	}
	return minId, minTime
}

func (c *Coordinator) monitor() {
	for {
		c.mutex.Lock()
		if len(c.StartedAt) > 0 {
			for tId, t := range c.StartedAt {
				// Requeue
				if time.Now().Unix()-t >= 10 {
					log.Printf("Requeue task id %d", tId)
					task := c.InProgress[tId]
					c.Queue = append(c.Queue, task)
					delete(c.InProgress, tId)
					delete(c.StartedAt, tId)
				}
			}
		} else if len(c.Queue) == 0 && c.State != TREDUCE {
			c.State = TREDUCE
			log.Println("Switching to reduce state")

			for bucket, files := range c.ReduceBuckets {
				c.createTask(TREDUCE, bucket, files)
			}
			log.Printf("%d tasks in queue", len(c.Queue))
		}
		c.mutex.Unlock()

		time.Sleep(time.Second)
	}
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()

	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)

	//l, e := net.Listen("tcp", ":1234")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {

	c.mutex.Lock()
	defer c.mutex.Unlock()

	if len(c.InProgress) == 0 && c.State == TREDUCE && len(c.Queue) == 0 {
		log.Println("Work is done, exit")
		return true
	}

	return false
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.NReduce = nReduce
	c.Queue = make([]Task, 0)
	c.InProgress = make(map[int]Task)
	c.StartedAt = make(map[int]int64)
	c.ReduceBuckets = make(map[int][]string)
	c.State = TMAP

	for _, file := range files {
		c.createTask(TMAP, c.NReduce, []string{file})
	}

	go c.monitor()
	c.server()
	return &c
}
