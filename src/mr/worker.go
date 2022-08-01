package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	for {
		task := CallAcquireTask()
		log.Println("Acquired task %s", task)

		switch task.TaskType {
		case TMAP:
			{
				bucketFiles := MapsTask(&task, mapf)
				CallSubmitTask(task.TaskId, bucketFiles)
			}
		case TREDUCE:
			{
				ReduceTask(&task, reducef)
				CallSubmitTask(task.TaskId, nil)
			}
		case TWAIT:
			{
				log.Println("Sleeping")
				time.Sleep(5 * time.Second)
			}
		}
	}
}

func MapsTask(task *Task, mapf func(string, string) []KeyValue) map[int][]string {
	bf := make(map[int][]KeyValue)

	for i := 0; i < task.NReduce; i++ {
		bf[i] = make([]KeyValue, 0)
	}

	for _, filename := range task.Files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))

		for _, kv := range kva {

			key := ihash(kv.Key) % task.NReduce
			bf[key] = append(bf[key], kv)
		}
	}

	BucketFiles := make(map[int][]string)

	for bucket, kvs := range bf {

		sort.Sort(ByKey(kvs))

		filename := fmt.Sprintf("mr-%d-%d", task.TaskId, bucket)
		WriteToFile(kvs, filename)
		BucketFiles[bucket] = append(BucketFiles[bucket], filename)
	}

	return BucketFiles
}

func ReduceTask(task *Task, reducef func(string, []string) string) {

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-%d
	//
	data := make(map[string][]string)

	for _, file := range task.Files {

		intermediate := ReadFromFile(file)
		i := 0
		for i < len(intermediate) {
			j := i + 1

			key := intermediate[i].Key
			for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, intermediate[k].Value)
			}

			// this is the correct format for each line of Reduce output.
			i = j

			data[key] = append(data[key], values...)
		}
	}

	oname := fmt.Sprintf("mr-out-%d", task.NReduce)
	ofile, _ := os.Create(oname)

	for key, values := range data {
		output := reducef(key, values)
		fmt.Fprintf(ofile, "%v %v\n", key, output)
	}

	ofile.Close()
}

func WriteToFile(kvs []KeyValue, filename string) {

	f, err := os.Create(filename)

	if err != nil {
		log.Fatal(err)
	}

	defer f.Close()

	for _, kv := range kvs {
		_, err2 := f.WriteString(fmt.Sprintf("%s %s\n", kv.Key, kv.Value))

		if err2 != nil {
			log.Fatal(err2)
		}
	}

}

func ReadFromFile(filename string) []KeyValue {
	result := make([]KeyValue, 0)

	readFile, err := os.Open(filename)

	if err != nil {
		log.Fatal(err)
	}
	fileScanner := bufio.NewScanner(readFile)

	fileScanner.Split(bufio.ScanLines)

	for fileScanner.Scan() {
		s := strings.Split(fileScanner.Text(), " ")
		result = append(result, KeyValue{s[0], s[1]})
	}
	readFile.Close()

	return result
}

func CallAcquireTask() Task {
	// declare a reply structure.
	task := Task{}

	ok := call("Coordinator.AcquireTask", &Empty{}, &task)

	if ok {
		fmt.Printf("reply %v\n", task)
	} else {
		fmt.Printf("call failed!\n")
	}

	return task
}

func CallSubmitTask(taskId int, bucketFiles map[int][]string) {
	sta := SubmitTaskArgs{taskId, bucketFiles}

	ok := call("Coordinator.SubmitTask", &sta, &Empty{})

	if ok {
		fmt.Printf("reply %v\n", ok)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	//
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)

	//c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
