package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"strings"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type TempFile struct {
	name    string
	filePtr *os.File
	writer  *bufio.Writer
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func executeMap(mapf func(string, string) []KeyValue, reply *Reply) {
	fmt.Printf("Start map %d\n", reply.TaskId)
	kvList := mapf(reply.Filename, reply.Contents)
	tempFiles := make([]TempFile, reply.N)
	for i := 0; i < reply.N; i++ {
		tempFiles[i].name = fmt.Sprintf("mr-%d-%d", reply.TaskId, i)
		tempFiles[i].filePtr, _ = os.CreateTemp("", "example")
		defer os.Remove(tempFiles[i].filePtr.Name())
		defer tempFiles[i].filePtr.Close()
		tempFiles[i].writer = bufio.NewWriter(tempFiles[i].filePtr)
	}
	for _, kv := range kvList {
		line := fmt.Sprintf("%s %s\n", kv.Key, kv.Value)
		_, _ = tempFiles[ihash(kv.Key)%reply.N].writer.WriteString(line)
	}
	for _, v := range tempFiles {
		v.writer.Flush()
		os.Rename(v.filePtr.Name(), v.name)
	}
	args := Args{
		TaskId: reply.TaskId,
		Task:   reply.Task,
	}
	CallTaskDone(&args)
	fmt.Printf("Done map %d\n", reply.TaskId)
}

func executeReduce(reducef func(string, []string) string, reply *Reply) {
	fmt.Printf("Start reduce %d\n", reply.TaskId)
	kvDict := make(map[string][]string)
	for i := 0; i < reply.M; i++ {
		filename := fmt.Sprintf("mr-%d-%d", i, reply.TaskId)
		mapFile, _ := os.Open(filename)
		sc := bufio.NewScanner(mapFile)
		for sc.Scan() {
			line := sc.Text()
			arr := strings.Split(line, " ")
			key := arr[0]
			val := arr[1]
			kvDict[key] = append(kvDict[key], val)
		}
		mapFile.Close()
	}
	reduceFile, _ := os.CreateTemp("", "example")
	defer os.Remove(reduceFile.Name())
	for key, val := range kvDict {
		finalVal := reducef(key, val)
		fmt.Fprintf(reduceFile, "%v %v\n", key, finalVal)
	}
	os.Rename(reduceFile.Name(), fmt.Sprintf("mr-out-%d", reply.TaskId))
	args := Args{
		TaskId: reply.TaskId,
		Task:   reply.Task,
	}
	CallTaskDone(&args)
	fmt.Printf("Done reduce %d\n", reply.TaskId)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		reply := CallDelegateTask()
		if reply.Task == WAIT {
			fmt.Println("Waiting for task")
			time.Sleep(20 * time.Millisecond)
		} else if reply.Task == REDUCE {
			executeReduce(reducef, reply)
		} else if reply.Task == MAP {
			executeMap(mapf, reply)
		} else {
			break
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	//CallExample()

}

func CallDelegateTask() *Reply {
	args := Args{}
	reply := Reply{}
	ok := call("Coordinator.DelegateTask", &args, &reply)
	if !ok {
		log.Fatalf("Call failed!\n")
	}
	return &reply
}

func CallTaskDone(args *Args) {
	reply := Reply{}
	ok := call("Coordinator.TaskDone", args, &reply)
	if !ok {
		log.Fatalf("Call failed!\n")
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
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
