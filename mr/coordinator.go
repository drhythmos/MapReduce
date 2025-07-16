package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	mMap, nReduce         int
	mapPhase              bool
	mapTasks, reduceTasks []Task
	mu                    sync.Mutex
}
type TaskState string

const (
	IDLE       TaskState = "idle"
	INPROGRESS TaskState = "in-progress"
	COMPLETED  TaskState = "completed"
)

type Task struct {
	inputFile string
	state     TaskState
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) checkTask(taskId int, taskType TaskType) {
	time.Sleep(10 * time.Second)
	c.mu.Lock()
	if taskType == MAP {
		if c.mapTasks[taskId].state == INPROGRESS {
			c.mapTasks[taskId].state = IDLE
		}
	} else {
		if c.reduceTasks[taskId].state == INPROGRESS {
			c.reduceTasks[taskId].state = IDLE
		}
	}
	c.mu.Unlock()
}

func (c *Coordinator) assignMapTask(reply *Reply) {
	inProgressTasks := 0
	completedTasks := 0
	taskFound := false
	c.mu.Lock()
	for idx, v := range c.mapTasks {
		if v.state == IDLE {
			reply.Filename = v.inputFile
			contentBytes, _ := os.ReadFile(v.inputFile)
			reply.Contents = string(contentBytes)
			reply.TaskId = idx
			reply.N = c.nReduce
			reply.Task = MAP
			c.mapTasks[idx].state = INPROGRESS
			taskFound = true
			break
			// v.state = INPROGRESS
		} else if v.state == INPROGRESS {
			inProgressTasks++
		} else {
			completedTasks++
		}
	}
	c.mu.Unlock()
	if taskFound {
		go c.checkTask(reply.TaskId, reply.Task)
		return
	}
	if inProgressTasks != 0 {
		reply.Task = WAIT
	}
	if completedTasks == c.mMap {
		c.mapPhase = false
		c.assignReduceTask(reply)
	}
}
func (c *Coordinator) assignReduceTask(reply *Reply) {
	inProgressTasks := 0
	completedTasks := 0
	taskFound := false
	c.mu.Lock()
	for idx, v := range c.reduceTasks {
		if v.state == IDLE {
			reply.TaskId = idx
			reply.N = c.nReduce
			reply.M = c.mMap
			c.reduceTasks[idx].state = INPROGRESS
			reply.Task = REDUCE
			taskFound = true
			break
		} else if v.state == INPROGRESS {
			inProgressTasks++
		} else {
			completedTasks++
		}
	}
	c.mu.Unlock()
	if taskFound {
		go c.checkTask(reply.TaskId, reply.Task)
		return
	}
	if inProgressTasks != 0 {
		reply.Task = WAIT
	}
	if completedTasks == c.nReduce {
		reply.Task = DONE
	}
}
func (c *Coordinator) DelegateTask(args *Args, reply *Reply) error {

	if c.mapPhase {
		c.assignMapTask(reply)
	} else {
		c.assignReduceTask(reply)
	}

	return nil
}
func (c *Coordinator) TaskDone(args *Args, reply *Reply) error {
	c.mu.Lock()
	if args.Task == MAP {
		c.mapTasks[args.TaskId].state = COMPLETED
	} else {
		c.reduceTasks[args.TaskId].state = COMPLETED
	}
	c.mu.Unlock()
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	cntCompleted := 0
	c.mu.Lock()
	for _, v := range c.reduceTasks {
		if v.state == COMPLETED {
			cntCompleted++
		}
	}
	c.mu.Unlock()
	if cntCompleted == c.nReduce {
		ret = true
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mMap:        len(files),
		nReduce:     nReduce,
		mapPhase:    true,
		mapTasks:    make([]Task, len(files)),
		reduceTasks: make([]Task, nReduce),
		mu:          sync.Mutex{},
	}

	// Your code here.
	for i, v := range files {
		c.mapTasks[i].inputFile = v
		c.mapTasks[i].state = IDLE
	}
	for i := 0; i < c.nReduce; i++ {
		c.reduceTasks[i].state = IDLE
	}
	c.server()
	return &c
}
