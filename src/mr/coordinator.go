package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	Mutex     sync.Mutex //锁
	Phase     int
	ReduceNum int
	Tasks     []*Task
	AllDone   bool
}

const (
	Waiting = iota
	Working
	AllDone
)

func (c *Coordinator) TaskRPC(args *TaskArgs, reply *TaskReply) error {
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
	return c.AllDone
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	tasks := []*Task{}
	for _, file := range files {
		tasks = append(tasks, &Task{
			filename: file,
			State:    Waiting,
		})
	}
	c := Coordinator{
		Tasks:     tasks,
		ReduceNum: nReduce,
		Phase:     Waiting,
		AllDone:   false,
	}
	c.Mutex.Lock()
	c.Phase = Working
	defer c.Mutex.Unlock()
	c.server()
	return &c
}
