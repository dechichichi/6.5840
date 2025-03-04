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

type Coordinator struct {
	Mutex sync.Mutex //锁
	//总量
	MapNum    int
	ReduceNum int
	//任务
	//tmp-M-R
	Tasks map[int]*Task
	//测心跳
	CheckBytes map[int]bool
	Phase      int
}
type Task struct {
	//个体id
	Id             int
	nReduce        int
	mapfilename    string
	reducefilename []string
	Work           int
}

const (
	MapWoring = iota
	ReduceWorking
	AllDone
)

const (
	Break = -1 + iota
	MapTask
	ReduceTask
	Done
)

// 在此基础上得捕捉心跳
// 考虑bool或者time
// 我们在map完全结束之后才考虑reduce
func (c *Coordinator) CollectWork(args *TaskArgs, reply *TaskReply) error {
	if c.Phase == MapWoring {
		for i := 0; i < c.MapNum; i++ {
			if c.Tasks[i].Work == MapTask {
				reply.ReturnTask = c.Tasks[i]
				return nil
			}
		}
		c.Phase = ReduceWorking
	}
	if c.Phase == ReduceWorking {
		for i := c.MapNum; i < c.MapNum+c.ReduceNum; i++ {
			if c.Tasks[i].Work == ReduceTask {
				reply.ReturnTask = c.Tasks[i]
				return nil
			}
		}
		c.Phase = AllDone
	}
	return fmt.Errorf("NO Work")
}

func (c *Coordinator) MapWorkDone(args *TaskArgs, reply *TaskReply) {
	c.Tasks[args.ReduceId].reducefilename = append(c.Tasks[args.ReduceId].reducefilename, c.Tasks[args.MapId].mapfilename)
	c.Tasks[args.MapId].Work = Done
}

func (c *Coordinator) ReduceWorkDone(args *TaskArgs, reply *TaskReply) {
	c.Tasks[args.ReduceId].Work = Done
}

func (c *Coordinator) ListenHeartBeat() {
	time.Sleep(10000)
	for i := 0; i < c.MapNum+c.ReduceNum; i++ {
		if c.Tasks[i].Work == Break {
			c.CheckBytes[i] = false
		}
	}
	for i := 0; i < c.MapNum; i++ {
		if !c.CheckBytes[i] {
			c.Tasks[i].Work = MapTask
		}
	}
	for i := c.MapNum; i < c.MapNum+c.ReduceNum; i++ {
		if !c.CheckBytes[i] {
			c.Tasks[i].Work = ReduceTask
		}
	}
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

func (c *Coordinator) Done() bool {
	return c.Phase == AllDone
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	//一个file对应一个maptask
	//nReduce个reducetask
	tasks := map[int]*Task{}
	for i, file := range files {
		//传入默认给maptask
		task := Task{
			mapfilename: file,
			nReduce:     i % nReduce,
			Id:          i,
			Work:        MapTask,
		}
		tasks[i] = &task
	}
	for i := len(files); i < len(files)+nReduce; i++ {
		task := Task{
			nReduce: i,
			Work:    ReduceTask,
		}
		tasks[i] = &task
	}
	c := Coordinator{
		Tasks:     tasks,
		ReduceNum: nReduce,
		Phase:     MapWoring,
		MapNum:    len(files),
	}
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	go c.ListenHeartBeat()
	c.server()
	return &c
}
