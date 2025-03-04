package mr

import (
	"os"
	"strconv"
)

type TaskArgs struct {
	MapId    int
	ReduceId int
}

type TaskReply struct {
	ReturnTask *Task
}

func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
