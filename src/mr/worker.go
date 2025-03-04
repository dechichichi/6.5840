package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type KeyValue struct {
	Key   string
	Value string
}

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		args := TaskArgs{}
		reply := TaskReply{}
		ok := call("Coordinator.CollectWork", &args, &reply)
		if !ok {
			continue
		}
		if reply.ReturnTask.Work == MapTask {
			DoMapWork(mapf, reply.ReturnTask)
			call("Coordinator.MapWorkDone",&args,&reply)
		} else if reply.ReturnTask.Work == ReduceTask {
			DoReduceWork(reducef, reply.ReturnTask)
			call("Coordinator.CoordinatorWorkDone",&args,&reply)
		}
	}

}

func sendheartbyte() {
	panic("not implemented")
}

func DoMapWork(mapf func(string, string) []KeyValue, task *Task) {
	file, err := os.Open(task.mapfilename)
	if err != nil {
		panic(err)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		panic(err)
	}
	KeyValueList := mapf(task.mapfilename, string(content))
	oname := fmt.Sprintf("mr-%d-%d", task.Id, task.nReduce)
	task.mapfilename = oname
	// 写入文件
	ofile, err := os.Create(oname)
	if err != nil {
		fmt.Println("Create file failed:", err)
	}
	defer ofile.Close()
	for _, kv := range KeyValueList {
		fmt.Fprintf(ofile, "%v %v\n", kv.Key, kv.Value)
	}
}

func DoReduceWork(reducef func(string, []string) string, task *Task) {
	// 读取所有Y为nReduce的文件
	var intermediate []KeyValue
	for _, file := range task.reducefilename {
		f, err := os.Open(file)
		if err != nil {
			panic(err)
		}
		defer f.Close()
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			line := scanner.Text()
			parts := strings.Split(line, "\n") // 假设键值对用制表符分隔
			if len(parts) != 2 {
				fmt.Printf("Invalid line format: %s\n", line)
				continue
			}
			key := parts[0]
			value := parts[1]
			intermediate = append(intermediate, KeyValue{Key: key, Value: value})
		}

		if err := scanner.Err(); err != nil {
			panic(err)
		}
	}
	// 对中间结果进行排序
	sort.Sort(ByKey(intermediate))
	// 生成当前 Reduce 任务的输出文件名
	outputFileName := fmt.Sprintf("mr-out-%d", task.nReduce)
	outputFile, err := os.Create(outputFileName)
	if err != nil {
		log.Printf("Failed to create output file %s: %v", outputFileName, err)
		return
	}
	defer outputFile.Close()

	// reduce操作
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := make([]string, j-i)
		for k := i; k < j; k++ {
			values[k-i] = intermediate[k].Value
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(outputFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}

	log.Printf("Reduce task %d completed, output file: %s", task.nReduce, outputFileName)
}

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
