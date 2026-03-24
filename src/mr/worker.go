package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		args := &AssignTaskArgs{}
		reply := &AssignTaskReply{}

		// 检查 RPC 调用是否成功
		ok := call("Coordinator.AssignTask", args, reply)
		if !ok {
			// Coordinator 可能已退出，worker 也退出
			break
		}

		// 检查 Task 是否为空
		if reply.Task == nil {
			// 没有任务，等待后重试
			time.Sleep(time.Second)
			continue
		}

		task := reply.Task
		if task.Type == mapTask {
			doMapTask(task, reply.NReduce, mapf)
		} else if task.Type == reduceTask {
			doReduceTask(task, reply.NMap, reducef)
		}

		call("Coordinator.ReportTask", &ReportTaskArgs{Task: task}, &ReportTaskReply{})
	}
}

func doMapTask(task *Task, NReduce int, mapf func(string, string) []KeyValue) {
    tmpFiles := make([]*os.File, NReduce)
    encoders := make([]*json.Encoder, NReduce)
    
    for i := 0; i < NReduce; i++ {
        fileName := fmt.Sprintf("mr-%v-%v", task.ID, i)
        f, err := os.Create(fileName)
        if err != nil {
            task.Status = taskStatusFailed
            return
        }
		defer f.Close()
        tmpFiles[i] = f
        encoders[i] = json.NewEncoder(f)
    }
    
    file, err := os.Open(task.File[0])
    if err != nil {
        task.Status = taskStatusFailed
        return
    }
    defer file.Close()
    
    content, err := io.ReadAll(file)
    if err != nil {
        task.Status = taskStatusFailed
        return
    }
    
    kva := mapf(task.File[0], string(content))
    for _, kv := range kva {
        reduceId := ihash(kv.Key) % NReduce
        if err := encoders[reduceId].Encode(&kv); err != nil {
            task.Status = taskStatusFailed
            return
        }
    }
    
    task.Status = taskStatusFinished
}


func doReduceTask(task *Task, NMap int, reducef func(string, []string) string) {
	fmt.Printf("do reduce task %v\n", task.ID)
	var kva []KeyValue
	for i := range NMap {
		fileName := fmt.Sprintf("mr-%v-%v", i, task.ID)
		file, err := os.Open(fileName)
		if err != nil {
			task.Status = taskStatusFailed
			log.Printf("open file error: %v", err)
			// return early
			return
		}
		defer file.Close()
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}
	// sort kva by key
	sort.SliceStable(kva, func(i, j int) bool {
		return kva[i].Key < kva[j].Key
	})
	// group kva by key
	groups := make(map[string][]string)
	for _, kv := range kva {
		groups[kv.Key] = append(groups[kv.Key], kv.Value)
	}
	toFile, err := os.Create(fmt.Sprintf("mr-out-%v", task.ID))
	if err != nil {
		task.Status = taskStatusFailed
	}
	defer toFile.Close()
	// reduce
	for k, v := range groups {
		output := reducef(k, v)
		fmt.Fprintf(toFile, "%v %v\n", k, output)
	}
	task.Status = taskStatusFinished
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
