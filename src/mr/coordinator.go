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

type taskType int
type taskStatus int

const (
	mapTask taskType = iota
	reduceTask
)

const (
	taskStatusUnAssigned taskStatus = iota
	taskStatusRunning
	taskStatusFailed
	taskStatusFinished
)

type Task struct {
	ID        int
	Type      taskType
	File      []string
	Status    taskStatus
	StartTime time.Time
}

type Coordinator struct {
	// Your definitions here.
	Files              []string
	NReduce            int
	NMap               int
	Tasks              []*Task
	FinishedTasks      int
	TaskChan           chan *Task
	StartedReduceTasks bool
	isDone             bool
	mu                 sync.RWMutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.isDone
}

func (c *Coordinator) InitMapReduceTask() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Tasks = make([]*Task, 0)
	for i := 0; i < c.NMap; i++ {
		task := &Task{
			ID:     i,
			Type:   mapTask,
			File:   []string{c.Files[i]},
			Status: taskStatusUnAssigned,
		}
		c.Tasks = append(c.Tasks, task)
		c.TaskChan <- task
	}

}

func (c *Coordinator) InitReduceTask() {
	for i := 0; i < c.NReduce; i++ {
		task := &Task{
			ID:     i,
			Type:   reduceTask,
			File:   []string{},
			Status: taskStatusUnAssigned,
		}
		c.Tasks = append(c.Tasks, task)
		c.TaskChan <- task
	}
	c.StartedReduceTasks = true
	log.Printf("init reduce tasks finished")
}

func (c *Coordinator) AssignTask(args *AssignTaskArgs, reply *AssignTaskReply) error {
	c.mu.Lock()
	if c.FinishedTasks == c.NMap && !c.StartedReduceTasks {
		log.Printf("all map tasks finished")
		c.InitReduceTask()
	} else if c.FinishedTasks == c.NReduce + c.NMap {
		log.Printf("all reduce tasks finished")
		c.isDone = true
	}
	c.mu.Unlock()
//	switch c.FinishedTasks {
//	case c.NMap:
//		if c.StartedReduceTasks {
//			c.mu.Unlock()
//			return nil
//		}
//		log.Printf("all map tasks finished")
//		c.mu.Unlock()
//		c.InitReduceTask()
//	case c.NReduce + c.NMap:
//		log.Printf("all reduce tasks finished")
//		c.isDone = true
//		c.mu.Unlock()
//	default:
//		c.mu.Unlock()
//	}
	reply.Task = c.OffTask()
	if reply.Task == nil {
		return nil
	}
	reply.Task.Status = taskStatusRunning
	reply.Task.StartTime = time.Now()
	reply.NMap = c.NMap
	reply.NReduce = c.NReduce
	log.Printf("task %d assign at %v, type %d", reply.Task.ID, reply.Task.StartTime, reply.Task.Type)
	return nil
}

func (c *Coordinator) OffTask() *Task {
	select {
	case task := <-c.TaskChan:
		return task
	default:
		return nil
	}
}

func (c *Coordinator) OnTask(task *Task) {
	log.Printf("task %d on, type %d", task.ID, task.Type)
	c.TaskChan <- task
}

func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	task := args.Task
	c.mu.Lock()
	defer c.mu.Unlock()
	var actualTask *Task
    for _, t := range c.Tasks {
        if t.ID == args.Task.ID && t.Type == args.Task.Type {
            actualTask = t
            break
        }
    }
	if actualTask == nil || actualTask.Status == taskStatusFinished {
		return nil
	}
	switch task.Status {
	case taskStatusFailed:
		actualTask.Status = taskStatusUnAssigned
		log.Printf("task %d failed, type %d, startTime %v", task.ID, task.Type, task.StartTime)
		c.TaskChan <- actualTask
	case taskStatusFinished:
		actualTask.Status = taskStatusFinished
		c.FinishedTasks++
		log.Printf("task %d finished, type %d, startTime %v, finishedTask %d", task.ID, task.Type, task.StartTime, c.FinishedTasks)
	default:
		return nil
	}
	return nil
}

func (c *Coordinator) taskCheck() {
	//定时器，每二十秒检查一次任务状态
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		c.mu.RLock()
		if c.isDone {
			c.mu.RUnlock()
			return
		}
		c.mu.RUnlock()
		<-ticker.C
		c.checkTaskStatus()
	}
}

func (c *Coordinator) checkTaskStatus() {
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, task := range c.Tasks {
		if task.Status != taskStatusRunning {
			continue
		}
		if now.Sub(task.StartTime) > 5*time.Second {
			task.Status = taskStatusUnAssigned
			select {
			case c.TaskChan <- task:
				log.Printf("Task %d-%d timeout, rescheduled", task.Type, task.ID)
			default:
				log.Printf("Error: Task channel full, cannot reschedule task %d-%d", task.Type, task.ID)
			}
		}
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		Files:         files,
		NReduce:       nReduce,
		NMap:          len(files),
		TaskChan:      make(chan *Task, len(files)+nReduce),
		isDone:        false,
		mu:            sync.RWMutex{},
		FinishedTasks: 0,
		Tasks:         make([]*Task, 0),
	}
	c.InitMapReduceTask()
	go c.taskCheck()
	c.server()
	return &c
}
