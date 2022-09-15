package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const timeout = time.Duration(10) * time.Second

type TaskStatus int

const (
	Processing TaskStatus = iota
	TimedOut
	Processed
)

var tasksQueue []string

var reduceTasksStarted = false

var assignedTaskStatus = make(map[string]TaskStatus)

var nReduceTasks int

var done = false

var mu sync.Mutex

type Coordinator struct {
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *WorkerArgs, reply *CoordinatorReply) error {

	reply.TaskFileName = assignTask(WorkerArgs{})
	reply.NReduceTasks = nReduceTasks

	return nil
}

func assignTask(args WorkerArgs) string {

	removeProcessedTasksFromQueue()

	allMapTasksProcessed := len(tasksQueue) == 0 && !reduceTasksStarted
	if allMapTasksProcessed {
		tasksQueue = findIntermediateFiles()
		reduceTasksStarted = true
		log.Println("All map tasks are finished.")
		log.Println("Starting up reduce tasks.")
	}

	if len(tasksQueue) > 0 {
		return nextAvailableTask(args)
	}

	log.Println("No more files to assign.")
	done = true
	return ""
}

func nextAvailableTask(args WorkerArgs) string {
	var fileName = tasksQueue[0]
	tasksQueue = tasksQueue[1:]

	mu.Lock()
	assignedTaskStatus[fileName] = Processing
	mu.Unlock()

	go func() {
		time.Sleep(timeout)

		mu.Lock()
		defer mu.Unlock()
		if assignedTaskStatus[fileName] == Processing {
			// FIXME reduce tasks are erroneously falling here
			// -> the problem is likely to be in removeProcessedTasksFromQueue
			assignedTaskStatus[fileName] = TimedOut
			tasksQueue = append(tasksQueue, fileName)
			log.Printf("The completion of %q task has just timed out. It is back to the queue.\n", fileName)
		}
	}()

	log.Printf("%q will be assigned to a worker.\n", fileName)
	return fileName
}

func removeProcessedTasksFromQueue() {
	var processedTasks = findIntermediateFiles()
	if len(processedTasks) < nReduceTasks {
		return
	}
	var normalizedTaskNames = removeMapOutputPrefix(processedTasks)
	mu.Lock()
	defer mu.Unlock()
	for _, processedFileName := range normalizedTaskNames {
		if assignedTaskStatus[processedFileName] == Processing {
			assignedTaskStatus[processedFileName] = Processed
			removeFromArray(tasksQueue, processedFileName)
			log.Printf("Removing task %q from queue.\n", processedFileName)
		}
	}
}

func removeMapOutputPrefix(processedTasks []string) []string {
	var normalizedTaskNames = make([]string, len(processedTasks))
	for i, v := range processedTasks {
		normalizedTaskNames[i] = extractTaskName(v)
	}
	return normalizedTaskNames
}

func extractTaskName(fileName string) string {
	return removeSuffix(removePrefix(fileName))
}

func removePrefix(v string) string {
	var _, after, _ = strings.Cut(v, intermediateFileNamePrefix)
	return after
}

func removeSuffix(v string) string {
	var suffixIndex = strings.LastIndexByte(v, '-')
	return v[:suffixIndex] + ".txt" // gives extension back
}

func findIntermediateFiles() []string {
	files, err := filepath.Glob(intermediateFileNamePrefix + "*")
	if err != nil {
		panic(err)
	}
	return files
}

func removeFromArray(s []string, r string) []string {
	for i, v := range s {
		if v == r {
			return append(s[:i], s[i+1:]...)
		}
	}
	return s
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
	return done
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	tasksQueue = files
	nReduceTasks = nReduce

	log.Printf("Starting up coordinator with files: %s\n", files)
	log.Printf("Starting up coordinator for %d reduce tasks.", nReduceTasks)

	c.server()
	return &c
}
