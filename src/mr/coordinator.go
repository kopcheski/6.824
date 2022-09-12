package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"strings"
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
	assignedTaskStatus[fileName] = Processing

	go func() {
		time.Sleep(timeout)
		// function to verify timed out tasks
		if assignedTaskStatus[fileName] == Processing {
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
	if len(processedTasks) == 0 {
		return
	}
	// TODO
	// worker creates nReduce intermediate files per task
	// this next function does not check that properly.
	// Ideally it should return the task name, instead it is also
	// returning the task file number too.
	// --> the normalization should remove this number.
	// --> does it any value to keep on handling the file extension?
	var normalizedTaskNames = removeMapOutputPrefix(processedTasks)
	for _, processedFileName := range normalizedTaskNames {
		if assignedTaskStatus[processedFileName] == Processing {
			assignedTaskStatus[processedFileName] = Processed
			removeFromArray(tasksQueue, processedFileName)
			log.Printf("Removing task %q from queue.\n", processedFileName)
		}
		// else {
		// 	log.Printf("%q has already timed out to process task %q. Another one should be assigned to it now.\n",
		// 		args.workerName, args.processedFileName)
		// }
	}
}

func removeMapOutputPrefix(processedTasks []string) []string {
	var normalizedTaskNames = make([]string, len(processedTasks))
	for i, v := range processedTasks {
		var _, after, _ = strings.Cut(v, intermediateFileNamePrefix)
		normalizedTaskNames[i] = after
	}
	return normalizedTaskNames
}

func findIntermediateFiles() []string {
	files, err := filepath.Glob("mr-*")
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
