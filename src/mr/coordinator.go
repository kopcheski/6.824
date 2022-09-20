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

var relativePath string

var nFilesToProcess int

var nFilesProcessed int

type Coordinator struct {
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *WorkerArgs, reply *CoordinatorReply) error {
	mu.Lock()
	defer mu.Unlock()

	log.Printf("%d/%d tasks processed.", nFilesProcessed, nFilesToProcess)
	if done {
		reply.TaskFileName = ""
		return nil
	}

	reply.TaskFileName = assignTask(WorkerArgs{})
	reply.NReduceTasks = nReduceTasks
	reply.Map = !reduceTasksStarted
	reply.RelativePath = relativePath

	return nil
}

func (c *Coordinator) FinishTask(args *WorkerArgs, reply *CoordinatorReply) error {
	mu.Lock()
	defer mu.Unlock()

	var taskName = args.TaskFileName
	if isProcessed(taskName) {
		removeTaskFromQueue(taskName)
	} else {
		log.Printf("[Coordinator] Task %q was requested to be removed from the queue, but it is not processed yet", taskName)
	}
	reply.JobDone = done

	return nil
}

func assignTask(args WorkerArgs) string {

	removeProcessedTasksFromQueue()

	allMapTasksProcessed := len(tasksQueue) == 0 && !reduceTasksStarted
	if allMapTasksProcessed {
		tasksQueue = findIntermediateFiles("")
		reduceTasksStarted = true
		log.Println("[Coordinator] All map tasks are finished.")
		log.Println("[Coordinator] Starting up reduce tasks.")
	}

	if len(tasksQueue) > 0 {
		return nextAvailableTask(args)
	}

	log.Println("[Coordinator] No more files to assign.")
	markDone()
	return ""
}

func markDone() {
	done = true
}

func nextAvailableTask(args WorkerArgs) string {
	var fileName = tasksQueue[0]
	tasksQueue = tasksQueue[1:]
	assignedTaskStatus[fileName] = Processing

	go func() {
		time.Sleep(timeout)

		mu.Lock()
		defer mu.Unlock()
		if assignedTaskStatus[fileName] == Processing {
			assignedTaskStatus[fileName] = TimedOut
			tasksQueue = append(tasksQueue, fileName)
			log.Printf("[Coordinator] The completion of %q task has just timed out. It is back to the queue.\n", fileName)
		}
	}()

	log.Printf("[Coordinator] %q will be assigned to a worker.\n", fileName)
	return fileName
}

func removeProcessedTasksFromQueue() {
	for key := range assignedTaskStatus {
		if isProcessed(key) {
			removeTaskFromQueue(key)
		}
	}
}

func removeTaskFromQueue(taskName string) {
	assignedTaskStatus[taskName] = Processed
	removeFromArray(tasksQueue, taskName)
	log.Printf("[Coordinator] Task %q was processed. Removing it from queue.\n", taskName)
	nFilesProcessed += 1
}

func isProcessed(taskName string) bool {
	if (reduceTasksStarted) {
		var processedTasks = findIntermediateFiles(taskName)
		return len(processedTasks) == 0
	} else {
		var processedTasks = findIntermediateFiles(taskName)
		return len(processedTasks) == nReduceTasks
	}
}

func findIntermediateFiles(taskName string) []string {
	var fileNameWithoutExtension = strings.TrimSuffix(taskName, filepath.Ext(taskName))
	var fileNamePattern = intermediateFileNamePrefix + fileNameWithoutExtension + "*"
	var fileNamePatterWithPath = filepath.Join(relativePath, fileNamePattern)
	log.Printf("[Coordinator] Looking for intermediate files: %q", fileNamePatterWithPath)
	files, err := filepath.Glob(fileNamePatterWithPath)
	if err != nil {
		panic(err)
	}
	for i, v := range files {
		files[i] = filepath.Base(v)
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
		log.Fatal("[Coordinator] listen error:", e)
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

	relativePath = filepath.Dir(files[0])
	nReduceTasks = nReduce
	for _, v := range files {
		tasksQueue = append(tasksQueue, filepath.Base(v))
	}

	nFilesToProcess = len(files) * (nReduce + 1);

	log.Printf("[Coordinator] Starting up coordinator with files: %s\n", files)
	log.Printf("[Coordinator] Starting up coordinator for %d reduce tasks.", nReduceTasks)

	c.server()
	return &c
}
