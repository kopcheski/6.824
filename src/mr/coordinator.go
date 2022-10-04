package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
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

var intermediateFileNamePrefix = "mr-"

var tasksQueue [][]string

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

func (c *Coordinator) Example(args *WorkerArgs, reply *CoordinatorReply) error {
	mu.Lock()
	defer mu.Unlock()

	if done {
		reply.TaskFileNames = []string{}
		return nil
	}

	reply.TaskFileNames = assignTask(WorkerArgs{})
	reply.NReduceTasks = nReduceTasks
	reply.Map = !reduceTasksStarted
	reply.RelativePath = relativePath

	return nil
}

func (c *Coordinator) FinishTask(args *WorkerArgs, reply *CoordinatorReply) error {
	mu.Lock()
	defer mu.Unlock()

	var taskNames = args.TaskFileNames
	log.Printf("[Coordinator] Received request to finish task %q.", taskNames)
	markAsProcessed(taskNames[0])
	reply.JobDone = done

	return nil
}

func assignTask(args WorkerArgs) []string {

	allMapTasksProcessed := isAllTasksProcessed() && !reduceTasksStarted
	if allMapTasksProcessed {
		var intermediateFiles = findIntermediateFiles("pg")
		tasksQueue = groupIntermediateFiles(intermediateFiles)
		reduceTasksStarted = true
		assignedTaskStatus = make(map[string]TaskStatus)
		log.Println("[Coordinator] All map tasks are finished.")
		log.Println("[Coordinator] Starting up reduce tasks.")
	}

	if len(tasksQueue) > 0 {
		return nextAvailableTasks(args)
	}

	if areThereTasksBeingProcessed() {
		log.Println("[Coordinator] Queue is empty but there are tasks being processed.")
		return []string{}
	} else {
		log.Println("[Coordinator] No more files to assign.")
		markDone()
		return []string{}
	}
}

func groupIntermediateFiles(files []string) [][]string {
	var queue = make([][]string, nReduceTasks)
	for _, fileName := range files {
		r, _ := regexp.Compile("[\\d]")
		var nReduceIndex, _ = strconv.Atoi(r.FindString(fileName))
		queue[nReduceIndex] = append(queue[nReduceIndex], fileName)
	}
	return queue
}

func markDone() {
	done = true
}

func nextAvailableTasks(args WorkerArgs) []string {
	var fileNames = tasksQueue[0]
	tasksQueue = tasksQueue[1:]
	if len(fileNames) == 0 {
		log.Println("[Coordinator] Queue is empty. Sending \"\" to worker with no extra control.")
		return []string{}
	}
	assignedTaskStatus[fileNames[0]] = Processing

	go func() {
		time.Sleep(timeout)

		mu.Lock()
		defer mu.Unlock()
		if assignedTaskStatus[fileNames[0]] == Processing {
			assignedTaskStatus[fileNames[0]] = TimedOut
			tasksQueue = append(tasksQueue, fileNames)
			log.Printf("[Coordinator] The completion of %q task has just timed out. It is back to the queue.\n", fileNames)
		}
	}()

	log.Printf("[Coordinator] %q will be assigned to a worker.\n", fileNames)
	return fileNames
}

func isAllTasksProcessed() bool {
	var allProcessed = true
	for key := range assignedTaskStatus {
		if !isProcessed(key) {
			allProcessed = false
			break
		}
	}
	return allProcessed && len(assignedTaskStatus) > 0
}

func areThereTasksBeingProcessed() bool {
	var processing = false
	for key := range assignedTaskStatus {
		if assignedTaskStatus[key] == Processing {
			processing = true
			break
		}
	}
	return processing && len(assignedTaskStatus) > 0
}

func markAsProcessed(taskName string) {
	log.Printf("[Coordinator] Marking %q as processed.\n", taskName)
	assignedTaskStatus[taskName] = Processed
	nFilesProcessed = nFilesProcessed + 1
}

func isProcessed(taskName string) bool {
	if reduceTasksStarted {
		var processedTasks = findIntermediateFiles(removeReduceCounterFromFileName(taskName))
		return len(processedTasks) == 0
	} else {
		var processedTasks = findIntermediateFiles(taskName)
		return len(processedTasks) == nReduceTasks
	}
}

func removeReduceCounterFromFileName(taskName string) string {
	var lastDash = strings.LastIndexByte(taskName, '-')
	if lastDash == -1 {
		log.Printf("[Coordinator] No dashes found in %q.", taskName)
		return taskName
	}
	return taskName[:lastDash]
}

func findIntermediateFiles(taskName string) []string {
	var intermediatePrefix = intermediateFileNamePrefix
	if strings.HasPrefix(taskName, intermediateFileNamePrefix) {
		intermediatePrefix = "" //smelly, but to avoid unwanted files to be found
	}
	var fileNameWithoutExtension = strings.TrimSuffix(taskName, filepath.Ext(taskName))
	var fileNamePattern = intermediatePrefix + fileNameWithoutExtension + "*"
	var fileNamePatterWithPath = filepath.Join(relativePath, fileNamePattern)
	// logMessage("Searching for file with the expression: %q", fileNamePatterWithPath)
	files, err := filepath.Glob(fileNamePatterWithPath)
	if err != nil {
		panic(err)
	}
	var intermediateFilesArray = make([]string, len(files))
	for i, v := range files {
		if strings.Contains(v, "mr-tmp") { // skips known directory
			log.Printf("[Coordinator] Skipping %q as a intermediate file.", v)
		} else {
			intermediateFilesArray[i] = filepath.Base(v)
		}
	}
	return intermediateFilesArray
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
	mu.Lock()
	defer mu.Unlock()
	return done
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	tasksQueue = make([][]string, len(files))
	relativePath = filepath.Dir(files[0])
	nReduceTasks = nReduce
	for i, v := range files {
		tasksQueue[i] = append(tasksQueue[i], filepath.Base(v))
	}

	nFilesToProcess = len(files) * (nReduce + 1)

	log.Printf("[Coordinator] Starting up coordinator with files: %s\n", files)
	log.Printf("[Coordinator] Starting up coordinator for %d reduce tasks.", nReduceTasks)

	c.server()
	return &c
}
