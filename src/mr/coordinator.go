package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

const timeout = time.Duration(10) * time.Second

type TaskStatus int

const (
	Processing TaskStatus = iota
	TimedOut
	Processed
)

var filesList []string

var assignedTasks = make(map[string]TaskStatus)

type Coordinator struct {
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *WorkerArgs, reply *FileNameReply) error {

	reply.taskFileName = assignTask(WorkerArgs{args.workerName, args.processedFileName})

	// TODO
	// receive request;
	// first file from list
	// empty?
	//   find older than 10s in the map
	//   empty?
	//      work is over
	//   reasign current time to map's value
	// add to the map with current timestamp

	return nil
}

func assignTask(args WorkerArgs) string {
	fmt.Print("\n\nAvailable tasks: ")
	fmt.Println(filesList)
	fmt.Print("Assigned tasks: ")
	fmt.Println(assignedTasks)
	if args.processedFileName != "" { // to remove already processed tasks from queue
		if assignedTasks[args.processedFileName] == Processing {
			assignedTasks[args.processedFileName] = Processed // non-thread safe with go func
			removeFromArray(filesList, args.processedFileName)
			fmt.Printf("%q finalized processing %q. Removing task from queue.\n",
				args.workerName, args.processedFileName)
		} else {
			fmt.Printf("%q has already timed out to process task %q. Another one should be assigned to it now.\n",
				args.workerName, args.processedFileName)
		}
	}

	var fileName string

	if len(filesList) == 0 {
		fmt.Println("No more files to assign.")
		return ""
	}

	fileName = filesList[0]
	filesList = filesList[1:]
	assignedTasks[fileName] = Processing

	fmt.Print("Scheduled at: ")
	fmt.Println(time.Now())
	go func() {
		if assignedTasks[fileName] == Processing { // function to verify timed out tasks
			assignedTasks[fileName] = TimedOut
			filesList = append(filesList, fileName)
			fmt.Printf("The completion of %q task has just timed out. It is back to the queue.\n", fileName)

			fmt.Print("Executed at: ")
			fmt.Println(time.Now())
			time.Sleep(timeout) // TODO should be in or out of the condition?
		}
	}()

	fmt.Printf("%q will be assigned to worker %q.\n", fileName, args.workerName)
	return fileName
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
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	filesList = files

	c.server()
	return &c
}
