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

var filesList []string
var assignedTasks map[string]time.Time

type Coordinator struct {
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *FileNameReply) error {

	reply.fileName = assignTask()

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

func assignTask() string {

	var fileName string

	if len(filesList) == 0 {
		fmt.Println("All files were served!")
		return ""
	}

	fileName = filesList[0]
	filesList = filesList[1:]

	return fileName
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	filesList = files

	c.server()
	return &c
}
