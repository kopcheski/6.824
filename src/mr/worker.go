package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
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

	// it gets two functions, the map and the reduce.
	// how to decide which one to call?
	// -> it depends on the coordinator response.
	// -> if beginsWith pg*, map. otherwise, reduce.
	// coord still does not know how to handle files create by the map function. How to inform it?
	// via the request? Looks more efficient than polling the filesystem
	// BUT, the coord will _have_ to poll the FS, as stated by the rules.

	var reply = RequestTask()

	Map(reply, mapf)
	// MAP impl:
	// read the content of the file and send it as the 2nd arg, 1st must be ignored.
	// it will return a KV pair
	// write it to a file
	// add it to the response.

	// REDUCE impl:

}

func Map(fileName string, mapf func(string, string) []KeyValue) {
	if len(fileName) == 0 {
		return
	}

	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	file.Close()
	kva := mapf(fileName, string(content))

	for key, element := range kva {
		var nReduce = ihash(string(key)) % nReduceTasks
		// TODO copy from the mrsequential.go
	}

	// A reasonable naming convention for intermediate files is mr-X-Y,
	// where X is the Map task number, and Y is the reduce task number.

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func RequestTask() string {

	// declare an argument structure.
	args := WorkerArgs{}

	// fill in the argument(s).
	args.processedFileName = ""
	args.workerName = ""

	// declare a reply structure.
	reply := FileNameReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		fmt.Printf("Got task %v to process.\n", reply.taskFileName)
		return reply.taskFileName
	} else {
		fmt.Printf("call failed!\n")
		return ""
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
