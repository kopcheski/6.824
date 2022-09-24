package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
	"github.com/google/uuid"
)

var intermediateFileNamePrefix = "mr-"

var nReduce int

var fileRelativePath string

var jobDone = false

var workerId = ""

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

func logMessage(message string, args ...string) {
	var fmtMessage = ""
	if (len(args) > 0 ) {
		fmtMessage = fmt.Sprintf(message, args)
	} else {
		fmtMessage = message
	}
	log.Printf("[Worker-%s] %s", workerId, fmtMessage)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	if (workerId == "") {
		workerId = uuid.New().String()
		workerId = workerId[len(workerId)-5:]
	}

	logMessage("Started.")
	for !jobDone {	
		var reply = RequestTask()
		fileRelativePath = reply.RelativePath
		
		if reply.TaskFileName == "" {
			log.Println("Coordinator did not send a task to this worker.")
			time.Sleep(time.Duration(5) * time.Second)
			continue
		} 
	
		if reply.Map {
			mapTextToKeyValue(reply.TaskFileName, mapf)
		} else {
			reduceKeyValue(reply.TaskFileName, reducef)
		} 
	
		FinishTask(reply.TaskFileName)
	}
	logMessage("Finished.")
}

func reduceKeyValue(fileName string, reducef func(string, []string) string) {
	if len(fileName) == 0 {
		return
	}

	kva := readIntermediateFileToKeyValue(fileName)

	oname := "mr-out-0"
	ofile, err := os.Create(oname)
	if (err != nil) {
		log.Panic(err)
	}
	defer ofile.Close()

	var toReduce []string
	var previousKey string
	for i, v := range kva {
		if i == 0 {
			previousKey = v.Key
		}
		if previousKey == v.Key {
			toReduce = append(toReduce, v.Key)
		} else {
			reduced := reducef(fileName, toReduce)
			previousKey = v.Key
			toReduce = nil
			fmt.Fprintf(ofile, "%v %v\n", v.Key, reduced)
		}
	}
	logMessage("Finished reducing the file %q.", fileName)
	var errRemove = os.Remove(filepath.Join(fileRelativePath, fileName))
	if errRemove != nil {
		log.Panic(errRemove)
	}
}

func readIntermediateFileToKeyValue(fileName string) []KeyValue {
	var fileNameWithPath = filepath.Join(fileRelativePath, fileName)
	var bytes, err = ioutil.ReadFile(fileNameWithPath)
	if (err != nil) {
		log.Panic(err)
	}

	var fileContent = string(bytes)
	jsonArray := []KeyValue{}
 
    json.Unmarshal([]byte(fileContent), &jsonArray)
	return jsonArray
}

func mapTextToKeyValue(fileName string, mapf func(string, string) []KeyValue) {
	if len(fileName) == 0 {
		return
	}

	content := readFileToString(fileName)

	kva := mapf(fileName, content)

	intermediateMap := splitIntoBuckets(kva)

	sortMap(intermediateMap)

	var b, _, _ = strings.Cut(fileName, ".txt")
	var fileNamePrefix = intermediateFileNamePrefix + b
	writeToIntermediateFiles(intermediateMap, fileNamePrefix)
}

func writeToIntermediateFiles(intermediateMap map[int][]KeyValue, fileNamePrefix string) {
	for key, element := range intermediateMap {
		jsonStr, err := json.Marshal(element)

		var fileName = fileNamePrefix + "-" + fmt.Sprint(key) + ".txt"
		oname := filepath.Join(fileRelativePath, fileName)
		ofile, _ := os.Create(oname)

		if err != nil {
			logMessage("Error: %s", err.Error())
		} else {
			logMessage("Writing file %q", oname)
			fmt.Fprintf(ofile, "%v\n", string(jsonStr))
		}

		ofile.Close()
	}

}

func sortMap(intermediateMap map[int][]KeyValue) {
	for _, element := range intermediateMap {
		sort.Sort(ByKey(element))
	}
}

func splitIntoBuckets(kva []KeyValue) map[int][]KeyValue {
	var intermediateMap = make(map[int][]KeyValue)

	for key, element := range kva {
		var nReduceKey = ihash(fmt.Sprint(key)) % nReduce
		intermediateMap[nReduceKey] = append(intermediateMap[nReduceKey], element)
	}
	return intermediateMap
}

func readFileToString(fileName string) string {
	var fileNameWithPath = filepath.Join(fileRelativePath, fileName)
	file, err := os.Open(fileNameWithPath)
	if err != nil {
		logMessage("cannot open %v", fileNameWithPath)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		logMessage("cannot read %v", fileNameWithPath)
	}
	file.Close()
	return string(content)
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func RequestTask() CoordinatorReply {

	// declare an argument structure.
	args := WorkerArgs{}

	// declare a reply structure.
	reply := CoordinatorReply{}
	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		nReduce = reply.NReduceTasks
		logMessage("Got task %v to process.", reply.TaskFileName)
	} else {
		log.Panicf("[Worker-%s] call failed!\n", workerId)
	}
	return reply

}

func FinishTask(taskName string) {
	logMessage("Finished processing %q, notifying coordinator.", taskName)
	args := WorkerArgs{taskName}
	reply := CoordinatorReply{}
	ok := call("Coordinator.FinishTask", &args, &reply)
	if !ok {
		log.Panicf("[Worker-%s] call to FinishTask failed!\n", workerId)
	}
	logMessage("Coordinator notified about %q.", taskName)
	jobDone = reply.JobDone
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal(fmt.Sprintf("[Worker-%s] dialing:", workerId), err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	log.Println(err)
	return false
}
