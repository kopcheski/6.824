package mr

import (
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

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

// func logMessage(message string, args ...any) {
// 	// fmtMessage := formatMessage(message, args)
// 	// log.Printf("[Worker-%s] %s", workerId, fmtMessage)
// }

// func logPanic(message string, args ...any) {
// 	fmtMessage := formatMessage(message, args)
// 	log.Panicf("[Worker-%s] %s", workerId, fmtMessage)
// }

// func logPanicNoArg(message any) {
// 	log.Panicf("[Worker-%s] %s", workerId, message)
// }

// func formatMessage(message string, args []any) string {
// 	var fmtMessage = ""
// 	if len(args) > 0 {
// 		fmtMessage = fmt.Sprintf(message, args)
// 	} else {
// 		fmtMessage = message
// 	}
// 	return fmtMessage
// }

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	if workerId == "" {
		workerId = uuid.New().String()
		workerId = workerId[len(workerId)-5:]
	}

	// logMessage("Started.")
	for !jobDone {
		var reply = RequestTask()
		fileRelativePath = reply.RelativePath

		if len(reply.TaskFileNames) == 0 {
			// logMessage("Coordinator did not send a task to this worker.")
			time.Sleep(time.Duration(5) * time.Second)
			continue
		}

		if reply.Map {
			mapTextToKeyValue(reply.TaskFileNames[0], mapf)
		} else {
			reduceKeyValue(reply.TaskFileNames, reducef)
		}

		FinishTask(reply.TaskFileNames)
	}
	// logMessage("Finished.")
}

func reduceKeyValue(fileNames []string, reducef func(string, []string) string) {
	if len(fileNames) == 0 {
		return
	}

	kva := readIntermediateFilesToKeyValue(fileNames)
	var tempMap = make(map[int][]KeyValue)
	tempMap[0] = kva
	sortMap(tempMap)

	var index = getNReduceFromFileName(fileNames[0])
	outputFileName := fmt.Sprintf("mr-out-%d", index)
	var tempFileName = fmt.Sprintf("%s-%d", outputFileName, time.Now().Unix())
	var tempFile, _ = os.Create(tempFileName)

	var toReduce []string
	var previousKey string
	for i, v := range kva {
		if i == 0 {
			previousKey = v.Key
		}
		if previousKey == v.Key { // can be simplified
			toReduce = append(toReduce, v.Key)
		} else {
			if len(toReduce) == 0 {
				continue
			}
			reduced := reducef("does it matter?", toReduce)
			fmt.Fprintf(tempFile, "%v %v\n", previousKey, reduced)
			previousKey = v.Key
			toReduce = nil
			toReduce = append(toReduce, v.Key)
		}
	}

	tempFile.Close()
	os.Rename(tempFileName, outputFileName)
	// logMessage("Finished reducing the file %q.", fileNames)
}

func getNReduceFromFileName(fileName string) int {
	r, _ := regexp.Compile("[\\d]")
	var nReduceIndex, _ = strconv.Atoi(r.FindString(fileName))
	return nReduceIndex
}

func readIntermediateFilesToKeyValue(fileNames []string) []KeyValue {
	jsonArrayAll := []KeyValue{}
	for _, fileName := range fileNames {
		var fileNameWithPath = filepath.Join(fileRelativePath, fileName)
		var bytes, err = os.ReadFile(fileNameWithPath)
		if err != nil {
			// logPanicNoArg(err)
		}

		var fileContent = string(bytes)

		jsonArray := []KeyValue{}
		json.Unmarshal([]byte(fileContent), &jsonArray)
		for _, v := range jsonArray {
			jsonArrayAll = append(jsonArrayAll, v)
		}
	}
	return jsonArrayAll
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
	var fileNamePrefix = "mr-" + b
	writeToIntermediateFiles(intermediateMap, fileNamePrefix)
}

func writeToIntermediateFiles(intermediateMap map[int][]KeyValue, fileNamePrefix string) {
	for key, element := range intermediateMap {
		jsonStr, err := json.Marshal(element)

		var fileName = fileNamePrefix + "-" + fmt.Sprint(key) + ".txt"
		var oname = filepath.Join(fileRelativePath, fileName)
		var tempFileName = fmt.Sprintf("%s-%d", oname, time.Now().Unix())
		var tempFile, _ = os.Create(tempFileName)

		if err != nil {
			// logMessage("Error: %s", err.Error())
		} else {
			// logMessage("Writing file %q", oname)
			fmt.Fprintf(tempFile, "%v\n", string(jsonStr))
		}

		tempFile.Close()
		os.Rename(tempFileName, oname)
	}

}

func sortMap(intermediateMap map[int][]KeyValue) {
	for _, element := range intermediateMap {
		sort.Sort(ByKey(element))
	}
}

func splitIntoBuckets(kva []KeyValue) map[int][]KeyValue {
	var intermediateMap = make(map[int][]KeyValue)
	for _, element := range kva {
		var nReduceKey = ihash(fmt.Sprint(element.Key)) % nReduce
		intermediateMap[nReduceKey] = append(intermediateMap[nReduceKey], element)
	}
	return intermediateMap
}

func readFileToString(fileName string) string {
	var fileNameWithPath = filepath.Join(fileRelativePath, fileName)
	file, err := os.Open(fileNameWithPath)
	if err != nil {
		// logMessage("cannot open %v", fileNameWithPath)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		// logMessage("cannot read %v", fileNameWithPath)
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
		// logMessage("Got tasks %v to process.", reply.TaskFileNames)
	} else {
		// logPanic("[Worker-%s] call failed!\n", workerId)
	}
	return reply

}

func FinishTask(taskNames []string) {
	// logMessage("Finished processing %q, notifying coordinator.", taskNames)
	args := WorkerArgs{taskNames}
	reply := CoordinatorReply{}
	ok := call("Coordinator.FinishTask", &args, &reply)
	if !ok {
		log.Panicf("[Worker-%s] call to FinishTask failed!\n", workerId)
	}
	// logMessage("Coordinator notified about %q.", taskNames)
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
