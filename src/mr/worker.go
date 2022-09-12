package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
)

var intermediateFileNamePrefix = "mr-"

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

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// coord still does not know how to handle files create by the map function. How to inform it?
	// the coord will _have_ to poll the FS, as stated by the rules.

	var reply = RequestTask()

	if strings.HasPrefix(reply, intermediateFileNamePrefix) {
		reduceKeyValue(reply, reducef)
	} else if strings.HasPrefix(reply, "pg-") {
		mapTextToKeyValue(reply, mapf)
	} else {
		log.Panicf("no appropiate function for file %q.", reply)
	}

}

func reduceKeyValue(fileName string, reducef func(string, []string) string) {
	if len(fileName) == 0 {
		return
	}

	content, err := readLines(fileName)
	if err != nil {
		log.Panicf("error reading file %q.\n%q", fileName, err)
	}

	reduced := reducef(fileName, content)
	log.Printf("%q was reduced to %q", content, reduced)

	// REDUCE impl:
	// [ ] write output to mr-out-X
	// [ ] one line per return of the reducef call
	// [ ] line should be formatted as "%v %v", key and value respectively

}

func readLines(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	return lines, scanner.Err()
}

func mapTextToKeyValue(fileName string, mapf func(string, string) []KeyValue) {
	if len(fileName) == 0 {
		return
	}

	content := readFileToString(fileName)

	kva := mapf(fileName, content)

	intermediateMap := splitIntoBuckets(kva)

	sortMap(intermediateMap)

	var fileNamePrefix = intermediateFileNamePrefix + fileName
	writeToFiles(intermediateMap, fileNamePrefix)
}

func writeToFiles(intermediateMap map[int][]KeyValue, fileNamePrefix string) {
	for key, element := range intermediateMap {

		oname := fileNamePrefix + fmt.Sprint(key)
		ofile, _ := os.Create(oname)

		for _, s := range element {
			fmt.Fprintf(ofile, "%v\n", s)
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
		var nReduce = ihash(fmt.Sprint(key)) % nReduceTasks
		intermediateMap[nReduce] = append(intermediateMap[nReduce], element)
	}
	return intermediateMap
}

func readFileToString(fileName string) string {
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	file.Close()
	return string(content)
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
