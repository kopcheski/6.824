// coordinator_test.go
package mr

import (
	"fmt"
	"os"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestEmptyFileList(t *testing.T) {
	var files [][]string

	tasksQueue = files

	var worker = WorkerArgs{}
	var fileName = assignTask(worker)

	if len(fileName) > 0 {
		t.Fatalf(`No file was expected. Got %q instead`, fileName)
	}
}

func TestFromIntermediateToOutput(t *testing.T) {

	var intermediate = []string{"mr-pg-being_ernest-0.txt"}

	reduceKeyValue(intermediate, func(key string, values []string) string {
		return strconv.Itoa(len(values))
	})
}

func TestRemoveReduceCounterFromFileName(t *testing.T) {
	match, _ := regexp.MatchString("p([a-z]+)ch", "peach")
	fmt.Println(match)
}

func TestAssignTheFirstFile(t *testing.T) {
	tasksQueue = make([][]string, 2)
	tasksQueue[0] = append(tasksQueue[0], "pg-being_ernest.txt")
	tasksQueue[1] = append(tasksQueue[1], "pg-dorian_grey.txt")

	var worker = WorkerArgs{}
	var fileName = assignTask(worker)

	if fileName[0] != "pg-being_ernest.txt" {
		t.Fatalf(`%q is not the expect file name`, fileName)
	}
}

func TestAssignAllFilesUntilThereAreNoMoreFilesLeft(t *testing.T) {
	deleteFilesStartingWith(intermediateFileNamePrefix)
	defer deleteFilesStartingWith(intermediateFileNamePrefix)

	tasksQueue = make([][]string, 2)
	tasksQueue[0] = append(tasksQueue[0], "pg-being_ernest.txt")
	tasksQueue[1] = append(tasksQueue[1], "pg-dorian_grey.txt")

	var fileName1 = assignTask(WorkerArgs{})
	createFile(intermediateFileNamePrefix + "pg-being_ernest.txt")
	var fileName2 = assignTask(WorkerArgs{})
	createFile(intermediateFileNamePrefix + "pg-dorian_grey.txt")
	// starts assign reduce tasks
	var fileName3 = assignTask(WorkerArgs{})
	var fileName4 = assignTask(WorkerArgs{})
	var fileName5 = assignTask(WorkerArgs{})

	assert.Equal(t, "pg-being_ernest.txt", fileName1)
	assert.Equal(t, "pg-dorian_grey.txt", fileName2)
	assert.Equal(t, "mr-pg-being_ernest.txt", fileName3)
	assert.Equal(t, "mr-pg-dorian_grey.txt", fileName4)
	assert.Empty(t, fileName5)
}

func TestTaskGoesBackToQueueWhenExecutionTimesOut(t *testing.T) {
	var files = [2][1]string{}
	files[0][0] = "pg-being_ernest.txt"
	files[1][0] = "pg-dorian_grey.txt"

	tasksQueue = make([][]string, 2)
	tasksQueue[0] = append(tasksQueue[0], "pg-being_ernest.txt")
	tasksQueue[1] = append(tasksQueue[1], "pg-dorian_grey.txt")

	var fileName = assignTask(WorkerArgs{})
	time.Sleep(timeout + (1 + time.Second))

	assert.Equal(t, assignedTaskStatus[fileName[0]], TimedOut)
	assert.ElementsMatch(t, files, tasksQueue)
}

func TestCoordinatorIsDoneWhenThereAreNoMoreTasksToProcess(t *testing.T) {
	t.Skip("The current implementation of this Done is now invalid. Fix it.")
	//var files = [1]string{"pg-being_ernest.txt"}
	//
	//tasksQueue = files[0:1]
	//
	//var c = Coordinator{}
	//assert.False(t, c.Done())
	//
	//var worker = WorkerArgs{}
	//assignTask(worker)
	//
	//assert.True(t, c.Done())
}

func createFile(fileName string) {
	ofile, _ := os.Create(fileName)
	ofile.Close()
}
