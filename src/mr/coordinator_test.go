// coordinator_test.go
package mr

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestEmptyFileList(t *testing.T) {
	var files []string

	tasksQueue = files

	var worker = WorkerArgs{"Worker-1", ""}
	var fileName = assignTask(worker)

	if fileName != "" {
		t.Fatalf(`No file was expected. Got %q instead`, fileName)
	}
}

func TestAssignTheFirstFile(t *testing.T) {
	var files = [2]string{"pg-being_ernest.txt", "pg-dorian_grey.txt"}

	tasksQueue = files[0:2]

	var worker = WorkerArgs{"Worker-1", ""}
	var fileName = assignTask(worker)

	if fileName != "pg-being_ernest.txt" {
		t.Fatalf(`%q is not the expect file name`, fileName)
	}
}

func TestAssignAllFilesUntilThereAreNoMoreFilesLeft(t *testing.T) {
	var files = [2]string{"pg-being_ernest.txt", "pg-dorian_grey.txt"}

	tasksQueue = files[0:2]

	var fileName1 = assignTask(WorkerArgs{"Worker-1", ""})
	var fileName2 = assignTask(WorkerArgs{"Worker-2", ""})
	var fileName3 = assignTask(WorkerArgs{"Worker-1", fileName1})

	assert.Equal(t, "pg-being_ernest.txt", fileName1)
	assert.Equal(t, "pg-dorian_grey.txt", fileName2)
	assert.Empty(t, fileName3)
}

func TestTaskGoesBackToQueueWhenExecutionTimesOut(t *testing.T) {
	var files = [2]string{"pg-being_ernest.txt", "pg-dorian_grey.txt"}

	tasksQueue = files[0:2]

	var fileName = assignTask(WorkerArgs{"Worker-1", ""})
	time.Sleep(timeout + (1 + time.Second))

	assert.Equal(t, assignedTaskStatus[fileName], TimedOut)
	assert.ElementsMatch(t, files, tasksQueue)
}

func TestCoordinatorIsDoneWhenThereAreNoMoreTasksToProcess(t *testing.T) {
	var files = [1]string{"pg-being_ernest.txt"}

	tasksQueue = files[0:1]

	var c = Coordinator{}
	assert.False(t, c.Done())

	var worker = WorkerArgs{"Worker-1", ""}
	assignTask(worker)

	assert.True(t, c.Done())
}
