// coordinator_test.go
package mr

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEmptyFileList(t *testing.T) {
	var files []string

	filesList = files

	var worker = WorkerArgs{"Worker-1", ""}
	var fileName = assignTask(worker)

	if fileName != "" {
		t.Fatalf(`No file was expected. Got %q instead`, fileName)
	}
}

func TestAssignTheFirstFile(t *testing.T) {
	var files = [2]string{"pg-being_ernest.txt", "pg-dorian_grey.txt"}

	filesList = files[0:2]

	var worker = WorkerArgs{"Worker-1", ""}
	var fileName = assignTask(worker)

	if fileName != "pg-being_ernest.txt" {
		t.Fatalf(`%q is not the expect file name`, fileName)
	}
}

func TestAssignAllFilesUntilThereAreNoMoreFilesLeft(t *testing.T) {
	var files = [2]string{"pg-being_ernest.txt", "pg-dorian_grey.txt"}

	filesList = files[0:2]

	var fileName1 = assignTask(WorkerArgs{"Worker-1", ""})
	var fileName2 = assignTask(WorkerArgs{"Worker-2", ""})
	var fileName3 = assignTask(WorkerArgs{"Worker-1", fileName1})

	assert.Equal(t, "pg-being_ernest.txt", fileName1)
	assert.Equal(t, "pg-dorian_grey.txt", fileName2)
	assert.Empty(t, fileName3)
}
