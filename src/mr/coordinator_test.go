// coordinator_test.go
package mr

import (
	"testing"
)

func TestEmptyFileList(t *testing.T) {
	var files []string

	filesList = files

	var fileName = assignTask()

	if fileName != "" {
		t.Fatalf(`No file was expected. Got %q instead`, fileName)
	}
}

func TestAssignTheFirstFile(t *testing.T) {
	var files = [2]string{"pg-being_ernest.txt", "pg-dorian_grey"}

	filesList = files[0:2]

	var fileName = assignTask()

	if fileName != "pg-being_ernest.txt" {
		t.Fatalf(`%q is not the expect file name`, fileName)
	}
}
