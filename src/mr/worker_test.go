package mr

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReadFileToMap(t *testing.T) {
	assert.Equal(t, "this is the content of the sample file", readFileToString("sample.txt"))
}

func TestSplitIntoBuckets(t *testing.T) {

	var intermediate [3]KeyValue
	intermediate[0] = KeyValue{"house", "1"}
	intermediate[1] = KeyValue{"sky", "1"}
	intermediate[2] = KeyValue{"boat", "1"}

	nReduceTasks = 2
	var intemediateMap = splitIntoBuckets(intermediate[:])

	assert.Equal(t, nReduceTasks, len(intemediateMap))
}

func TestSortMap(t *testing.T) {

	var intermediate [3]KeyValue
	intermediate[0] = KeyValue{"house", "1"}
	intermediate[1] = KeyValue{"sky", "1"}
	intermediate[2] = KeyValue{"boat", "1"}

	var intermediateMap = make(map[int][]KeyValue)
	intermediateMap[0] = intermediate[0:3]

	var expected [3]KeyValue
	expected[0] = KeyValue{"boat", "1"}
	expected[1] = KeyValue{"house", "1"}
	expected[2] = KeyValue{"sky", "1"}

	sortMap(intermediateMap)
	assert.EqualValues(t, expected[0:], intermediateMap[0])
}

func TestWriteMapToFiles(t *testing.T) {
	var fileNamePrefix = "mr-"
	deleteFilesStartingWith(fileNamePrefix)

	var intermediate [3]KeyValue
	intermediate[0] = KeyValue{"house", "1"}
	intermediate[1] = KeyValue{"sky", "1"}
	intermediate[2] = KeyValue{"boat", "1"}

	var intermediateMap = make(map[int][]KeyValue)
	intermediateMap[0] = intermediate[0:2]
	intermediateMap[1] = intermediate[2:3]

	writeToFiles(intermediateMap, fileNamePrefix)

	assert.Equal(t, "{house 1}\n{sky 1}\n", readFileToString("mr-0"))
	assert.Equal(t, "{boat 1}\n", readFileToString("mr-1"))

	defer deleteFilesStartingWith(fileNamePrefix)
}

func deleteFilesStartingWith(fileNamePrefix string) {
	files, err := filepath.Glob(fileNamePrefix + "*")
	if err != nil {
		panic(err)
	}
	for _, f := range files {
		if err := os.Remove(f); err != nil {
			panic(err)
		}
	}
}
