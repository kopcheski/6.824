package mr

import (
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
