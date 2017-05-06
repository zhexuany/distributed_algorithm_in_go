package mapreduce

import (
	"encoding/json"
	"hash/fnv"
	"io/ioutil"
	"log"
	"os"
)

// doMap manages one map task: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
func doMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {
	// first get the actual contents from disk
	contents, _ := ioutil.ReadFile(inFile) // skip error, reading from disk is unlikely failed.
	fileHanlde := make([]*os.File, nReduce)

	// Create file handles for future reduce.
	for i := 0; i < nReduce; i++ {
		file, _ := os.Create(reduceName(jobName, mapTaskNumber, i))
		fileHanlde[i] = file
	}

	// mapF(key, value) -> list(k2, v2)
	for _, kv := range mapF(inFile, string(contents)) {
		// In industry, this usually is done via protubuf or thrift.
		file := fileHanlde[ihash(kv.Key)%nReduce]
		encoder := json.NewEncoder(file)
		if err := encoder.Encode(&kv); err != nil {
			log.Printf("failed to encoder kv pair %v with error % v on file %s.", kv, err, file.Name())
		}
	}

	for _, file := range fileHanlde {
		_ = file.Close()
	}
}

func ihash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() & 0x7fffffff)
}
