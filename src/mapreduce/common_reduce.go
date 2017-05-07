package mapreduce

import (
	"encoding/json"
	"log"
	"os"
	"sort"
)

// doReduce manages one reduce task: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	kvs := make(map[string][]string)
	for i := 0; i < nMap; i++ {
		fileName := reduceName(jobName, i, reduceTaskNumber)
		file, err := os.Open(fileName)
		if err != nil {
			log.Panic(err)
		}
		defer file.Close()
		var kv KeyValue
		decoder := json.NewDecoder(file)
		for {
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			kvs[kv.Key] = append(kvs[kv.Key], kv.Value)
		}
	}

	// reduceF(k2, list(v2)) -> list(v2)
	keys := []string{}
	for key := range kvs {
		keys = append(keys, key)
	}

	sort.Strings(keys)
	outF, err := os.Create(outFile)
	if err != nil {
		log.Panic(err)
	}
	defer outF.Close()
	encoder := json.NewEncoder(outF)

	for _, key := range keys {
		err := encoder.Encode(&KeyValue{key, reduceF(key, kvs[key])})
		if err != nil {
			log.Panic(err)
		}
	}
}
