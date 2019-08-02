package mapreduce

import (
	"sort"
	"os"
	"encoding/json"
	"log"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//
	// Prepare and decode each of the files
	kvs := make([]KeyValue, 0)
	for i := 0; i < nMap; i++ {
		var kv KeyValue
		inFileName := reduceName(jobName, i, reduceTask)
		inFile, _ := os.Open(inFileName)
		dec := json.NewDecoder(inFile)
		for dec.More() {
			err := dec.Decode(&kv)
			if err != nil {
				log.Fatal(err)
			}
			kvs = append(kvs, kv)	
		}
	}

	// Sort by key
	sort.Slice(kvs, func(i, j int) bool {
		return kvs[i].Key < kvs[j].Key
	})

	// Go through the kvs, segment with every single key
	oFile, _ := os.Create(outFile)
	defer oFile.Close()
	enc := json.NewEncoder(oFile)
	var values []string
	var key string
	for i, kv := range kvs {
		if i == 0 || kvs[i - 1].Key != kv.Key {
			if i > 0 {
				err := enc.Encode(KeyValue{key, reduceF(key, values)})
				if err != nil {
					log.Fatal(err)
				}
			}
			key = kv.Key
			values = make([]string, 0)
		}
		values = append(values, kv.Value)
	}
}
