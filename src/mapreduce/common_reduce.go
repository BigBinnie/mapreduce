package mapreduce

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
)
type ByKey [] KeyValue
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool {
	return  a[i].Key< a[j].Key
}

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
	// 1. Read one intermediate file from each map task
	var kvs []KeyValue
	for i := 0; i < nMap; i++ {
		var filename = reduceName(jobName, i, reduceTask)
		rf,err1 := os.OpenFile(filename,os.O_RDWR,0755)
		if err1 != nil{
			fmt.Println(err1)
		}
		// 2. Decode the intermediate file into a key/value array
		enc := json.NewDecoder(rf)
		var err2 error = nil
		var kv KeyValue
		for err2 == nil {
			err2 = enc.Decode(&kv)
			kvs = append(kvs,kv)
		}
		rf.Close()
	}
	// 3. Sort the key/value array
	sort.Sort(ByKey(kvs))
	//fmt.Println(kvs)
	// 4. Call reduceF for each key
	var curKey string
	var curValues []string
	var newKeyValues [] KeyValue
	for _, kv := range kvs{
		if curKey != kv.Key {
			if len(curValues) != 0 {
				newKeyValues = append(newKeyValues, KeyValue{curKey,reduceF(curKey,curValues) })
			}
			curKey = kv.Key
			curValues = []string{}
			curValues = append(curValues, kv.Value)
		} else {
			curValues = append(curValues, kv.Value)
		}
	}
	if len(curValues) != 0 {
		newKeyValues = append(newKeyValues, KeyValue{curKey,reduceF(curKey,curValues) })
	}

	//5. write to the output file
	f, error := os.OpenFile(outFile, os.O_RDWR|os.O_CREATE, 0644)
	if error != nil {
		fmt.Println(error)
	}
	enc := json.NewEncoder(f)
	for _,kv := range newKeyValues {
		error := enc.Encode(&kv)
		if error != nil {
			fmt.Println(error)
		}
	}
	f.Close()
}
