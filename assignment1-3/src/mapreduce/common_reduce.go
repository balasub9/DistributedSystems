package mapreduce

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
)

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {

	fmt.Printf("\n\n ----- Reduce  JOB Started  --------\n\n")

	fmt.Printf("\n Job name : %v \n ReduceTaskNumber : %v  \n nMap : %v \n ", jobName,
		reduceTaskNumber, nMap)

	reducedKeyValuePairs := make(map[string][]string)
	for mapTaskNumber := 0; mapTaskNumber < nMap; mapTaskNumber++ {
		fileName := reduceName(jobName, mapTaskNumber, reduceTaskNumber)
		file, errInFileOpen := os.Open(fileName)
		if errInFileOpen != nil {
			log.Fatal(errInFileOpen)
		}
		defer file.Close()
		decoder := json.NewDecoder(file)
		//Decode Json to KeyValue until Decoder return error
		for {
			var jsonToken KeyValue
			errorInDecode := decoder.Decode(&jsonToken)
			if errorInDecode != nil {
				break
			}
			reducedKeyValuePairs[jsonToken.Key] =
				append(reducedKeyValuePairs[jsonToken.Key], jsonToken.Value)
		}
	}
	// Sort the Keys
	sortedkeys := make([]string, 0, len(reducedKeyValuePairs))
	for key := range reducedKeyValuePairs {
		sortedkeys = append(sortedkeys, key)
	}
	sort.Strings(sortedkeys)
	newReducedFile, err := os.Create(mergeName(jobName, reduceTaskNumber))
	if err != nil {
		log.Fatal("Error in file open")
	}
	// Write  the reduced data to a file in json
	encoder := json.NewEncoder(newReducedFile)
	for _, key := range sortedkeys {
		encoder.Encode(KeyValue{key, reduceF(key, reducedKeyValuePairs[key])})
	}
	newReducedFile.Close()

	fmt.Printf("\n\n ----- Reduce  JOB Completed  --------\n\n")

}
