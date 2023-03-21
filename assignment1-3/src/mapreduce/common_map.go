package mapreduce

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"os"
)

// doMap does the job of a map worker: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
func doMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {

	fmt.Printf("\n\n ----- MAP JOB  Started  Started --------\n\n")
	fmt.Printf("\n Job name : %v \n mapTaskNumber : %v  \n nReduce : %v \n nFile : \n %v",
		jobName, mapTaskNumber, nReduce, inFile)

	//Read Input File
	inputFile, errInFileRead := ioutil.ReadFile(inFile)

	if errInFileRead != nil {
		log.Fatal("Error in File Read")
	}

	//Retrive Key/Value pairs from MapF Function
	KeyValuePair := mapF(inFile, string(inputFile))
	//Create Intermediate File maps
	//to split the key value pairs to corresponding files
	intermediateFiles := make(map[string]*os.File)
	fileEncoders := make(map[string]*json.Encoder)

	for _, key := range KeyValuePair {
		//Key Hash Partioning
		hash_value := ihash(key.Key) % uint32(nReduce)
		filename := reduceName(jobName, mapTaskNumber, int(hash_value))
		// Check if the Filename dosent exist in map
		if intermediateFiles[filename] == nil {
			newfile, errorInFileCreate := os.Create(filename)
			if errorInFileCreate != nil {
				log.Fatal("Error In file Create")
			}
			intermediateFiles[filename] = newfile
			//Create a new Json Encode for the new intermediate file
			fileEncoders[filename] = json.NewEncoder(newfile)
			defer newfile.Close()
		}
		//Encode the key to write Json data to intermediate file
		err := fileEncoders[filename].Encode(&key)
		if err != nil {
			log.Fatal("Error In file Encoding")
		}
	}

	fmt.Printf("\n\n ----- MAP JOB Completed  --------\n\n")

}

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
