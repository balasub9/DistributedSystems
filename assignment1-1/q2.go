package cos418_hw1_1

import (
	"bufio"
	"io"
	"log"
	"os"
	"strconv"
	"sync"
)

var waitGroup sync.WaitGroup

// Sum numbers from channel `nums` and output sum to `out`.
// You should only output to `out` once.
// Do NOT modify function signature.
func sumWorker(nums chan int, out chan int) {
	// TODO: implement me
	// HINT: use for loop over `nums`
	sum := 0
	defer waitGroup.Done()
	for e := range nums {
		sum += e
	}
	out <- sum
}

// Read integers from the file `fileName` and return sum of all values.
// This function must launch `num` go routines running
// `sumWorker` to find the sum of the values concurrently.
// You should use `checkError` to handle potential errors.
// Do NOT modify function signature.
func sum(num int, fileName string) int {
	// TODO: implement me
	// HINT: use `readInts` and `sumWorkers`
	// HINT: used buffered channels for splitting numbers between workers

	file, err := os.Open(fileName)
	if err != nil {
		log.Fatal("")
	}
	//Read List of Integer values from file
	listOfInteger, err := readInts(file)
	inputSize := len(listOfInteger)
	// Batch size is number of array elements for each Go routine
	batchSize := inputSize / num

	var inputChannels []chan int
	outChannel := make(chan int, num)

	//Will launch 'num' Go Routines
	for i := 0; i < num; i++ {
		temp := i * 100
		waitGroup.Add(1)
		//Arrray of Input channels for GO Subroutines to recieve splited load of data
		inputChannels = append(inputChannels, make(chan int, batchSize))
		for j := 0; j < batchSize && temp+j < inputSize; j++ {
			inputChannels[i] <- listOfInteger[temp+j]
		}
		go sumWorker(inputChannels[i], outChannel)
		close(inputChannels[i])
	}
	//Wait for all GO Routines to Complete
	waitGroup.Wait()
	finalSum := 0
	close(outChannel)
	// Add all sum values sent by  Go Routines in Out Channel
	for sumValuesFromWorkers := range outChannel {
		finalSum += sumValuesFromWorkers
	}
	return finalSum
}

// Read a list of integers separated by whitespace from `r`.
// Return the integers successfully read with no error, or
// an empty slice of integers and the error that occurred.
// Do NOT modify this function.
func readInts(r io.Reader) ([]int, error) {
	scanner := bufio.NewScanner(r)
	scanner.Split(bufio.ScanWords)
	var elems []int
	for scanner.Scan() {
		val, err := strconv.Atoi(scanner.Text())
		if err != nil {
			return elems, err
		}
		elems = append(elems, val)
	}
	return elems, nil
}
