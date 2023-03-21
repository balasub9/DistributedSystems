package mapreduce

import "fmt"

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	debug("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	jobName := mr.jobName
	mapFiles := mr.files
	registerChan := mr.registerChannel
	countOfFinishedtasks := 0
	unFinishedtasks := make(chan int, ntasks)
	finishedtasks := make(chan int)

	for i := 0; i < ntasks; i++ {
		unFinishedtasks <- i
	}

	for {
		if countOfFinishedtasks == ntasks {
			break
		}
		select {
		case task := <-unFinishedtasks:
			fileName := ""
			if phase == mapPhase {
				fileName = mapFiles[task]
			}
			go func() {
				fmt.Print("Go Runner Started for job ", task)
				address := <-registerChan
				status := call(address, "Worker.DoTask",
					DoTaskArgs{jobName, fileName, phase, task, nios}, nil)
				if status {
					finishedtasks <- 1
					registerChan <- address
				} else {
					unFinishedtasks <- task
				}
			}()

		case <-finishedtasks:
			countOfFinishedtasks += 1
			fmt.Print("\n\n Count of Completed tasks is ", countOfFinishedtasks)
		}
	}

	debug("Schedule: %v phase done\n", phase)
}
