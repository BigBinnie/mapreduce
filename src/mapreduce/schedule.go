package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	var wg sync.WaitGroup
	a := make(chan int, ntasks)
	for i:=0;i<ntasks;i++ {
		a <-i
	}
	wg.Add(ntasks)
	Loop: for {
		select {
			case i :=<-a:
			fmt.Printf("begin handle %d\n",i)
			//1. arrange a worker for each task if any idle worker exists
			addr :=<-registerChan
			//2. prepare DoTaskArgs
			var args DoTaskArgs
			switch phase {
			case mapPhase:
				args = DoTaskArgs{jobName, mapFiles[i], phase, i, n_other}
			case reducePhase:
				args = DoTaskArgs{jobName, "", phase, i, n_other}
			}
			//3. start goroutine to handle the tasks
			go func(i int) {
				var index = i
				ret := call(addr, "Worker.DoTask", args, nil)
				if ret == true {
					fmt.Printf("Success handle %d task\n", index)
					wg.Done()
					registerChan <- addr
				}else{
					fmt.Printf("Fail handle %d task\n", index)
					a <- index
				}
			}(i)
		default:
			fmt.Printf("There is no other tasks to be done\n")
			break Loop
		}
	}
	//3. waite for all the tasks to end
	wg.Wait()
	fmt.Printf("Schedule: %v done\n", phase)
}
