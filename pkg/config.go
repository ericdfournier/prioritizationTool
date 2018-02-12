package prioritizationTool

import (
    "runtime"
)

// MaxParallelism function
func MaxParallelism() int {

	// get the number of go processes
	maxProcs := runtime.GOMAXPROCS(0)

	// get tthe maximum number of cpus on the local machine
	numCPU := runtime.NumCPU()
	if maxProcs < numCPU {
		return maxProcs
	}

	return numCPU
}

