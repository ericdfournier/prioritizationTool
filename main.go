package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path"
	"path/filepath"
	"sync"
	"time"

	"github.com/ericdfournier/prioritizationTool/pkg"
	"gopkg.in/cheggaaa/pb.v1"
)

func main() {

	// start timer
	start := time.Now()

	// print status
	log.Println("Parsing Arguments...")

	// get current working directory
	wd, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}

	// get working directory
	base := path.Dir(wd)

	// set input filepath cli flags
	resultsOutputPath := flag.String("o",
		filepath.Join(base, "prioritizationTool/out/results.csv"),
		"Filepath for the output results csv file")
	supplyProfilePath := flag.String("s",
		filepath.Join(base, "prioritizationTool/in/supply_profile.csv"),
		"Filepath to the supply profile csv file")
	demandProfilePath := flag.String("d",
		filepath.Join(base, "prioritizationTool/in/demand_profile.csv"),
		"Filepath to the demand profile csv file")
	circuitGroupDataPath := flag.String("c",
		filepath.Join(base, "prioritizationTool/in/circuit_groups.csv"),
		"Filepath to the circuit group csv file")
	parcelDataPath := flag.String("p",
		filepath.Join(base, "prioritizationTool/in/parcels.csv"),
		"Filepath to the parcel csv file")

	// parse cli input flags
	flag.Parse()

	// print filepaths used
	if *resultsOutputPath == "" {
		flag.PrintDefaults()
		os.Exit(1)
	} else {
		fmt.Printf("\tResults: %s \n\tSupply Profile: %s \n\tDemand Profile: %s \n\tCircuit Group Data: %s \n\tParcel Data: %s \n",
			filepath.Base(*resultsOutputPath),
			filepath.Base(*supplyProfilePath),
			filepath.Base(*demandProfilePath),
			filepath.Base(*circuitGroupDataPath),
			filepath.Base(*parcelDataPath))
	}

	// print status
	log.Println("Loading Data...")

	// parse input data
	supplyProfile := prioritizationTool.LoadSupplyProfileData(*supplyProfilePath)
	demandProfileMap := prioritizationTool.LoadDemandProfileData(*demandProfilePath)
	groups, circuitGroupPool, circuitGroupChan := prioritizationTool.LoadCircuitGroupData(*circuitGroupDataPath)
	circuitGroupPool = prioritizationTool.LoadParcelData(circuitGroupPool, *parcelDataPath)

	// print status
	log.Println("Beginning Work...")

	// generate results channel
	results := make(chan *prioritizationTool.CircuitGroup, groups)

	// set worker pool size
	limit := prioritizationTool.MaxParallelism()

	// create mapper wait group
	var workerWaitGroup sync.WaitGroup

	// initialize progress bai
	bar := pb.StartNew(len(circuitGroupChan))
	bar.ShowTimeLeft = false

	// enter parallel map loop
	for m := 0; m <= limit; m++ {

		// add map worker to wait group
		workerWaitGroup.Add(1)

		// launch map worker
		go prioritizationTool.Worker(
			&workerWaitGroup,
			supplyProfile,
			demandProfileMap,
			circuitGroupPool,
			circuitGroupChan,
			results,
			bar)
	}

	// launch a monitor mapper to synchronize the wait group
	go func() {
		workerWaitGroup.Wait()
		close(circuitGroupChan)
	}()

	// write results to file
	prioritizationTool.WriteCircuitGroupData(results, *resultsOutputPath)

	// print status
	bar.FinishPrint("\tFinished Work")

	// stop timer and print to console
	elapsed := time.Since(start)
	log.Printf("Elapsed Time: %s", elapsed)
}
