package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"time"

	"gonum.org/v1/gonum/mat"
)

// set global constants
const hrs int = 8760 // hours per year
const cnt int = 1752 // max circuit group index

// Parcel type
type Parcel struct {
	ParcelID       string     // parcel id
	CircuitGroupID string     // circuit group id
	AnnualSupply   float64    // annual rooftop solar output energy supply in kWh
	AnnualDemand   float64    // annual building account energy demand in kWh
	HourlySupply   *mat.Dense // hourly rooftop solar output energy supply in kWh
	HourlyDemand   *mat.Dense // hourly building account energy demand in kWh
	HourlyNet      *mat.Dense // hourly net solar output to the grid in kWh
}

// CircuitGroup type
type CircuitGroup struct {
	CircuitGroupID     string       // circuit group id
	ParcelCount        int          // parcel count
	Parcels            chan *Parcel // collection of parcels within the circuit group
	AnnualNetSupply    float64      // annual total net grid exports
	MaxHourlyNetSupply float64      // net grid exports in worst case hour
}

// NewParcel generator
func NewParcel(
	pID string,
	cgID string,
	anSupply, anDemand float64) *Parcel {

	// set default parameter values
	var (
		hrSupply *mat.Dense
		hrDemand *mat.Dense
		hrNet    *mat.Dense
	)

	// return output
	return &Parcel{
		ParcelID:       pID,
		CircuitGroupID: cgID,
		AnnualSupply:   anSupply,
		AnnualDemand:   anDemand,
		HourlySupply:   hrSupply,
		HourlyDemand:   hrDemand,
		HourlyNet:      hrNet,
	}
}

// NewCircuitGroup generator
func NewCircuitGroup(
	cgID string,
	parcelCount int) *CircuitGroup {

	// set default parameter values
	var (
		anNetSup = 0.0
		maxHrSup = 0.0
	)

	// create parcel channel
	parcelChannel := make(chan *Parcel, parcelCount)

	// return output
	return &CircuitGroup{
		CircuitGroupID:     cgID,
		ParcelCount:        parcelCount,
		Parcels:            parcelChannel,
		AnnualNetSupply:    anNetSup,
		MaxHourlyNetSupply: maxHrSup,
	}
}

// LoadProfileData function
func LoadProfileData(
	profilePath string) *mat.Dense {

	// open supply profile file
	profileFile, err := os.Open(profilePath)
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	// close files on completion
	defer profileFile.Close()

	// generate new reader from open supply file
	profileReader := csv.NewReader(profileFile)
	profileReader.FieldsPerRecord = -1

	// use reader to read raw csv data
	rawProfileData, err := profileReader.ReadAll()
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	// initialize output matrix
	rows := len(rawProfileData)

	// preallocated supply matrix
	profileMat := mat.NewDense(1, hrs, nil)

	// write values from rawCSVdata to domain matrix
	for i := 0; i < rows; i++ {

		// get string values and convert to float
		valStr := rawProfileData[i][0]
		val, err := strconv.ParseFloat(valStr, 64)
		if err != nil {
			log.Println(err)
			os.Exit(1)
		}

		// write value to matrix
		profileMat.Set(0, i, val)
	}

	// print status
	fmt.Println("\tProfile Data Loaded")

	return profileMat
}

// LoadCircuitGroupData function
func LoadCircuitGroupData(
	circuitGroupPath string) (int, []*CircuitGroup, chan int) {

	// open circuit group file
	circuitGroupFile, err := os.Open(circuitGroupPath)
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	// close files on completion
	defer circuitGroupFile.Close()

	// generate new reader from open circuitGroup file
	circuitGroupReader := csv.NewReader(circuitGroupFile)
	circuitGroupReader.FieldsPerRecord = -1

	// use reader to read raw csv data
	rawCircuitGroupData, err := circuitGroupReader.ReadAll()
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	// initialize output matrix
	rows := len(rawCircuitGroupData)

	// initialize circuit group pool syncronous map
	circuitGroupPool := make([]*CircuitGroup, cnt)
	circuitGroupChan := make(chan int, cnt)

	// write values from rawCSVdata to domain matrix
	for i := 0; i < rows; i++ {

		// get string values
		cgid := rawCircuitGroupData[i][0]

		// get string values and convert to float
		countStr := rawCircuitGroupData[i][1]
		count, err := strconv.Atoi(countStr)
		if err != nil {
			log.Println(err)
			os.Exit(1)
		}

		// generate map key
		key, err := strconv.Atoi(cgid)
		if err != nil {
			log.Println(err)
			os.Exit(1)
		}

		// generate new parcel type
		circuitGroupPool[key-1] = NewCircuitGroup(cgid, count)
		circuitGroupChan <- key - 1

	}

	// print status
	fmt.Println("\tCircuit Group Data Loaded")

	return rows, circuitGroupPool, circuitGroupChan

}

// LoadParcelData function
func LoadParcelData(
	circuitGroupPool []*CircuitGroup,
	parcelPath string) []*CircuitGroup {

	// open consumption file
	parcelFile, err := os.Open(parcelPath)
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	// close files on completion
	defer parcelFile.Close()

	// generate new reader from open consumption file
	parcelReader := csv.NewReader(parcelFile)
	parcelReader.FieldsPerRecord = -1

	// use reader to read raw csv data
	rawParcelData, err := parcelReader.ReadAll()
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	// initialize output matrix
	rows := len(rawParcelData)

	// write values from rawCSVdata to domain matrix
	for i := 0; i < rows; i++ {

		// get string values and convert to float
		pid := rawParcelData[i][0]

		// get string values
		cgid := rawParcelData[i][1]

		// get string values and convert to float
		supplyStr := rawParcelData[i][2]
		supply, err := strconv.ParseFloat(supplyStr, 64)
		if err != nil {
			log.Println(err)
			os.Exit(1)
		}

		// get string values and convert to float
		demandStr := rawParcelData[i][3]
		demand, err := strconv.ParseFloat(demandStr, 64)
		if err != nil {
			log.Println(err)
			os.Exit(1)
		}

		// scrub null values
		if demand == -7777.0 {
			supply = 0.0
			demand = 0.0
		}

		// generate map key
		key, err := strconv.Atoi(cgid)
		if err != nil {
			log.Println(err)
			os.Exit(1)
		}

		// generate new parcel type
		circuitGroupPool[key-1].Parcels <- NewParcel(pid, cgid, supply, demand)
	}

	// print status
	fmt.Println("\tParcel Data Loaded")

	return circuitGroupPool

}

// WriteCircuitGroupData function
func WriteCircuitGroupData(
	results chan *CircuitGroup,
	circuitGroupPath string) {

	// open circuit group file
	circuitGroupFile, err := os.Create(circuitGroupPath)
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	// close files on completion
	defer circuitGroupFile.Close()

	// create new writer
	circuitGroupWriter := csv.NewWriter(circuitGroupFile)
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	// flush writer
	defer circuitGroupWriter.Flush()

	// allocated iterator
	i := 0

	// loop through and write results
	for r := range results {

		// iterate counter
		i++

		// perform string conversions where necessary
		cgidString := r.CircuitGroupID
		anNetString := strconv.FormatFloat(r.AnnualNetSupply, 'f', 8, 64)
		anMaxHourString := strconv.FormatFloat(r.MaxHourlyNetSupply, 'f', 8, 64)

		// write strings to file
		err := circuitGroupWriter.Write([]string{cgidString, anNetString, anMaxHourString})
		if err != nil {
			log.Println(err)
			os.Exit(1)
		}

		// close results channel
		if i == cap(results) {
			close(results)
		}
	}

	return
}

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

// HourlyExpansion method
func (p Parcel) HourlyExpansion(
	supplyProfile, demandProfile *mat.Dense) *Parcel {

	// allocate empty expanded hourly arrays
	p.HourlySupply = mat.NewDense(1, hrs, nil)
	p.HourlyDemand = mat.NewDense(1, hrs, nil)
	p.HourlyNet = mat.NewDense(1, hrs, nil)

	// perform net output calculations
	p.HourlySupply.Scale(p.AnnualSupply, supplyProfile)
	p.HourlyDemand.Scale(p.AnnualDemand, demandProfile)
	p.HourlyNet.Sub(p.HourlySupply, p.HourlyDemand)

	return &p
}

// Worker function
func Worker(
	workerWaitGroup *sync.WaitGroup,
	supplyProfile, demandProfile *mat.Dense,
	circuitGroupPool []*CircuitGroup,
	circuitGroupChan chan int,
	results chan *CircuitGroup) {

	// defer waitgroup closure
	defer workerWaitGroup.Done()

	// pull keys from the circuit group channel
	for key := range circuitGroupChan {

		// dereference the circuit group for expansion
		cg := circuitGroupPool[key]

		// Allocate empty parcel hourly net export matrix
		parcelMat := mat.NewDense(cg.ParcelCount, hrs, nil)

		// Loop through parcels and populate matrix
		for i := 0; i < cg.ParcelCount; i++ {
			p := <-cg.Parcels
			p = p.HourlyExpansion(supplyProfile, demandProfile)
			parcelMat.SetRow(i, p.HourlyNet.RawRowView(0))
		}

		// Compute Annual Net supply
		cg.AnnualNetSupply = mat.Sum(parcelMat)

		// Compute Maximum Hour and Maximum Net Supply
		cg.MaxHourlyNetSupply = mat.Norm(parcelMat, 1)

		// Write to results channel
		results <- cg

		// print status
		fmt.Printf("\tCircuit Group: %v Finished \n", key)
	}

	return
}

func main() {

	// start timer
	start := time.Now()

	// print status
	log.Println("Parsing Arguments...")

	// get current working directory
	base, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}

	// set input filepath cli flags
	resultsOutputPath := flag.String("o",
		filepath.Join(base, "results.csv"),
		"User must supply the local filepath for the output results csv file")
	supplyProfilePath := flag.String("sp",
		filepath.Join(base, "supply_profile.csv"),
		"User must supply the local filepath to the supply profile csv file")
	demandProfilePath := flag.String("dp",
		filepath.Join(base, "demand_profile.csv"),
		"User must supply the local filepath to the demand profile csv file")
	circuitGroupDataPath := flag.String("cg",
		filepath.Join(base, "circuit_groups.csv"),
		"User must supply the local filepath to the circuit group csv file")
	parcelDataPath := flag.String("p",
		filepath.Join(base, "parcels.csv"),
		"User must supply the local filepath to the parcel csv file")

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
	supplyProfile := LoadProfileData(*supplyProfilePath)
	demandProfile := LoadProfileData(*demandProfilePath)
	groups, circuitGroupPool, circuitGroupChan := LoadCircuitGroupData(*circuitGroupDataPath)
	circuitGroupPool = LoadParcelData(circuitGroupPool, *parcelDataPath)

	// print status
	log.Println("Beginning Work...")

	// generate results channel
	results := make(chan *CircuitGroup, groups)

	// set worker pool size
	limit := MaxParallelism()

	// create mapper wait group
	var workerWaitGroup sync.WaitGroup

	// enter parallel map loop
	for m := 0; m <= limit; m++ {

		// add map worker to wait group
		workerWaitGroup.Add(1)

		// launch map worker
		go Worker(
			&workerWaitGroup,
			supplyProfile,
			demandProfile,
			circuitGroupPool,
			circuitGroupChan,
			results)
	}

	// launch a monitor mapper to synchronize the wait group
	go func() {
		workerWaitGroup.Wait()
		close(circuitGroupChan)
	}()

	// write results to file
	WriteCircuitGroupData(results, *resultsOutputPath)

	// print status
	log.Println("Finished Work!")

	// stop timer and print to console
	elapsed := time.Since(start)
	log.Printf("Elapsed Time: %s", elapsed)
}
