package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"gonum.org/v1/gonum/mat"
	"gopkg.in/cheggaaa/pb.v1"
)

// set global constants
const   hrs             int = 8760      // hours per year
const   cnt             int = 1752      // max circuit group index

// Profile map

type ProfileMap struct {
    sync.Map
}

// Profile type
type Profile struct{
    ProfileUsetype      string          // profile usetype
    HourlyFraction      *mat.Dense      // hourly fraction of supply or demand, unitless
}

// Parcel type
type Parcel struct {
	ParcelID            string          // parcel id
    ParcelUsetype       string          // parcel usetype category 
	CircuitGroupID      string          // circuit group id
	AnnualSupply        float64         // annual rooftop solar output energy supply in kWh
	AnnualDemand        float64         // annual building account energy demand in kWh
}

// CircuitGroup type
type CircuitGroup struct {
	CircuitGroupID      string          // circuit group id
	ParcelCount         int             // parcel count
	Parcels             chan *Parcel    // collection of parcels within the cg
	HourlyNetSupply     *mat.Dense      // annual hourly net supply
	AnnualNetSupply     float64         // annual total net grid exports
	MaxHourlyNetSupply  float64         // net grid exports in worst case hour
}

// Add method for profileMap
func (m *ProfileMap) Add(key, value interface{}) {

    // Attempt to load key value
    m.Map.Store(key, value)

    // return status
    return
}

// NewProfile generator
func NewProfile(
    pUse                 string) *Profile {

    // allocate empty hourly fraction array
    var (
        hrFraction = mat.NewDense(1, hrs, nil)
    )

    // return output
    return &Profile{
        ProfileUsetype: pUse,
        HourlyFraction: hrFraction,
    }
}

// NewParcel generator
func NewParcel(
	pID                 string,
    pUse                string,
	cgID                string,
	anSupply, anDemand  float64) *Parcel {

	// return output
	return &Parcel{
		ParcelID:       pID,
        ParcelUsetype:  pUse,
		CircuitGroupID: cgID,
		AnnualSupply:   anSupply,
		AnnualDemand:   anDemand,
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
		hrNetSup = mat.NewDense(hrs, 1, nil)
	)

	// create parcel channel
	parcelChannel := make(chan *Parcel, parcelCount)

	// return output
	return &CircuitGroup{
		CircuitGroupID:     cgID,
		ParcelCount:        parcelCount,
		Parcels:            parcelChannel,
		HourlyNetSupply:    hrNetSup,
		AnnualNetSupply:    anNetSup,
		MaxHourlyNetSupply: maxHrSup,
	}
}

// LoadSupplyProfileData function
func LoadSupplyProfileData(
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
	profileVec := mat.NewDense(hrs, 1, nil)

	// allocate status bar
	bar := pb.StartNew(rows-1)
	bar.ShowTimeLeft = false

	// write values from rawCSVdata to domain matrix
	for i := 1; i < rows; i++ {

		// get string values and convert to float
		valStr := rawProfileData[i][0]
		val, err := strconv.ParseFloat(valStr, 64)
		if err != nil {
			log.Println(err)
			os.Exit(1)
		}

		// write value to matrix
		profileVec.Set(i-1, 0, val)

		// increment status bar
		bar.Increment()
	}

	// close status bar
	bar.FinishPrint("\tSupply Profile Data Loaded")

	return profileVec
}

// LoadDemandProfileData function
func LoadDemandProfileData(
	profilePath string) *ProfileMap {

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
    cols := len(rawProfileData[0])

	// preallocated profile map
    profileMap := &ProfileMap{}

	// allocate status bar
	bar := pb.StartNew((rows*cols)-cols)
	bar.ShowTimeLeft = false

    // loop through columns in data
    for j := 0; j < cols; j++ {

        // preallocated profile matrix
	    profileVec := mat.NewDense(1, hrs, nil)

        // get profile id
        profileUsetype := rawProfileData[0][j]

	    // loop through rows in column
	    for i := 1; i < rows; i++ {

		    // get string values and convert to float
		    valStr := rawProfileData[i][j]
		    val, err := strconv.ParseFloat(valStr, 64)
		    if err != nil {
			    log.Println(err)
			    os.Exit(1)
		    }

		    // write value to matrix
		    profileVec.Set(0, i-1, val)

		    // increment status bar
		    bar.Increment()
        }

        // write data to profile
        profile := NewProfile(profileUsetype)
        profile.HourlyFraction = profileVec

        // map profile
        profileMap.Add(profileUsetype, profile)

	}

	// close status bar
	bar.FinishPrint("\tDemand Profile Data Loaded")

	return profileMap
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
	circuitGroupPool := make([]*CircuitGroup, cnt, cnt)
	circuitGroupChan := make(chan int, cnt)

	// allocate status bar
	bar := pb.StartNew(rows-1)
	bar.ShowTimeLeft = false

	// write values from rawCSVdata to domain matrix
	for i := 1; i < rows; i++ {

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

		// increment bar
		bar.Increment()
	}

	// print status
	bar.FinishPrint("\tCircuit Group Data Loaded")

	return rows-1, circuitGroupPool, circuitGroupChan

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

	// allocate status bar
	bar := pb.StartNew(rows-1)
	bar.ShowTimeLeft = false

	// write values from rawCSVdata to domain matrix
	for i := 1; i < rows; i++ {

		// get string values and convert to float
		pid := rawParcelData[i][0]

        // get string values
        puse := rawParcelData[i][1]

		// get string values
		cgid := rawParcelData[i][2]

		// get string values and convert to float
		supplyStr := rawParcelData[i][3]
		supply, err := strconv.ParseFloat(supplyStr, 64)
		if err != nil {
			log.Println(err)
			os.Exit(1)
		}

		// get string values and convert to float
		demandStr := rawParcelData[i][4]
		demand, err := strconv.ParseFloat(demandStr, 64)
		if err != nil {
			log.Println(err)
			os.Exit(1)
		}

		// scrub null values
		if demand < 0.0 {
			demand = 0.0
		}

		// set supply to zero if demand missing
		if demand == 0.0 {
			supply = 0.0
		}

		// generate map key
		key, err := strconv.Atoi(cgid)
		if err != nil {
			log.Println(err)
			os.Exit(1)
		}

		// generate new parcel type
		circuitGroupPool[key-1].Parcels <- NewParcel(pid, puse,  cgid, supply, demand)

		// increment bar
		bar.Increment()

	}

	// print status
	bar.FinishPrint("\tParcel Data Loaded")

	return circuitGroupPool
}

// WriteCircuitGroupData function
func WriteCircuitGroupData(
	results chan *CircuitGroup,
	circuitGroupPath string) {

	// generate file names
	dir, file := filepath.Split(circuitGroupPath)
	ext := filepath.Ext(file)
	name := strings.TrimSuffix(file, ext)
	annualStem := name + "_annualNet" + ext
	hourlyStem := name + "_hourlyNet" + ext
	circuitGroupAnnualPath := filepath.Join(dir, annualStem)
	circuitGroupHourlyPath := filepath.Join(dir, hourlyStem)

	// open circuit group annual file
	circuitGroupAnnualFile, err := os.Create(circuitGroupAnnualPath)
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	// close file on completion
	defer circuitGroupAnnualFile.Close()

	// open circuit group hourly file
	circuitGroupHourlyFile, err := os.Create(circuitGroupHourlyPath)
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	// close file on completion
	defer circuitGroupHourlyFile.Close()

	// create new writer
	circuitGroupAnnualWriter := csv.NewWriter(circuitGroupAnnualFile)
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	// flush writer
	defer circuitGroupAnnualWriter.Flush()

	// create new writer
	circuitGroupHourlyWriter := csv.NewWriter(circuitGroupHourlyFile)
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	// flush writer
	defer circuitGroupHourlyWriter.Flush()

	// allocated iterator
	i := 0

	// write header strings to annual file
    err = circuitGroupAnnualWriter.Write(
		[]string{   "Circuit_Group_ID",
                    "Circuit_Group_Count",
                    "Annual_Net_Supply_MWh",
                    "Annual_Max_Net_Supply_MWh"})
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

    // generate hours string for headers
    hrSlice := make([]string, hrs)
    for h := 0; h < hrs; h++{
        hrSlice[h] = strconv.Itoa(h+1)
    }

    // write header strings to hourly file
    err = circuitGroupHourlyWriter.Write(
        append([]string{"Circuit_Group_ID"}, hrSlice...))
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	// loop through and write results
	for r := range results {

		// iterate counter
		i++

		// convert annual data to strings
		cgidString := r.CircuitGroupID
		cgCountString := strconv.Itoa(r.ParcelCount)
		anNetString := strconv.FormatFloat(r.AnnualNetSupply, 'f', 8, 64)
		anMaxHourString := strconv.FormatFloat(r.MaxHourlyNetSupply, 'f', 8, 64)

		// write strings to annual file
		err := circuitGroupAnnualWriter.Write(
			[]string{cgidString, cgCountString, anNetString, anMaxHourString})
		if err != nil {
			log.Println(err)
			os.Exit(1)
		}

		// allocate hourly string slice
		hourlyStringSlice := make([]string, 0, hrs)

		// loop through array and perform converison
		for j := 0; j < hrs; j++ {
			text := strconv.FormatFloat(r.HourlyNetSupply.At(j, 0), 'f', 8, 64)
			hourlyStringSlice = append(hourlyStringSlice, text)
		}

		// write strings to hourly file
		err = circuitGroupHourlyWriter.Write(append([]string{cgidString}, hourlyStringSlice...))
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

/*TODO:
Need to think about how to provide the demand profiles to the works
so as not to encounter issues with concurrent memory access. It may 
just make sense to create a new deep copy of the demand profile map 
and pass that to each worker so that they can independently perform 
the lookups themselves. 
*/

// Worker function
func Worker(
	workerWaitGroup *sync.WaitGroup,
	supplyProfile *mat.Dense,
    demandProfileMap *ProfileMap,
	circuitGroupPool []*CircuitGroup,
	circuitGroupChan chan int,
	results chan *CircuitGroup,
	bar *pb.ProgressBar) {

    // defer waitgroup closure
	defer workerWaitGroup.Done()

	// pull keys from the circuit group channel
	for key := range circuitGroupChan {

		// dereference the circuit group for expansion
		cg := circuitGroupPool[key]

		// Allocate annual supply and demand vectors
        parcelDemVec := mat.NewDense(1, hrs, nil)
        parcelSupVec := mat.NewDense(1, cg.ParcelCount, nil)

		// Allocate receiver matrices
        parcelSupMat := mat.NewDense(hrs, cg.ParcelCount, nil)
		parcelDemMat := mat.NewDense(hrs, cg.ParcelCount, nil)
		parcelNetMat := mat.NewDense(hrs, cg.ParcelCount, nil)

        // Loop through parcels and populate arrays
		for i := 0; i < cg.ParcelCount; i++ {

            // Extract parcel
            p := <-cg.Parcels

            // Set Supply Variable
			parcelSupVec.Set(0, i, p.AnnualSupply)

            // Lookup parcel demand profile
            profile, _ := demandProfileMap.Load(p.ParcelUsetype)

            // Scale hourly profile by annual demand
            parcelDemVec.Scale(p.AnnualDemand, profile.(*Profile).HourlyFraction)

            // Compute demand matrix by vector multiplication (implicit row->col transpose)
            parcelDemMat.SetCol(i, parcelDemVec.RawRowView(0))
        }

		// Compute supply matrix by vector dot product
		parcelSupMat.Mul(supplyProfile, parcelSupVec)

		// subtract demand matrix from supply matrix
		parcelNetMat.Sub(parcelSupMat, parcelDemMat)

		// Compute Annual Net supply
		cg.AnnualNetSupply = mat.Sum(parcelNetMat) * 0.001 // convert to MW

		// Compute Hourly Net Supply
		for j := 0; j < hrs; j++ {
			hourSlice := parcelNetMat.RawRowView(j)
			hourVec := mat.NewDense(cg.ParcelCount, 1, hourSlice)
			cg.HourlyNetSupply.Set(j, 0, mat.Sum(hourVec))
		}

		// write maximum hourly net supply
		cg.MaxHourlyNetSupply = mat.Max(cg.HourlyNetSupply) * 0.001 // convert to MW

		// Write to results channel
		results <- cg

		// increment bar
		bar.Increment()
	}

	return
}

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
		filepath.Join(base, "out/results.csv"),
		"Filepath for the output results csv file")
	supplyProfilePath := flag.String("s",
		filepath.Join(base, "in/supply_profile.csv"),
		"Filepath to the supply profile csv file")
	demandProfilePath := flag.String("d",
		filepath.Join(base, "in/demand_profile.csv"),
		"Filepath to the demand profile csv file")
	circuitGroupDataPath := flag.String("c",
		filepath.Join(base, "in/circuit_groups.csv"),
		"Filepath to the circuit group csv file")
	parcelDataPath := flag.String("p",
		filepath.Join(base, "in/parcels.csv"),
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
    supplyProfile := LoadSupplyProfileData(*supplyProfilePath)
    demandProfileMap := LoadDemandProfileData(*demandProfilePath)
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

	// initialize progress bai
    bar := pb.StartNew(len(circuitGroupChan))
    bar.ShowTimeLeft = false

	// enter parallel map loop
    for m := 0; m <= limit; m++ {

		// add map worker to wait group
        workerWaitGroup.Add(1)

		// launch map worker
        go Worker(
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
    WriteCircuitGroupData(results, *resultsOutputPath)

	// print status
    bar.FinishPrint("\tFinished Work")

	// stop timer and print to console
	elapsed := time.Since(start)
	log.Printf("Elapsed Time: %s", elapsed)

}
