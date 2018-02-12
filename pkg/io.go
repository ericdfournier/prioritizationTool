package prioritizationTool

import (
	"encoding/csv"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"gonum.org/v1/gonum/mat"
	"gopkg.in/cheggaaa/pb.v1"
)

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
	bar := pb.StartNew(rows - 1)
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
	bar := pb.StartNew((rows * cols) - cols)
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
	bar := pb.StartNew(rows - 1)
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

	return rows - 1, circuitGroupPool, circuitGroupChan

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
	bar := pb.StartNew(rows - 1)
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
		circuitGroupPool[key-1].Parcels <- NewParcel(pid, puse, cgid, supply, demand)

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
		[]string{"Circuit_Group_ID",
			"Circuit_Group_Count",
			"Annual_Net_Supply_MWh",
			"Annual_Max_Net_Supply_MWh"})
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	// generate hours string for headers
	hrSlice := make([]string, hrs)
	for h := 0; h < hrs; h++ {
		hrSlice[h] = strconv.Itoa(h + 1)
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
