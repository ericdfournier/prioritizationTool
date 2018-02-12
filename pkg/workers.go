package prioritizationTool

import (
    "sync"

    "gonum.org/v1/gonum/mat"
    "gopkg.in/cheggaaa/pb.v1"
)

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
