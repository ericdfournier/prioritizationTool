package prioritizationTool

import (
	"gonum.org/v1/gonum/mat"
)

// NewProfile generator
func NewProfile(
	pUse string) *Profile {

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
	pID string,
	pUse string,
	cgID string,
	anSupply, anDemand float64) *Parcel {

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
