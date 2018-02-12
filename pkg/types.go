package prioritizationTool

import (
    "sync"

    "gonum.org/v1/gonum/mat"
)

// Profile map
type ProfileMap struct {
    sync.Map                            // concurrent map structure
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
