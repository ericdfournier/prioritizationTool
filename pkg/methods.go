package prioritizationTool

// Add method for profileMap
func (m *ProfileMap) Add(key, value interface{}) {

    // Attempt to load key value
    m.Map.Store(key, value)

    // return status
    return
}
