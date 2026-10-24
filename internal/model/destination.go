package model

// Represents the storage destination for a routed log entry.
type Destination string

const (
	DestinationService1 Destination = "service1"
	DestinationService2 Destination = "service2"
	DestinationService3 Destination = "service3"
)
