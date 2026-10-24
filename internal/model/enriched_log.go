package model

import "time"

// Represents a log entry after enrichment and routing, carrying all fields needed for storage and retry tracking.
type EnrichedLog struct {
	LogEntry
	BatchID     string
	ReceivedAt  time.Time
	SourceIP    string
	Environment string
	Destination Destination
	RetryCount  int
}
