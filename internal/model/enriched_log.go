package model

import "time"

// Represents a log entry after enrichment and routing, carrying all fields needed for storage and retry tracking.
type EnrichedLog struct {
	LogEntry
	LogID       string      `json:"log_id"`
	BatchID     string      `json:"batch_id"`
	ReceivedAt  time.Time   `json:"received_at"`
	SourceIP    string      `json:"source_ip"`
	Environment string      `json:"environment"`
	Destination Destination `json:"destination"`
	RetryCount  int         `json:"retry_count"`
}
