package model

import "time"

// Represents a log that exhausted every storage retry along with its failure details.
type DeadLetterEntry struct {
	Log          EnrichedLog `json:"log"`
	ErrorMessage string      `json:"error"`
	FailedAt     time.Time   `json:"failed_at"`
}
