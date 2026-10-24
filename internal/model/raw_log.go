package model

import "time"

// Represents a validated log entry with edge-captured metadata, ready for the raw_logs channel.
type RawLog struct {
	LogEntry
	BatchID    string
	SourceIP   string
	ReceivedAt time.Time
}
