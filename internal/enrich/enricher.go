package enrich

import (
	"time"

	"github.com/google/uuid"
	"github.com/johneliud/log_ingestion_engine/internal/model"
)

const unknownSourceIP = "unknown"

// Enriches raw logs with received_at, source_ip, and environment.
type LogEnricher struct {
	applicationEnvironment string
	currentTimeProvider    func() time.Time
}

// Returns a new log enricher.
func NewLogEnricher(applicationEnvironment string, currentTimeProvider func() time.Time) *LogEnricher {
	if applicationEnvironment == "" {
		applicationEnvironment = "production"
	}
	if currentTimeProvider == nil {
		currentTimeProvider = time.Now
	}
	return &LogEnricher{
		applicationEnvironment: applicationEnvironment,
		currentTimeProvider:    currentTimeProvider,
	}
}

// Enriches a raw log entry.
func (logEnricher *LogEnricher) Enrich(rawLog model.RawLog) model.EnrichedLog {
	receivedAt := rawLog.ReceivedAt
	if receivedAt.IsZero() {
		receivedAt = logEnricher.currentTimeProvider()
	}

	sourceIP := rawLog.SourceIP
	if sourceIP == "" {
		sourceIP = unknownSourceIP
	}

	return model.EnrichedLog{
		LogEntry:    rawLog.LogEntry,
		LogID:       logEnricher.logIDOrDefault(rawLog.LogID),
		BatchID:     rawLog.BatchID,
		ReceivedAt:  receivedAt,
		SourceIP:    sourceIP,
		Environment: logEnricher.applicationEnvironment,
		RetryCount:  0,
	}
}

// Returns the raw log's identifier, generating one when the raw log carries none.
func (logEnricher *LogEnricher) logIDOrDefault(rawLogID string) string {
	if rawLogID != "" {
		return rawLogID
	}
	return uuid.NewString()
}
