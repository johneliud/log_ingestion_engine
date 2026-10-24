package ingest

import (
	"fmt"
	"time"

	"github.com/johneliud/log_ingestion_engine/internal/model"
)

const (
	maximumServiceLengthInCharacters = 100
	maximumMessageLengthInCharacters = 10000
)

const acceptedLevelsMessage = "level must be one of INFO, WARN, ERROR, DEBUG"

// Returns a field-level error for every rule the entry breaks, or nil when the entry is valid.
func validateLogEntry(logEntry model.LogEntry) []model.FieldError {
	var fieldErrors []model.FieldError

	if logEntry.Timestamp == "" {
		fieldErrors = append(fieldErrors, model.FieldError{Field: "timestamp", Message: "timestamp is required"})
	} else if _, parseError := time.Parse(time.RFC3339Nano, logEntry.Timestamp); parseError != nil {
		fieldErrors = append(fieldErrors, model.FieldError{Field: "timestamp", Message: "timestamp must be a valid RFC 3339 timestamp"})
	}

	if logEntry.Service == "" {
		fieldErrors = append(fieldErrors, model.FieldError{Field: "service", Message: "service is required"})
	} else if len(logEntry.Service) > maximumServiceLengthInCharacters {
		fieldErrors = append(fieldErrors, model.FieldError{Field: "service", Message: fmt.Sprintf("service must be at most %d characters", maximumServiceLengthInCharacters)})
	}

	if logEntry.Level == "" {
		fieldErrors = append(fieldErrors, model.FieldError{Field: "level", Message: "level is required"})
	} else if !isValidLogLevel(logEntry.Level) {
		fieldErrors = append(fieldErrors, model.FieldError{Field: "level", Message: acceptedLevelsMessage})
	}

	if logEntry.Message == "" {
		fieldErrors = append(fieldErrors, model.FieldError{Field: "message", Message: "message is required"})
	} else if len(logEntry.Message) > maximumMessageLengthInCharacters {
		fieldErrors = append(fieldErrors, model.FieldError{Field: "message", Message: fmt.Sprintf("message must be at most %d characters", maximumMessageLengthInCharacters)})
	}

	return fieldErrors
}

// Separates valid entries from rejected entries, preserving the original array index for each rejection.
func validateLogEntries(logEntries []model.LogEntry) (validEntries []model.LogEntry, rejectedEntries []model.RejectedEntry) {
	for entryIndex, logEntry := range logEntries {
		fieldErrors := validateLogEntry(logEntry)
		if len(fieldErrors) > 0 {
			rejectedEntries = append(rejectedEntries, model.RejectedEntry{
				Index:  entryIndex,
				Errors: fieldErrors,
			})
		} else {
			validEntries = append(validEntries, logEntry)
		}
	}
	return validEntries, rejectedEntries
}

// Reports whether the given level is one of the accepted log levels.
func isValidLogLevel(level model.LogLevel) bool {
	switch level {
	case model.LogLevelInfo, model.LogLevelWarn, model.LogLevelError, model.LogLevelDebug:
		return true
	default:
		return false
	}
}
