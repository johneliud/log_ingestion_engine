package ingest

import (
	"strings"
	"testing"

	"github.com/johneliud/log_ingestion_engine/internal/model"
)

// Returns a log entry that passes every validation rule.
func aValidLogEntry() model.LogEntry {
	return model.LogEntry{
		Timestamp: "2026-10-24T10:00:00Z",
		Service:   "claims",
		Level:     model.LogLevelError,
		Message:   "timeout connecting to database",
	}
}

// Returns a log entry whose service field has exactly the given number of characters.
func anEntryWithServiceLength(lengthInCharacters int) model.LogEntry {
	entry := aValidLogEntry()
	entry.Service = strings.Repeat("a", lengthInCharacters)
	return entry
}

// Returns a log entry whose message field has exactly the given number of characters.
func anEntryWithMessageLength(lengthInCharacters int) model.LogEntry {
	entry := aValidLogEntry()
	entry.Message = strings.Repeat("b", lengthInCharacters)
	return entry
}

// Finds the field error for the given field name, returning an empty string when absent.
func errorMessageForField(fieldErrors []model.FieldError, fieldName string) string {
	for _, fieldError := range fieldErrors {
		if fieldError.Field == fieldName {
			return fieldError.Message
		}
	}
	return ""
}

func TestValidateLogEntry_AcceptsValidEntry(t *testing.T) {
	t.Parallel()
	fieldErrors := validateLogEntry(aValidLogEntry())
	if len(fieldErrors) != 0 {
		t.Errorf("expected no field errors for a valid entry, got %d: %v", len(fieldErrors), fieldErrors)
	}
}

func TestValidateLogEntry_RejectsMissingFields(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name                 string
		fieldToClear         string
		expectedFieldName    string
		expectedErrorMessage string
	}{
		{
			name:                 "rejects a missing timestamp",
			fieldToClear:         "timestamp",
			expectedFieldName:    "timestamp",
			expectedErrorMessage: "timestamp is required",
		},
		{
			name:                 "rejects a missing service",
			fieldToClear:         "service",
			expectedFieldName:    "service",
			expectedErrorMessage: "service is required",
		},
		{
			name:                 "rejects a missing level",
			fieldToClear:         "level",
			expectedFieldName:    "level",
			expectedErrorMessage: "level is required",
		},
		{
			name:                 "rejects a missing message",
			fieldToClear:         "message",
			expectedFieldName:    "message",
			expectedErrorMessage: "message is required",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			entry := aValidLogEntry()
			switch testCase.fieldToClear {
			case "timestamp":
				entry.Timestamp = ""
			case "service":
				entry.Service = ""
			case "level":
				entry.Level = ""
			case "message":
				entry.Message = ""
			}

			fieldErrors := validateLogEntry(entry)
			if len(fieldErrors) != 1 {
				t.Fatalf("expected 1 field error, got %d: %v", len(fieldErrors), fieldErrors)
			}

			if fieldErrors[0].Field != testCase.expectedFieldName {
				t.Errorf("field = %q, want %q", fieldErrors[0].Field, testCase.expectedFieldName)
			}

			if fieldErrors[0].Message != testCase.expectedErrorMessage {
				t.Errorf("message = %q, want %q", fieldErrors[0].Message, testCase.expectedErrorMessage)
			}
		})
	}
}

func TestValidateLogEntry_RejectsInvalidTimestamp(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name      string
		timestamp string
	}{
		{"rejects a timestamp without timezone", "2026-10-24T10:00:00"},
		{"rejects a timestamp using a space instead of T", "2026-10-24 10:00:00Z"},
		{"rejects a non-date string", "yesterday"},
		{"rejects a date-only string", "2026-10-24"},
		{"rejects a lowercase timestamp", "2026-10-24t10:00:00z"},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			entry := aValidLogEntry()
			entry.Timestamp = testCase.timestamp

			fieldErrors := validateLogEntry(entry)
			errorMessage := errorMessageForField(fieldErrors, "timestamp")
			if errorMessage != "timestamp must be a valid RFC 3339 timestamp" {
				t.Errorf("timestamp error = %q, want %q", errorMessage, "timestamp must be a valid RFC 3339 timestamp")
			}
		})
	}
}

func TestValidateLogEntry_RejectsLevelOutsideAllowedSet(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name  string
		level model.LogLevel
	}{
		{"rejects TRACE", "TRACE"},
		{"rejects lowercase info", "info"},
		{"rejects FATAL", "FATAL"},
		{"rejects a numeric string", "1"},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			entry := aValidLogEntry()
			entry.Level = testCase.level

			fieldErrors := validateLogEntry(entry)
			errorMessage := errorMessageForField(fieldErrors, "level")
			if errorMessage != acceptedLevelsMessage {
				t.Errorf("level error = %q, want %q", errorMessage, acceptedLevelsMessage)
			}
		})
	}
}

func TestValidateLogEntry_AcceptsAllValidLevels(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name  string
		level model.LogLevel
	}{
		{"accepts INFO", model.LogLevelInfo},
		{"accepts WARN", model.LogLevelWarn},
		{"accepts ERROR", model.LogLevelError},
		{"accepts DEBUG", model.LogLevelDebug},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			entry := aValidLogEntry()
			entry.Level = testCase.level

			fieldErrors := validateLogEntry(entry)
			if len(fieldErrors) != 0 {
				t.Errorf("expected no field errors for level %q, got %d: %v", testCase.level, len(fieldErrors), fieldErrors)
			}
		})
	}
}

func TestValidateLogEntry_ServiceLengthBoundaries(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name               string
		lengthInCharacters int
		expectValid        bool
	}{
		{"accepts service at exactly 100 characters", 100, true},
		{"rejects service at 101 characters", 101, false},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			entry := anEntryWithServiceLength(testCase.lengthInCharacters)
			fieldErrors := validateLogEntry(entry)
			serviceError := errorMessageForField(fieldErrors, "service")

			if testCase.expectValid {
				if serviceError != "" {
					t.Errorf("expected no service error at %d characters, got %q", testCase.lengthInCharacters, serviceError)
				}
			} else {
				if serviceError == "" {
					t.Errorf("expected a service error at %d characters, got none", testCase.lengthInCharacters)
				}
			}
		})
	}
}

func TestValidateLogEntry_MessageLengthBoundaries(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name               string
		lengthInCharacters int
		expectValid        bool
	}{
		{"accepts message at exactly 10000 characters", 10000, true},
		{"rejects message at 10001 characters", 10001, false},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			entry := anEntryWithMessageLength(testCase.lengthInCharacters)
			fieldErrors := validateLogEntry(entry)
			messageError := errorMessageForField(fieldErrors, "message")

			if testCase.expectValid {
				if messageError != "" {
					t.Errorf("expected no message error at %d characters, got %q", testCase.lengthInCharacters, messageError)
				}
			} else {
				if messageError == "" {
					t.Errorf("expected a message error at %d characters, got none", testCase.lengthInCharacters)
				}
			}
		})
	}
}

func TestValidateLogEntry_ReturnsMultipleFieldErrorsForOneEntry(t *testing.T) {
	t.Parallel()
	entry := model.LogEntry{
		Timestamp: "not-a-timestamp",
		Service:   "",
		Level:     "TRACE",
		Message:   "",
	}

	fieldErrors := validateLogEntry(entry)
	if len(fieldErrors) != 4 {
		t.Fatalf("expected 4 field errors, got %d: %v", len(fieldErrors), fieldErrors)
	}

	expectedFields := map[string]bool{"timestamp": false, "service": false, "level": false, "message": false}
	for _, fieldError := range fieldErrors {
		expectedFields[fieldError.Field] = true
	}

	for fieldName, found := range expectedFields {
		if !found {
			t.Errorf("expected a field error for %q, but none was returned", fieldName)
		}
	}
}

func TestValidateLogEntries_AcceptsAllValidEntries(t *testing.T) {
	t.Parallel()
	entries := []model.LogEntry{aValidLogEntry(), aValidLogEntry(), aValidLogEntry()}
	validEntries, rejectedEntries := ValidateLogEntries(entries)

	if len(validEntries) != 3 {
		t.Errorf("valid entries = %d, want 3", len(validEntries))
	}

	if len(rejectedEntries) != 0 {
		t.Errorf("rejected entries = %d, want 0", len(rejectedEntries))
	}
}

func TestValidateLogEntries_PartialBatchPreservesValidAndRejectedWithCorrectIndices(t *testing.T) {
	t.Parallel()
	invalidEntry := aValidLogEntry()
	invalidEntry.Level = "TRACE"

	entries := []model.LogEntry{aValidLogEntry(), invalidEntry, aValidLogEntry()}
	validEntries, rejectedEntries := ValidateLogEntries(entries)

	if len(validEntries) != 2 {
		t.Errorf("valid entries = %d, want 2", len(validEntries))
	}

	if len(rejectedEntries) != 1 {
		t.Fatalf("rejected entries = %d, want 1", len(rejectedEntries))
	}

	if rejectedEntries[0].Index != 1 {
		t.Errorf("rejected entry index = %d, want 1", rejectedEntries[0].Index)
	}

	if len(rejectedEntries[0].Errors) != 1 {
		t.Errorf("rejected entry error count = %d, want 1", len(rejectedEntries[0].Errors))
	}

	if rejectedEntries[0].Errors[0].Field != "level" {
		t.Errorf("rejected entry error field = %q, want %q", rejectedEntries[0].Errors[0].Field, "level")
	}
}

func TestValidateLogEntries_AllInvalidReturnsZeroValidAndAllRejected(t *testing.T) {
	t.Parallel()
	invalidEntry := aValidLogEntry()
	invalidEntry.Level = "TRACE"

	entries := []model.LogEntry{invalidEntry, invalidEntry}
	validEntries, rejectedEntries := ValidateLogEntries(entries)

	if len(validEntries) != 0 {
		t.Errorf("valid entries = %d, want 0", len(validEntries))
	}

	if len(rejectedEntries) != 2 {
		t.Errorf("rejected entries = %d, want 2", len(rejectedEntries))
	}

	if rejectedEntries[0].Index != 0 {
		t.Errorf("first rejected entry index = %d, want 0", rejectedEntries[0].Index)
	}

	if rejectedEntries[1].Index != 1 {
		t.Errorf("second rejected entry index = %d, want 1", rejectedEntries[1].Index)
	}
}

func TestValidateLogEntry_ServiceLengthCountsRunesNotBytes(t *testing.T) {
	t.Parallel()
	cjkCharacter := "中"
	testCases := []struct {
		name               string
		lengthInCharacters int
		expectValid        bool
	}{
		{"accepts 100 multibyte characters at the service limit", 100, true},
		{"rejects 101 multibyte characters over the service limit", 101, false},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			entry := aValidLogEntry()
			entry.Service = strings.Repeat(cjkCharacter, testCase.lengthInCharacters)
			fieldErrors := validateLogEntry(entry)
			serviceError := errorMessageForField(fieldErrors, "service")

			if testCase.expectValid {
				if serviceError != "" {
					t.Errorf("expected no service error at %d runes (%d bytes), got %q", testCase.lengthInCharacters, len(entry.Service), serviceError)
				}
			} else {
				if serviceError == "" {
					t.Errorf("expected a service error at %d runes (%d bytes), got none", testCase.lengthInCharacters, len(entry.Service))
				}
			}
		})
	}
}

func TestValidateLogEntry_MessageLengthCountsRunesNotBytes(t *testing.T) {
	t.Parallel()
	cjkCharacter := "文"
	testCases := []struct {
		name               string
		lengthInCharacters int
		expectValid        bool
	}{
		{"accepts 10000 multibyte characters at the message limit", 10000, true},
		{"rejects 10001 multibyte characters over the message limit", 10001, false},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			entry := aValidLogEntry()
			entry.Message = strings.Repeat(cjkCharacter, testCase.lengthInCharacters)
			fieldErrors := validateLogEntry(entry)
			messageError := errorMessageForField(fieldErrors, "message")

			if testCase.expectValid {
				if messageError != "" {
					t.Errorf("expected no message error at %d runes (%d bytes), got %q", testCase.lengthInCharacters, len(entry.Message), messageError)
				}
			} else {
				if messageError == "" {
					t.Errorf("expected a message error at %d runes (%d bytes), got none", testCase.lengthInCharacters, len(entry.Message))
				}
			}
		})
	}
}
