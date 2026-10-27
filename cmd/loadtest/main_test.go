package main

import (
	"encoding/json"
	"testing"
	"time"
)

func TestBuildSourceIPAddresses_ReturnsOneAddressPerRequestedSourceIP(t *testing.T) {
	t.Parallel()

	sourceIPAddresses := buildSourceIPAddresses(3)
	if len(sourceIPAddresses) != 3 {
		t.Fatalf("source IP count = %d, want 3", len(sourceIPAddresses))
	}
	if sourceIPAddresses[0] != "198.51.100.001" || sourceIPAddresses[2] != "198.51.100.003" {
		t.Errorf("source IPs = %v, want the sequential 198.51.100.0/24 addresses", sourceIPAddresses)
	}
}

func TestBuildLogBatchRequestBody_HoldsValidEntriesForEveryRequestedLog(t *testing.T) {
	t.Parallel()

	requestBody := buildLogBatchRequestBody(5, 12, testSentAt())
	if requestBody[0] != '[' || requestBody[len(requestBody)-1] != ']' {
		t.Fatalf("request body is not a JSON array: %s", requestBody)
	}

	var logEntries []map[string]string
	if unmarshalError := json.Unmarshal(requestBody, &logEntries); unmarshalError != nil {
		t.Fatalf("body is not a valid JSON array: %v", unmarshalError)
	}
	if len(logEntries) != 5 {
		t.Fatalf("entry count = %d, want 5", len(logEntries))
	}
	for _, logEntry := range logEntries {
		if logEntry["service"] != loadTestServiceName || logEntry["level"] != loadTestLogLevel {
			t.Errorf("entry fields = %v, want the load test service and level", logEntry)
		}
	}
}

func testSentAt() time.Time {
	return time.Date(2026, time.October, 27, 12, 0, 0, 0, time.UTC)
}
