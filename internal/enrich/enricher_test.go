package enrich

import (
	"strings"
	"testing"
	"time"

	"github.com/johneliud/log_ingestion_engine/internal/model"
)

func TestLogEnricher_AddsReceivedAtSourceIPAndEnvironment(t *testing.T) {
	t.Parallel()
	fixedTime := time.Date(2026, 10, 24, 10, 0, 0, 123456000, time.UTC)
	enricher := NewLogEnricher("staging", func() time.Time { return fixedTime })

	rawLog := model.RawLog{
		LogEntry:   model.LogEntry{Timestamp: "2026-10-24T10:00:00Z", Service: "claims", Level: model.LogLevelError, Message: "timeout"},
		BatchID:    "batch-1",
		SourceIP:   "203.0.113.5",
		ReceivedAt: fixedTime,
	}

	enrichedLog := enricher.Enrich(rawLog)
	if enrichedLog.SourceIP != "203.0.113.5" {
		t.Errorf("SourceIP = %q, want %q", enrichedLog.SourceIP, "203.0.113.5")
	}
	if enrichedLog.Environment != "staging" {
		t.Errorf("Environment = %q, want %q", enrichedLog.Environment, "staging")
	}
	if enrichedLog.ReceivedAt.IsZero() {
		t.Errorf("ReceivedAt should not be zero")
	}
}

func TestLogEnricher_UsesDefaultEnvironmentWhenEmpty(t *testing.T) {
	t.Parallel()
	enricher := NewLogEnricher("", time.Now)
	rawLog := model.RawLog{LogEntry: model.LogEntry{Timestamp: "2026-10-24T10:00:00Z", Service: "claims", Level: model.LogLevelError, Message: "timeout"}}
	enrichedLog := enricher.Enrich(rawLog)
	if enrichedLog.Environment != "production" {
		t.Errorf("Environment = %q, want %q", enrichedLog.Environment, "production")
	}
}

func TestLogEnricher_FallsBackToUnknownSourceIP(t *testing.T) {
	t.Parallel()
	enricher := NewLogEnricher("production", time.Now)
	rawLog := model.RawLog{LogEntry: model.LogEntry{Timestamp: "2026-10-24T10:00:00Z", Service: "claims", Level: model.LogLevelError, Message: "timeout"}, SourceIP: ""}
	enrichedLog := enricher.Enrich(rawLog)
	if enrichedLog.SourceIP != "unknown" {
		t.Errorf("SourceIP = %q, want %q", enrichedLog.SourceIP, "unknown")
	}
}

func TestLogEnricher_FormatsMicrosecondPrecision(t *testing.T) {
	t.Parallel()
	fixedTime := time.Date(2026, 10, 24, 10, 0, 0, 123456000, time.UTC)
	formatted := fixedTime.UTC().Format("2006-01-02T15:04:05.000000Z07:00")
	if !strings.Contains(formatted, "123456") {
		t.Errorf("formatted time should contain microseconds, got %q", formatted)
	}
}
