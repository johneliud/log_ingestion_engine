package spool

import (
	"testing"
	"time"

	"github.com/johneliud/log_ingestion_engine/internal/model"
)

func testRawLog(logID string, batchID string) model.RawLog {
	return model.RawLog{
		LogEntry: model.LogEntry{
			Timestamp: "2026-01-01T00:00:00Z",
			Service:   "service1",
			Level:     model.LogLevelInfo,
			Message:   "test message",
		},
		LogID:      logID,
		BatchID:    batchID,
		SourceIP:   "10.0.0.1",
		ReceivedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}
}

func TestSpoolAppendAndUnacked(t *testing.T) {
	temporaryDirectory := t.TempDir()
	spoolPath := temporaryDirectory + "/ingestion-spool.log"

	spoolInstance, openError := NewSpool(spoolPath)
	if openError != nil {
		t.Fatalf("opening the spool failed: %v", openError)
	}
	defer spoolInstance.Close()

	logs := []model.RawLog{
		testRawLog("log-1", "batch-1"),
		testRawLog("log-2", "batch-1"),
		testRawLog("log-3", "batch-1"),
	}
	if appendError := spoolInstance.Append(logs); appendError != nil {
		t.Fatalf("appending logs to the spool failed: %v", appendError)
	}

	unacked, readError := spoolInstance.Unacked()
	if readError != nil {
		t.Fatalf("reading unacked logs failed: %v", readError)
	}
	if len(unacked) != len(logs) {
		t.Fatalf("expected %d unacked logs, got %d", len(logs), len(unacked))
	}
	for index, unackedLog := range unacked {
		if unackedLog.LogID != logs[index].LogID {
			t.Fatalf("unacked log %d has id %q, want %q", index, unackedLog.LogID, logs[index].LogID)
		}
	}
}

func TestSpoolAckExcludesFromUnacked(t *testing.T) {
	temporaryDirectory := t.TempDir()
	spoolPath := temporaryDirectory + "/ingestion-spool.log"

	spoolInstance, openError := NewSpool(spoolPath)
	if openError != nil {
		t.Fatalf("opening the spool failed: %v", openError)
	}
	defer spoolInstance.Close()

	logs := []model.RawLog{
		testRawLog("log-1", "batch-1"),
		testRawLog("log-2", "batch-1"),
		testRawLog("log-3", "batch-1"),
	}
	if appendError := spoolInstance.Append(logs); appendError != nil {
		t.Fatalf("appending logs to the spool failed: %v", appendError)
	}

	spoolInstance.Ack(logs[1].LogID)

	unacked, readError := spoolInstance.Unacked()
	if readError != nil {
		t.Fatalf("reading unacked logs failed: %v", readError)
	}
	if len(unacked) != 2 {
		t.Fatalf("expected 2 unacked logs after one ack, got %d", len(unacked))
	}
	for _, unackedLog := range unacked {
		if unackedLog.LogID == logs[1].LogID {
			t.Fatalf("acked log %q was still returned as unacked", logs[1].LogID)
		}
	}

	// Acking again is idempotent.
	spoolInstance.Ack(logs[1].LogID)
	unacked, readError = spoolInstance.Unacked()
	if readError != nil {
		t.Fatalf("reading unacked logs failed: %v", readError)
	}
	if len(unacked) != 2 {
		t.Fatalf("expected 2 unacked logs after duplicate ack, got %d", len(unacked))
	}
}

func TestSpoolCompactRemovesFullyAckedLogs(t *testing.T) {
	temporaryDirectory := t.TempDir()
	spoolPath := temporaryDirectory + "/ingestion-spool.log"

	spoolInstance, openError := NewSpool(spoolPath)
	if openError != nil {
		t.Fatalf("opening the spool failed: %v", openError)
	}
	defer spoolInstance.Close()

	if openError := spoolInstance.Append([]model.RawLog{testRawLog("log-1", "batch-1")}); openError != nil {
		t.Fatalf("appending a log to the spool failed: %v", openError)
	}

	spoolInstance.Ack("log-1")
	spoolInstance.Compact()

	unacked, readError := spoolInstance.Unacked()
	if readError != nil {
		t.Fatalf("reading unacked logs failed: %v", readError)
	}
	if len(unacked) != 0 {
		t.Fatalf("expected no unacked logs after compact, got %d", len(unacked))
	}
}
