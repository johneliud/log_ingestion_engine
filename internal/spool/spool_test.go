package spool

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
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

// Simulates disk failures after bytes have been written, rather than failing before the operation starts.
type failingSpoolFile struct {
	spoolFile
	failWrite bool
	failSync  bool
}

func (file *failingSpoolFile) Write(payload []byte) (int, error) {
	if file.failWrite {
		n, _ := file.spoolFile.Write(payload[:len(payload)/2])
		return n, errors.New("injected partial write")
	}
	return file.spoolFile.Write(payload)
}

func (file *failingSpoolFile) Sync() error {
	if file.failSync {
		file.failSync = false
		return errors.New("injected sync failure")
	}
	return file.spoolFile.Sync()
}

func TestSpoolFailedAppendCannotReplayAnyOfTheBatch(t *testing.T) {
	for _, failure := range []string{"partial write", "sync"} {
		t.Run(failure, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "spool")
			s, err := NewSpool(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.Append([]model.RawLog{testRawLog("accepted", "before")}); err != nil {
				t.Fatal(err)
			}
			s.file = &failingSpoolFile{spoolFile: s.file, failWrite: failure == "partial write", failSync: failure == "sync"}
			if err := s.Append([]model.RawLog{testRawLog("rejected-1", "failed"), testRawLog("rejected-2", "failed")}); err == nil {
				t.Fatal("expected append failure")
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s, err = NewSpool(path)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			logs, err := s.Unacked()
			if err != nil || len(logs) != 1 || logs[0].LogID != "accepted" {
				t.Fatalf("replay after failed append: %+v, %v", logs, err)
			}
		})
	}
}

func TestSpoolRecoversInterruptedAppendAndInterruptedRollback(t *testing.T) {
	for _, stage := range []string{"partial batch", "complete batch", "rollback truncated"} {
		t.Run(stage, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "spool")
			s, err := NewSpool(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.Append([]model.RawLog{testRawLog("accepted", "before")}); err != nil {
				t.Fatal(err)
			}
			info, err := s.file.Stat()
			if err != nil {
				t.Fatal(err)
			}
			if err := writeJournal(path, info.Size()); err != nil {
				t.Fatal(err)
			}
			payload, _ := json.Marshal(testRawLog("uncommitted", "interrupted"))
			if stage == "partial batch" {
				payload = payload[:len(payload)/2]
			} else {
				payload = append(payload, '\n')
			}
			if _, err := s.file.Write(payload); err != nil {
				t.Fatal(err)
			}
			if err := s.file.Sync(); err != nil {
				t.Fatal(err)
			}
			if stage == "rollback truncated" {
				if err := s.file.Truncate(info.Size()); err != nil {
					t.Fatal(err)
				}
			}
			s.Close() // Simulate a crash with the undo record still present.
			s, err = NewSpool(path)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			if err := s.Append([]model.RawLog{testRawLog("next", "after")}); err != nil {
				t.Fatal(err)
			}
			logs, err := s.Unacked()
			if err != nil || len(logs) != 2 || logs[0].LogID != "accepted" || logs[1].LogID != "next" {
				t.Fatalf("recovered replay: %+v, %v", logs, err)
			}
		})
	}
}

func TestSpoolAckAllFailurePreservesReplayAndState(t *testing.T) {
	for _, failure := range []string{"partial write", "sync"} {
		t.Run(failure, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "spool")
			s, err := NewSpool(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.Append([]model.RawLog{testRawLog("one", "batch"), testRawLog("two", "batch")}); err != nil {
				t.Fatal(err)
			}
			s.ackFile = &failingSpoolFile{spoolFile: s.ackFile, failWrite: failure == "partial write", failSync: failure == "sync"}
			if err := s.AckAll([]string{"one", "two"}); err == nil {
				t.Fatal("expected ack error")
			}
			if len(s.spooled) != 2 || len(s.acked) != 0 {
				t.Fatal("failed acknowledgement updated state")
			}
			s.Compact()
			s.Close()
			s, err = NewSpool(path)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			logs, err := s.Unacked()
			if err != nil || len(logs) != 2 {
				t.Fatalf("replay after failed ack: %+v, %v", logs, err)
			}
		})
	}
}

func TestSpoolMalformedLinesAndPartialTailDoNotBlockReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "spool")
	valid, _ := json.Marshal(testRawLog("existing", "batch"))
	payload := append([]byte("not json\n"), valid...)
	payload = append(payload, []byte("\n{\"log_id\":\"partial")...)
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	s, err := NewSpool(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Append([]model.RawLog{testRawLog("next", "batch")}); err != nil {
		t.Fatal(err)
	}
	logs, err := s.Unacked()
	if err != nil || len(logs) != 2 || logs[1].LogID != "next" {
		t.Fatalf("replay with corrupt lines: %+v, %v", logs, err)
	}
}

func TestSpoolUnknownAndDuplicateAcksCannotCompactPendingLogs(t *testing.T) {
	path := filepath.Join(t.TempDir(), "spool")
	s, err := NewSpool(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Append([]model.RawLog{testRawLog("one", "batch"), testRawLog("two", "batch")}); err != nil {
		t.Fatal(err)
	}
	if err := s.AckAll([]string{"one", "one", "unknown"}); err != nil {
		t.Fatal(err)
	}
	s.Close()
	// Legacy sidecars can contain duplicate or unknown IDs too.
	file, err := os.OpenFile(path+".acked", os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteString("one\nunknown\n"); err != nil {
		t.Fatal(err)
	}
	file.Close()
	s, err = NewSpool(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.Compact()
	logs, err := s.Unacked()
	if err != nil || len(logs) != 1 || logs[0].LogID != "two" {
		t.Fatalf("compaction lost pending log: %+v, %v", logs, err)
	}
	if err := s.Ack("two"); err != nil {
		t.Fatal(err)
	}
	s.Compact()
	info, err := os.Stat(path)
	if err != nil || info.Size() != 0 {
		t.Fatalf("fully acknowledged spool not compacted: %v, %v", info, err)
	}
}
