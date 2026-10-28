package storage

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/johneliud/log_ingestion_engine/internal/config"
	"github.com/johneliud/log_ingestion_engine/internal/model"
)

// Builds the glob pattern that matches every rotated dead letter file.
func rotatedDeadLetterFilePattern(deadLetterFilePath string) string {
	extension := filepath.Ext(deadLetterFilePath)
	return strings.TrimSuffix(deadLetterFilePath, extension) + ".*" + extension
}

// Builds a dead letter entry with deterministic timestamps.
func aDeadLetterEntry(messageSuffix string) model.DeadLetterEntry {
	return model.DeadLetterEntry{
		Log: model.EnrichedLog{
			LogEntry: model.LogEntry{
				Timestamp: "2026-10-24T10:00:00Z",
				Service:   "claims",
				Level:     model.LogLevelError,
				Message:   "claim failed " + messageSuffix,
			},
			BatchID:     "batch-1",
			ReceivedAt:  time.Date(2026, 10, 24, 10, 0, 0, 123456000, time.UTC),
			SourceIP:    "203.0.113.7",
			Environment: "production",
			Destination: model.DestinationService1,
			RetryCount:  3,
		},
		ErrorMessage: "disk full",
		FailedAt:     time.Date(2026, 10, 24, 10, 0, 5, 0, time.UTC),
	}
}

// Reads every dead letter entry from one newline-delimited JSON file.
func readDeadLetterEntries(t *testing.T, deadLetterFilePath string) []model.DeadLetterEntry {
	t.Helper()
	deadLetterFile, openError := os.Open(deadLetterFilePath)
	if openError != nil {
		t.Fatalf("opening dead letter file %q: %v", deadLetterFilePath, openError)
	}
	defer deadLetterFile.Close()

	deadLetterEntries := make([]model.DeadLetterEntry, 0)
	lineScanner := bufio.NewScanner(deadLetterFile)
	for lineScanner.Scan() {
		var deadLetterEntry model.DeadLetterEntry
		if unmarshalError := json.Unmarshal(lineScanner.Bytes(), &deadLetterEntry); unmarshalError != nil {
			t.Fatalf("parsing dead letter line: %v", unmarshalError)
		}
		deadLetterEntries = append(deadLetterEntries, deadLetterEntry)
	}
	if scanError := lineScanner.Err(); scanError != nil {
		t.Fatalf("reading dead letter file %q: %v", deadLetterFilePath, scanError)
	}
	return deadLetterEntries
}

func waitForDeadLetterWriter(t *testing.T, completed <-chan struct{}) {
	t.Helper()
	select {
	case <-completed:
	case <-time.After(2 * time.Second):
		t.Fatal("dead letter writer did not stop after Close")
	}
}

func TestDeadLetterWriter_WritesEntriesAsNewlineDelimitedJSON(t *testing.T) {
	t.Parallel()
	deadLetterFilePath := filepath.Join(t.TempDir(), "logs-failed.json")
	deadLetterWriter := NewDeadLetterWriter(config.ApplicationConfiguration{
		DeadLetterFilePath:               deadLetterFilePath,
		DeadLetterMaximumFileSizeInBytes: 1024 * 1024,
	})
	completed := deadLetterWriter.Run()

	submittedEntry := aDeadLetterEntry("one")
	if submitError := deadLetterWriter.Submit(submittedEntry); submitError != nil {
		t.Fatalf("Submit returned error: %v", submitError)
	}
	if closeError := deadLetterWriter.Close(); closeError != nil {
		t.Fatalf("Close returned error: %v", closeError)
	}
	waitForDeadLetterWriter(t, completed)

	deadLetterEntries := readDeadLetterEntries(t, deadLetterFilePath)
	if len(deadLetterEntries) != 1 {
		t.Fatalf("dead letter entry count = %d, want 1", len(deadLetterEntries))
	}
	storedEntry := deadLetterEntries[0]
	if storedEntry.Log.BatchID != submittedEntry.Log.BatchID || storedEntry.Log.Message != submittedEntry.Log.Message {
		t.Errorf("stored log = %+v, want %+v", storedEntry.Log, submittedEntry.Log)
	}
	if storedEntry.Log.RetryCount != submittedEntry.Log.RetryCount || storedEntry.Log.Destination != submittedEntry.Log.Destination {
		t.Errorf("stored retry count and destination = %d %q, want %d %q", storedEntry.Log.RetryCount, storedEntry.Log.Destination, submittedEntry.Log.RetryCount, submittedEntry.Log.Destination)
	}
	if storedEntry.ErrorMessage != submittedEntry.ErrorMessage {
		t.Errorf("stored error message = %q, want %q", storedEntry.ErrorMessage, submittedEntry.ErrorMessage)
	}
	if !storedEntry.FailedAt.Equal(submittedEntry.FailedAt) {
		t.Errorf("stored failure time = %s, want %s", storedEntry.FailedAt, submittedEntry.FailedAt)
	}
}

func TestDeadLetterWriter_CloseWaitsForQueuedEntries(t *testing.T) {
	t.Parallel()
	deadLetterFilePath := filepath.Join(t.TempDir(), "logs-failed.json")
	deadLetterWriter := NewDeadLetterWriter(config.ApplicationConfiguration{
		DeadLetterFilePath:               deadLetterFilePath,
		DeadLetterMaximumFileSizeInBytes: 1024 * 1024,
	})
	completed := deadLetterWriter.Run()

	submittedEntry := aDeadLetterEntry("one")
	if submitError := deadLetterWriter.Submit(submittedEntry); submitError != nil {
		t.Fatalf("Submit returned error: %v", submitError)
	}
	if closeError := deadLetterWriter.Close(); closeError != nil {
		t.Fatalf("Close returned error: %v", closeError)
	}
	waitForDeadLetterWriter(t, completed)

	deadLetterEntries := readDeadLetterEntries(t, deadLetterFilePath)
	if len(deadLetterEntries) != 1 {
		t.Fatalf("dead letter entry count = %d, want 1", len(deadLetterEntries))
	}
}

func TestDeadLetterWriter_RotatesAfterMaximumFileSize(t *testing.T) {
	t.Parallel()
	deadLetterFilePath := filepath.Join(t.TempDir(), "logs-failed.json")
	maximumFileSizeInBytes := 500
	deadLetterWriter := NewDeadLetterWriter(config.ApplicationConfiguration{
		DeadLetterFilePath:               deadLetterFilePath,
		DeadLetterMaximumFileSizeInBytes: maximumFileSizeInBytes,
	})
	completed := deadLetterWriter.Run()

	submittedEntryCount := 5
	for entryIndex := 0; entryIndex < submittedEntryCount; entryIndex++ {
		if submitError := deadLetterWriter.Submit(aDeadLetterEntry("entry")); submitError != nil {
			t.Fatalf("Submit returned error: %v", submitError)
		}
	}
	if closeError := deadLetterWriter.Close(); closeError != nil {
		t.Fatalf("Close returned error: %v", closeError)
	}
	waitForDeadLetterWriter(t, completed)

	rotatedFilePaths, globError := filepath.Glob(rotatedDeadLetterFilePattern(deadLetterFilePath))
	if globError != nil {
		t.Fatalf("globbing rotated dead letter files: %v", globError)
	}
	allDeadLetterFilePaths := append([]string{deadLetterFilePath}, rotatedFilePaths...)

	totalDeadLetterEntryCount := 0
	for _, deadLetterFilePath := range allDeadLetterFilePaths {
		fileInformation, statError := os.Stat(deadLetterFilePath)
		if statError != nil {
			t.Fatalf("reading dead letter file %q: %v", deadLetterFilePath, statError)
		}
		if fileInformation.Size() > int64(maximumFileSizeInBytes) {
			t.Errorf("dead letter file %q size = %d, want at most %d", deadLetterFilePath, fileInformation.Size(), maximumFileSizeInBytes)
		}
		totalDeadLetterEntryCount += len(readDeadLetterEntries(t, deadLetterFilePath))
	}
	if len(rotatedFilePaths) == 0 {
		t.Fatal("expected at least one rotated dead letter file")
	}
	if totalDeadLetterEntryCount != submittedEntryCount {
		t.Errorf("total dead letter entry count = %d, want %d", totalDeadLetterEntryCount, submittedEntryCount)
	}
}

func TestBatchWriter_DeadLettersLogsAfterFinalStoreFailure(t *testing.T) {
	t.Parallel()
	storeAttemptCount := 0
	deadLetteredEntries := make([]model.DeadLetterEntry, 0)
	failedAt := time.Date(2026, 10, 24, 10, 0, 5, 0, time.UTC)
	batchWriter := NewBatchWriter(BatchWriterDependencies{
		WriteBatchSize: 1,
		StoreBatch: func(destination model.Destination, enrichedLogs []model.EnrichedLog) error {
			storeAttemptCount++
			return errors.New("disk full")
		},
		DeadLetterSink: func(deadLetterEntry model.DeadLetterEntry) error {
			deadLetteredEntries = append(deadLetteredEntries, deadLetterEntry)
			return nil
		},
		CurrentTimeProvider: func() time.Time { return failedAt },
	})

	enrichedLog := anEnrichedLog()
	if storeError := batchWriter.StoreBatch(model.DestinationService1, []model.EnrichedLog{enrichedLog}); storeError != nil {
		t.Fatalf("StoreBatch returned error for a dead-lettered batch: %v", storeError)
	}

	if storeAttemptCount != 1 {
		t.Errorf("store attempt count = %d, want 1", storeAttemptCount)
	}
	if len(deadLetteredEntries) != 1 {
		t.Fatalf("dead letter entry count = %d, want 1", len(deadLetteredEntries))
	}
	deadLetteredEntry := deadLetteredEntries[0]
	if deadLetteredEntry.Log.BatchID != enrichedLog.BatchID || deadLetteredEntry.Log.Destination != model.DestinationService1 {
		t.Errorf("dead letter log = %+v, want batch %q at %q", deadLetteredEntry.Log, enrichedLog.BatchID, model.DestinationService1)
	}
	if deadLetteredEntry.ErrorMessage != "disk full" {
		t.Errorf("dead letter error message = %q, want %q", deadLetteredEntry.ErrorMessage, "disk full")
	}
	if !deadLetteredEntry.FailedAt.Equal(failedAt) {
		t.Errorf("dead letter failure time = %s, want %s", deadLetteredEntry.FailedAt, failedAt)
	}

	if flushError := batchWriter.FlushAll(); flushError != nil {
		t.Fatalf("flushing the dead-lettered batch failed: %v", flushError)
	}
	if storeAttemptCount != 1 {
		t.Errorf("store attempt count after flush = %d, want 1; dead-lettered logs must not be retried again", storeAttemptCount)
	}
}

func TestBatchWriter_DeadLettersOnlyAfterEveryRetry(t *testing.T) {
	t.Parallel()
	storeAttemptCount := 0
	deadLetteredEntries := make([]model.DeadLetterEntry, 0)
	batchWriter := NewBatchWriter(BatchWriterDependencies{
		WriteBatchSize:     1,
		RetryBackoffDelays: []time.Duration{time.Second, 5 * time.Second, 10 * time.Second},
		StoreBatch: func(destination model.Destination, enrichedLogs []model.EnrichedLog) error {
			storeAttemptCount++
			return errors.New("disk full")
		},
		WaitForRetryDelay: func(applicationContext context.Context, retryDelay time.Duration) error {
			return nil
		},
		DeadLetterSink: func(deadLetterEntry model.DeadLetterEntry) error {
			deadLetteredEntries = append(deadLetteredEntries, deadLetterEntry)
			return nil
		},
	})

	if storeError := batchWriter.StoreBatch(model.DestinationService1, []model.EnrichedLog{anEnrichedLog()}); storeError != nil {
		t.Fatalf("StoreBatch returned error for a dead-lettered batch: %v", storeError)
	}

	if storeAttemptCount != 4 {
		t.Errorf("store attempt count = %d, want 4", storeAttemptCount)
	}
	if len(deadLetteredEntries) != 1 {
		t.Fatalf("dead letter entry count = %d, want 1", len(deadLetteredEntries))
	}
	if deadLetteredEntries[0].Log.RetryCount != 3 {
		t.Errorf("dead letter retry count = %d, want 3", deadLetteredEntries[0].Log.RetryCount)
	}
}

func TestReplayDeadLetters_DeliversLogsBackToThePipeline(t *testing.T) {
	t.Parallel()
	deadLetterFilePath := filepath.Join(t.TempDir(), "logs-failed.json")
	firstEntry := aDeadLetterEntry("one")
	secondEntry := aDeadLetterEntry("two")
	serializedEntries, marshalError := json.Marshal(firstEntry)
	if marshalError != nil {
		t.Fatalf("marshalling the first dead letter entry: %v", marshalError)
	}
	secondSerializedEntry, secondMarshalError := json.Marshal(secondEntry)
	if secondMarshalError != nil {
		t.Fatalf("marshalling the second dead letter entry: %v", secondMarshalError)
	}
	fileContents := append(append(serializedEntries, '\n'), append(secondSerializedEntry, '\n')...)
	if writeError := os.WriteFile(deadLetterFilePath, fileContents, 0o644); writeError != nil {
		t.Fatalf("writing the dead letter file: %v", writeError)
	}

	replayedLogs := make([]model.EnrichedLog, 0)
	replayedLogCount, replayError := ReplayDeadLetters(deadLetterFilePath, func(enrichedLog model.EnrichedLog) error {
		replayedLogs = append(replayedLogs, enrichedLog)
		return nil
	})
	if replayError != nil {
		t.Fatalf("ReplayDeadLetters returned error: %v", replayError)
	}
	if replayedLogCount != 2 {
		t.Errorf("replayed log count = %d, want 2", replayedLogCount)
	}
	if len(replayedLogs) != 2 {
		t.Fatalf("delivered log count = %d, want 2", len(replayedLogs))
	}
	if replayedLogs[0].Message != firstEntry.Log.Message || replayedLogs[1].Message != secondEntry.Log.Message {
		t.Errorf("delivered log messages = %q %q, want %q %q", replayedLogs[0].Message, replayedLogs[1].Message, firstEntry.Log.Message, secondEntry.Log.Message)
	}
	if replayedLogs[0].Destination != model.DestinationService1 || replayedLogs[0].RetryCount != firstEntry.Log.RetryCount {
		t.Errorf("delivered log metadata = %+v, want destination %q and retry count %d", replayedLogs[0], model.DestinationService1, firstEntry.Log.RetryCount)
	}
}

func TestDeadLetterSubmitReportsDurableWriteFailures(t *testing.T) {
	for _, target := range []string{"directory", "sync failure"} {
		t.Run(target, func(t *testing.T) {
			path := t.TempDir()
			if target == "sync failure" {
				path = "/dev/null"
			}
			writer := NewDeadLetterWriter(config.ApplicationConfiguration{DeadLetterFilePath: path, DeadLetterMaximumFileSizeInBytes: 1 << 20})
			completed := writer.Run()
			defer func() { writer.Close(); waitForDeadLetterWriter(t, completed) }()
			if err := writer.Submit(aDeadLetterEntry("failed")); err == nil {
				t.Fatal("Submit succeeded despite failed durable write")
			}
		})
	}
}
