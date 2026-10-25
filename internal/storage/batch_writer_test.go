package storage

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/johneliud/log_ingestion_engine/internal/config"
	"github.com/johneliud/log_ingestion_engine/internal/model"
)

// Builds a complete enriched log for storage tests.
func anEnrichedLog() model.EnrichedLog {
	return model.EnrichedLog{
		LogEntry: model.LogEntry{
			Timestamp: "2026-10-24T10:00:00.000000Z",
			Service:   "claims",
			Level:     model.LogLevelError,
			Message:   "claim failed",
		},
		BatchID:     "batch-1",
		ReceivedAt:  time.Date(2026, 10, 24, 10, 0, 0, 123456000, time.UTC),
		SourceIP:    "203.0.113.7",
		Environment: "production",
		Destination: model.DestinationService1,
	}
}

type recordedBatch struct {
	destination  model.Destination
	enrichedLogs []model.EnrichedLog
}

// Returns a batch store that records every batch it receives on the channel.
func aRecordingBatchStore(recordedBatches chan<- recordedBatch) batchStore {
	return func(destination model.Destination, enrichedLogs []model.EnrichedLog) error {
		recordedBatches <- recordedBatch{destination: destination, enrichedLogs: enrichedLogs}
		return nil
	}
}

// Returns a batch store that records every failed attempt and reports an error.
func aFailingBatchStore(attemptedBatches chan<- []model.EnrichedLog) batchStore {
	return func(destination model.Destination, enrichedLogs []model.EnrichedLog) error {
		attemptedBatches <- enrichedLogs
		return errors.New("store unavailable")
	}
}

// Returns a retry waiter that records delays instead of sleeping.
func aRecordingRetryWaiter(recordedDelays chan<- time.Duration) retryWaiter {
	return func(applicationContext context.Context, retryDelay time.Duration) error {
		recordedDelays <- retryDelay
		return nil
	}
}

func TestBatchWriter_FlushesWhenWriteBatchSizeIsReached(t *testing.T) {
	t.Parallel()
	recordedBatches := make(chan recordedBatch, 1)
	batchWriter := NewBatchWriter(BatchWriterDependencies{WriteBatchSize: 3, StoreBatch: aRecordingBatchStore(recordedBatches)})

	storeError := batchWriter.StoreBatch(model.DestinationService1, []model.EnrichedLog{anEnrichedLog(), anEnrichedLog(), anEnrichedLog()})
	if storeError != nil {
		t.Fatalf("StoreBatch returned error: %v", storeError)
	}

	select {
	case recordedBatch := <-recordedBatches:
		if recordedBatch.destination != model.DestinationService1 {
			t.Errorf("destination = %q, want %q", recordedBatch.destination, model.DestinationService1)
		}
		if len(recordedBatch.enrichedLogs) != 3 {
			t.Errorf("flushed log count = %d, want 3", len(recordedBatch.enrichedLogs))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("batch was not flushed after reaching the size trigger")
	}
}

func TestBatchWriter_DoesNotFlushBelowWriteBatchSize(t *testing.T) {
	t.Parallel()
	recordedBatches := make(chan recordedBatch, 1)
	batchWriter := NewBatchWriter(BatchWriterDependencies{WriteBatchSize: 3, StoreBatch: aRecordingBatchStore(recordedBatches)})

	if storeError := batchWriter.StoreBatch(model.DestinationService1, []model.EnrichedLog{anEnrichedLog(), anEnrichedLog()}); storeError != nil {
		t.Fatalf("StoreBatch returned error: %v", storeError)
	}

	select {
	case <-recordedBatches:
		t.Fatal("batch flushed before reaching the size trigger")
	default:
	}
}

func TestBatchWriter_FlushesWhenTriggerSignalFires(t *testing.T) {
	t.Parallel()
	recordedBatches := make(chan recordedBatch, 1)
	batchWriter := NewBatchWriter(BatchWriterDependencies{WriteBatchSize: 100, StoreBatch: aRecordingBatchStore(recordedBatches)})
	flushTrigger := make(chan struct{}, 1)
	applicationContext, applicationContextCancellation := context.WithCancel(context.Background())
	defer applicationContextCancellation()
	completed := batchWriter.Run(applicationContext, flushTrigger)

	if storeError := batchWriter.StoreBatch(model.DestinationService1, []model.EnrichedLog{anEnrichedLog()}); storeError != nil {
		t.Fatalf("StoreBatch returned error: %v", storeError)
	}
	flushTrigger <- struct{}{}

	select {
	case recordedBatch := <-recordedBatches:
		if len(recordedBatch.enrichedLogs) != 1 {
			t.Errorf("flushed log count = %d, want 1", len(recordedBatch.enrichedLogs))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("batch was not flushed after the trigger signal")
	}

	applicationContextCancellation()
	waitForBatchWriter(t, completed)
}

func TestBatchWriter_RetainsLogsWhenStoreFails(t *testing.T) {
	t.Parallel()
	storeAttemptCount := 0
	batchWriter := NewBatchWriter(BatchWriterDependencies{
		WriteBatchSize: 1,
		StoreBatch: func(destination model.Destination, enrichedLogs []model.EnrichedLog) error {
			storeAttemptCount++
			return errors.New("store unavailable")
		},
	})

	flushError := batchWriter.StoreBatch(model.DestinationService1, []model.EnrichedLog{anEnrichedLog()})
	if flushError == nil {
		t.Fatal("expected the store failure to be reported")
	}

	batchWriter.FlushAll()
	if storeAttemptCount != 2 {
		t.Fatalf("store attempt count = %d, want 2; failed logs must stay buffered", storeAttemptCount)
	}
}

func TestBatchWriter_RetriesAtConfiguredBackoffIntervalsAndTracksRetryCount(t *testing.T) {
	t.Parallel()
	attemptedBatches := make(chan []model.EnrichedLog, 10)
	recordedDelays := make(chan time.Duration, 10)
	batchWriter := NewBatchWriter(BatchWriterDependencies{
		WriteBatchSize:     1,
		RetryBackoffDelays: []time.Duration{time.Second, 5 * time.Second, 10 * time.Second},
		StoreBatch:         aFailingBatchStore(attemptedBatches),
		WaitForRetryDelay:  aRecordingRetryWaiter(recordedDelays),
	})

	storeError := batchWriter.StoreBatch(model.DestinationService1, []model.EnrichedLog{anEnrichedLog()})
	if storeError == nil {
		t.Fatal("expected the store failure to be reported after every retry")
	}

	expectedAttemptCount := 4
	var lastAttemptedBatch []model.EnrichedLog
	for attemptIndex := 0; attemptIndex < expectedAttemptCount; attemptIndex++ {
		select {
		case lastAttemptedBatch = <-attemptedBatches:
		case <-time.After(2 * time.Second):
			t.Fatalf("store attempt %d did not happen", attemptIndex+1)
		}
	}
	select {
	case <-attemptedBatches:
		t.Fatal("store was attempted more times than the initial attempt plus every retry")
	default:
	}

	expectedRetryDelays := []time.Duration{time.Second, 5 * time.Second, 10 * time.Second}
	for retryIndex, expectedRetryDelay := range expectedRetryDelays {
		select {
		case recordedDelay := <-recordedDelays:
			if recordedDelay != expectedRetryDelay {
				t.Errorf("retry delay %d = %s, want %s", retryIndex, recordedDelay, expectedRetryDelay)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("retry delay %d was not requested", retryIndex)
		}
	}

	if lastAttemptedBatch[0].RetryCount != len(expectedRetryDelays) {
		t.Errorf("last attempted log retry count = %d, want %d", lastAttemptedBatch[0].RetryCount, len(expectedRetryDelays))
	}
}

func TestBatchWriter_FlushesDestinationsConcurrently(t *testing.T) {
	t.Parallel()
	enteredDestinations := make(chan model.Destination, 3)
	releaseFlushes := make(chan struct{})
	batchWriter := NewBatchWriter(BatchWriterDependencies{
		WriteBatchSize: 100,
		StoreBatch: func(destination model.Destination, enrichedLogs []model.EnrichedLog) error {
			enteredDestinations <- destination
			<-releaseFlushes
			return nil
		},
	})
	if storeError := batchWriter.StoreBatch(model.DestinationService1, []model.EnrichedLog{anEnrichedLog()}); storeError != nil {
		t.Fatalf("storing destination service1 log: %v", storeError)
	}
	if storeError := batchWriter.StoreBatch(model.DestinationService2, []model.EnrichedLog{anEnrichedLog()}); storeError != nil {
		t.Fatalf("storing destination service2 log: %v", storeError)
	}

	flushCompleted := make(chan struct{})
	go func() {
		batchWriter.FlushAll()
		close(flushCompleted)
	}()

	enteredDestinationNames := make(map[model.Destination]bool)
	for len(enteredDestinationNames) < 2 {
		select {
		case destination := <-enteredDestinations:
			enteredDestinationNames[destination] = true
		case <-time.After(2 * time.Second):
			t.Fatal("one destination's flush stalled the other destination")
		}
	}

	close(releaseFlushes)
	select {
	case <-flushCompleted:
	case <-time.After(2 * time.Second):
		t.Fatal("FlushAll did not finish after releasing the destinations")
	}
}

func waitForBatchWriter(t *testing.T, completed <-chan struct{}) {
	t.Helper()
	select {
	case <-completed:
	case <-time.After(2 * time.Second):
		t.Fatal("batch writer did not stop after context cancellation")
	}
}

// Builds a configuration that points the store at a temporary database directory.
func aTemporaryDatabaseConfiguration(t *testing.T) config.ApplicationConfiguration {
	t.Helper()
	return config.ApplicationConfiguration{
		DatabaseDirectoryPath: t.TempDir(),
		DatabaseBusyTimeout:   5 * time.Second,
	}
}

func TestNewSQLiteLogStore_CreatesEveryDatabaseFileAndTableFromNothing(t *testing.T) {
	t.Parallel()
	applicationConfiguration := aTemporaryDatabaseConfiguration(t)

	sqliteLogStore, storeError := NewSQLiteLogStore(applicationConfiguration)
	if storeError != nil {
		t.Fatalf("NewSQLiteLogStore returned error: %v", storeError)
	}
	t.Cleanup(func() { sqliteLogStore.Close() })

	expectedDatabaseFileNames := []string{
		config.DatabaseFileService1Name,
		config.DatabaseFileService2Name,
		config.DatabaseFileService3Name,
	}
	for _, databaseFileName := range expectedDatabaseFileNames {
		databasePath := filepath.Join(applicationConfiguration.DatabaseDirectoryPath, databaseFileName)
		if _, fileError := os.Stat(databasePath); fileError != nil {
			t.Errorf("expected database %q to exist: %v", databaseFileName, fileError)
		}
	}

	for destination, destinationDatabase := range sqliteLogStore.databases {
		var rowCount int
		if countError := destinationDatabase.database.QueryRow("SELECT COUNT(*) FROM logs").Scan(&rowCount); countError != nil {
			t.Fatalf("counting rows for destination %q: %v", destination, countError)
		}
		if rowCount != 0 {
			t.Errorf("destination %q row count = %d, want 0", destination, rowCount)
		}

		var journalMode string
		if journalModeError := destinationDatabase.database.QueryRow("PRAGMA journal_mode").Scan(&journalMode); journalModeError != nil {
			t.Fatalf("reading journal mode for destination %q: %v", destination, journalModeError)
		}
		if journalMode != "wal" {
			t.Errorf("destination %q journal mode = %q, want wal", destination, journalMode)
		}

		var busyTimeoutInMilliseconds int
		if busyTimeoutError := destinationDatabase.database.QueryRow("PRAGMA busy_timeout").Scan(&busyTimeoutInMilliseconds); busyTimeoutError != nil {
			t.Fatalf("reading busy timeout for destination %q: %v", destination, busyTimeoutError)
		}
		if int64(busyTimeoutInMilliseconds) != applicationConfiguration.DatabaseBusyTimeout.Milliseconds() {
			t.Errorf("destination %q busy timeout = %d, want %d", destination, busyTimeoutInMilliseconds, applicationConfiguration.DatabaseBusyTimeout.Milliseconds())
		}
	}
}

func TestSQLiteLogStore_StoresEveryColumnForADestination(t *testing.T) {
	t.Parallel()
	applicationConfiguration := aTemporaryDatabaseConfiguration(t)

	sqliteLogStore, storeError := NewSQLiteLogStore(applicationConfiguration)
	if storeError != nil {
		t.Fatalf("NewSQLiteLogStore returned error: %v", storeError)
	}
	t.Cleanup(func() { sqliteLogStore.Close() })

	enrichedLog := anEnrichedLog()
	if storeBatchError := sqliteLogStore.StoreBatch(model.DestinationService2, []model.EnrichedLog{enrichedLog}); storeBatchError != nil {
		t.Fatalf("StoreBatch returned error: %v", storeBatchError)
	}

	var (
		timestamp   string
		service     string
		level       string
		message     string
		receivedAt  string
		sourceIP    string
		environment string
	)
	queryError := sqliteLogStore.databases[model.DestinationService2].database.QueryRow(
		"SELECT timestamp, service, level, message, received_at, source_ip, env FROM logs",
	).Scan(&timestamp, &service, &level, &message, &receivedAt, &sourceIP, &environment)
	if queryError != nil {
		t.Fatalf("reading stored log row: %v", queryError)
	}

	if timestamp != enrichedLog.Timestamp || service != enrichedLog.Service || level != string(enrichedLog.Level) || message != enrichedLog.Message {
		t.Errorf("stored log fields do not match the enriched log: %q %q %q %q", timestamp, service, level, message)
	}
	if receivedAt != "2026-10-24T10:00:00.123456Z" {
		t.Errorf("stored received_at = %q, want microsecond precision timestamp", receivedAt)
	}
	if sourceIP != enrichedLog.SourceIP || environment != enrichedLog.Environment {
		t.Errorf("stored source_ip and env = %q %q, want %q %q", sourceIP, environment, enrichedLog.SourceIP, enrichedLog.Environment)
	}

	var otherDestinationRowCount int
	if countError := sqliteLogStore.databases[model.DestinationService1].database.QueryRow("SELECT COUNT(*) FROM logs").Scan(&otherDestinationRowCount); countError != nil {
		t.Fatalf("counting rows for the other destination: %v", countError)
	}
	if otherDestinationRowCount != 0 {
		t.Errorf("other destination row count = %d, want 0", otherDestinationRowCount)
	}
}
