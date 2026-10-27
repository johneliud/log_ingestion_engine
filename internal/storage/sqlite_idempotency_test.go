package storage

import (
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/johneliud/log_ingestion_engine/internal/model"
)

// Counts the rows in the logs table of one destination database.
func countLogsTableRows(t *testing.T, databasePath string) int {
	t.Helper()
	database, openError := sql.Open("sqlite", databasePath)
	if openError != nil {
		t.Fatalf("opening database %q: %v", databasePath, openError)
	}
	defer database.Close()

	var rowCount int
	if scanError := database.QueryRow("SELECT COUNT(*) FROM logs").Scan(&rowCount); scanError != nil {
		t.Fatalf("counting rows in database %q: %v", databasePath, scanError)
	}
	return rowCount
}

func TestSQLiteLogStore_StoreBatchIsIdempotentOnRepeatedLogIdentifiers(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name             string
		batches          [][]model.EnrichedLog
		expectedRowCount int
	}{
		{
			name: "the same batch written twice stores one row per log",
			batches: [][]model.EnrichedLog{
				{anEnrichedLog()},
				{anEnrichedLog()},
			},
			expectedRowCount: 1,
		},
		{
			name: "a replayed dead-letter entry deduplicated against its stored original",
			batches: [][]model.EnrichedLog{
				{anOriginalEnrichedLog()},
				{aRedeliveredEnrichedLog()},
			},
			expectedRowCount: 1,
		},
		{
			name: "logs with distinct identifiers each store one row",
			batches: [][]model.EnrichedLog{
				{anEnrichedLog(), aSecondEnrichedLog()},
			},
			expectedRowCount: 2,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			applicationConfiguration := aTemporaryDatabaseConfiguration(t)
			sqliteLogStore, storeError := NewSQLiteLogStore(applicationConfiguration)
			if storeError != nil {
				t.Fatalf("NewSQLiteLogStore returned error: %v", storeError)
			}
			defer sqliteLogStore.Close()

			for _, batch := range testCase.batches {
				if storeError := sqliteLogStore.StoreBatch(model.DestinationService1, batch); storeError != nil {
					t.Fatalf("StoreBatch returned error: %v", storeError)
				}
			}

			databasePath := filepath.Join(applicationConfiguration.DatabaseDirectoryPath, "logs_service1.db")
			rowCount := countLogsTableRows(t, databasePath)
			if rowCount != testCase.expectedRowCount {
				t.Errorf("row count = %d, want %d", rowCount, testCase.expectedRowCount)
			}
		})
	}
}

func TestSQLiteLogStore_OpensALegacyDatabaseAndStoresLogsIdempotently(t *testing.T) {
	t.Parallel()
	applicationConfiguration := aTemporaryDatabaseConfiguration(t)
	databasePath := filepath.Join(applicationConfiguration.DatabaseDirectoryPath, "logs_service1.db")

	legacyDatabase, openError := sql.Open("sqlite", databasePath)
	if openError != nil {
		t.Fatalf("opening the legacy database: %v", openError)
	}
	legacyCreateStatement := "CREATE TABLE logs (id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TEXT NOT NULL, service TEXT NOT NULL, level TEXT NOT NULL, message TEXT NOT NULL, received_at TEXT NOT NULL, source_ip TEXT NOT NULL, env TEXT NOT NULL)"
	if _, createError := legacyDatabase.Exec(legacyCreateStatement); createError != nil {
		legacyDatabase.Close()
		t.Fatalf("creating the legacy table: %v", createError)
	}
	legacyInsertStatement := "INSERT INTO logs (timestamp, service, level, message, received_at, source_ip, env) VALUES ('2026-01-01T00:00:00Z', 'claims', 'ERROR', 'legacy row', '2026-01-01T00:00:00Z', '203.0.113.7', 'production')"
	if _, insertError := legacyDatabase.Exec(legacyInsertStatement); insertError != nil {
		legacyDatabase.Close()
		t.Fatalf("inserting the legacy row: %v", insertError)
	}
	legacyDatabase.Close()

	sqliteLogStore, storeError := NewSQLiteLogStore(applicationConfiguration)
	if storeError != nil {
		t.Fatalf("NewSQLiteLogStore returned error for a legacy database: %v", storeError)
	}

	enrichedLog := aRedeliveredEnrichedLog()
	if storeError := sqliteLogStore.StoreBatch(model.DestinationService1, []model.EnrichedLog{enrichedLog}); storeError != nil {
		t.Fatalf("StoreBatch returned error for a legacy database: %v", storeError)
	}
	if storeError := sqliteLogStore.StoreBatch(model.DestinationService1, []model.EnrichedLog{enrichedLog}); storeError != nil {
		t.Fatalf("repeated StoreBatch returned error for a legacy database: %v", storeError)
	}
	if closeError := sqliteLogStore.Close(); closeError != nil {
		t.Fatalf("Close returned error: %v", closeError)
	}

	rowCount := countLogsTableRows(t, databasePath)
	if rowCount != 2 {
		t.Errorf("row count = %d, want 2 (one legacy row, one deduplicated log)", rowCount)
	}
}

func aSecondEnrichedLog() model.EnrichedLog {
	enrichedLog := anEnrichedLog()
	enrichedLog.LogID = "log-id-2"
	enrichedLog.Message = "claim retried"
	enrichedLog.ReceivedAt = enrichedLog.ReceivedAt.Add(time.Second)
	return enrichedLog
}

func anOriginalEnrichedLog() model.EnrichedLog {
	enrichedLog := anEnrichedLog()
	enrichedLog.LogID = "log-id-1"
	return enrichedLog
}

func aRedeliveredEnrichedLog() model.EnrichedLog {
	enrichedLog := anOriginalEnrichedLog()
	enrichedLog.RetryCount = 1
	return enrichedLog
}
