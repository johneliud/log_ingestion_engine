package main

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/johneliud/log_ingestion_engine/internal/config"
	"github.com/johneliud/log_ingestion_engine/internal/model"
	"github.com/johneliud/log_ingestion_engine/internal/storage"
)

// Replays dead-lettered logs from a file back into their destination SQLite databases.
func main() {
	applicationConfiguration, configurationError := config.NewApplicationConfiguration()
	if configurationError != nil {
		slog.Error("loading the application configuration failed", "configuration_error", configurationError)
		os.Exit(1)
	}

	deadLetterFilePath := applicationConfiguration.DeadLetterFilePath
	if len(os.Args) > 1 {
		deadLetterFilePath = os.Args[1]
	}

	sqliteLogStore, storeError := storage.NewSQLiteLogStore(applicationConfiguration)
	if storeError != nil {
		slog.Error("opening the SQLite log store failed", "store_error", storeError)
		os.Exit(1)
	}
	defer sqliteLogStore.Close()

	replayedLogCount, replayError := storage.ReplayDeadLetters(deadLetterFilePath, func(enrichedLog model.EnrichedLog) error {
		return sqliteLogStore.StoreBatch(enrichedLog.Destination, []model.EnrichedLog{enrichedLog})
	})
	if replayError != nil {
		slog.Error("replaying dead letter logs failed", "replay_error", replayError, "replayed_log_count", replayedLogCount)
		os.Exit(1)
	}
	fmt.Printf("replayed %d dead-lettered logs from %s\n", replayedLogCount, deadLetterFilePath)
}
