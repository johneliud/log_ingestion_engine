package storage

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"

	"github.com/johneliud/log_ingestion_engine/internal/model"
)

const maximumDeadLetterLineSizeInBytes = 1024 * 1024

// Delivers every dead-lettered log in the file back to the provided pipeline sink.
func ReplayDeadLetters(deadLetterFilePath string, replaySink func(enrichedLog model.EnrichedLog) error) (int, error) {
	deadLetterFile, openError := os.Open(deadLetterFilePath)
	if openError != nil {
		return 0, fmt.Errorf("opening dead letter file %q: %w", deadLetterFilePath, openError)
	}
	defer deadLetterFile.Close()

	lineScanner := bufio.NewScanner(deadLetterFile)
	lineScanner.Buffer(make([]byte, 0, 64*1024), maximumDeadLetterLineSizeInBytes)

	replayedLogCount := 0
	for lineScanner.Scan() {
		line := lineScanner.Bytes()
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}

		var deadLetterEntry model.DeadLetterEntry
		if unmarshalError := json.Unmarshal(line, &deadLetterEntry); unmarshalError != nil {
			return replayedLogCount, fmt.Errorf("parsing dead letter entry: %w", unmarshalError)
		}
		
		if replayError := replaySink(deadLetterEntry.Log); replayError != nil {
			return replayedLogCount, fmt.Errorf("replaying dead letter entry: %w", replayError)
		}
		replayedLogCount++
	}
	
	if scanError := lineScanner.Err(); scanError != nil {
		return replayedLogCount, fmt.Errorf("reading dead letter file %q: %w", deadLetterFilePath, scanError)
	}
	return replayedLogCount, nil
}
