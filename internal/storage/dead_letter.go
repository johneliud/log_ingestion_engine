package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/johneliud/log_ingestion_engine/internal/config"
	"github.com/johneliud/log_ingestion_engine/internal/model"
)

const defaultDeadLetterBufferSize = 1000

var errDeadLetterWriterStopped = errors.New("dead letter writer is stopped")

// Writes dead letter entries as newline-delimited JSON from one goroutine.
type DeadLetterWriter struct {
	deadLetterFilePath     string
	maximumFileSizeInBytes int64
	entriesChannel         chan model.DeadLetterEntry
	stoppedChannel         chan struct{}
	mutex                  sync.Mutex
	closed                 bool
	writerDone             chan struct{}
}

// Returns a writer that accepts dead letter entries for serialized file writes.
func NewDeadLetterWriter(applicationConfiguration config.ApplicationConfiguration) *DeadLetterWriter {
	return &DeadLetterWriter{
		deadLetterFilePath:     applicationConfiguration.DeadLetterFilePath,
		maximumFileSizeInBytes: int64(applicationConfiguration.DeadLetterMaximumFileSizeInBytes),
		entriesChannel:         make(chan model.DeadLetterEntry, defaultDeadLetterBufferSize),
		stoppedChannel:         make(chan struct{}),
		writerDone:             make(chan struct{}),
	}
}

// Enqueues a dead letter entry, waiting for room unless the writer has stopped.
func (deadLetterWriter *DeadLetterWriter) Submit(deadLetterEntry model.DeadLetterEntry) error {
	deadLetterWriter.mutex.Lock()
	defer deadLetterWriter.mutex.Unlock()
	if deadLetterWriter.closed {
		return errDeadLetterWriterStopped
	}
	select {
	case deadLetterWriter.entriesChannel <- deadLetterEntry:
		return nil
	case <-deadLetterWriter.stoppedChannel:
		return errDeadLetterWriterStopped
	}
}

// Runs the single writer goroutine until the entries channel is closed, then drains every queued entry.
func (deadLetterWriter *DeadLetterWriter) Run() <-chan struct{} {
	completed := make(chan struct{})
	go func() {
		defer close(completed)
		deadLetterWriter.writeEntriesUntilClosed()
		close(deadLetterWriter.stoppedChannel)
		close(deadLetterWriter.writerDone)
	}()
	return completed
}

// Closes the dead letter writer after the batch writer has completed.
func (deadLetterWriter *DeadLetterWriter) Close() error {
	deadLetterWriter.mutex.Lock()
	defer deadLetterWriter.mutex.Unlock()
	if deadLetterWriter.closed {
		return nil
	}
	deadLetterWriter.closed = true
	close(deadLetterWriter.entriesChannel)
	<-deadLetterWriter.writerDone
	return nil
}

// Writes queued entries until the channel is closed, then drains whatever remains.
func (deadLetterWriter *DeadLetterWriter) writeEntriesUntilClosed() {
	var deadLetterFile *os.File
	currentFileSizeInBytes := int64(0)
	defer func() {
		if deadLetterFile != nil {
			deadLetterFile.Close()
		}
	}()

	for deadLetterEntry := range deadLetterWriter.entriesChannel {
		deadLetterWriter.writeEntry(&deadLetterFile, &currentFileSizeInBytes, deadLetterEntry)
	}
	deadLetterWriter.drainEntries(&deadLetterFile, &currentFileSizeInBytes)
}

// Writes every entry still queued after cancellation before the writer exits.
func (deadLetterWriter *DeadLetterWriter) drainEntries(deadLetterFile **os.File, currentFileSizeInBytes *int64) {
	for {
		select {
		case deadLetterEntry, ok := <-deadLetterWriter.entriesChannel:
			if !ok {
				return
			}
			deadLetterWriter.writeEntry(deadLetterFile, currentFileSizeInBytes, deadLetterEntry)
		default:
			return
		}
	}
}

// Writes one entry, opening or rotating the file when necessary, and logs any failure with its details.
func (deadLetterWriter *DeadLetterWriter) writeEntry(deadLetterFile **os.File, currentFileSizeInBytes *int64, deadLetterEntry model.DeadLetterEntry) {
	serializedEntry, marshalError := json.Marshal(deadLetterEntry)
	if marshalError != nil {
		slog.Error("marshalling dead letter entry failed", "error", marshalError, "batch_id", deadLetterEntry.Log.BatchID, "destination", deadLetterEntry.Log.Destination)
		return
	}
	serializedEntry = append(serializedEntry, '\n')

	if *deadLetterFile == nil {
		openedFile, fileSizeInBytes, openError := openOrCreateDeadLetterFile(deadLetterWriter.deadLetterFilePath)
		if openError != nil {
			slog.Error("opening dead letter file failed", "error", openError, "dead_letter_path", deadLetterWriter.deadLetterFilePath)
			return
		}
		*deadLetterFile = openedFile
		*currentFileSizeInBytes = fileSizeInBytes
	}

	if *currentFileSizeInBytes > 0 && *currentFileSizeInBytes+int64(len(serializedEntry)) > deadLetterWriter.maximumFileSizeInBytes {
		if closeError := (*deadLetterFile).Close(); closeError != nil {
			slog.Error("closing dead letter file before rotation failed", "error", closeError, "dead_letter_path", deadLetterWriter.deadLetterFilePath)
		}

		*deadLetterFile = nil

		if rotateError := rotateDeadLetterFile(deadLetterWriter.deadLetterFilePath); rotateError != nil {
			slog.Error("rotating dead letter file failed", "error", rotateError, "dead_letter_path", deadLetterWriter.deadLetterFilePath)
			return
		}

		openedFile, fileSizeInBytes, openError := openOrCreateDeadLetterFile(deadLetterWriter.deadLetterFilePath)
		if openError != nil {
			slog.Error("opening dead letter file after rotation failed", "error", openError, "dead_letter_path", deadLetterWriter.deadLetterFilePath)
			return
		}
		*deadLetterFile = openedFile
		*currentFileSizeInBytes = fileSizeInBytes
	}

	writtenByteCount, writeError := (*deadLetterFile).Write(serializedEntry)
	if writeError != nil {
		slog.Error("writing dead letter entry failed", "error", writeError, "batch_id", deadLetterEntry.Log.BatchID, "destination", deadLetterEntry.Log.Destination)
		return
	}
	*currentFileSizeInBytes += int64(writtenByteCount)
}

// Opens the dead letter file for appending and reports its current size.
func openOrCreateDeadLetterFile(deadLetterFilePath string) (*os.File, int64, error) {
	if directoryCreationError := os.MkdirAll(filepath.Dir(deadLetterFilePath), 0o755); directoryCreationError != nil {
		return nil, 0, fmt.Errorf("creating dead letter directory: %w", directoryCreationError)
	}

	deadLetterFile, openError := os.OpenFile(deadLetterFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if openError != nil {
		return nil, 0, fmt.Errorf("opening dead letter file %q: %w", deadLetterFilePath, openError)
	}

	fileInformation, statError := deadLetterFile.Stat()
	if statError != nil {
		deadLetterFile.Close()
		return nil, 0, fmt.Errorf("reading dead letter file size: %w", statError)
	}
	return deadLetterFile, fileInformation.Size(), nil
}

// Renames the full dead letter file to the next available rotated name.
func rotateDeadLetterFile(deadLetterFilePath string) error {
	for rotationIndex := 1; ; rotationIndex++ {
		rotatedFilePath := rotatedDeadLetterFilePath(deadLetterFilePath, rotationIndex)
		_, statError := os.Stat(rotatedFilePath)
		if errors.Is(statError, os.ErrNotExist) {
			return os.Rename(deadLetterFilePath, rotatedFilePath)
		}

		if statError != nil {
			return fmt.Errorf("checking rotated dead letter file %q: %w", rotatedFilePath, statError)
		}
	}
}

// Builds the numbered rotated file name that keeps the original extension.
func rotatedDeadLetterFilePath(deadLetterFilePath string, rotationIndex int) string {
	extension := filepath.Ext(deadLetterFilePath)
	return fmt.Sprintf("%s.%d%s", strings.TrimSuffix(deadLetterFilePath, extension), rotationIndex, extension)
}
