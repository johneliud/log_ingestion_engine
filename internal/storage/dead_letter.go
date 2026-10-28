package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/johneliud/log_ingestion_engine/internal/config"
	"github.com/johneliud/log_ingestion_engine/internal/model"
)

const defaultDeadLetterBufferSize = 1000

type deadLetterSubmission struct {
	entry  model.DeadLetterEntry
	result chan error
}

var errDeadLetterWriterStopped = errors.New("dead letter writer is stopped")

// Writes dead letter entries as newline-delimited JSON from one goroutine.
type DeadLetterWriter struct {
	deadLetterFilePath     string
	maximumFileSizeInBytes int64
	entriesChannel         chan deadLetterSubmission
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
		entriesChannel:         make(chan deadLetterSubmission, defaultDeadLetterBufferSize),
		stoppedChannel:         make(chan struct{}),
		writerDone:             make(chan struct{}),
	}
}

// Submits an entry and waits for its durable write and sync result.
func (deadLetterWriter *DeadLetterWriter) Submit(deadLetterEntry model.DeadLetterEntry) error {
	submission := deadLetterSubmission{entry: deadLetterEntry, result: make(chan error, 1)}
	deadLetterWriter.mutex.Lock()
	if deadLetterWriter.closed {
		deadLetterWriter.mutex.Unlock()
		return errDeadLetterWriterStopped
	}
	select {
	case deadLetterWriter.entriesChannel <- submission:
		deadLetterWriter.mutex.Unlock()
		return <-submission.result
	case <-deadLetterWriter.stoppedChannel:
		deadLetterWriter.mutex.Unlock()
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

	for submission := range deadLetterWriter.entriesChannel {
		err := deadLetterWriter.writeEntry(&deadLetterFile, &currentFileSizeInBytes, submission.entry)
		if err != nil {
			slog.Error("writing dead letter entry failed", "error", err, "batch_id", submission.entry.Log.BatchID)
		}
		submission.result <- err
	}
}

// Writes one entry, opening or rotating the file when necessary, and returns any write or sync failure.
func (deadLetterWriter *DeadLetterWriter) writeEntry(deadLetterFile **os.File, currentFileSizeInBytes *int64, deadLetterEntry model.DeadLetterEntry) error {
	serializedEntry, marshalError := json.Marshal(deadLetterEntry)
	if marshalError != nil {
		return fmt.Errorf("marshalling dead letter entry failed: %w", marshalError)
	}
	serializedEntry = append(serializedEntry, '\n')

	if *deadLetterFile == nil {
		openedFile, fileSizeInBytes, openError := openOrCreateDeadLetterFile(deadLetterWriter.deadLetterFilePath)
		if openError != nil {
			return fmt.Errorf("opening dead letter file failed: %w", openError)
		}
		*deadLetterFile = openedFile
		*currentFileSizeInBytes = fileSizeInBytes
	}

	if *currentFileSizeInBytes > 0 && *currentFileSizeInBytes+int64(len(serializedEntry)) > deadLetterWriter.maximumFileSizeInBytes {
		if closeError := (*deadLetterFile).Close(); closeError != nil {
			*deadLetterFile = nil
			return fmt.Errorf("closing dead letter file before rotation: %w", closeError)
		}

		*deadLetterFile = nil

		if rotateError := rotateDeadLetterFile(deadLetterWriter.deadLetterFilePath); rotateError != nil {
			return fmt.Errorf("rotating dead letter file failed: %w", rotateError)
		}

		openedFile, fileSizeInBytes, openError := openOrCreateDeadLetterFile(deadLetterWriter.deadLetterFilePath)
		if openError != nil {
			return fmt.Errorf("opening dead letter file after rotation failed: %w", openError)
		}
		*deadLetterFile = openedFile
		*currentFileSizeInBytes = fileSizeInBytes
	}

	writtenByteCount, writeError := (*deadLetterFile).Write(serializedEntry)
	if writeError != nil {
		return fmt.Errorf("writing dead letter entry failed: %w", writeError)
	}
	*currentFileSizeInBytes += int64(writtenByteCount)

	if writtenByteCount != len(serializedEntry) {
		return io.ErrShortWrite
	}
	if syncError := (*deadLetterFile).Sync(); syncError != nil {
		return fmt.Errorf("syncing dead letter file: %w", syncError)
	}
	return nil
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
