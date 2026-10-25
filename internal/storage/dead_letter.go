package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

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
}

// Returns a writer that accepts dead letter entries for serialized file writes.
func NewDeadLetterWriter(applicationConfiguration config.ApplicationConfiguration) *DeadLetterWriter {
	return &DeadLetterWriter{
		deadLetterFilePath:     applicationConfiguration.DeadLetterFilePath,
		maximumFileSizeInBytes: int64(applicationConfiguration.DeadLetterMaximumFileSizeInBytes),
		entriesChannel:         make(chan model.DeadLetterEntry, defaultDeadLetterBufferSize),
		stoppedChannel:         make(chan struct{}),
	}
}

// Enqueues a dead letter entry, waiting for room unless the writer has stopped.
func (deadLetterWriter *DeadLetterWriter) Submit(deadLetterEntry model.DeadLetterEntry) error {
	select {
	case <-deadLetterWriter.stoppedChannel:
		return errDeadLetterWriterStopped
	default:
	}

	select {
	case deadLetterWriter.entriesChannel <- deadLetterEntry:
		return nil
	case <-deadLetterWriter.stoppedChannel:
		return errDeadLetterWriterStopped
	}
}

