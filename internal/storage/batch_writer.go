package storage

import (
	"context"
	"log/slog"
	"sync"

	"github.com/johneliud/log_ingestion_engine/internal/model"
)

// Stores one destination batch in durable storage.
type batchStore func(destination model.Destination, enrichedLogs []model.EnrichedLog) error

// Buffers logs per destination and flushes them at a size or interval trigger.
type BatchWriter struct {
	mutex                    sync.Mutex
	writeBatchSize           int
	pendingLogsByDestination map[model.Destination][]model.EnrichedLog
	storeBatch               batchStore
}

// Returns a batch writer that flushes a destination once it reaches the configured size.
func NewBatchWriter(writeBatchSize int, storeBatch batchStore) *BatchWriter {
	return &BatchWriter{
		writeBatchSize:           writeBatchSize,
		pendingLogsByDestination: make(map[model.Destination][]model.EnrichedLog),
		storeBatch:               storeBatch,
	}
}

// Appends logs to their destination buffer and flushes once the size trigger is met.
func (batchWriter *BatchWriter) StoreBatch(destination model.Destination, enrichedLogs []model.EnrichedLog) error {
	batchWriter.mutex.Lock()
	batchWriter.pendingLogsByDestination[destination] = append(batchWriter.pendingLogsByDestination[destination], enrichedLogs...)
	if len(batchWriter.pendingLogsByDestination[destination]) < batchWriter.writeBatchSize {
		batchWriter.mutex.Unlock()
		return nil
	}
	logsToFlush := batchWriter.pendingLogsByDestination[destination]
	batchWriter.pendingLogsByDestination[destination] = nil
	batchWriter.mutex.Unlock()

	return batchWriter.flush(destination, logsToFlush)
}
