package storage

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/johneliud/log_ingestion_engine/internal/model"
)

// Stores one destination batch in durable storage.
type batchStore func(destination model.Destination, enrichedLogs []model.EnrichedLog) error

// Waits for a retry delay or reports that the application is shutting down.
type retryWaiter func(applicationContext context.Context, retryDelay time.Duration) error

// Supplies the buffering, retry settings and storage side effect for a batch writer.
type BatchWriterDependencies struct {
	ApplicationContext context.Context
	WriteBatchSize     int
	RetryBackoffDelays []time.Duration
	StoreBatch         batchStore
	WaitForRetryDelay  retryWaiter
}

// Represents the buffered logs and in-flight flush state for one destination.
type destinationBuffer struct {
	mutex       sync.Mutex
	pendingLogs []model.EnrichedLog
	isFlushing  bool
}

// Buffers logs per destination and flushes them at a size or interval trigger with retries.
type BatchWriter struct {
	applicationContext context.Context
	writeBatchSize     int
	retryBackoffDelays []time.Duration
	storeBatch         batchStore
	waitForRetryDelay  retryWaiter
	buffers            map[model.Destination]*destinationBuffer
}

// Returns a batch writer that flushes a destination once it reaches the configured size.
func NewBatchWriter(dependencies BatchWriterDependencies) *BatchWriter {
	applicationContext := dependencies.ApplicationContext
	if applicationContext == nil {
		applicationContext = context.Background()
	}

	waitForRetryDelay := dependencies.WaitForRetryDelay
	if waitForRetryDelay == nil {
		waitForRetryDelay = waitForRetryDuration
	}

	buffers := make(map[model.Destination]*destinationBuffer, 3)
	for _, destination := range []model.Destination{model.DestinationService1, model.DestinationService2, model.DestinationService3} {
		buffers[destination] = &destinationBuffer{}
	}
	return &BatchWriter{
		applicationContext: applicationContext,
		writeBatchSize:     dependencies.WriteBatchSize,
		retryBackoffDelays: dependencies.RetryBackoffDelays,
		storeBatch:         dependencies.StoreBatch,
		waitForRetryDelay:  waitForRetryDelay,
		buffers:            buffers,
	}
}

// Appends logs to their destination buffer and flushes once the size trigger is met.
func (batchWriter *BatchWriter) StoreBatch(destination model.Destination, enrichedLogs []model.EnrichedLog) error {
	destinationBuffer, destinationExists := batchWriter.buffers[destination]
	if !destinationExists {
		return fmt.Errorf("no buffer configured for destination %q", destination)
	}
	
	destinationBuffer.mutex.Lock()
	destinationBuffer.pendingLogs = append(destinationBuffer.pendingLogs, enrichedLogs...)
	shouldFlush := len(destinationBuffer.pendingLogs) >= batchWriter.writeBatchSize && !destinationBuffer.isFlushing
	destinationBuffer.mutex.Unlock()

	if !shouldFlush {
		return nil
	}
	return batchWriter.flushDestination(destination)
}

// Flushes every destination buffer concurrently when the trigger fires or the context is cancelled.
func (batchWriter *BatchWriter) Run(applicationContext context.Context, flushTrigger <-chan struct{}) <-chan struct{} {
	completed := make(chan struct{})
	go func() {
		defer close(completed)
		for {
			select {
			case <-applicationContext.Done():
				batchWriter.FlushAll()
				return
			case <-flushTrigger:
				batchWriter.FlushAll()
			}
		}
	}()
	return completed
}

// Flushes every destination buffer without letting one destination's retries stall the others.
func (batchWriter *BatchWriter) FlushAll() {
	var waitGroup sync.WaitGroup
	for destination := range batchWriter.buffers {
		waitGroup.Add(1)
		go func(destinationValue model.Destination) {
			defer waitGroup.Done()
			batchWriter.flushDestination(destinationValue)
		}(destination)
	}
	waitGroup.Wait()
}

// Writes the pending logs for one destination, waiting for any in-flight flush to finish first.
func (batchWriter *BatchWriter) flushDestination(destination model.Destination) error {
	destinationBuffer := batchWriter.buffers[destination]
	destinationBuffer.mutex.Lock()
	if destinationBuffer.isFlushing || len(destinationBuffer.pendingLogs) == 0 {
		destinationBuffer.mutex.Unlock()
		return nil
	}

	destinationBuffer.isFlushing = true
	logsToFlush := destinationBuffer.pendingLogs
	destinationBuffer.pendingLogs = nil
	destinationBuffer.mutex.Unlock()

	storeError := batchWriter.storeWithRetries(destination, logsToFlush)

	destinationBuffer.mutex.Lock()
	destinationBuffer.isFlushing = false
	if storeError != nil {
		destinationBuffer.pendingLogs = append(logsToFlush, destinationBuffer.pendingLogs...)
	}
	destinationBuffer.mutex.Unlock()
	return storeError
}

// Attempts the batch store, retrying with the configured backoff while updating each log's retry count.
func (batchWriter *BatchWriter) storeWithRetries(destination model.Destination, enrichedLogs []model.EnrichedLog) error {
	for retryIndex := 0; ; retryIndex++ {
		storeError := batchWriter.storeBatch(destination, enrichedLogs)
		if storeError == nil {
			return nil
		}

		if retryIndex >= len(batchWriter.retryBackoffDelays) {
			for logIndex := range enrichedLogs {
				enrichedLogs[logIndex].RetryCount = retryIndex
			}
			slog.Error("storing log batch failed after retries", "error", storeError, "destination", destination, "log_count", len(enrichedLogs), "retry_count", retryIndex)
			return storeError
		}

		retryDelay := batchWriter.retryBackoffDelays[retryIndex]
		for logIndex := range enrichedLogs {
			enrichedLogs[logIndex].RetryCount = retryIndex + 1
		}
		slog.Warn("retrying failed log batch write", "error", storeError, "destination", destination, "log_count", len(enrichedLogs), "retry_count", retryIndex+1, "retry_delay", retryDelay)

		if waitError := batchWriter.waitForRetryDelay(batchWriter.applicationContext, retryDelay); waitError != nil {
			return waitError
		}
	}
}

// Waits for the retry delay unless the application context is cancelled first.
func waitForRetryDuration(applicationContext context.Context, retryDelay time.Duration) error {
	retryTimer := time.NewTimer(retryDelay)
	defer retryTimer.Stop()
	select {
	case <-applicationContext.Done():
		return applicationContext.Err()
	case <-retryTimer.C:
		return nil
	}
}
