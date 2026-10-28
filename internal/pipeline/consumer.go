package pipeline

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/johneliud/log_ingestion_engine/internal/model"
)

type enricher interface {
	Enrich(rawLog model.RawLog) model.EnrichedLog
}

type router interface {
	Route(enrichedLog model.EnrichedLog) (model.EnrichedLog, error)
}

type queuePublisher interface {
	PublishBatch(enrichedLogs []model.EnrichedLog) error
}

// Records a log that could not be processed so it is not lost.
type deadLetterSink func(deadLetterEntry model.DeadLetterEntry) error

// Supplies the queue, the worker settings and the processing steps of a batch consumer.
type BatchConsumerDependencies struct {
	RawLogsQueue        *RawLogsQueue
	ConsumerBatchSize   int
	WorkerCount         int
	Enricher            enricher
	Router              router
	QueuePublisher      queuePublisher
	DeadLetterSink      deadLetterSink
	DefaultDestination  model.Destination
	CurrentTimeProvider func() time.Time
}

// Consumes batches from raw logs, enriches, routes, and publishes.
type BatchConsumer struct {
	rawLogsQueue        *RawLogsQueue
	consumerBatchSize   int
	enricher            enricher
	router              router
	queuePublisher      queuePublisher
	deadLetterSink      deadLetterSink
	defaultDestination  model.Destination
	currentTimeProvider func() time.Time
	workerCount         int
}

// Returns a new batch consumer.
func NewBatchConsumer(dependencies BatchConsumerDependencies) *BatchConsumer {
	currentTimeProvider := dependencies.CurrentTimeProvider
	if currentTimeProvider == nil {
		currentTimeProvider = time.Now
	}

	return &BatchConsumer{
		rawLogsQueue:        dependencies.RawLogsQueue,
		consumerBatchSize:   dependencies.ConsumerBatchSize,
		enricher:            dependencies.Enricher,
		router:              dependencies.Router,
		queuePublisher:      dependencies.QueuePublisher,
		deadLetterSink:      dependencies.DeadLetterSink,
		defaultDestination:  dependencies.DefaultDestination,
		currentTimeProvider: currentTimeProvider,
		workerCount:         dependencies.WorkerCount,
	}
}

// Starts workers that process batches until context is done and the queue is drained.
func (batchConsumer *BatchConsumer) Start(applicationContext context.Context) <-chan struct{} {
	completed := make(chan struct{})
	var waitGroup sync.WaitGroup
	waitGroup.Add(batchConsumer.workerCount)
	for workerIndex := 0; workerIndex < batchConsumer.workerCount; workerIndex++ {
		go func() {
			defer waitGroup.Done()
			for {
				batch, queueOpen := batchConsumer.collectBatch(applicationContext)
				if !queueOpen {
					return
				}
				if len(batch) == 0 {
					continue
				}
				batchConsumer.processBatch(batch)
			}
		}()
	}
	go func() {
		waitGroup.Wait()
		close(completed)
	}()
	return completed
}

// Enriches, routes and publishes one batch of raw logs, dead-lettering the logs that fail either step.
func (batchConsumer *BatchConsumer) processBatch(batch []model.RawLog) {
	publishableLogs := make([]model.EnrichedLog, 0, len(batch))
	for _, rawLog := range batch {
		enrichedLog := batchConsumer.enricher.Enrich(rawLog)
		routedLog, routeError := batchConsumer.router.Route(enrichedLog)
		if routeError != nil {
			slog.Error("routing log failed", "error", routeError, "batch_id", enrichedLog.BatchID, "service", enrichedLog.Service)
			enrichedLog.Destination = batchConsumer.defaultDestinationOrFallback()
			batchConsumer.deadLetterUnprocessedLog(enrichedLog, routeError.Error())
			continue
		}
		publishableLogs = append(publishableLogs, routedLog)
	}

	if batchConsumer.queuePublisher == nil || len(publishableLogs) == 0 {
		return
	}
	if publishError := batchConsumer.queuePublisher.PublishBatch(publishableLogs); publishError != nil {
		slog.Error("publishing logs failed", "error", publishError, "log_count", len(publishableLogs))
		for _, routedLog := range publishableLogs {
			batchConsumer.deadLetterUnprocessedLog(routedLog, publishError.Error())
		}
	}
}

// Returns the destination used for a log that could not be routed.
func (batchConsumer *BatchConsumer) defaultDestinationOrFallback() model.Destination {
	if batchConsumer.defaultDestination == "" {
		return model.DestinationService1
	}
	return batchConsumer.defaultDestination
}

// Records a log that could not be processed so it is not lost.
func (batchConsumer *BatchConsumer) deadLetterUnprocessedLog(enrichedLog model.EnrichedLog, failureMessage string) {
	if batchConsumer.deadLetterSink == nil {
		return
	}

	deadLetterEntry := model.DeadLetterEntry{
		Log:          enrichedLog,
		ErrorMessage: failureMessage,
		FailedAt:     batchConsumer.currentTimeProvider(),
	}
	if submitError := batchConsumer.deadLetterSink(deadLetterEntry); submitError != nil {
		slog.Error("writing the dead letter entry failed", "error", submitError, "batch_id", enrichedLog.BatchID, "service", enrichedLog.Service)
	}
}

// Blocks until at least one raw log is available, then gathers up to the consumer batch size without blocking.
func (batchConsumer *BatchConsumer) collectBatch(applicationContext context.Context) ([]model.RawLog, bool) {
	firstRawLog, queueOpen := batchConsumer.rawLogsQueue.Receive(applicationContext)
	if !queueOpen {
		return nil, false
	}

	batch := make([]model.RawLog, 0, batchConsumer.consumerBatchSize)
	batch = append(batch, firstRawLog)
	for len(batch) < batchConsumer.consumerBatchSize {
		nextRawLog, hasMore := batchConsumer.rawLogsQueue.TryReceive()
		if !hasMore {
			break
		}
		batch = append(batch, nextRawLog)
	}
	return batch, true
}
