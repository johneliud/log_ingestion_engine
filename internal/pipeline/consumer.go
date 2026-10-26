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
	Publish(enrichedLog model.EnrichedLog) error
}

// Records a log that could not be processed so it is not lost.
type deadLetterSink func(deadLetterEntry model.DeadLetterEntry) error

// Supplies the queue, the worker settings and the processing steps of a batch consumer.
type BatchConsumerDependencies struct {
	RawLogsChannel      <-chan model.RawLog
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
	rawLogsChannel      <-chan model.RawLog
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
		rawLogsChannel:      dependencies.RawLogsChannel,
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

// Starts workers that process batches until context is done and channel is drained.
func (batchConsumer *BatchConsumer) Start(applicationContext context.Context) <-chan struct{} {
	completed := make(chan struct{})
	var waitGroup sync.WaitGroup
	waitGroup.Add(batchConsumer.workerCount)
	for workerIndex := 0; workerIndex < batchConsumer.workerCount; workerIndex++ {
		go func() {
			defer waitGroup.Done()
			for {
				batch := batchConsumer.collectBatch()
				if len(batch) == 0 {
					select {
					case <-applicationContext.Done():
						return
					default:
						time.Sleep(10 * time.Millisecond)
						continue
					}
				}

				for _, rawLog := range batch {
					batchConsumer.processRawLog(rawLog)
				}
			}
		}()
	}
	go func() {
		waitGroup.Wait()
		close(completed)
	}()
	return completed
}

// Enriches, routes and publishes one raw log, dead-lettering it when a step fails.
func (batchConsumer *BatchConsumer) processRawLog(rawLog model.RawLog) {
	enrichedLog := batchConsumer.enricher.Enrich(rawLog)
	routedLog, routeError := batchConsumer.router.Route(enrichedLog)
	if routeError != nil {
		slog.Error("routing log failed", "error", routeError, "batch_id", enrichedLog.BatchID, "service", enrichedLog.Service)
		enrichedLog.Destination = batchConsumer.defaultDestinationOrFallback()
		batchConsumer.deadLetterUnprocessedLog(enrichedLog, routeError.Error())
		return
	}

	if batchConsumer.queuePublisher == nil {
		return
	}
	if publishError := batchConsumer.queuePublisher.Publish(routedLog); publishError != nil {
		slog.Error("publishing log failed", "error", publishError, "batch_id", routedLog.BatchID, "service", routedLog.Service)
		batchConsumer.deadLetterUnprocessedLog(routedLog, publishError.Error())
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

func (batchConsumer *BatchConsumer) collectBatch() []model.RawLog {
	batch := make([]model.RawLog, 0, batchConsumer.consumerBatchSize)
	for len(batch) < batchConsumer.consumerBatchSize {
		select {
		case rawLog, channelOpen := <-batchConsumer.rawLogsChannel:
			if !channelOpen {
				return batch
			}
			batch = append(batch, rawLog)
		default:
			if len(batch) > 0 {
				return batch
			}
			return batch
		}
	}
	return batch
}
