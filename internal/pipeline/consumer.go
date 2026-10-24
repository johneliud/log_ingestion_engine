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

// Consumes batches from raw logs, enriches, routes, and publishes.
type BatchConsumer struct {
	rawLogsChannel    <-chan model.RawLog
	consumerBatchSize int
	enricher          enricher
	router            router
	queuePublisher    queuePublisher
	workerCount       int
}

// Returns a new batch consumer.
func NewBatchConsumer(rawLogsChannel <-chan model.RawLog, consumerBatchSize int, workerCount int, enricher enricher, router router, queuePublisher queuePublisher) *BatchConsumer {
	return &BatchConsumer{
		rawLogsChannel:    rawLogsChannel,
		consumerBatchSize: consumerBatchSize,
		workerCount:       workerCount,
		enricher:          enricher,
		router:            router,
		queuePublisher:    queuePublisher,
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
					enrichedLog := batchConsumer.enricher.Enrich(rawLog)
					routedLog, routeError := batchConsumer.router.Route(enrichedLog)
					if routeError != nil {
						slog.Error("routing log failed", "error", routeError, "batch_id", enrichedLog.BatchID, "service", enrichedLog.Service)
						continue
					}
					if batchConsumer.queuePublisher != nil {
						if publishError := batchConsumer.queuePublisher.Publish(routedLog); publishError != nil {
							slog.Error("publishing log failed", "error", publishError, "batch_id", routedLog.BatchID, "service", routedLog.Service)
						}
					}
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
