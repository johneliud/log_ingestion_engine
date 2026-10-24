package pipeline

import (
	"context"
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
func (batchConsumer *BatchConsumer) Start(applicationContext context.Context) {
	var waitGroup sync.WaitGroup
	waitGroup.Add(batchConsumer.workerCount)
	for workerIndex := 0; workerIndex < batchConsumer.workerCount; workerIndex++ {
		go func() {
			defer waitGroup.Done()
			for {
				select {
				case <-applicationContext.Done():
					return
				default:
				}

				batch := batchConsumer.collectBatch(applicationContext)
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
					select {
					case <-applicationContext.Done():
						return
					default:
					}
					enrichedLog := batchConsumer.enricher.Enrich(rawLog)
					routedLog, routeError := batchConsumer.router.Route(enrichedLog)
					if routeError != nil {
						continue
					}
					if batchConsumer.queuePublisher != nil {
						_ = batchConsumer.queuePublisher.Publish(routedLog)
					}
				}
			}
		}()
	}
	go func() {
		waitGroup.Wait()
	}()
}

func (batchConsumer *BatchConsumer) collectBatch(applicationContext context.Context) []model.RawLog {
	batch := make([]model.RawLog, 0, batchConsumer.consumerBatchSize)
	for len(batch) < batchConsumer.consumerBatchSize {
		select {
		case <-applicationContext.Done():
			return batch
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
