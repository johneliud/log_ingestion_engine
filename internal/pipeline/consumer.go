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
