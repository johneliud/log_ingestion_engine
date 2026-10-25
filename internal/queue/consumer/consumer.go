package consumer

import (
	"context"
	"log/slog"
	"sync"

	"github.com/johneliud/log_ingestion_engine/internal/config"
	"github.com/johneliud/log_ingestion_engine/internal/model"
	amqp "github.com/rabbitmq/amqp091-go"
)

// StorageHandler handles storing a batch of logs for a destination.
type StorageHandler interface {
	StoreBatch(destination model.Destination, logs []model.EnrichedLog) error
}

// QueueConsumer consumes from RabbitMQ or fallback.
type QueueConsumer struct {
	applicationConfiguration config.ApplicationConfiguration
	storageHandler           StorageHandler
	fallbackChannel          <-chan model.EnrichedLog
}

// Returns a new queue consumer.
func NewQueueConsumer(applicationConfiguration config.ApplicationConfiguration, storageHandler StorageHandler, fallbackChannel <-chan model.EnrichedLog) *QueueConsumer {
	return &QueueConsumer{
		applicationConfiguration: applicationConfiguration,
		storageHandler:           storageHandler,
		fallbackChannel:          fallbackChannel,
	}
}

// Start starts consumers.
func (queueConsumer *QueueConsumer) Start(applicationContext context.Context) <-chan struct{} {
	completed := make(chan struct{})
	var waitGroup sync.WaitGroup
	waitGroup.Go(func() {
		queueConsumer.consume(applicationContext)
	})
	go func() {
		waitGroup.Wait()
		close(completed)
	}()
	return completed
}

func (queueConsumer *QueueConsumer) consume(applicationContext context.Context) {
	for {
		select {
		case <-applicationContext.Done():
			return
		case enrichedLog, ok := <-queueConsumer.fallbackChannel:
			if !ok {
				return
			}
			if err := queueConsumer.storageHandler.StoreBatch(enrichedLog.Destination, []model.EnrichedLog{enrichedLog}); err != nil {
				slog.Error("failed to store batch", "error", err, "destination", enrichedLog.Destination)
			}
		}
	}
}

// AcknowledgeMessage acknowledges a message only after storage completes.
func (queueConsumer *QueueConsumer) AcknowledgeMessage(delivery amqp.Delivery) error {
	return delivery.Ack(false)
}

// NackMessage nacks a message if not processed.
func (queueConsumer *QueueConsumer) NackMessage(delivery amqp.Delivery, requeue bool) error {
	return delivery.Nack(false, requeue)
}
