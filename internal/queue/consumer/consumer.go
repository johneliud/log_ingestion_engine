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

// Start starts consumers per destination.
func (queueConsumer *QueueConsumer) Start(applicationContext context.Context) <-chan struct{} {
	completed := make(chan struct{})
	var waitGroup sync.WaitGroup
	destinations := []model.Destination{model.DestinationService1, model.DestinationService2, model.DestinationService3}
	waitGroup.Add(len(destinations))
	for _, destination := range destinations {
		go func(destinationValue model.Destination) {
			defer waitGroup.Done()
			queueConsumer.consumeForDestination(applicationContext, destinationValue)
		}(destination)
	}
	go func() {
		waitGroup.Wait()
		close(completed)
	}()
	return completed
}

func (queueConsumer *QueueConsumer) consumeForDestination(applicationContext context.Context, destination model.Destination) {
	for {
		select {
		case <-applicationContext.Done():
			return
		case enrichedLog, ok := <-queueConsumer.fallbackChannel:
			if !ok {
				return
			}
			if enrichedLog.Destination == destination {
				logs := []model.EnrichedLog{enrichedLog}
				if err := queueConsumer.storageHandler.StoreBatch(destination, logs); err != nil {
					slog.Error("failed to store batch", "error", err, "destination", destination)
					return
				}
			}
		default:
			return
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
