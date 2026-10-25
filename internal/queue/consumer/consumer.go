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

