package consumer

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"

	"github.com/johneliud/log_ingestion_engine/internal/model"
	amqp "github.com/rabbitmq/amqp091-go"
)

const defaultQueuePrefetchCount = 1

// StorageHandler handles storing a batch of logs for a destination.
type StorageHandler interface {
	StoreBatch(destination model.Destination, logs []model.EnrichedLog) error
}

// Opens a delivery channel for a queue and returns its close function.
type queueSubscriber func(applicationContext context.Context, queueName string) (<-chan amqp.Delivery, func() error, error)

// Supplies the storage handler, queue subscription and fallback channel for a queue consumer.
type QueueConsumerDependencies struct {
	StorageHandler     StorageHandler
	RabbitMQConnection *amqp.Connection
	QueueName          string
	Destination        model.Destination
	FallbackChannel    <-chan model.EnrichedLog
	QueueSubscriber    queueSubscriber
}

// QueueConsumer consumes one RabbitMQ queue or the in-memory fallback and stores what it receives.
type QueueConsumer struct {
	storageHandler  StorageHandler
	queueName       string
	destination     model.Destination
	fallbackChannel <-chan model.EnrichedLog
	queueSubscriber queueSubscriber
}

// Returns a queue consumer that subscribes to RabbitMQ when a connection and queue name are supplied.
func NewQueueConsumer(dependencies QueueConsumerDependencies) *QueueConsumer {
	queueSubscriber := dependencies.QueueSubscriber
	if queueSubscriber == nil && dependencies.RabbitMQConnection != nil && dependencies.QueueName != "" {
		queueSubscriber = subscribeToRabbitMQQueue(dependencies.RabbitMQConnection)
	}
	return &QueueConsumer{
		storageHandler:  dependencies.StorageHandler,
		queueName:       dependencies.QueueName,
		destination:     dependencies.Destination,
		fallbackChannel: dependencies.FallbackChannel,
		queueSubscriber: queueSubscriber,
	}
}

// Starts the configured queue subscription and fallback reader until the context is cancelled.
func (queueConsumer *QueueConsumer) Start(applicationContext context.Context) <-chan struct{} {
	completed := make(chan struct{})
	var waitGroup sync.WaitGroup
	if queueConsumer.queueSubscriber != nil && queueConsumer.queueName != "" {
		waitGroup.Go(func() {
			queueConsumer.consumeQueue(applicationContext)
		})
	}
	if queueConsumer.fallbackChannel != nil {
		waitGroup.Go(func() {
			queueConsumer.consumeFallback(applicationContext)
		})
	}
	go func() {
		waitGroup.Wait()
		close(completed)
	}()
	return completed
}

// Reads deliveries from the queue until the context is cancelled or the delivery channel closes.
func (queueConsumer *QueueConsumer) consumeQueue(applicationContext context.Context) {
	deliveries, closeDeliveries, subscriptionError := queueConsumer.queueSubscriber(applicationContext, queueConsumer.queueName)
	if subscriptionError != nil {
		slog.Error("subscribing to rabbitmq queue failed", "error", subscriptionError, "queue", queueConsumer.queueName)
		return
	}
	defer closeDeliveries()

	for {
		select {
		case <-applicationContext.Done():
			return
		case delivery, isOpen := <-deliveries:
			if !isOpen {
				return
			}
			queueConsumer.storeDelivery(delivery)
		}
	}
}

// Stores one delivery before acknowledging it, requeueing it when storage fails.
func (queueConsumer *QueueConsumer) storeDelivery(delivery amqp.Delivery) {
	var enrichedLog model.EnrichedLog
	if decodeError := json.Unmarshal(delivery.Body, &enrichedLog); decodeError != nil {
		slog.Error("decoding rabbitmq queue message failed", "error", decodeError, "queue", queueConsumer.queueName)
		queueConsumer.nackDelivery(delivery, false)
		return
	}

	enrichedLog.Destination = queueConsumer.destination
	if storeError := queueConsumer.storageHandler.StoreBatch(queueConsumer.destination, []model.EnrichedLog{enrichedLog}); storeError != nil {
		slog.Error("storing rabbitmq queue message failed, requeueing", "error", storeError, "queue", queueConsumer.queueName, "destination", queueConsumer.destination)
		queueConsumer.nackDelivery(delivery, true)
		return
	}

	if acknowledgeError := delivery.Ack(false); acknowledgeError != nil {
		slog.Error("acknowledging rabbitmq queue message failed", "error", acknowledgeError, "queue", queueConsumer.queueName)
	}
}

// Negatively acknowledges a delivery, optionally asking the broker to requeue it.
func (queueConsumer *QueueConsumer) nackDelivery(delivery amqp.Delivery, shouldRequeue bool) {
	if nackError := delivery.Nack(false, shouldRequeue); nackError != nil {
		slog.Error("negatively acknowledging rabbitmq queue message failed", "error", nackError, "queue", queueConsumer.queueName)
	}
}

// Reads fallback logs until the context is cancelled, then stores whatever remains buffered.
func (queueConsumer *QueueConsumer) consumeFallback(applicationContext context.Context) {
	for {
		select {
		case <-applicationContext.Done():
			queueConsumer.drainFallback()
			return
		case enrichedLog, isOpen := <-queueConsumer.fallbackChannel:
			if !isOpen {
				return
			}
			queueConsumer.storeFallbackLog(enrichedLog)
		}
	}
}

// Stores buffered fallback logs before shutdown completes.
func (queueConsumer *QueueConsumer) drainFallback() {
	for {
		select {
		case enrichedLog, isOpen := <-queueConsumer.fallbackChannel:
			if !isOpen {
				return
			}
			queueConsumer.storeFallbackLog(enrichedLog)
		default:
			return
		}
	}
}

// Stores one fallback log under its routed destination.
func (queueConsumer *QueueConsumer) storeFallbackLog(enrichedLog model.EnrichedLog) {
	if storeError := queueConsumer.storageHandler.StoreBatch(enrichedLog.Destination, []model.EnrichedLog{enrichedLog}); storeError != nil {
		slog.Error("storing fallback log failed", "error", storeError, "destination", enrichedLog.Destination)
	}
}

// Opens a channel on the connection and consumes one queue without automatic acknowledgements, leaving the channel close to the caller so no library cancellation goroutine races it.
func subscribeToRabbitMQQueue(connection *amqp.Connection) queueSubscriber {
	return func(applicationContext context.Context, queueName string) (<-chan amqp.Delivery, func() error, error) {
		channel, channelError := connection.Channel()
		if channelError != nil {
			return nil, nil, channelError
		}

		if qualityOfServiceError := channel.Qos(defaultQueuePrefetchCount, 0, false); qualityOfServiceError != nil {
			channel.Close()
			return nil, nil, qualityOfServiceError
		}

		deliveries, consumeError := channel.Consume(queueName, "", false, false, false, false, nil)
		if consumeError != nil {
			channel.Close()
			return nil, nil, consumeError
		}
		return deliveries, channel.Close, nil
	}
}
