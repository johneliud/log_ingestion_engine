package consumer

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/johneliud/log_ingestion_engine/internal/config"
	"github.com/johneliud/log_ingestion_engine/internal/model"
	"github.com/johneliud/log_ingestion_engine/internal/queue"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Opens one broker connection for the destination consumers.
type brokerDialOperation func() (*amqp.Connection, error)

// Waits between reconnect attempts and reports cancellation.
type reconnectWaiter func(applicationContext context.Context, reconnectDelay time.Duration) error

// Pairs a queue name with the destination its messages belong to.
type DestinationQueue struct {
	QueueName   string
	Destination model.Destination
}

// Supplies the broker settings and the destination queues for a consumer supervisor.
type SupervisorDependencies struct {
	ApplicationConfiguration config.ApplicationConfiguration
	StorageHandler           StorageHandler
	DestinationQueues        []DestinationQueue
	ReconnectBackoffDelays   []time.Duration
	Dial                     brokerDialOperation
	WaitForReconnectDelay    reconnectWaiter
}

// Keeps one broker connection alive and runs the destination consumers on it.
type Supervisor struct {
	applicationConfiguration config.ApplicationConfiguration
	storageHandler           StorageHandler
	destinationQueues        []DestinationQueue
	reconnectBackoffDelays   []time.Duration
	dial                     brokerDialOperation
	waitForReconnectDelay    reconnectWaiter
}

// Returns a supervisor that keeps the destination consumers connected to the broker.
func NewSupervisor(dependencies SupervisorDependencies) *Supervisor {
	dial := dependencies.Dial
	if dial == nil {
		dial = func() (*amqp.Connection, error) {
			return queue.DialBroker(dependencies.ApplicationConfiguration)
		}
	}
	waitForReconnectDelay := dependencies.WaitForReconnectDelay
	if waitForReconnectDelay == nil {
		waitForReconnectDelay = waitForReconnectDuration
	}
	return &Supervisor{
		applicationConfiguration: dependencies.ApplicationConfiguration,
		storageHandler:           dependencies.StorageHandler,
		destinationQueues:        dependencies.DestinationQueues,
		reconnectBackoffDelays:   dependencies.ReconnectBackoffDelays,
		dial:                     dial,
		waitForReconnectDelay:    waitForReconnectDelay,
	}
}

// Starts the destination consumers and reconnects to the broker until the context is cancelled.
func (supervisor *Supervisor) Start(applicationContext context.Context) <-chan struct{} {
	completed := make(chan struct{})
	go func() {
		defer close(completed)
		reconnectAttemptIndex := 0
		for {
			if applicationContext.Err() != nil {
				return
			}

			connection, dialError := supervisor.dial()
			if dialError != nil {
				slog.Warn("connecting to rabbitmq for consumption failed, retrying", "error", dialError)
				if waitError := supervisor.waitForReconnectDelay(applicationContext, supervisor.reconnectDelayAt(reconnectAttemptIndex)); waitError != nil {
					return
				}
				reconnectAttemptIndex++
				continue
			}

			if topologyError := supervisor.declareTopology(connection); topologyError != nil {
				slog.Warn("declaring the rabbitmq topology for consumption failed, retrying", "error", topologyError)
				connection.Close()
				if waitError := supervisor.waitForReconnectDelay(applicationContext, supervisor.reconnectDelayAt(reconnectAttemptIndex)); waitError != nil {
					return
				}
				reconnectAttemptIndex++
				continue
			}

			reconnectAttemptIndex = 0
			slog.Info("connected to rabbitmq for consumption")
			supervisor.consumeUntilDisconnected(applicationContext, connection)
			connection.Close()
			if applicationContext.Err() != nil {
				return
			}
			slog.Warn("rabbitmq consumption connection lost, reconnecting")
			if waitError := supervisor.waitForReconnectDelay(applicationContext, supervisor.reconnectDelayAt(reconnectAttemptIndex)); waitError != nil {
				return
			}
		}
	}()
	return completed
}

// Declares the exchanges, queues and bindings the destination consumers read from.
func (supervisor *Supervisor) declareTopology(connection *amqp.Connection) error {
	channel, channelError := connection.Channel()
	if channelError != nil {
		return channelError
	}
	defer channel.Close()

	return queue.DeclareTopology(channel)
}

// Runs the destination consumers on one connection until they all stop.
func (supervisor *Supervisor) consumeUntilDisconnected(applicationContext context.Context, connection *amqp.Connection) {
	var waitGroup sync.WaitGroup
	for _, destinationQueue := range supervisor.destinationQueues {
		queueConsumer := NewQueueConsumer(QueueConsumerDependencies{
			StorageHandler:     supervisor.storageHandler,
			RabbitMQConnection: connection,
			QueueName:          destinationQueue.QueueName,
			Destination:        destinationQueue.Destination,
		})
		consumerCompletion := queueConsumer.Start(applicationContext)
		waitGroup.Go(func() {
			<-consumerCompletion
		})
	}
	waitGroup.Wait()
}

// Returns the backoff delay for a reconnect attempt, reusing the last configured delay.
func (supervisor *Supervisor) reconnectDelayAt(reconnectAttemptIndex int) time.Duration {
	if len(supervisor.reconnectBackoffDelays) == 0 {
		return time.Second
	}
	if reconnectAttemptIndex >= len(supervisor.reconnectBackoffDelays) {
		return supervisor.reconnectBackoffDelays[len(supervisor.reconnectBackoffDelays)-1]
	}
	return supervisor.reconnectBackoffDelays[reconnectAttemptIndex]
}

// Waits for a reconnect delay unless the application context is cancelled first.
func waitForReconnectDuration(applicationContext context.Context, reconnectDelay time.Duration) error {
	reconnectTimer := time.NewTimer(reconnectDelay)
	defer reconnectTimer.Stop()
	select {
	case <-applicationContext.Done():
		return applicationContext.Err()
	case <-reconnectTimer.C:
		return nil
	}
}
