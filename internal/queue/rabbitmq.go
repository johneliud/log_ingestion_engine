package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/johneliud/log_ingestion_engine/internal/config"
	"github.com/johneliud/log_ingestion_engine/internal/model"
	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	ErrFallbackFull = errors.New("in-memory fallback is full")
	ErrNotConnected = errors.New("not connected to rabbitmq")
)

type enrichedLogMessage struct {
	EnrichedLog model.EnrichedLog
}

// Publisher publishes enriched logs to RabbitMQ with timeout and fallback.
type Publisher struct {
	mutex                    sync.RWMutex
	applicationConfiguration config.ApplicationConfiguration
	connection               *amqp.Connection
	channel                  *amqp.Channel
	fallbackChannel          chan enrichedLogMessage
	fallbackCapacity         int
	isConnected              bool
}

// Returns a new publisher.
func NewPublisher(applicationConfiguration config.ApplicationConfiguration, fallbackCapacity int) *Publisher {
	if fallbackCapacity <= 0 {
		fallbackCapacity = 10000
	}
	return &Publisher{
		applicationConfiguration: applicationConfiguration,
		fallbackChannel:          make(chan enrichedLogMessage, fallbackCapacity),
		fallbackCapacity:         fallbackCapacity,
		isConnected:              false,
	}
}

// Connects to RabbitMQ and declares topology.
func (publisher *Publisher) Connect() error {
	publisher.mutex.Lock()
	defer publisher.mutex.Unlock()

	connectionString := fmt.Sprintf("amqp://%s:%s@%s:%d/",
		publisher.applicationConfiguration.RabbitMQUsername,
		publisher.applicationConfiguration.RabbitMQPassword,
		publisher.applicationConfiguration.RabbitMQHost,
		publisher.applicationConfiguration.RabbitMQPort,
	)

	connection, connectionError := amqp.Dial(connectionString)
	if connectionError != nil {
		publisher.isConnected = false
		slog.Warn("failed to connect to rabbitmq, activating in-memory fallback", "error", connectionError)
		return connectionError
	}

	channel, channelError := connection.Channel()
	if channelError != nil {
		connection.Close()
		publisher.isConnected = false
		slog.Warn("failed to open rabbitmq channel, activating in-memory fallback", "error", channelError)
		return channelError
	}

	if err := publisher.declareTopology(channel); err != nil {
		channel.Close()
		connection.Close()
		publisher.isConnected = false
		slog.Warn("failed to declare rabbitmq topology, activating in-memory fallback", "error", err)
		return err
	}

	publisher.connection = connection
	publisher.channel = channel
	publisher.isConnected = true
	return nil
}

func (publisher *Publisher) declareTopology(channel *amqp.Channel) error {
	exchanges := []string{config.ExchangeService1Name, config.ExchangeService2Name, config.ExchangeService3Name}
	queues := []string{config.QueueService1Name, config.QueueService2Name, config.QueueService3Name}

	for _, exchangeName := range exchanges {
		if err := channel.ExchangeDeclare(exchangeName, "direct", true, false, false, false, nil); err != nil {
			return err
		}
	}

	for index, queueName := range queues {
		if _, err := channel.QueueDeclare(queueName, true, false, false, false, nil); err != nil {
			return err
		}
		if err := channel.QueueBind(queueName, config.RoutingKeyLogWrite, exchanges[index], false, nil); err != nil {
			return err
		}
	}
	return nil
}

func (publisher *Publisher) publishToFallback(enrichedLog model.EnrichedLog) error {
	select {
	case publisher.fallbackChannel <- enrichedLogMessage{EnrichedLog: enrichedLog}:
		return nil
	default:
		slog.Error("in-memory fallback is full, cannot drop logs silently", "destination", enrichedLog.Destination, "batch_id", enrichedLog.BatchID)
		return ErrFallbackFull
	}
}

// FallbackChannel returns the fallback channel.
func (publisher *Publisher) FallbackChannel() <-chan enrichedLogMessage {
	return publisher.fallbackChannel
}

// GetFallbackMessage returns fallback messages as enriched logs.
func (publisher *Publisher) GetFallbackMessage() (model.EnrichedLog, bool) {
	select {
	case message := <-publisher.fallbackChannel:
		return message.EnrichedLog, true
	default:
		return model.EnrichedLog{}, false
	}
}

// IsConnected returns connection status.
func (publisher *Publisher) IsConnected() bool {
	publisher.mutex.RLock()
	defer publisher.mutex.RUnlock()
	return publisher.isConnected
}

func exchangeNameForDestination(destination model.Destination) string {
	switch destination {
	case model.DestinationService1:
		return config.ExchangeService1Name
	case model.DestinationService2:
		return config.ExchangeService2Name
	case model.DestinationService3:
		return config.ExchangeService3Name
	default:
		return config.ExchangeService1Name
	}
}
