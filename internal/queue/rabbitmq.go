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
