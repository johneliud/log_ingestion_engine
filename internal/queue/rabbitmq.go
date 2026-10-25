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
