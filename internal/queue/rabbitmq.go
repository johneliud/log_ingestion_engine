package queue

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/url"
	"strconv"
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
	returns                  chan amqp.Return
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
		returns:                  make(chan amqp.Return, 1),
	}
}

// Connects to RabbitMQ and declares topology.
func (publisher *Publisher) Connect() error {
	connectionURL := url.URL{
		Scheme: brokerSchemeForHost(publisher.applicationConfiguration.RabbitMQHost),
		User:   url.UserPassword(publisher.applicationConfiguration.RabbitMQUsername, publisher.applicationConfiguration.RabbitMQPassword),
		Host:   net.JoinHostPort(publisher.applicationConfiguration.RabbitMQHost, strconv.Itoa(publisher.applicationConfiguration.RabbitMQPort)),
	}
	connectionString := connectionURL.String()

	connection, connectionError := amqp.Dial(connectionString)
	if connectionError != nil {
		publisher.mutex.Lock()
		publisher.isConnected = false
		publisher.mutex.Unlock()
		slog.Warn("failed to connect to rabbitmq, activating in-memory fallback", "error", connectionError)
		return connectionError
	}

	channel, channelError := connection.Channel()
	if channelError != nil {
		connection.Close()
		publisher.mutex.Lock()
		publisher.isConnected = false
		publisher.mutex.Unlock()
		slog.Warn("failed to open rabbitmq channel, activating in-memory fallback", "error", channelError)
		return channelError
	}

	if err := publisher.declareTopology(channel); err != nil {
		channel.Close()
		connection.Close()
		publisher.mutex.Lock()
		publisher.isConnected = false
		publisher.mutex.Unlock()
		slog.Warn("failed to declare rabbitmq topology, activating in-memory fallback", "error", err)
		return err
	}

	if confirmError := channel.Confirm(false); confirmError != nil {
		channel.Close()
		connection.Close()
		publisher.mutex.Lock()
		publisher.isConnected = false
		publisher.mutex.Unlock()
		slog.Warn("failed to confirm rabbitmq publishing, activating in-memory fallback", "error", confirmError)
		return confirmError
	}

	publisher.mutex.Lock()
	previousChannel := publisher.channel
	previousConnection := publisher.connection
	if previousChannel != nil {
		previousChannel.Close()
	}
	if previousConnection != nil {
		previousConnection.Close()
	}
	publisher.connection = connection
	publisher.channel = channel
	publisher.returns = channel.NotifyReturn(make(chan amqp.Return, 64))
	publisher.isConnected = true
	publisher.mutex.Unlock()

	return nil
}

// Returns the AMQP scheme for the broker host, using TLS for remote deployments.
func brokerSchemeForHost(host string) string {
	if host == "" || host == "localhost" || host == "127.0.0.1" || host == "::1" {
		return "amqp"
	}
	return "amqps"
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

// Publish publishes a log with timeout and activates fallback on failure.
func (publisher *Publisher) Publish(enrichedLog model.EnrichedLog) error {
	publisher.mutex.RLock()
	isConnected := publisher.isConnected
	channel := publisher.channel
	publisher.mutex.RUnlock()

	if isConnected && channel != nil {
		publishContext, publishContextCancel := context.WithTimeout(context.Background(), publisher.applicationConfiguration.PublishTimeout)
		defer publishContextCancel()

		body, marshalError := json.Marshal(enrichedLog)
		if marshalError != nil {
			return marshalError
		}

		if confirmError := channel.Confirm(false); confirmError != nil {
			slog.Error("confirming rabbitmq publishing failed, using fallback", "error", confirmError, "destination", enrichedLog.Destination)
			publisher.mutex.Lock()
			publisher.isConnected = false
			publisher.mutex.Unlock()
			return publisher.publishToFallback(enrichedLog)
		}

		deferredConfirmation, publishError := channel.PublishWithDeferredConfirmWithContext(
			publishContext,
			exchangeNameForDestination(enrichedLog.Destination),
			config.RoutingKeyLogWrite,
			true,
			false,
			amqp.Publishing{
				DeliveryMode: amqp.Persistent,
				ContentType:  "application/json",
				Body:         body,
			},
		)
		if publishError != nil {
			slog.Error("publishing to rabbitmq failed, using fallback", "error", publishError, "destination", enrichedLog.Destination)
			publisher.mutex.Lock()
			publisher.isConnected = false
			publisher.mutex.Unlock()
			return publisher.publishToFallback(enrichedLog)
		}

		publishConfirmed := false
		publishConfirmationError := error(nil)
		if deferredConfirmation != nil {
			publishConfirmed, publishConfirmationError = deferredConfirmation.WaitContext(publishContext)
		}
		unroutable := false
		select {
		case <-publisher.returns:
			unroutable = true
		default:
		}
		if publishConfirmationError != nil || !publishConfirmed || unroutable {
			slog.Error("publishing to rabbitmq was not confirmed by the broker, using fallback", "error", publishConfirmationError, "destination", enrichedLog.Destination, "confirmed", publishConfirmed, "unroutable", unroutable)
			publisher.mutex.Lock()
			publisher.isConnected = false
			publisher.mutex.Unlock()
			return publisher.publishToFallback(enrichedLog)
		}

		return nil
	} else {
		slog.Warn("rabbitmq not connected, using in-memory fallback", "destination", enrichedLog.Destination)
	}

	return publisher.publishToFallback(enrichedLog)
}

// Reconnects to RabbitMQ and drains buffered fallback logs.
func (publisher *Publisher) Reconnect() error {
	reconnectError := publisher.Connect()
	if reconnectError != nil {
		return reconnectError
	}
	return publisher.drainFallback()
}

// Drains buffered fallback logs into RabbitMQ.
func (publisher *Publisher) drainFallback() error {
	for {
		enrichedLog, hasBufferedLog := publisher.GetFallbackMessage()
		if !hasBufferedLog {
			return nil
		}
		if publishError := publisher.Publish(enrichedLog); publishError != nil {
			return publishError
		}
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
