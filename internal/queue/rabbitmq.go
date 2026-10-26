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
	"time"

	"github.com/johneliud/log_ingestion_engine/internal/config"
	"github.com/johneliud/log_ingestion_engine/internal/model"
	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	ErrFallbackFull    = errors.New("in-memory fallback is full")
	ErrNotConnected    = errors.New("not connected to rabbitmq")
	ErrPublisherClosed = errors.New("the publisher is closed")
)

// Attempts to establish the broker connection used by the publisher.
type brokerConnectOperation func() error

// Publishes one enriched log to the currently connected broker.
type brokerPublishOperation func(enrichedLog model.EnrichedLog) error

// Waits between reconnect attempts and reports cancellation.
type reconnectWaiter func(applicationContext context.Context, reconnectDelay time.Duration) error

// Counts the messages handed to the broker or the in-memory fallback.
type publishMetricsRecorder interface {
	RecordQueueMessagePublished()
}

// Holds the optional dependencies of the publisher.
type publisherConfiguration struct {
	metricsRecorder publishMetricsRecorder
}

// PublisherOption configures the publisher.
type PublisherOption func(*publisherConfiguration)

// WithPublishedMessageRecorder configures the recorder that counts published messages.
func WithPublishedMessageRecorder(metricsRecorder publishMetricsRecorder) PublisherOption {
	return func(configuration *publisherConfiguration) {
		configuration.metricsRecorder = metricsRecorder
	}
}

// Publisher publishes enriched logs to RabbitMQ with timeout and fallback.
type Publisher struct {
	mutex                    sync.RWMutex
	applicationConfiguration config.ApplicationConfiguration
	connection               *amqp.Connection
	channel                  *amqp.Channel
	fallbackChannel          chan model.EnrichedLog
	fallbackCapacity         int
	isConnected              bool
	isClosed                 bool
	returns                  chan amqp.Return
	connectionLostChannel    chan struct{}
	connectOperation         brokerConnectOperation
	publishOperation         brokerPublishOperation
	waitForReconnectDelay    reconnectWaiter
	metricsRecorder          publishMetricsRecorder
}

// Returns a new publisher.
func NewPublisher(applicationConfiguration config.ApplicationConfiguration, fallbackCapacity int, options ...PublisherOption) *Publisher {
	if fallbackCapacity <= 0 {
		fallbackCapacity = 10000
	}
	publisherConfiguration := publisherConfiguration{}
	for _, option := range options {
		if option != nil {
			option(&publisherConfiguration)
		}
	}
	publisher := &Publisher{
		applicationConfiguration: applicationConfiguration,
		fallbackChannel:          make(chan model.EnrichedLog, fallbackCapacity),
		fallbackCapacity:         fallbackCapacity,
		isConnected:              false,
		returns:                  make(chan amqp.Return, 1),
		connectionLostChannel:    make(chan struct{}, 1),
		metricsRecorder:          publisherConfiguration.metricsRecorder,
	}
	publisher.connectOperation = publisher.connect
	publisher.publishOperation = publisher.publishToBroker
	publisher.waitForReconnectDelay = waitForReconnectDuration
	return publisher
}

// Connects to RabbitMQ and declares topology.
func (publisher *Publisher) Connect() error {
	return publisher.connect()
}

// Connects to RabbitMQ, declares topology and starts watching for connection loss.
func (publisher *Publisher) connect() error {
	connection, connectionError := DialBroker(publisher.applicationConfiguration)
	if connectionError != nil {
		publisher.markDisconnected()
		slog.Warn("failed to connect to rabbitmq, activating in-memory fallback", "error", connectionError)
		return connectionError
	}

	channel, channelError := connection.Channel()
	if channelError != nil {
		connection.Close()
		publisher.markDisconnected()
		slog.Warn("failed to open rabbitmq channel, activating in-memory fallback", "error", channelError)
		return channelError
	}

	if err := DeclareTopology(channel); err != nil {
		channel.Close()
		connection.Close()
		publisher.markDisconnected()
		slog.Warn("failed to declare rabbitmq topology, activating in-memory fallback", "error", err)
		return err
	}

	if confirmError := channel.Confirm(false); confirmError != nil {
		channel.Close()
		connection.Close()
		publisher.markDisconnected()
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

	select {
	case <-publisher.connectionLostChannel:
	default:
	}
	go publisher.watchConnectionClose(connection, channel)

	slog.Info("connected to rabbitmq", "host", publisher.applicationConfiguration.RabbitMQHost, "port", publisher.applicationConfiguration.RabbitMQPort)
	return nil
}

// Declares the exchanges, queues and bindings used by the publisher and the consumers.
func DeclareTopology(channel *amqp.Channel) error {
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

// Runs the connection loop until cancellation, reconnecting with backoff after failures.
func (publisher *Publisher) RunReconnectLoop(applicationContext context.Context) <-chan struct{} {
	completed := make(chan struct{})
	go func() {
		defer close(completed)
		reconnectAttemptIndex := 0
		for {
			if applicationContext.Err() != nil {
				return
			}
			if publisher.IsClosed() {
				return
			}

			if publisher.IsConnected() {
				reconnectAttemptIndex = 0
				select {
				case <-applicationContext.Done():
					return
				case <-publisher.connectionLostChannel:
					continue
				}
			}

			connectError := publisher.connectOperation()
			if connectError == nil {
				reconnectAttemptIndex = 0
				continue
			}

			reconnectDelay := reconnectDelayAt(publisher.applicationConfiguration.RabbitMQReconnectBackoffDelays, reconnectAttemptIndex)
			reconnectAttemptIndex++
			slog.Warn("reconnecting to rabbitmq failed, retrying", "error", connectError, "reconnect_delay", reconnectDelay, "reconnect_attempt", reconnectAttemptIndex)
			if waitError := publisher.waitForReconnectDelay(applicationContext, reconnectDelay); waitError != nil {
				return
			}
		}
	}()
	return completed
}

// Watches the current broker connection and marks the publisher disconnected when it closes.
func (publisher *Publisher) watchConnectionClose(connection *amqp.Connection, channel *amqp.Channel) {
	connectionCloseNotifications := connection.NotifyClose(make(chan *amqp.Error, 1))
	channelCloseNotifications := channel.NotifyClose(make(chan *amqp.Error, 1))
	select {
	case <-connectionCloseNotifications:
	case <-channelCloseNotifications:
	}

	publisher.mutex.RLock()
	isCurrentConnection := publisher.connection == connection
	publisher.mutex.RUnlock()
	if isCurrentConnection {
		publisher.markDisconnected()
	}
}

// Publish publishes a log with timeout and activates fallback on failure.
func (publisher *Publisher) Publish(enrichedLog model.EnrichedLog) error {
	if publisher.IsClosed() {
		return ErrPublisherClosed
	}
	if !publisher.IsConnected() {
		slog.Warn("rabbitmq not connected, using in-memory fallback", "destination", enrichedLog.Destination, "batch_id", enrichedLog.BatchID)
		return publisher.publishToFallbackAndRecord(enrichedLog)
	}

	if publishError := publisher.publishOperation(enrichedLog); publishError != nil {
		publisher.markDisconnected()
		slog.Warn("publishing to rabbitmq failed, using in-memory fallback", "error", publishError, "destination", enrichedLog.Destination, "batch_id", enrichedLog.BatchID)
		return publisher.publishToFallbackAndRecord(enrichedLog)
	}

	publisher.recordPublishedMessage()
	return nil
}

// Publishes an enriched log to the in-memory fallback and counts it as in flight.
func (publisher *Publisher) publishToFallbackAndRecord(enrichedLog model.EnrichedLog) error {
	if fallbackError := publisher.publishToFallback(enrichedLog); fallbackError != nil {
		return fallbackError
	}
	publisher.recordPublishedMessage()
	return nil
}

// Counts one message that is now waiting to be stored.
func (publisher *Publisher) recordPublishedMessage() {
	if publisher.metricsRecorder == nil {
		return
	}
	publisher.metricsRecorder.RecordQueueMessagePublished()
}

// Publishes an enriched log to RabbitMQ and reports broker failures.
func (publisher *Publisher) publishToBroker(enrichedLog model.EnrichedLog) error {
	publisher.mutex.RLock()
	channel := publisher.channel
	returns := publisher.returns
	publisher.mutex.RUnlock()
	if channel == nil {
		return ErrNotConnected
	}

	publishContext, publishContextCancel := context.WithTimeout(context.Background(), publisher.applicationConfiguration.PublishTimeout)
	defer publishContextCancel()

	body, marshalError := json.Marshal(enrichedLog)
	if marshalError != nil {
		return marshalError
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
		return publishError
	}

	publishConfirmed := false
	publishConfirmationError := error(nil)
	if deferredConfirmation != nil {
		publishConfirmed, publishConfirmationError = deferredConfirmation.WaitContext(publishContext)
	}
	unroutable := false
	select {
	case <-returns:
		unroutable = true
	default:
	}
	if publishConfirmationError != nil || !publishConfirmed || unroutable {
		return errors.New("publishing to rabbitmq was not confirmed by the broker")
	}

	return nil
}

// Publishes an enriched log to the bounded in-memory fallback, reporting a full buffer.
func (publisher *Publisher) publishToFallback(enrichedLog model.EnrichedLog) error {
	select {
	case publisher.fallbackChannel <- enrichedLog:
		return nil
	default:
		slog.Error("in-memory fallback is full, cannot drop logs silently", "destination", enrichedLog.Destination, "batch_id", enrichedLog.BatchID)
		return ErrFallbackFull
	}
}

// Updates connection state after a broker failure and wakes the reconnect loop.
func (publisher *Publisher) markDisconnected() {
	publisher.mutex.Lock()
	wasConnected := publisher.isConnected
	publisher.isConnected = false
	publisher.mutex.Unlock()
	if !wasConnected {
		return
	}

	slog.Warn("rabbitmq connection lost, activating in-memory fallback")
	select {
	case publisher.connectionLostChannel <- struct{}{}:
	default:
	}
}

// Opens a broker connection with the configured host, port, credentials and virtual host.
func DialBroker(applicationConfiguration config.ApplicationConfiguration) (*amqp.Connection, error) {
	scheme := "amqp"
	if applicationConfiguration.RabbitMQUseTLS {
		scheme = "amqps"
	}
	virtualHostPath, virtualHostRawPath := virtualHostPaths(applicationConfiguration.RabbitMQVirtualHost)
	connectionURL := url.URL{
		Scheme:  scheme,
		User:    url.UserPassword(applicationConfiguration.RabbitMQUsername, applicationConfiguration.RabbitMQPassword),
		Host:    net.JoinHostPort(applicationConfiguration.RabbitMQHost, strconv.Itoa(applicationConfiguration.RabbitMQPort)),
		Path:    virtualHostPath,
		RawPath: virtualHostRawPath,
	}
	return amqp.Dial(connectionURL.String())
}

// Returns the decoded and escaped URL paths that select the virtual host.
func virtualHostPaths(rabbitMQVirtualHost string) (string, string) {
	if rabbitMQVirtualHost == "" {
		rabbitMQVirtualHost = config.DefaultRabbitMQVirtualHost
	}
	return "/" + rabbitMQVirtualHost, "/" + url.PathEscape(rabbitMQVirtualHost)
}

// Returns the fallback channel.
func (publisher *Publisher) FallbackChannel() <-chan model.EnrichedLog {
	return publisher.fallbackChannel
}

// Closes the broker connection and channel held by the publisher.
func (publisher *Publisher) Close() error {
	publisher.mutex.Lock()
	defer publisher.mutex.Unlock()

	publisher.isClosed = true
	var closeError error
	if publisher.channel != nil {
		if channelCloseError := publisher.channel.Close(); channelCloseError != nil && !errors.Is(channelCloseError, amqp.ErrClosed) {
			closeError = channelCloseError
		}
		publisher.channel = nil
	}
	if publisher.connection != nil {
		if connectionCloseError := publisher.connection.Close(); connectionCloseError != nil && !errors.Is(connectionCloseError, amqp.ErrClosed) {
			if closeError == nil {
				closeError = connectionCloseError
			}
		}
		publisher.connection = nil
	}
	publisher.isConnected = false
	return closeError
}

// IsConnected returns connection status.
func (publisher *Publisher) IsConnected() bool {
	publisher.mutex.RLock()
	defer publisher.mutex.RUnlock()
	return publisher.isConnected
}

// IsClosed returns whether the publisher stopped accepting logs.
func (publisher *Publisher) IsClosed() bool {
	publisher.mutex.RLock()
	defer publisher.mutex.RUnlock()
	return publisher.isClosed
}

// Returns the backoff delay for a reconnect attempt, reusing the last configured delay.
func reconnectDelayAt(reconnectBackoffDelays []time.Duration, reconnectAttemptIndex int) time.Duration {
	if len(reconnectBackoffDelays) == 0 {
		return time.Second
	}
	if reconnectAttemptIndex >= len(reconnectBackoffDelays) {
		return reconnectBackoffDelays[len(reconnectBackoffDelays)-1]
	}
	return reconnectBackoffDelays[reconnectAttemptIndex]
}

// Waits for a reconnect delay or returns when the context is cancelled.
func waitForReconnectDuration(applicationContext context.Context, reconnectDelay time.Duration) error {
	timer := time.NewTimer(reconnectDelay)
	defer timer.Stop()
	select {
	case <-applicationContext.Done():
		return applicationContext.Err()
	case <-timer.C:
		return nil
	}
}

// Returns the exchange name for a destination, falling back to service1.
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
