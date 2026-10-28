package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

// The error reported when the broker accepted no part of a publish batch.
var errPublishNotConfirmed = errors.New("publishing to rabbitmq was not confirmed by the broker")

// Attempts to establish the broker connection used by the publisher.
type brokerConnectOperation func() error

// Publishes one batch of enriched logs to the currently connected broker.
type brokerPublishOperation func(enrichedLogs []model.EnrichedLog) error

// Waits between reconnect attempts and reports cancellation.
type reconnectWaiter func(applicationContext context.Context, reconnectDelay time.Duration) error

// Counts the messages handed to the broker or the in-memory fallback.
type publishMetricsRecorder interface {
	RecordQueueMessagePublished()
}

// Holds the optional dependencies of the publisher.
type publisherConfiguration struct {
	metricsRecorder publishMetricsRecorder
	publishSessions int
}

// PublisherOption configures the publisher.
type PublisherOption func(*publisherConfiguration)

// WithPublishedMessageRecorder configures the recorder that counts published messages.
func WithPublishedMessageRecorder(metricsRecorder publishMetricsRecorder) PublisherOption {
	return func(configuration *publisherConfiguration) {
		configuration.metricsRecorder = metricsRecorder
	}
}

// Configures how many broker channels publish concurrently.
func WithPublishSessionCount(publishSessions int) PublisherOption {
	return func(configuration *publisherConfiguration) {
		configuration.publishSessions = publishSessions
	}
}

const (
	defaultPublishSessionCount = 4
	bufferedReturnCapacity     = 64
)

// Owns one broker channel with its own confirm mode and return notifications.
type publishSession struct {
	channel *amqp.Channel
	returns chan amqp.Return
}

// Publisher publishes enriched logs to RabbitMQ with timeout and fallback.
type Publisher struct {
	mutex                    sync.RWMutex
	applicationConfiguration config.ApplicationConfiguration
	connection               *amqp.Connection
	sessions                 chan *publishSession
	fallbackChannel          chan model.EnrichedLog
	fallbackCapacity         int
	publishSessionCount      int
	isConnected              bool
	isClosed                 bool
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
	publisherConfiguration := publisherConfiguration{publishSessions: defaultPublishSessionCount}
	for _, option := range options {
		if option != nil {
			option(&publisherConfiguration)
		}
	}
	if publisherConfiguration.publishSessions <= 0 {
		publisherConfiguration.publishSessions = defaultPublishSessionCount
	}
	publisher := &Publisher{
		applicationConfiguration: applicationConfiguration,
		fallbackChannel:          make(chan model.EnrichedLog, fallbackCapacity),
		fallbackCapacity:         fallbackCapacity,
		publishSessionCount:      publisherConfiguration.publishSessions,
		sessions:                 make(chan *publishSession, publisherConfiguration.publishSessions),
		isConnected:              false,
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

// Connects to RabbitMQ, declares topology, opens the publish sessions and starts watching for connection loss.
func (publisher *Publisher) connect() error {
	connection, connectionError := DialBroker(publisher.applicationConfiguration)
	if connectionError != nil {
		publisher.markDisconnected()
		slog.Warn("failed to connect to rabbitmq, activating in-memory fallback", "error", connectionError)
		return connectionError
	}

	sessions, sessionsError := publisher.openPublishSessions(connection)
	if sessionsError != nil {
		connection.Close()
		publisher.markDisconnected()
		slog.Warn("failed to open rabbitmq publish sessions, activating in-memory fallback", "error", sessionsError)
		return sessionsError
	}

	publisher.mutex.Lock()
	previousSessions := publisher.sessions
	previousConnection := publisher.connection
	publisher.connection = connection
	publisher.sessions = sessions
	publisher.isConnected = true
	publisher.mutex.Unlock()
	closePreviousSessions(previousSessions)
	if previousConnection != nil {
		previousConnection.Close()
	}

	select {
	case <-publisher.connectionLostChannel:
	default:
	}
	go publisher.watchConnectionClose(connection)

	slog.Info("connected to rabbitmq", "host", publisher.applicationConfiguration.RabbitMQHost, "port", publisher.applicationConfiguration.RabbitMQPort)
	return nil
}

// Opens one channel per publish session, declares the topology once and enables confirm mode plus return notifications on every session channel.
func (publisher *Publisher) openPublishSessions(connection *amqp.Connection) (chan *publishSession, error) {
	topologyChannel, topologyChannelError := connection.Channel()
	if topologyChannelError != nil {
		return nil, fmt.Errorf("opening the topology channel: %w", topologyChannelError)
	}
	defer topologyChannel.Close()

	if topologyError := DeclareTopology(topologyChannel); topologyError != nil {
		return nil, fmt.Errorf("declaring the rabbitmq topology: %w", topologyError)
	}

	sessions := make(chan *publishSession, publisher.publishSessionCount)
	for sessionIndex := range publisher.publishSessionCount {
		sessionChannel, channelError := connection.Channel()
		if channelError != nil {
			closePublishSessions(sessions)
			return nil, fmt.Errorf("opening publish session %d: %w", sessionIndex, channelError)
		}
		if confirmError := sessionChannel.Confirm(false); confirmError != nil {
			sessionChannel.Close()
			closePublishSessions(sessions)
			return nil, fmt.Errorf("enabling publisher confirms on session %d: %w", sessionIndex, confirmError)
		}
		sessions <- &publishSession{
			channel: sessionChannel,
			returns: sessionChannel.NotifyReturn(make(chan amqp.Return, bufferedReturnCapacity)),
		}
	}
	return sessions, nil
}

// Closes every session in the channel.
func closePublishSessions(sessions chan *publishSession) {
	for {
		select {
		case session := <-sessions:
			session.channel.Close()
		default:
			return
		}
	}
}

// Closes the previous generation of sessions after a reconnect replaced them.
func closePreviousSessions(previousSessions chan *publishSession) {
	if previousSessions == nil {
		return
	}
	go closePublishSessions(previousSessions)
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
func (publisher *Publisher) watchConnectionClose(connection *amqp.Connection) {
	connectionCloseNotifications := connection.NotifyClose(make(chan *amqp.Error, 1))
	<-connectionCloseNotifications

	publisher.mutex.RLock()
	isCurrentConnection := publisher.connection == connection
	publisher.mutex.RUnlock()
	if isCurrentConnection {
		publisher.markDisconnected()
	}
}

// Publish publishes one log with timeout and activates fallback on failure.
func (publisher *Publisher) Publish(enrichedLog model.EnrichedLog) error {
	return publisher.PublishBatch([]model.EnrichedLog{enrichedLog})
}

// PublishBatch publishes a batch of logs with one broker confirm and activates fallback for every log when the publish fails.
func (publisher *Publisher) PublishBatch(enrichedLogs []model.EnrichedLog) error {
	if len(enrichedLogs) == 0 {
		return nil
	}
	if publisher.IsClosed() {
		return ErrPublisherClosed
	}
	if !publisher.IsConnected() {
		slog.Warn("rabbitmq not connected, using in-memory fallback", "log_count", len(enrichedLogs), "batch_id", enrichedLogs[0].BatchID)
		return publisher.publishBatchToFallbackAndRecord(enrichedLogs)
	}

	if publishError := publisher.publishOperation(enrichedLogs); publishError != nil {
		publisher.markDisconnected()
		slog.Warn("publishing to rabbitmq failed, using in-memory fallback", "error", publishError, "log_count", len(enrichedLogs), "batch_id", enrichedLogs[0].BatchID)
		return publisher.publishBatchToFallbackAndRecord(enrichedLogs)
	}

	publisher.recordPublishedMessages(len(enrichedLogs))
	return nil
}

// Publishes a batch of enriched logs to the in-memory fallback and counts them as in flight.
func (publisher *Publisher) publishBatchToFallbackAndRecord(enrichedLogs []model.EnrichedLog) error {
	var fallbackErrors []error
	recordedCount := 0
	for _, enrichedLog := range enrichedLogs {
		if fallbackError := publisher.publishToFallback(enrichedLog); fallbackError != nil {
			fallbackErrors = append(fallbackErrors, fallbackError)
			continue
		}
		recordedCount++
	}
	publisher.recordPublishedMessages(recordedCount)
	return errors.Join(fallbackErrors...)
}

// Counts the messages that are now waiting to be stored.
func (publisher *Publisher) recordPublishedMessages(messageCount int) {
	if publisher.metricsRecorder == nil {
		return
	}
	for range make([]struct{}, messageCount) {
		publisher.metricsRecorder.RecordQueueMessagePublished()
	}
}

// Publishes a batch of enriched logs on one checked-out session and waits for a single cumulative confirm covering the whole batch.
func (publisher *Publisher) publishToBroker(enrichedLogs []model.EnrichedLog) error {
	session, sessionError := publisher.checkoutSession()
	if sessionError != nil {
		return sessionError
	}
	defer publisher.returnSession(session)

	publishContext, publishContextCancel := context.WithTimeout(context.Background(), publisher.applicationConfiguration.PublishTimeout)
	defer publishContextCancel()

	var lastConfirmation *amqp.DeferredConfirmation
	for _, enrichedLog := range enrichedLogs {
		body, marshalError := json.Marshal(enrichedLog)
		if marshalError != nil {
			return marshalError
		}

		confirmation, publishError := session.channel.PublishWithDeferredConfirmWithContext(
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
		lastConfirmation = confirmation
	}

	if lastConfirmation != nil {
		publishConfirmed, publishConfirmationError := lastConfirmation.WaitContext(publishContext)
		if publishConfirmationError != nil || !publishConfirmed {
			return errPublishNotConfirmed
		}
	}

	// The broker sends a return before the confirm of an unroutable message, so any return waiting on this exclusive session belongs to this batch.
	select {
	case <-session.returns:
		return errPublishNotConfirmed
	default:
		return nil
	}
}

// Checks out one publish session, waiting for a free one until the publish timeout elapses.
func (publisher *Publisher) checkoutSession() (*publishSession, error) {
	publisher.mutex.RLock()
	sessions := publisher.sessions
	publisher.mutex.RUnlock()
	if sessions == nil {
		return nil, ErrNotConnected
	}

	publishContext, publishContextCancel := context.WithTimeout(context.Background(), publisher.applicationConfiguration.PublishTimeout)
	defer publishContextCancel()

	select {
	case session := <-sessions:
		if session.channel.IsClosed() {
			return nil, ErrNotConnected
		}
		return session, nil
	case <-publishContext.Done():
		return nil, ErrNotConnected
	}
}

// Gives a publish session back to the pool.
func (publisher *Publisher) returnSession(session *publishSession) {
	publisher.mutex.RLock()
	sessions := publisher.sessions
	publisher.mutex.RUnlock()
	if sessions == nil || session == nil {
		return
	}
	select {
	case sessions <- session:
	default:
		session.channel.Close()
	}
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

// Closes the broker connection and every publish session held by the publisher.
func (publisher *Publisher) Close() error {
	publisher.mutex.Lock()
	defer publisher.mutex.Unlock()

	publisher.isClosed = true
	sessions := publisher.sessions
	publisher.sessions = nil
	var closeError error
	if sessions != nil {
		for drained := false; !drained; {
			select {
			case session := <-sessions:
				if session == nil {
					continue
				}
				if channelCloseError := session.channel.Close(); channelCloseError != nil && !errors.Is(channelCloseError, amqp.ErrClosed) {
					closeError = channelCloseError
				}
			default:
				drained = true
			}
		}
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
