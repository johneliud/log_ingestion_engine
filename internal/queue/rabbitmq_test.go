package queue

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/johneliud/log_ingestion_engine/internal/config"
	"github.com/johneliud/log_ingestion_engine/internal/model"
)

func TestPublisher_ReconnectsAfterBrokerRestoreWithoutRepublishingFallback(t *testing.T) {
	t.Parallel()

	isBrokerAvailable := atomic.Bool{}
	isBrokerAvailable.Store(true)

	publisher := NewPublisher(config.ApplicationConfiguration{
		PublishTimeout:                 time.Second,
		RabbitMQReconnectBackoffDelays: []time.Duration{time.Second, 5 * time.Second, 10 * time.Second},
	}, 10)

	var connectAttemptCount atomic.Int32
	connectedNotifications := make(chan struct{}, 10)
	publisher.connectOperation = func() error {
		connectAttemptCount.Add(1)
		if !isBrokerAvailable.Load() {
			return errors.New("connection refused")
		}
		publisher.mutex.Lock()
		publisher.isConnected = true
		publisher.mutex.Unlock()
		select {
		case connectedNotifications <- struct{}{}:
		default:
		}
		return nil
	}

	brokerPublishedLogs := make([]model.EnrichedLog, 0)
	publisher.publishOperation = func(enrichedLog model.EnrichedLog) error {
		if !isBrokerAvailable.Load() {
			return errors.New("broker unavailable")
		}
		brokerPublishedLogs = append(brokerPublishedLogs, enrichedLog)
		return nil
	}

	reconnectWaits := make(chan time.Duration, 10)
	allowReconnect := make(chan struct{})
	publisher.waitForReconnectDelay = func(applicationContext context.Context, reconnectDelay time.Duration) error {
		reconnectWaits <- reconnectDelay
		select {
		case <-allowReconnect:
			return nil
		case <-applicationContext.Done():
			return applicationContext.Err()
		}
	}

	applicationContext, cancel := context.WithCancel(context.Background())
	completed := publisher.RunReconnectLoop(applicationContext)
	waitForReconnectSignal(t, connectedNotifications, "the initial broker connection")

	firstOutageLog := aQueueTestEnrichedLog("batch-outage-1")
	secondOutageLog := aQueueTestEnrichedLog("batch-outage-2")
	isBrokerAvailable.Store(false)
	for _, outageLog := range []model.EnrichedLog{firstOutageLog, secondOutageLog} {
		if publishError := publisher.Publish(outageLog); publishError != nil {
			t.Fatalf("expected the outage log to enter the fallback, got error: %v", publishError)
		}
	}

	storedLogs := []model.EnrichedLog{readQueueTestFallbackLog(t, publisher.FallbackChannel())}

	firstReconnectDelay := readQueueTestReconnectDelay(t, reconnectWaits)
	if firstReconnectDelay != time.Second {
		t.Errorf("first reconnect delay = %s, want %s", firstReconnectDelay, time.Second)
	}

	isBrokerAvailable.Store(true)
	close(allowReconnect)
	waitForReconnectSignal(t, connectedNotifications, "the restored broker connection")

	if len(brokerPublishedLogs) != 0 {
		t.Fatalf("publisher re-published %d fallback logs after reconnecting, want none", len(brokerPublishedLogs))
	}

	remainingFallbackLog := readQueueTestFallbackLog(t, publisher.FallbackChannel())
	if remainingFallbackLog != secondOutageLog {
		t.Errorf("remaining fallback log = %+v, want %+v", remainingFallbackLog, secondOutageLog)
	}
	storedLogs = append(storedLogs, remainingFallbackLog)

	restoredLog := aQueueTestEnrichedLog("batch-restored")
	if publishError := publisher.Publish(restoredLog); publishError != nil {
		t.Fatalf("expected the restored broker to accept the log, got error: %v", publishError)
	}

	if len(storedLogs) != 2 || storedLogs[0] != firstOutageLog || storedLogs[1] != secondOutageLog {
		t.Errorf("stored logs = %+v, want %+v in order", storedLogs, []model.EnrichedLog{firstOutageLog, secondOutageLog})
	}
	if len(brokerPublishedLogs) != 1 || brokerPublishedLogs[0] != restoredLog {
		t.Errorf("broker published logs = %+v, want only %+v", brokerPublishedLogs, restoredLog)
	}
	if connectAttemptCount.Load() != 3 {
		t.Errorf("connection attempts = %d, want 3", connectAttemptCount.Load())
	}

	cancel()
	waitForReconnectCompletion(t, completed)
}

func TestPublisher_ReconnectLoopUsesEachConfiguredBackoffDelay(t *testing.T) {
	t.Parallel()

	publisher := NewPublisher(config.ApplicationConfiguration{
		RabbitMQReconnectBackoffDelays: []time.Duration{time.Second, 5 * time.Second, 10 * time.Second},
	}, 10)

	var connectAttemptCount atomic.Int32
	connectedNotifications := make(chan struct{}, 1)
	publisher.connectOperation = func() error {
		connectAttemptCount.Add(1)
		if connectAttemptCount.Load() <= 3 {
			return errors.New("connection refused")
		}
		publisher.mutex.Lock()
		publisher.isConnected = true
		publisher.mutex.Unlock()
		select {
		case connectedNotifications <- struct{}{}:
		default:
		}
		return nil
	}

	recordedReconnectDelays := make(chan time.Duration, 10)
	publisher.waitForReconnectDelay = func(applicationContext context.Context, reconnectDelay time.Duration) error {
		recordedReconnectDelays <- reconnectDelay
		return nil
	}

	applicationContext, cancel := context.WithCancel(context.Background())
	completed := publisher.RunReconnectLoop(applicationContext)
	waitForReconnectSignal(t, connectedNotifications, "the restored broker connection")

	expectedReconnectDelays := []time.Duration{time.Second, 5 * time.Second, 10 * time.Second}
	for reconnectIndex, expectedReconnectDelay := range expectedReconnectDelays {
		recordedReconnectDelay := readQueueTestReconnectDelay(t, recordedReconnectDelays)
		if recordedReconnectDelay != expectedReconnectDelay {
			t.Errorf("reconnect delay %d = %s, want %s", reconnectIndex, recordedReconnectDelay, expectedReconnectDelay)
		}
	}
	if connectAttemptCount.Load() != 4 {
		t.Errorf("connection attempts = %d, want 4", connectAttemptCount.Load())
	}

	cancel()
	waitForReconnectCompletion(t, completed)
}

// Counts the messages the publisher sent to the broker or the fallback.
type recordingPublishMetricsRecorder struct {
	mutex          sync.Mutex
	publishedCount int
}

// Counts one published message.
func (recordingRecorder *recordingPublishMetricsRecorder) RecordQueueMessagePublished() {
	recordingRecorder.mutex.Lock()
	defer recordingRecorder.mutex.Unlock()
	recordingRecorder.publishedCount++
}

// Returns the number of counted published messages.
func (recordingRecorder *recordingPublishMetricsRecorder) publishedMessageCount() int {
	recordingRecorder.mutex.Lock()
	defer recordingRecorder.mutex.Unlock()
	return recordingRecorder.publishedCount
}

func TestPublisher_CountsMessagesSentToTheBrokerAndToTheFallback(t *testing.T) {
	t.Parallel()

	recordingRecorder := &recordingPublishMetricsRecorder{}
	publisher := NewPublisher(config.ApplicationConfiguration{PublishTimeout: time.Second}, 10, WithPublishedMessageRecorder(recordingRecorder))
	publisher.mutex.Lock()
	publisher.isConnected = true
	publisher.mutex.Unlock()
	publisher.publishOperation = func(enrichedLog model.EnrichedLog) error {
		return nil
	}

	if publishError := publisher.Publish(aQueueTestEnrichedLog("batch-broker")); publishError != nil {
		t.Fatalf("expected the broker to accept the log, got error: %v", publishError)
	}

	publisher.mutex.Lock()
	publisher.isConnected = false
	publisher.mutex.Unlock()

	if publishError := publisher.Publish(aQueueTestEnrichedLog("batch-fallback")); publishError != nil {
		t.Fatalf("expected the log to enter the fallback, got error: %v", publishError)
	}

	if recordingRecorder.publishedMessageCount() != 2 {
		t.Errorf("published message count = %d, want 2", recordingRecorder.publishedMessageCount())
	}
}

func TestPublisher_DoesNotCountAMessageWhenTheFallbackIsFull(t *testing.T) {
	t.Parallel()

	recordingRecorder := &recordingPublishMetricsRecorder{}
	publisher := NewPublisher(config.ApplicationConfiguration{PublishTimeout: time.Second}, 1, WithPublishedMessageRecorder(recordingRecorder))
	publisher.mutex.Lock()
	publisher.isConnected = false
	publisher.mutex.Unlock()

	if publishError := publisher.Publish(aQueueTestEnrichedLog("batch-full-1")); publishError != nil {
		t.Fatalf("expected the first log to enter the fallback, got error: %v", publishError)
	}
	if publishError := publisher.Publish(aQueueTestEnrichedLog("batch-full-2")); !errors.Is(publishError, ErrFallbackFull) {
		t.Fatalf("second publish error = %v, want %v", publishError, ErrFallbackFull)
	}

	if recordingRecorder.publishedMessageCount() != 1 {
		t.Errorf("published message count = %d, want 1", recordingRecorder.publishedMessageCount())
	}
}

func TestPublisher_RejectsTheLogsItReceivesAfterItIsClosed(t *testing.T) {
	t.Parallel()

	publisher := NewPublisher(config.ApplicationConfiguration{PublishTimeout: time.Second}, 10)
	publisher.mutex.Lock()
	publisher.isConnected = true
	publisher.mutex.Unlock()
	publisher.publishOperation = func(enrichedLog model.EnrichedLog) error {
		return nil
	}

	if closeError := publisher.Close(); closeError != nil {
		t.Fatalf("closing the publisher failed: %v", closeError)
	}

	if publishError := publisher.Publish(aQueueTestEnrichedLog("batch-closed")); !errors.Is(publishError, ErrPublisherClosed) {
		t.Fatalf("publish error = %v, want %v", publishError, ErrPublisherClosed)
	}
}

func TestVirtualHostPaths_EscapesTheDefaultAndNamedVirtualHosts(t *testing.T) {
	t.Parallel()

	virtualHostCases := []struct {
		name                string
		rabbitMQVirtualHost string
		expectedPath        string
		expectedRawPath     string
	}{
		{name: "the default virtual host", rabbitMQVirtualHost: "", expectedPath: "//", expectedRawPath: "/%2F"},
		{name: "a named virtual host", rabbitMQVirtualHost: "log-engine", expectedPath: "/log-engine", expectedRawPath: "/log-engine"},
		{name: "a virtual host with a space", rabbitMQVirtualHost: "log engine", expectedPath: "/log engine", expectedRawPath: "/log%20engine"},
	}

	for _, virtualHostCase := range virtualHostCases {
		t.Run(virtualHostCase.name, func(t *testing.T) {
			t.Parallel()

			virtualHostPath, virtualHostRawPath := virtualHostPaths(virtualHostCase.rabbitMQVirtualHost)
			if virtualHostPath != virtualHostCase.expectedPath {
				t.Errorf("virtual host path = %q, want %q", virtualHostPath, virtualHostCase.expectedPath)
			}
			if virtualHostRawPath != virtualHostCase.expectedRawPath {
				t.Errorf("virtual host raw path = %q, want %q", virtualHostRawPath, virtualHostCase.expectedRawPath)
			}
		})
	}
}

func TestReconnectDelayAt_ReusesTheLastConfiguredDelay(t *testing.T) {
	t.Parallel()

	reconnectBackoffDelays := []time.Duration{time.Second, 5 * time.Second}
	if reconnectDelay := reconnectDelayAt(reconnectBackoffDelays, 5); reconnectDelay != 5*time.Second {
		t.Errorf("reconnect delay for an attempt beyond the configured list = %s, want %s", reconnectDelay, 5*time.Second)
	}

	if reconnectDelay := reconnectDelayAt(nil, 0); reconnectDelay != time.Second {
		t.Errorf("reconnect delay without configured delays = %s, want %s", reconnectDelay, time.Second)
	}
}

func aQueueTestEnrichedLog(batchID string) model.EnrichedLog {
	return model.EnrichedLog{
		LogEntry: model.LogEntry{
			Timestamp: "2026-10-25T00:00:00Z",
			Service:   "checkout",
			Level:     model.LogLevelInfo,
			Message:   "processed " + batchID,
		},
		BatchID:     batchID,
		ReceivedAt:  time.Date(2026, time.October, 25, 0, 0, 0, 0, time.UTC),
		SourceIP:    "203.0.113.10",
		Environment: "test",
		Destination: model.DestinationService1,
	}
}

func readQueueTestFallbackLog(t *testing.T, fallbackChannel <-chan model.EnrichedLog) model.EnrichedLog {
	t.Helper()
	select {
	case enrichedLog := <-fallbackChannel:
		return enrichedLog
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for a fallback log")
		return model.EnrichedLog{}
	}
}

func readQueueTestReconnectDelay(t *testing.T, reconnectWaits <-chan time.Duration) time.Duration {
	t.Helper()
	select {
	case reconnectDelay := <-reconnectWaits:
		return reconnectDelay
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for a reconnect delay")
		return 0
	}
}

func waitForReconnectSignal(t *testing.T, notifications <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-notifications:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", description)
	}
}

func waitForReconnectCompletion(t *testing.T, completed <-chan struct{}) {
	t.Helper()
	select {
	case <-completed:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the reconnect loop to stop")
	}
}
