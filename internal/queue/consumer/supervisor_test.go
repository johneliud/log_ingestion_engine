package consumer

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/johneliud/log_ingestion_engine/internal/config"
	"github.com/johneliud/log_ingestion_engine/internal/model"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Records the batches a consumer stores.
type recordingStorageHandler struct {
	mutex         sync.Mutex
	storedBatches [][]model.EnrichedLog
}

// Stores one batch and records it.
func (recordingStorageHandler *recordingStorageHandler) StoreBatch(destination model.Destination, enrichedLogs []model.EnrichedLog) error {
	recordingStorageHandler.mutex.Lock()
	defer recordingStorageHandler.mutex.Unlock()
	recordingStorageHandler.storedBatches = append(recordingStorageHandler.storedBatches, enrichedLogs)
	return nil
}

// Counts the recorded batches.
func (recordingStorageHandler *recordingStorageHandler) recordedBatchCount() int {
	recordingStorageHandler.mutex.Lock()
	defer recordingStorageHandler.mutex.Unlock()
	return len(recordingStorageHandler.storedBatches)
}

// Records the dial attempts and the reconnect delays a supervisor uses.
type supervisorDialRecorder struct {
	mutex              sync.Mutex
	dialAttempts       int
	reconnectWait      []time.Duration
	dialAttemptChannel chan struct{}
}

// Returns the number of dial attempts made so far.
func (dialRecorder *supervisorDialRecorder) attemptCount() int {
	dialRecorder.mutex.Lock()
	defer dialRecorder.mutex.Unlock()
	return dialRecorder.dialAttempts
}

// Returns the reconnect delays the supervisor waited for.
func (dialRecorder *supervisorDialRecorder) waitedDelays() []time.Duration {
	dialRecorder.mutex.Lock()
	defer dialRecorder.mutex.Unlock()
	return append([]time.Duration(nil), dialRecorder.reconnectWait...)
}

// Returns a supervisor that fails every dial and records the reconnect delays.
func failingSupervisor(dialRecorder *supervisorDialRecorder, reconnectBackoffDelays []time.Duration) *Supervisor {
	return NewSupervisor(SupervisorDependencies{
		ApplicationConfiguration: config.ApplicationConfiguration{},
		StorageHandler:           &recordingStorageHandler{},
		DestinationQueues: []DestinationQueue{
			{QueueName: config.QueueService1Name, Destination: model.DestinationService1},
		},
		ReconnectBackoffDelays: reconnectBackoffDelays,
		Dial: func() (*amqp.Connection, error) {
			dialRecorder.mutex.Lock()
			dialRecorder.dialAttempts++
			dialRecorder.mutex.Unlock()
			if dialRecorder.dialAttemptChannel != nil {
				select {
				case dialRecorder.dialAttemptChannel <- struct{}{}:
				default:
				}
			}
			return nil, errors.New("broker unavailable")
		},
		WaitForReconnectDelay: func(applicationContext context.Context, reconnectDelay time.Duration) error {
			dialRecorder.mutex.Lock()
			dialRecorder.reconnectWait = append(dialRecorder.reconnectWait, reconnectDelay)
			dialRecorder.mutex.Unlock()
			<-applicationContext.Done()
			return applicationContext.Err()
		},
	})
}

// Reports when a consumer stops and when it observes the cancellation of its context.
type observableDestinationConsumer struct {
	stoppedChannel   chan struct{}
	cancelledChannel chan struct{}
}

// Stops the consumer when its context is cancelled.
func (observableConsumer *observableDestinationConsumer) Start(applicationContext context.Context) <-chan struct{} {
	completed := make(chan struct{})
	go func() {
		defer close(completed)
		<-applicationContext.Done()
		close(observableConsumer.cancelledChannel)
	}()
	return completed
}

func TestSupervisor_StopsTheSiblingConsumersWhenOneDestinationConsumerStops(t *testing.T) {
	t.Parallel()

	consumerContext, cancelConsumers := context.WithCancel(context.Background())
	defer cancelConsumers()

	stoppedConsumerCompletion := make(chan struct{})
	close(stoppedConsumerCompletion)
	siblingConsumer := &observableDestinationConsumer{
		stoppedChannel:   make(chan struct{}),
		cancelledChannel: make(chan struct{}),
	}

	supervisor := &Supervisor{}
	supervisor.stopConsumersWhenOneStops(context.Background(), cancelConsumers, []<-chan struct{}{stoppedConsumerCompletion, siblingConsumer.Start(consumerContext)})

	select {
	case <-siblingConsumer.cancelledChannel:
	case <-time.After(time.Second):
		t.Fatal("the sibling consumer was not stopped after the first consumer ended")
	}
}

func TestSupervisor_ReturnsWithoutConsumersInsteadOfWaitingForever(t *testing.T) {
	t.Parallel()

	applicationContext, applicationContextCancellation := context.WithCancel(context.Background())
	applicationContextCancellation()

	supervisor := &Supervisor{}
	stoppedWithoutConsumers := make(chan struct{})
	go func() {
		defer close(stoppedWithoutConsumers)
		supervisor.stopConsumersWhenOneStops(applicationContext, func() {}, nil)
	}()

	select {
	case <-stoppedWithoutConsumers:
	case <-time.After(time.Second):
		t.Fatal("the supervisor waited for consumers that do not exist")
	}
}

func TestSupervisor_RetriesTheDialAndStopsWhenTheContextIsCancelled(t *testing.T) {
	t.Parallel()

	dialRecorder := &supervisorDialRecorder{}
	supervisor := failingSupervisor(dialRecorder, []time.Duration{5 * time.Millisecond, 10 * time.Millisecond})

	applicationContext, applicationContextCancellation := context.WithCancel(context.Background())
	applicationContextCancellation()
	completed := supervisor.Start(applicationContext)

	select {
	case <-completed:
	case <-time.After(time.Second):
		t.Fatal("the supervisor did not stop after the context was cancelled")
	}

	if dialRecorder.attemptCount() != 0 {
		t.Errorf("dial attempt count = %d, want 0", dialRecorder.attemptCount())
	}
}

func TestSupervisor_UsesTheConfiguredBackoffAndStopsAfterTheWait(t *testing.T) {
	t.Parallel()

	dialRecorder := &supervisorDialRecorder{dialAttemptChannel: make(chan struct{}, 1)}
	supervisor := failingSupervisor(dialRecorder, []time.Duration{5 * time.Millisecond, 10 * time.Millisecond})

	applicationContext, applicationContextCancellation := context.WithCancel(context.Background())
	completed := supervisor.Start(applicationContext)

	select {
	case <-dialRecorder.dialAttemptChannel:
	case <-time.After(time.Second):
		t.Fatal("the supervisor did not attempt to reach the broker")
	}
	applicationContextCancellation()

	select {
	case <-completed:
	case <-time.After(time.Second):
		t.Fatal("the supervisor did not stop after the context was cancelled")
	}

	if dialRecorder.attemptCount() != 1 {
		t.Errorf("dial attempt count = %d, want 1", dialRecorder.attemptCount())
	}
	waitedDelays := dialRecorder.waitedDelays()
	if len(waitedDelays) != 1 {
		t.Fatalf("reconnect wait count = %d, want 1", len(waitedDelays))
	}
	if waitedDelays[0] != 5*time.Millisecond {
		t.Errorf("first reconnect delay = %v, want %v", waitedDelays[0], 5*time.Millisecond)
	}
}

func TestSupervisor_ReconnectDelayAtUsesTheConfiguredBackoffAndReusesTheLastDelay(t *testing.T) {
	t.Parallel()

	supervisor := failingSupervisor(&supervisorDialRecorder{}, []time.Duration{5 * time.Millisecond, 10 * time.Millisecond})

	delayCases := []struct {
		name                  string
		reconnectAttemptIndex int
		expectedDelay         time.Duration
	}{
		{name: "first attempt", reconnectAttemptIndex: 0, expectedDelay: 5 * time.Millisecond},
		{name: "second attempt", reconnectAttemptIndex: 1, expectedDelay: 10 * time.Millisecond},
		{name: "beyond the configured delays", reconnectAttemptIndex: 7, expectedDelay: 10 * time.Millisecond},
	}

	for _, delayCase := range delayCases {
		t.Run(delayCase.name, func(t *testing.T) {
			t.Parallel()

			reconnectDelay := supervisor.reconnectDelayAt(delayCase.reconnectAttemptIndex)
			if reconnectDelay != delayCase.expectedDelay {
				t.Errorf("reconnect delay = %v, want %v", reconnectDelay, delayCase.expectedDelay)
			}
		})
	}
}

func TestSupervisor_ReconnectDelayAtFallsBackToOneSecondWithoutConfiguredDelays(t *testing.T) {
	t.Parallel()

	supervisor := failingSupervisor(&supervisorDialRecorder{}, nil)

	reconnectDelay := supervisor.reconnectDelayAt(0)
	if reconnectDelay != time.Second {
		t.Errorf("reconnect delay = %v, want %v", reconnectDelay, time.Second)
	}
}
