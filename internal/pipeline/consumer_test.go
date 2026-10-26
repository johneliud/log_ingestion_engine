package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/johneliud/log_ingestion_engine/internal/model"
)

type fakeEnricher struct{}

func (fakeEnricherInstance fakeEnricher) Enrich(rawLog model.RawLog) model.EnrichedLog {
	return model.EnrichedLog{
		LogEntry:    rawLog.LogEntry,
		BatchID:     rawLog.BatchID,
		ReceivedAt:  rawLog.ReceivedAt,
		SourceIP:    rawLog.SourceIP,
		Environment: "production",
		Destination: model.DestinationService1,
	}
}

type fakeRouter struct{}

func (fakeRouterInstance fakeRouter) Route(enrichedLog model.EnrichedLog) (model.EnrichedLog, error) {
	return enrichedLog, nil
}

type fakePublisher struct {
	mutex     sync.Mutex
	published []model.EnrichedLog
}

func (fakePublisherInstance *fakePublisher) Publish(enrichedLog model.EnrichedLog) error {
	fakePublisherInstance.mutex.Lock()
	defer fakePublisherInstance.mutex.Unlock()
	fakePublisherInstance.published = append(fakePublisherInstance.published, enrichedLog)
	return nil
}

func (fakePublisherInstance *fakePublisher) count() int {
	fakePublisherInstance.mutex.Lock()
	defer fakePublisherInstance.mutex.Unlock()
	return len(fakePublisherInstance.published)
}

func TestBatchConsumer_ProcessesBatchesThroughEnrichmentAndRouting(t *testing.T) {
	t.Parallel()
	rawLogsChannel := make(chan model.RawLog, 100)
	publisher := &fakePublisher{}
	consumer := NewBatchConsumer(BatchConsumerDependencies{
		RawLogsChannel:    rawLogsChannel,
		ConsumerBatchSize: 50,
		WorkerCount:       4,
		Enricher:          fakeEnricher{},
		Router:            fakeRouter{},
		QueuePublisher:    publisher,
	})

	applicationContext, applicationContextCancellation := context.WithCancel(context.Background())
	defer applicationContextCancellation()
	completed := consumer.Start(applicationContext)

	rawLogsChannel <- model.RawLog{BatchID: "b1", LogEntry: model.LogEntry{Service: "claims", Level: model.LogLevelError, Message: "x", Timestamp: "2026-10-24T10:00:00Z"}}
	rawLogsChannel <- model.RawLog{BatchID: "b1", LogEntry: model.LogEntry{Service: "billing", Level: model.LogLevelInfo, Message: "y", Timestamp: "2026-10-24T10:00:01Z"}}

	applicationContextCancellation()
	waitForConsumer(t, completed)

	if publisher.count() != 2 {
		t.Fatalf("published count = %d, want 2", publisher.count())
	}
}

func waitForConsumer(t *testing.T, completed <-chan struct{}) {
	t.Helper()
	select {
	case <-completed:
	case <-time.After(2 * time.Second):
		t.Fatal("consumer did not finish draining")
	}
}

func TestBatchConsumer_DrainsAfterCancellation(t *testing.T) {
	t.Parallel()
	rawLogsChannel := make(chan model.RawLog, 11)
	for range cap(rawLogsChannel) {
		rawLogsChannel <- model.RawLog{BatchID: "queued"}
	}
	applicationContext, cancel := context.WithCancel(context.Background())
	cancel()
	publisher := &fakePublisher{}
	consumer := NewBatchConsumer(BatchConsumerDependencies{
		RawLogsChannel:    rawLogsChannel,
		ConsumerBatchSize: 2,
		WorkerCount:       4,
		Enricher:          fakeEnricher{},
		Router:            fakeRouter{},
		QueuePublisher:    publisher,
	})
	waitForConsumer(t, consumer.Start(applicationContext))
	if publisher.count() != 11 || len(rawLogsChannel) != 0 {
		t.Fatalf("published %d, queued %d; want 11 published and no queued logs", publisher.count(), len(rawLogsChannel))
	}
}

type publisherFunc func(model.EnrichedLog) error

func (publish publisherFunc) Publish(log model.EnrichedLog) error { return publish(log) }

type routerFunc func(model.EnrichedLog) (model.EnrichedLog, error)

func (route routerFunc) Route(log model.EnrichedLog) (model.EnrichedLog, error) { return route(log) }

func TestBatchConsumer_FinishesCurrentBatchAndQueuedBatches(t *testing.T) {
	t.Parallel()
	rawLogsChannel := make(chan model.RawLog, 5)
	for range cap(rawLogsChannel) {
		rawLogsChannel <- model.RawLog{BatchID: "queued"}
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	var firstPublish sync.Once
	publisher := &fakePublisher{}
	blockingPublisher := publisherFunc(func(log model.EnrichedLog) error {
		firstPublish.Do(func() {
			close(entered)
			<-release
		})
		return publisher.Publish(log)
	})
	applicationContext, cancel := context.WithCancel(context.Background())
	defer cancel()
	consumer := NewBatchConsumer(BatchConsumerDependencies{
		RawLogsChannel:    rawLogsChannel,
		ConsumerBatchSize: 2,
		WorkerCount:       1,
		Enricher:          fakeEnricher{},
		Router:            fakeRouter{},
		QueuePublisher:    blockingPublisher,
	})
	completed := consumer.Start(applicationContext)
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("consumer never started publishing")
	}
	cancel()
	select {
	case <-completed:
		t.Fatal("consumer completed while a publish was still running")
	default:
	}
	unblock()
	waitForConsumer(t, completed)
	if publisher.count() != 5 {
		t.Fatalf("published count = %d, want 5", publisher.count())
	}
}

func TestBatchConsumer_DeadLettersTheLogsItCouldNotPublish(t *testing.T) {
	t.Parallel()
	rawLogsChannel := make(chan model.RawLog, 2)
	rawLogsChannel <- model.RawLog{BatchID: "b1", LogEntry: model.LogEntry{Service: "claims", Level: model.LogLevelError, Message: "x", Timestamp: "2026-10-24T10:00:00Z"}}
	rawLogsChannel <- model.RawLog{BatchID: "b1", LogEntry: model.LogEntry{Service: "billing", Level: model.LogLevelInfo, Message: "y", Timestamp: "2026-10-24T10:00:01Z"}}

	deadLetteredEntries := make([]model.DeadLetterEntry, 0)
	deadLetterSink := func(deadLetterEntry model.DeadLetterEntry) error {
		deadLetteredEntries = append(deadLetteredEntries, deadLetterEntry)
		return nil
	}
	publish := publisherFunc(func(log model.EnrichedLog) error {
		if log.Service == "claims" {
			return errors.New("publish failed")
		}
		return nil
	})

	applicationContext, cancel := context.WithCancel(context.Background())
	cancel()
	waitForConsumer(t, NewBatchConsumer(BatchConsumerDependencies{
		RawLogsChannel:      rawLogsChannel,
		ConsumerBatchSize:   2,
		WorkerCount:         1,
		Enricher:            fakeEnricher{},
		Router:              fakeRouter{},
		QueuePublisher:      publish,
		DeadLetterSink:      deadLetterSink,
		CurrentTimeProvider: func() time.Time { return time.Unix(0, 0).UTC() },
	}).Start(applicationContext))

	if len(deadLetteredEntries) != 1 {
		t.Fatalf("dead letter entry count = %d, want 1", len(deadLetteredEntries))
	}
	if deadLetteredEntries[0].Log.Service != "claims" {
		t.Errorf("dead lettered log service = %q, want %q", deadLetteredEntries[0].Log.Service, "claims")
	}
	if deadLetteredEntries[0].ErrorMessage != "publish failed" {
		t.Errorf("dead letter error message = %q, want %q", deadLetteredEntries[0].ErrorMessage, "publish failed")
	}
	if !deadLetteredEntries[0].FailedAt.Equal(time.Unix(0, 0).UTC()) {
		t.Errorf("dead letter failure time = %s, want %s", deadLetteredEntries[0].FailedAt, time.Unix(0, 0).UTC())
	}
}

func TestBatchConsumer_DeadLettersTheLogsItCouldNotRouteWithTheDefaultDestination(t *testing.T) {
	t.Parallel()
	rawLogsChannel := make(chan model.RawLog, 1)
	rawLogsChannel <- model.RawLog{BatchID: "b1", LogEntry: model.LogEntry{Service: "claims"}}

	deadLetteredEntries := make([]model.DeadLetterEntry, 0)
	route := routerFunc(func(log model.EnrichedLog) (model.EnrichedLog, error) {
		return model.EnrichedLog{}, errors.New("route failed")
	})

	applicationContext, cancel := context.WithCancel(context.Background())
	cancel()
	waitForConsumer(t, NewBatchConsumer(BatchConsumerDependencies{
		RawLogsChannel:    rawLogsChannel,
		ConsumerBatchSize: 1,
		WorkerCount:       1,
		Enricher:          fakeEnricher{},
		Router:            route,
		QueuePublisher:    &fakePublisher{},
		DeadLetterSink: func(deadLetterEntry model.DeadLetterEntry) error {
			deadLetteredEntries = append(deadLetteredEntries, deadLetterEntry)
			return nil
		},
		DefaultDestination:  model.DestinationService3,
		CurrentTimeProvider: func() time.Time { return time.Unix(0, 0).UTC() },
	}).Start(applicationContext))

	if len(deadLetteredEntries) != 1 {
		t.Fatalf("dead letter entry count = %d, want 1", len(deadLetteredEntries))
	}
	if deadLetteredEntries[0].Log.Destination != model.DestinationService3 {
		t.Errorf("dead lettered log destination = %q, want %q", deadLetteredEntries[0].Log.Destination, model.DestinationService3)
	}
}

func TestBatchConsumer_LogsErrorsAndContinues(t *testing.T) {
	var output bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&output, nil)))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })
	rawLogsChannel := make(chan model.RawLog, 3)
	for _, service := range []string{"route-failure", "publish-failure", "success"} {
		rawLogsChannel <- model.RawLog{BatchID: "batch-" + service, LogEntry: model.LogEntry{Service: service}}
	}
	published := &fakePublisher{}
	route := routerFunc(func(log model.EnrichedLog) (model.EnrichedLog, error) {
		if log.Service == "route-failure" {
			return model.EnrichedLog{}, errors.New("route failed")
		}
		return log, nil
	})
	publish := publisherFunc(func(log model.EnrichedLog) error {
		_ = published.Publish(log)
		if log.Service == "publish-failure" {
			return errors.New("publish failed")
		}
		return nil
	})
	applicationContext, cancel := context.WithCancel(context.Background())
	cancel()
	waitForConsumer(t, NewBatchConsumer(BatchConsumerDependencies{
		RawLogsChannel:    rawLogsChannel,
		ConsumerBatchSize: 3,
		WorkerCount:       1,
		Enricher:          fakeEnricher{},
		Router:            route,
		QueuePublisher:    publish,
	}).Start(applicationContext))
	if published.count() != 2 {
		t.Fatalf("publish attempts = %d, want 2 (skip route failure and continue after publish failure)", published.count())
	}
	decoder := json.NewDecoder(&output)
	for _, service := range []string{"route-failure", "publish-failure"} {
		var entry map[string]any
		if err := decoder.Decode(&entry); err != nil {
			t.Fatal(err)
		}
		if entry["level"] != "ERROR" || entry["batch_id"] != "batch-"+service || entry["service"] != service || entry["error"] == "" {
			t.Fatalf("error log missing expected details: %v", entry)
		}
	}
	if decoder.More() {
		t.Fatal("unexpected additional error log")
	}
}
