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

func (fakePublisherInstance *fakePublisher) PublishBatch(enrichedLogs []model.EnrichedLog) error {
	fakePublisherInstance.mutex.Lock()
	defer fakePublisherInstance.mutex.Unlock()
	fakePublisherInstance.published = append(fakePublisherInstance.published, enrichedLogs...)
	return nil
}

func (fakePublisherInstance *fakePublisher) count() int {
	fakePublisherInstance.mutex.Lock()
	defer fakePublisherInstance.mutex.Unlock()
	return len(fakePublisherInstance.published)
}

// Returns a raw logs queue pre-filled with one log per batch identifier.
func aConsumerTestQueue(t *testing.T, batchIdentifiers ...string) *RawLogsQueue {
	t.Helper()
	rawLogsQueue := NewRawLogsQueue(max(len(batchIdentifiers), 1), time.Second)
	rawLogs := make([]model.RawLog, 0, len(batchIdentifiers))
	for _, batchIdentifier := range batchIdentifiers {
		rawLogs = append(rawLogs, model.RawLog{BatchID: batchIdentifier})
	}
	if pushError := rawLogsQueue.PushBatch(rawLogs); pushError != nil {
		t.Fatalf("filling the raw logs queue failed: %v", pushError)
	}
	return rawLogsQueue
}

func TestBatchConsumer_ProcessesBatchesThroughEnrichmentAndRouting(t *testing.T) {
	t.Parallel()
	rawLogsQueue := aConsumerTestQueue(t, "b1", "b1")
	publisher := &fakePublisher{}
	consumer := NewBatchConsumer(BatchConsumerDependencies{
		RawLogsQueue:      rawLogsQueue,
		ConsumerBatchSize: 50,
		WorkerCount:       4,
		Enricher:          fakeEnricher{},
		Router:            fakeRouter{},
		QueuePublisher:    publisher,
	})

	applicationContext, applicationContextCancellation := context.WithCancel(context.Background())
	defer applicationContextCancellation()
	completed := consumer.Start(applicationContext)

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
	rawLogsQueue := aConsumerTestQueue(t, "queued", "queued", "queued", "queued", "queued", "queued", "queued", "queued", "queued", "queued", "queued")
	applicationContext, cancel := context.WithCancel(context.Background())
	cancel()
	publisher := &fakePublisher{}
	consumer := NewBatchConsumer(BatchConsumerDependencies{
		RawLogsQueue:      rawLogsQueue,
		ConsumerBatchSize: 2,
		WorkerCount:       4,
		Enricher:          fakeEnricher{},
		Router:            fakeRouter{},
		QueuePublisher:    publisher,
	})
	waitForConsumer(t, consumer.Start(applicationContext))
	if publisher.count() != 11 || rawLogsQueue.Len() != 0 {
		t.Fatalf("published %d, queued %d; want 11 published and no queued logs", publisher.count(), rawLogsQueue.Len())
	}
}

type publisherFunc func([]model.EnrichedLog) error

func (publish publisherFunc) PublishBatch(logs []model.EnrichedLog) error { return publish(logs) }

type routerFunc func(model.EnrichedLog) (model.EnrichedLog, error)

func (route routerFunc) Route(log model.EnrichedLog) (model.EnrichedLog, error) { return route(log) }

func TestBatchConsumer_FinishesCurrentBatchAndQueuedBatches(t *testing.T) {
	t.Parallel()
	rawLogsQueue := aConsumerTestQueue(t, "queued", "queued", "queued", "queued", "queued")
	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	var firstPublish sync.Once
	publisher := &fakePublisher{}
	blockingPublisher := publisherFunc(func(logs []model.EnrichedLog) error {
		firstPublish.Do(func() {
			close(entered)
			<-release
		})
		return publisher.PublishBatch(logs)
	})
	applicationContext, cancel := context.WithCancel(context.Background())
	defer cancel()
	consumer := NewBatchConsumer(BatchConsumerDependencies{
		RawLogsQueue:      rawLogsQueue,
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
	rawLogsQueue := aConsumerTestQueue(t, "b1", "b1")

	deadLetteredEntries := make([]model.DeadLetterEntry, 0)
	deadLetterSink := func(deadLetterEntry model.DeadLetterEntry) error {
		deadLetteredEntries = append(deadLetteredEntries, deadLetterEntry)
		return nil
	}
	publish := publisherFunc(func(logs []model.EnrichedLog) error {
		return errors.New("publish failed")
	})

	applicationContext, cancel := context.WithCancel(context.Background())
	cancel()
	waitForConsumer(t, NewBatchConsumer(BatchConsumerDependencies{
		RawLogsQueue:        rawLogsQueue,
		ConsumerBatchSize:   2,
		WorkerCount:         1,
		Enricher:            fakeEnricher{},
		Router:              fakeRouter{},
		QueuePublisher:      publish,
		DeadLetterSink:      deadLetterSink,
		CurrentTimeProvider: func() time.Time { return time.Unix(0, 0).UTC() },
	}).Start(applicationContext))

	if len(deadLetteredEntries) != 2 {
		t.Fatalf("dead letter entry count = %d, want 2", len(deadLetteredEntries))
	}
	for _, deadLetteredEntry := range deadLetteredEntries {
		if deadLetteredEntry.ErrorMessage != "publish failed" {
			t.Errorf("dead letter error message = %q, want %q", deadLetteredEntry.ErrorMessage, "publish failed")
		}
		if !deadLetteredEntry.FailedAt.Equal(time.Unix(0, 0).UTC()) {
			t.Errorf("dead letter failure time = %s, want %s", deadLetteredEntry.FailedAt, time.Unix(0, 0).UTC())
		}
	}
}

func TestBatchConsumer_DeadLettersTheLogsItCouldNotRouteWithTheDefaultDestination(t *testing.T) {
	t.Parallel()
	rawLogsQueue := aConsumerTestQueue(t, "b1")

	deadLetteredEntries := make([]model.DeadLetterEntry, 0)
	route := routerFunc(func(log model.EnrichedLog) (model.EnrichedLog, error) {
		return model.EnrichedLog{}, errors.New("route failed")
	})

	applicationContext, cancel := context.WithCancel(context.Background())
	cancel()
	waitForConsumer(t, NewBatchConsumer(BatchConsumerDependencies{
		RawLogsQueue:      rawLogsQueue,
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
	rawLogsQueue := aConsumerTestQueue(t, "batch-route-failure", "batch-publish-failure", "batch-success")
	published := &fakePublisher{}
	route := routerFunc(func(log model.EnrichedLog) (model.EnrichedLog, error) {
		if log.Service == "route-failure" {
			return model.EnrichedLog{}, errors.New("route failed")
		}
		return log, nil
	})
	publish := publisherFunc(func(logs []model.EnrichedLog) error {
		_ = published.PublishBatch(logs)
		for _, log := range logs {
			if log.Service == "publish-failure" {
				return errors.New("publish failed")
			}
		}
		return nil
	})
	// Attach the service names through the enricher so the router and publisher fakes can branch on them.
	consumer := NewBatchConsumer(BatchConsumerDependencies{
		RawLogsQueue:      rawLogsQueue,
		ConsumerBatchSize: 3,
		WorkerCount:       1,
		Enricher: enricherFunc(func(rawLog model.RawLog) model.EnrichedLog {
			enrichedLog := fakeEnricher{}.Enrich(rawLog)
			switch rawLog.BatchID {
			case "batch-route-failure":
				enrichedLog.Service = "route-failure"
			case "batch-publish-failure":
				enrichedLog.Service = "publish-failure"
			default:
				enrichedLog.Service = "success"
			}
			return enrichedLog
		}),
		Router:         route,
		QueuePublisher: publish,
	})
	applicationContext, cancel := context.WithCancel(context.Background())
	cancel()
	waitForConsumer(t, consumer.Start(applicationContext))
	if published.count() != 2 {
		t.Fatalf("publish attempts = %d, want 2 (skip route failure and continue after publish failure)", published.count())
	}
	decoder := json.NewDecoder(&output)
	for range 2 {
		var entry map[string]any
		if err := decoder.Decode(&entry); err != nil {
			t.Fatal(err)
		}
		errorMessage, isString := entry["error"].(string)
		if entry["level"] != "ERROR" || !isString || errorMessage == "" {
			t.Fatalf("error log missing expected details: %v", entry)
		}
	}
	if decoder.More() {
		t.Fatal("unexpected additional error log")
	}
}

type enricherFunc func(model.RawLog) model.EnrichedLog

func (enrich enricherFunc) Enrich(rawLog model.RawLog) model.EnrichedLog { return enrich(rawLog) }

func TestBatchConsumer_PublishesRoutedLogsWithTheirPerLogDestinations(t *testing.T) {
	t.Parallel()
	rawLogsQueue := NewRawLogsQueue(4, time.Second)
	rawLogs := []model.RawLog{
		{BatchID: "b1", LogEntry: model.LogEntry{Service: "claims"}},
		{BatchID: "b1", LogEntry: model.LogEntry{Service: "policy"}},
		{BatchID: "b1", LogEntry: model.LogEntry{Service: "claims"}},
	}
	if pushError := rawLogsQueue.PushBatch(rawLogs); pushError != nil {
		t.Fatalf("filling the raw logs queue failed: %v", pushError)
	}

	publishBatches := make(chan []model.EnrichedLog, 4)
	publish := publisherFunc(func(logs []model.EnrichedLog) error {
		publishBatches <- logs
		return nil
	})
	route := routerFunc(func(log model.EnrichedLog) (model.EnrichedLog, error) {
		if log.Service == "policy" {
			log.Destination = model.DestinationService3
		}
		return log, nil
	})

	applicationContext, cancel := context.WithCancel(context.Background())
	cancel()
	waitForConsumer(t, NewBatchConsumer(BatchConsumerDependencies{
		RawLogsQueue:      rawLogsQueue,
		ConsumerBatchSize: 10,
		WorkerCount:       1,
		Enricher:          fakeEnricher{},
		Router:            route,
		QueuePublisher:    publish,
	}).Start(applicationContext))

	batchedLogCount := 0
	for len(publishBatches) > 0 {
		publishBatch := <-publishBatches
		batchedLogCount += len(publishBatch)
		for _, enrichedLog := range publishBatch {
			if enrichedLog.Service == "claims" && enrichedLog.Destination != model.DestinationService1 {
				t.Errorf("claims log routed to %q, want %q", enrichedLog.Destination, model.DestinationService1)
			}
			if enrichedLog.Service == "policy" && enrichedLog.Destination != model.DestinationService3 {
				t.Errorf("policy log routed to %q, want %q", enrichedLog.Destination, model.DestinationService3)
			}
		}
	}
	if batchedLogCount != 3 {
		t.Errorf("published log count = %d, want 3", batchedLogCount)
	}
}
