package pipeline

import (
	"context"
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
	consumer := NewBatchConsumer(rawLogsChannel, 50, 4, fakeEnricher{}, fakeRouter{}, publisher)

	applicationContext, applicationContextCancellation := context.WithCancel(context.Background())
	consumer.Start(applicationContext)

	rawLogsChannel <- model.RawLog{BatchID: "b1", LogEntry: model.LogEntry{Service: "claims", Level: model.LogLevelError, Message: "x", Timestamp: "2026-10-24T10:00:00Z"}}
	rawLogsChannel <- model.RawLog{BatchID: "b1", LogEntry: model.LogEntry{Service: "billing", Level: model.LogLevelInfo, Message: "y", Timestamp: "2026-10-24T10:00:01Z"}}

	deadline := time.Now().Add(time.Second)
	for publisher.count() < 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	applicationContextCancellation()
	time.Sleep(10 * time.Millisecond)

	if publisher.count() != 2 {
		t.Fatalf("published count = %d, want 2", publisher.count())
	}
}
