package consumer

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/johneliud/log_ingestion_engine/internal/config"
	"github.com/johneliud/log_ingestion_engine/internal/model"
)

type fakeStorageHandler struct {
	mutex      sync.Mutex
	storedLogs []model.EnrichedLog
}

func (fakeStorageHandlerInstance *fakeStorageHandler) StoreBatch(destination model.Destination, logs []model.EnrichedLog) error {
	fakeStorageHandlerInstance.mutex.Lock()
	defer fakeStorageHandlerInstance.mutex.Unlock()
	fakeStorageHandlerInstance.storedLogs = append(fakeStorageHandlerInstance.storedLogs, logs...)
	return nil
}

func (fakeStorageHandlerInstance *fakeStorageHandler) storedLogCount() int {
	fakeStorageHandlerInstance.mutex.Lock()
	defer fakeStorageHandlerInstance.mutex.Unlock()
	return len(fakeStorageHandlerInstance.storedLogs)
}

func waitForConsumerCompletion(t *testing.T, completed <-chan struct{}) {
	t.Helper()
	select {
	case <-completed:
	case <-time.After(2 * time.Second):
		t.Fatal("consumer did not complete")
	}
}

func TestQueueConsumerDrainsBufferedFallbackLogsOnCancel(t *testing.T) {
	t.Parallel()
	storageHandler := &fakeStorageHandler{}
	fallbackChannel := make(chan model.EnrichedLog, 1)
	fallbackChannel <- model.EnrichedLog{BatchID: "batch-1", Destination: model.DestinationService1}

	applicationContext, applicationContextCancellation := context.WithCancel(context.Background())
	defer applicationContextCancellation()

	queueConsumer := NewQueueConsumer(config.ApplicationConfiguration{}, storageHandler, fallbackChannel)
	consumerCompleted := queueConsumer.Start(applicationContext)
	applicationContextCancellation()
	waitForConsumerCompletion(t, consumerCompleted)

	if storedLogCount := storageHandler.storedLogCount(); storedLogCount != 1 {
		t.Fatalf("stored log count = %d, want 1", storedLogCount)
	}
}

func TestFallbackProcessorDrainsBufferedFallbackLogsOnCancel(t *testing.T) {
	t.Parallel()
	storageHandler := &fakeStorageHandler{}
	fallbackChannel := make(chan model.EnrichedLog, 1)
	fallbackChannel <- model.EnrichedLog{BatchID: "batch-1", Destination: model.DestinationService1}

	applicationContext, applicationContextCancellation := context.WithCancel(context.Background())
	defer applicationContextCancellation()

	fallbackProcessor := NewFallbackProcessor(fallbackChannel, storageHandler)
	fallbackCompleted := fallbackProcessor.Start(applicationContext)
	applicationContextCancellation()
	waitForConsumerCompletion(t, fallbackCompleted)

	if storedLogCount := storageHandler.storedLogCount(); storedLogCount != 1 {
		t.Fatalf("stored log count = %d, want 1", storedLogCount)
	}
}
