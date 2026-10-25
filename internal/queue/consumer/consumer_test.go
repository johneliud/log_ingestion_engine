package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/johneliud/log_ingestion_engine/internal/model"
	amqp "github.com/rabbitmq/amqp091-go"
)

type fakeStorageHandler struct {
	mutex              sync.Mutex
	storedLogs         []model.EnrichedLog
	storedDestinations []model.Destination
	storeError         error
	storeStarted       chan<- struct{}
	releaseStore       <-chan struct{}
}

func (fakeStorageHandlerInstance *fakeStorageHandler) StoreBatch(destination model.Destination, logs []model.EnrichedLog) error {
	if fakeStorageHandlerInstance.storeStarted != nil {
		select {
		case fakeStorageHandlerInstance.storeStarted <- struct{}{}:
		default:
		}
	}
	if fakeStorageHandlerInstance.releaseStore != nil {
		<-fakeStorageHandlerInstance.releaseStore
	}

	fakeStorageHandlerInstance.mutex.Lock()
	defer fakeStorageHandlerInstance.mutex.Unlock()
	fakeStorageHandlerInstance.storedDestinations = append(fakeStorageHandlerInstance.storedDestinations, destination)
	fakeStorageHandlerInstance.storedLogs = append(fakeStorageHandlerInstance.storedLogs, logs...)
	return fakeStorageHandlerInstance.storeError
}

func (fakeStorageHandlerInstance *fakeStorageHandler) storedLogCount() int {
	fakeStorageHandlerInstance.mutex.Lock()
	defer fakeStorageHandlerInstance.mutex.Unlock()
	return len(fakeStorageHandlerInstance.storedLogs)
}

func (fakeStorageHandlerInstance *fakeStorageHandler) storedDestinationAt(index int) model.Destination {
	fakeStorageHandlerInstance.mutex.Lock()
	defer fakeStorageHandlerInstance.mutex.Unlock()
	return fakeStorageHandlerInstance.storedDestinations[index]
}

func (fakeStorageHandlerInstance *fakeStorageHandler) storedLogAt(index int) model.EnrichedLog {
	fakeStorageHandlerInstance.mutex.Lock()
	defer fakeStorageHandlerInstance.mutex.Unlock()
	return fakeStorageHandlerInstance.storedLogs[index]
}

type fakeAcknowledger struct {
	mutex               sync.Mutex
	ackCount            int
	requeueValues       []bool
	acknowledgedChannel chan struct{}
	nackedChannel       chan bool
}

func newFakeAcknowledger() *fakeAcknowledger {
	return &fakeAcknowledger{
		acknowledgedChannel: make(chan struct{}, 10),
		nackedChannel:       make(chan bool, 10),
	}
}

func (fakeAcknowledgerInstance *fakeAcknowledger) Ack(deliveryTag uint64, multiple bool) error {
	fakeAcknowledgerInstance.mutex.Lock()
	fakeAcknowledgerInstance.ackCount++
	fakeAcknowledgerInstance.mutex.Unlock()
	fakeAcknowledgerInstance.acknowledgedChannel <- struct{}{}
	return nil
}

func (fakeAcknowledgerInstance *fakeAcknowledger) Nack(deliveryTag uint64, multiple bool, requeue bool) error {
	fakeAcknowledgerInstance.mutex.Lock()
	fakeAcknowledgerInstance.requeueValues = append(fakeAcknowledgerInstance.requeueValues, requeue)
	fakeAcknowledgerInstance.mutex.Unlock()
	fakeAcknowledgerInstance.nackedChannel <- requeue
	return nil
}

func (fakeAcknowledgerInstance *fakeAcknowledger) Reject(deliveryTag uint64, requeue bool) error {
	return nil
}

func (fakeAcknowledgerInstance *fakeAcknowledger) acknowledgementCount() int {
	fakeAcknowledgerInstance.mutex.Lock()
	defer fakeAcknowledgerInstance.mutex.Unlock()
	return fakeAcknowledgerInstance.ackCount
}

func aDeliveryWithLog(t *testing.T, enrichedLog model.EnrichedLog) amqp.Delivery {
	t.Helper()
	serializedLog, marshalError := json.Marshal(enrichedLog)
	if marshalError != nil {
		t.Fatalf("marshalling test log: %v", marshalError)
	}
	return amqp.Delivery{Body: serializedLog, Acknowledger: newFakeAcknowledger()}
}

func aDeliverySubscriber(deliveries <-chan amqp.Delivery) queueSubscriber {
	return func(applicationContext context.Context, queueName string) (<-chan amqp.Delivery, func() error, error) {
		return deliveries, func() error { return nil }, nil
	}
}

func startQueueConsumer(t *testing.T, queueConsumer *QueueConsumer) (context.CancelFunc, <-chan struct{}) {
	t.Helper()
	applicationContext, applicationContextCancellation := context.WithCancel(context.Background())
	consumerCompleted := queueConsumer.Start(applicationContext)
	return func() {
		applicationContextCancellation()
		waitForConsumerCompletion(t, consumerCompleted)
	}, consumerCompleted
}

func waitForConsumerCompletion(t *testing.T, completed <-chan struct{}) {
	t.Helper()
	select {
	case <-completed:
	case <-time.After(2 * time.Second):
		t.Fatal("consumer did not complete")
	}
}

func waitForSignal(t *testing.T, signal <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", description)
	}
}

func TestQueueConsumer_AcknowledgesOnlyAfterStorageCompletes(t *testing.T) {
	t.Parallel()

	storeStarted := make(chan struct{}, 1)
	releaseStore := make(chan struct{})
	storageHandler := &fakeStorageHandler{storeStarted: storeStarted, releaseStore: releaseStore}
	deliveries := make(chan amqp.Delivery, 1)
	delivery := aDeliveryWithLog(t, model.EnrichedLog{BatchID: "batch-1"})
	acknowledger := delivery.Acknowledger.(*fakeAcknowledger)

	queueConsumer := NewQueueConsumer(QueueConsumerDependencies{
		StorageHandler:  storageHandler,
		QueueName:       "queue_service1",
		Destination:     model.DestinationService1,
		QueueSubscriber: aDeliverySubscriber(deliveries),
	})
	stopConsumer, _ := startQueueConsumer(t, queueConsumer)
	defer stopConsumer()

	deliveries <- delivery
	waitForSignal(t, storeStarted, "storage to start")
	if acknowledgementCount := acknowledger.acknowledgementCount(); acknowledgementCount != 0 {
		t.Fatalf("acknowledgements before storage completed = %d, want 0", acknowledgementCount)
	}

	close(releaseStore)
	waitForSignal(t, acknowledger.acknowledgedChannel, "the delivery acknowledgement")
	if acknowledgmentCount := acknowledger.acknowledgementCount(); acknowledgmentCount != 1 {
		t.Fatalf("acknowledgements = %d, want 1", acknowledgmentCount)
	}
	if storedLogCount := storageHandler.storedLogCount(); storedLogCount != 1 {
		t.Fatalf("stored log count = %d, want 1", storedLogCount)
	}
}

func TestQueueConsumer_RequeuesDeliveryWhenStorageFails(t *testing.T) {
	t.Parallel()

	storageHandler := &fakeStorageHandler{storeError: errors.New("storage unavailable")}
	deliveries := make(chan amqp.Delivery, 1)
	delivery := aDeliveryWithLog(t, model.EnrichedLog{BatchID: "batch-1"})
	acknowledger := delivery.Acknowledger.(*fakeAcknowledger)

	queueConsumer := NewQueueConsumer(QueueConsumerDependencies{
		StorageHandler:  storageHandler,
		QueueName:       "queue_service1",
		Destination:     model.DestinationService1,
		QueueSubscriber: aDeliverySubscriber(deliveries),
	})
	stopConsumer, _ := startQueueConsumer(t, queueConsumer)
	defer stopConsumer()

	deliveries <- delivery
	var shouldRequeue bool
	select {
	case shouldRequeue = <-acknowledger.nackedChannel:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the delivery rejection")
	}

	if !shouldRequeue {
		t.Error("delivery was rejected without requeue, want requeue")
	}
	if acknowledgementCount := acknowledger.acknowledgementCount(); acknowledgementCount != 0 {
		t.Errorf("acknowledgements = %d, want 0", acknowledgementCount)
	}
	if storedLogCount := storageHandler.storedLogCount(); storedLogCount != 1 {
		t.Errorf("stored log count = %d, want 1 storage attempt", storedLogCount)
	}
}

func TestQueueConsumer_RejectsUndecodableDeliveryWithoutRequeue(t *testing.T) {
	t.Parallel()

	storageHandler := &fakeStorageHandler{}
	deliveries := make(chan amqp.Delivery, 1)
	acknowledger := newFakeAcknowledger()

	queueConsumer := NewQueueConsumer(QueueConsumerDependencies{
		StorageHandler:  storageHandler,
		QueueName:       "queue_service1",
		Destination:     model.DestinationService1,
		QueueSubscriber: aDeliverySubscriber(deliveries),
	})
	stopConsumer, _ := startQueueConsumer(t, queueConsumer)
	defer stopConsumer()

	deliveries <- amqp.Delivery{Body: []byte("not-json"), Acknowledger: acknowledger}
	var shouldRequeue bool
	select {
	case shouldRequeue = <-acknowledger.nackedChannel:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the delivery rejection")
	}

	if shouldRequeue {
		t.Error("undecodable delivery was requeued, want rejection without requeue")
	}
	if storedLogCount := storageHandler.storedLogCount(); storedLogCount != 0 {
		t.Errorf("stored log count = %d, want 0", storedLogCount)
	}
}

func TestQueueConsumer_StoresDeliveryUnderTheQueueDestination(t *testing.T) {
	t.Parallel()

	storageHandler := &fakeStorageHandler{}
	deliveries := make(chan amqp.Delivery, 1)
	delivery := aDeliveryWithLog(t, model.EnrichedLog{BatchID: "batch-1", Destination: model.DestinationService3})
	acknowledger := delivery.Acknowledger.(*fakeAcknowledger)

	queueConsumer := NewQueueConsumer(QueueConsumerDependencies{
		StorageHandler:  storageHandler,
		QueueName:       "queue_service2",
		Destination:     model.DestinationService2,
		QueueSubscriber: aDeliverySubscriber(deliveries),
	})
	stopConsumer, _ := startQueueConsumer(t, queueConsumer)
	defer stopConsumer()

	deliveries <- delivery
	waitForSignal(t, acknowledger.acknowledgedChannel, "the delivery acknowledgement")

	if storedLogCount := storageHandler.storedLogCount(); storedLogCount != 1 {
		t.Fatalf("stored log count = %d, want 1", storedLogCount)
	}
	if storedDestination := storageHandler.storedDestinationAt(0); storedDestination != model.DestinationService2 {
		t.Errorf("stored destination = %q, want %q", storedDestination, model.DestinationService2)
	}
	if storedLogDestination := storageHandler.storedLogAt(0).Destination; storedLogDestination != model.DestinationService2 {
		t.Errorf("stored log destination = %q, want %q", storedLogDestination, model.DestinationService2)
	}
}

func TestQueueConsumer_DrainsBufferedFallbackLogsOnCancel(t *testing.T) {
	t.Parallel()

	storageHandler := &fakeStorageHandler{}
	fallbackChannel := make(chan model.EnrichedLog, 1)
	fallbackChannel <- model.EnrichedLog{BatchID: "batch-1", Destination: model.DestinationService1}

	queueConsumer := NewQueueConsumer(QueueConsumerDependencies{
		StorageHandler:  storageHandler,
		FallbackChannel: fallbackChannel,
	})
	stopConsumer, _ := startQueueConsumer(t, queueConsumer)
	stopConsumer()

	if storedLogCount := storageHandler.storedLogCount(); storedLogCount != 1 {
		t.Fatalf("stored log count = %d, want 1", storedLogCount)
	}
	if storedDestination := storageHandler.storedDestinationAt(0); storedDestination != model.DestinationService1 {
		t.Fatalf("stored destination = %q, want %q", storedDestination, model.DestinationService1)
	}
}
