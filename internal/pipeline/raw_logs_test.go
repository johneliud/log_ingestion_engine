package pipeline

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/johneliud/log_ingestion_engine/internal/model"
)

func TestRawLogsQueue_PushBatchRejectsWholeBatchOnTimeout(t *testing.T) {
	t.Parallel()
	rawLogsQueue := NewRawLogsQueue(1, 50*time.Millisecond)

	firstBatch := []model.RawLog{{BatchID: "b1"}}
	if err := rawLogsQueue.PushBatch(firstBatch); err != nil {
		t.Fatalf("first batch should fit")
	}

	secondBatch := []model.RawLog{{BatchID: "b2"}}
	if err := rawLogsQueue.PushBatch(secondBatch); !errors.Is(err, ErrQueueOverloaded) {
		t.Fatalf("second batch should be rejected when queue is full and timeout elapses")
	}
}

func TestRawLogsQueue_DrainRemainingLogsClosesTheQueueToEveryProducer(t *testing.T) {
	t.Parallel()
	rawLogsQueue := NewRawLogsQueue(2, 50*time.Millisecond)

	rawLogsBodies := []model.RawLog{{BatchID: "b1"}}
	if pushError := rawLogsQueue.PushBatch(rawLogsBodies); pushError != nil {
		t.Fatalf("the first batch should fit: %v", pushError)
	}

	blockedProducerResults := make(chan error, 1)
	blockedRawLogs := []model.RawLog{{BatchID: "b2"}, {BatchID: "b2"}}
	go func() {
		blockedProducerResults <- rawLogsQueue.PushBatch(blockedRawLogs)
	}()

	undrainedLogs := rawLogsQueue.DrainRemainingLogs()
	if len(undrainedLogs) != 1 {
		t.Fatalf("drained log count = %d, want the log that was waiting", len(undrainedLogs))
	}

	if pushError := <-blockedProducerResults; pushError != ErrQueueOverloaded && pushError != ErrQueueClosed {
		t.Fatalf("blocked producer error = %v, want rejection (overloaded or closed)", pushError)
	}

	postDrainRawLogs := []model.RawLog{{BatchID: "b1"}}
	if pushError := rawLogsQueue.PushBatch(postDrainRawLogs); !errors.Is(pushError, ErrQueueClosed) {
		t.Fatalf("post-drain producer error = %v, want ErrQueueClosed", pushError)
	}
}

func TestRawLogsQueue_PushBatchDrains(t *testing.T) {
	t.Parallel()
	rawLogsQueue := NewRawLogsQueue(10, 100*time.Millisecond)
	batch := []model.RawLog{{BatchID: "b1"}, {BatchID: "b1"}}
	if err := rawLogsQueue.PushBatch(batch); err != nil {
		t.Fatalf("batch should fit")
	}

	count := 0
	for count < 2 {
		if _, hasMore := rawLogsQueue.TryReceive(); !hasMore {
			t.Fatalf("expected items in the queue")
		}
		count++
	}
}

func TestRawLogsQueue_DrainRemainingLogsRemovesTheWaitingLogs(t *testing.T) {
	t.Parallel()
	rawLogsQueue := NewRawLogsQueue(10, 100*time.Millisecond)
	batch := []model.RawLog{{BatchID: "first"}, {BatchID: "second"}}
	if err := rawLogsQueue.PushBatch(batch); err != nil {
		t.Fatalf("batch should fit")
	}

	undrainedLogs := rawLogsQueue.DrainRemainingLogs()
	if len(undrainedLogs) != 2 {
		t.Fatalf("undrained log count = %d, want 2", len(undrainedLogs))
	}
	if undrainedLogs[0].BatchID != "first" || undrainedLogs[1].BatchID != "second" {
		t.Errorf("undrained logs = %+v, want the queued logs in order", undrainedLogs)
	}
	if remainingLogs := rawLogsQueue.DrainRemainingLogs(); len(remainingLogs) != 0 {
		t.Errorf("undrained log count after the drain = %d, want 0", len(remainingLogs))
	}
}

func TestRawLogsQueue_FailedReservationEnqueuesNothing(t *testing.T) {
	t.Parallel()
	queue := NewRawLogsQueue(3, 20*time.Millisecond)
	if err := queue.PushBatch([]model.RawLog{{BatchID: "existing"}, {BatchID: "existing"}}); err != nil {
		t.Fatal(err)
	}
	if err := queue.PushBatch([]model.RawLog{{BatchID: "rejected"}, {BatchID: "rejected"}}); !errors.Is(err, ErrQueueOverloaded) {
		t.Fatalf("PushBatch error = %v, want overload", err)
	}
	if queue.Len() != 2 {
		t.Fatalf("queue length = %d, want 2; failed batch must enqueue nothing", queue.Len())
	}
	for range 2 {
		rawLog, hasMore := queue.TryReceive()
		if !hasMore || rawLog.BatchID != "existing" {
			t.Fatalf("unexpected entry from failed reservation: %+v", rawLog)
		}
	}
}

func TestRawLogsQueue_RejectsOversizedBatchDistinctly(t *testing.T) {
	t.Parallel()
	queue := NewRawLogsQueue(1, time.Second)
	if err := queue.PushBatch([]model.RawLog{{}, {}}); !errors.Is(err, ErrBatchTooLarge) {
		t.Fatalf("PushBatch error = %v, want oversized batch", err)
	}
	if queue.Len() != 0 {
		t.Fatal("oversized batch must enqueue nothing")
	}
	if err := queue.PushBatch(nil); err != nil {
		t.Fatalf("empty batch error = %v", err)
	}
}

func TestRawLogsQueue_WaitsForCapacityForEntireBatch(t *testing.T) {
	t.Parallel()
	queue := NewRawLogsQueue(3, time.Second)
	if err := queue.PushBatch([]model.RawLog{{BatchID: "existing"}, {BatchID: "existing"}, {BatchID: "existing"}}); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- queue.PushBatch([]model.RawLog{{BatchID: "new"}, {BatchID: "new"}}) }()
	if rawLog, hasMore := queue.TryReceive(); !hasMore || rawLog.BatchID != "existing" {
		t.Fatalf("expected an existing log, got %+v", rawLog)
	}
	if rawLog, hasMore := queue.TryReceive(); !hasMore || rawLog.BatchID != "existing" {
		t.Fatalf("expected an existing log, got %+v", rawLog)
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("producer did not finish after capacity became available")
	}
	for _, batchID := range []string{"existing", "new", "new"} {
		if rawLog, hasMore := queue.TryReceive(); !hasMore || rawLog.BatchID != batchID {
			t.Fatalf("BatchID = %q, want %q", rawLog.BatchID, batchID)
		}
	}
}

func TestRawLogsQueue_ProducerWaitIsBounded(t *testing.T) {
	t.Parallel()
	queue := NewRawLogsQueue(1, 20*time.Millisecond)
	if err := queue.PushBatch([]model.RawLog{{BatchID: "full"}}); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- queue.PushBatch([]model.RawLog{{}}) }()
	select {
	case err := <-result:
		if !errors.Is(err, ErrQueueOverloaded) {
			t.Fatalf("PushBatch error = %v, want overload", err)
		}
	case <-time.After(time.Second):
		t.Fatal("waiting for a full queue exceeded the timeout")
	}
	if queue.Len() != 1 {
		t.Fatal("timed out producer must enqueue nothing")
	}
}

func TestRawLogsQueue_ReceiveFreesCapacityForABlockedProducer(t *testing.T) {
	t.Parallel()
	queue := NewRawLogsQueue(1, 2*time.Second)
	if err := queue.PushBatch([]model.RawLog{{BatchID: "queued"}}); err != nil {
		t.Fatal(err)
	}

	producerResult := make(chan error, 1)
	go func() { producerResult <- queue.PushBatch([]model.RawLog{{BatchID: "new"}}) }()

	applicationContext, cancel := context.WithCancel(context.Background())
	defer cancel()
	if rawLog, queueOpen := queue.Receive(applicationContext); !queueOpen || rawLog.BatchID != "queued" {
		t.Fatalf("expected the queued log, got %+v (open=%v)", rawLog, queueOpen)
	}

	select {
	case pushError := <-producerResult:
		if pushError != nil {
			t.Fatalf("the blocked producer should succeed after capacity was freed: %v", pushError)
		}
	case <-time.After(time.Second):
		t.Fatal("the blocked producer did not wake after capacity was freed")
	}

	if rawLog, hasMore := queue.TryReceive(); !hasMore || rawLog.BatchID != "new" {
		t.Fatalf("expected the producer batch, got %+v", rawLog)
	}
}

func TestRawLogsQueue_ReceiveStopsWhenTheContextIsCancelledOnAnEmptyQueue(t *testing.T) {
	t.Parallel()
	queue := NewRawLogsQueue(1, time.Second)

	applicationContext, cancel := context.WithCancel(context.Background())
	receiveCompleted := make(chan bool, 1)
	go func() {
		_, queueOpen := queue.Receive(applicationContext)
		receiveCompleted <- queueOpen
	}()

	cancel()
	select {
	case queueOpen := <-receiveCompleted:
		if queueOpen {
			t.Fatal("Receive reported an open queue after the context was cancelled on an empty queue")
		}
	case <-time.After(time.Second):
		t.Fatal("Receive did not stop after the context was cancelled")
	}
}

func TestRawLogsQueue_ConcurrentBatchesDoNotInterleave(t *testing.T) {
	t.Parallel()
	const producerCount, batchSize = 16, 16
	queue := NewRawLogsQueue(producerCount*batchSize, time.Second)
	var producers sync.WaitGroup
	for producer := range producerCount {
		producers.Go(func() {
			batch := make([]model.RawLog, batchSize)
			for i := range batch {
				batch[i].BatchID = fmt.Sprint(producer)
			}
			if err := queue.PushBatch(batch); err != nil {
				t.Errorf("PushBatch error = %v", err)
			}
		})
	}
	producers.Wait()
	if queue.Len() != producerCount*batchSize {
		t.Fatalf("queue length = %d, want %d", queue.Len(), producerCount*batchSize)
	}
	seen := make(map[string]bool)
	for range producerCount {
		first, hasMore := queue.TryReceive()
		if !hasMore {
			t.Fatal("expected a queued log")
		}
		if seen[first.BatchID] {
			t.Fatalf("batch %q appeared more than once", first.BatchID)
		}
		seen[first.BatchID] = true
		for range batchSize - 1 {
			next, hasMore := queue.TryReceive()
			if !hasMore || next.BatchID != first.BatchID {
				t.Fatalf("batches interleaved: %q followed by %q", first.BatchID, next.BatchID)
			}
		}
	}
}

func TestRawLogsQueue_PushBatchWhenCapacityFreesQueuesEveryLogEvenBeyondTheCapacity(t *testing.T) {
	queue := NewRawLogsQueue(10, 20*time.Millisecond)
	logs := make([]model.RawLog, 25)
	for logIndex := range logs {
		logs[logIndex] = model.RawLog{LogID: fmt.Sprint(logIndex)}
	}

	pushCompletion := make(chan error, 1)
	go func() {
		pushCompletion <- queue.PushBatchWhenCapacityFrees(context.Background(), logs)
	}()

	receivedLogCount := 0
	deadline := time.Now().Add(5 * time.Second)
	for {
		for {
			if _, hasMore := queue.TryReceive(); !hasMore {
				break
			}
			receivedLogCount++
		}
		select {
		case pushError := <-pushCompletion:
			if pushError != nil {
				t.Fatalf("PushBatchWhenCapacityFrees error = %v", pushError)
			}
			for {
				if _, hasMore := queue.TryReceive(); !hasMore {
					break
				}
				receivedLogCount++
			}
			if receivedLogCount != len(logs) {
				t.Fatalf("received log count = %d, want %d", receivedLogCount, len(logs))
			}
			return
		case <-time.After(20 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			t.Fatalf("received log count = %d, want %d", receivedLogCount, len(logs))
		}
	}
}

func TestRawLogsQueue_PushBatchWhenCapacityFreesStopsWhenTheContextIsCancelled(t *testing.T) {
	queue := NewRawLogsQueue(1, 10*time.Millisecond)
	if pushError := queue.PushBatch([]model.RawLog{{LogID: "queued"}}); pushError != nil {
		t.Fatalf("filling the queue failed: %v", pushError)
	}

	applicationContext, cancelApplication := context.WithCancel(context.Background())
	cancelApplication()
	pushError := queue.PushBatchWhenCapacityFrees(applicationContext, []model.RawLog{{LogID: "replayed"}})
	if !errors.Is(pushError, context.Canceled) {
		t.Errorf("PushBatchWhenCapacityFrees error = %v, want %v", pushError, context.Canceled)
	}
}
