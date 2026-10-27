package pipeline

import (
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

	rawLogsChannel := rawLogsQueue.RawLogsChannel()
	count := 0
	for count < 2 {
		select {
		case <-rawLogsChannel:
			count++
		default:
			t.Fatalf("expected items in channel")
		}
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
	if len(queue.channel) != 2 {
		t.Fatalf("queue length = %d, want 2; failed batch must enqueue nothing", len(queue.channel))
	}
	for range 2 {
		if rawLog := <-queue.RawLogsChannel(); rawLog.BatchID != "existing" {
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
	if len(queue.channel) != 0 {
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
	<-queue.RawLogsChannel()
	<-queue.RawLogsChannel()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("producer did not finish after capacity became available")
	}
	for _, batchID := range []string{"existing", "new", "new"} {
		if rawLog := <-queue.RawLogsChannel(); rawLog.BatchID != batchID {
			t.Fatalf("BatchID = %q, want %q", rawLog.BatchID, batchID)
		}
	}
}

func TestRawLogsQueue_ProducerWaitIsBounded(t *testing.T) {
	t.Parallel()
	queue := NewRawLogsQueue(1, 20*time.Millisecond)
	queue.producerLock <- struct{}{}
	defer func() { <-queue.producerLock }()
	result := make(chan error, 1)
	go func() { result <- queue.PushBatch([]model.RawLog{{}}) }()
	select {
	case err := <-result:
		if !errors.Is(err, ErrQueueOverloaded) {
			t.Fatalf("PushBatch error = %v, want overload", err)
		}
	case <-time.After(time.Second):
		t.Fatal("waiting for another producer exceeded the timeout")
	}
	if len(queue.channel) != 0 {
		t.Fatal("timed out producer must enqueue nothing")
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
	if len(queue.channel) != producerCount*batchSize {
		t.Fatalf("queue length = %d, want %d", len(queue.channel), producerCount*batchSize)
	}
	seen := make(map[string]bool)
	for range producerCount {
		first := <-queue.RawLogsChannel()
		if seen[first.BatchID] {
			t.Fatalf("batch %q appeared more than once", first.BatchID)
		}
		seen[first.BatchID] = true
		for range batchSize - 1 {
			if next := <-queue.RawLogsChannel(); next.BatchID != first.BatchID {
				t.Fatalf("batches interleaved: %q followed by %q", first.BatchID, next.BatchID)
			}
		}
	}
}
