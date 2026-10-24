package pipeline

import (
	"testing"
	"time"

	"github.com/johneliud/log_ingestion_engine/internal/model"
)

func TestRawLogsQueue_PushBatchRejectsWholeBatchOnTimeout(t *testing.T) {
	t.Parallel()
	rawLogsQueue := NewRawLogsQueue(1, 50*time.Millisecond)

	firstBatch := []model.RawLog{{BatchID: "b1"}}
	if !rawLogsQueue.PushBatch(firstBatch) {
		t.Fatalf("first batch should fit")
	}

	secondBatch := []model.RawLog{{BatchID: "b2"}}
	if rawLogsQueue.PushBatch(secondBatch) {
		t.Fatalf("second batch should be rejected when queue is full and timeout elapses")
	}
}

func TestRawLogsQueue_PushBatchDrains(t *testing.T) {
	t.Parallel()
	rawLogsQueue := NewRawLogsQueue(10, 100*time.Millisecond)
	batch := []model.RawLog{{BatchID: "b1"}, {BatchID: "b1"}}
	if !rawLogsQueue.PushBatch(batch) {
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
