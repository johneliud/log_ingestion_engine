package pipeline

import (
	"context"
	"time"

	"github.com/johneliud/log_ingestion_engine/internal/model"
)

// Holds the raw logs channel and send configuration.
type RawLogsQueue struct {
	channel     chan model.RawLog
	sendTimeout time.Duration
}

// Returns a new raw logs queue with bounded capacity.
func NewRawLogsQueue(bufferSize int, sendTimeout time.Duration) *RawLogsQueue {
	return &RawLogsQueue{
		channel:     make(chan model.RawLog, bufferSize),
		sendTimeout: sendTimeout,
	}
}

// Returns the raw logs channel.
func (rawLogsQueue *RawLogsQueue) RawLogsChannel() <-chan model.RawLog {
	return rawLogsQueue.channel
}

// Pushes a batch of raw logs into the queue. If the whole batch cannot fit without blocking beyond the timeout, nothing is sent and the function reports failure.
func (rawLogsQueue *RawLogsQueue) PushBatch(rawLogs []model.RawLog) bool {
	if len(rawLogs) == 0 {
		return true
	}

	if len(rawLogs) > cap(rawLogsQueue.channel) {
		return false
	}

	sendContext, sendContextCancellation := context.WithTimeout(context.Background(), rawLogsQueue.sendTimeout)
	defer sendContextCancellation()

	for _, rawLog := range rawLogs {
		select {
		case rawLogsQueue.channel <- rawLog:
			continue
		case <-sendContext.Done():
			return false
		}
	}
	return true
}
