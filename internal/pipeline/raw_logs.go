package pipeline

import (
	"context"
	"errors"
	"time"

	"github.com/johneliud/log_ingestion_engine/internal/model"
)

var (
	ErrBatchTooLarge   = errors.New("batch exceeds raw logs queue capacity")
	ErrQueueOverloaded = errors.New("raw logs queue reservation timed out")
	ErrQueueClosed     = errors.New("raw logs queue is closed")
)

// Holds the raw logs channel and send configuration.
type RawLogsQueue struct {
	channel      chan model.RawLog
	sendTimeout  time.Duration
	producerLock chan struct{}
	// isClosed is read and written only while holding the producer lock.
	isClosed bool
}

// Returns a new raw logs queue with bounded capacity.
func NewRawLogsQueue(bufferSize int, sendTimeout time.Duration) *RawLogsQueue {
	return &RawLogsQueue{
		channel:      make(chan model.RawLog, bufferSize),
		sendTimeout:  sendTimeout,
		producerLock: make(chan struct{}, 1),
	}
}

// Returns the raw logs channel.
func (rawLogsQueue *RawLogsQueue) RawLogsChannel() <-chan model.RawLog {
	return rawLogsQueue.channel
}

// Drains the queue for shutdown: it blocks new producers, marks the queue closed and removes the logs still waiting so they are not lost.
func (rawLogsQueue *RawLogsQueue) DrainRemainingLogs() []model.RawLog {
	rawLogsQueue.producerLock <- struct{}{}
	defer func() { <-rawLogsQueue.producerLock }()

	rawLogsQueue.isClosed = true

	undrainedLogs := make([]model.RawLog, 0, len(rawLogsQueue.channel))
	for {
		select {
		case rawLog := <-rawLogsQueue.channel:
			undrainedLogs = append(undrainedLogs, rawLog)
		default:
			return undrainedLogs
		}
	}
}

// Pushes a batch of raw logs into the queue. If the whole batch cannot fit without blocking beyond the timeout, nothing is sent and the function reports failure.
func (rawLogsQueue *RawLogsQueue) PushBatch(rawLogs []model.RawLog) error {
	if len(rawLogs) == 0 {
		return nil
	}

	if len(rawLogs) > cap(rawLogsQueue.channel) {
		return ErrBatchTooLarge
	}

	sendContext, sendContextCancellation := context.WithTimeout(context.Background(), rawLogsQueue.sendTimeout)
	defer sendContextCancellation()

	// Include time waiting for other producers in the reservation timeout.
	select {
	case rawLogsQueue.producerLock <- struct{}{}:
		defer func() { <-rawLogsQueue.producerLock }()
		if rawLogsQueue.isClosed {
			return ErrQueueClosed
		}
	case <-sendContext.Done():
		return ErrQueueOverloaded
	}

	capacityCheck := time.NewTicker(time.Millisecond)
	defer capacityCheck.Stop()
	for len(rawLogs) > cap(rawLogsQueue.channel)-len(rawLogsQueue.channel) {
		select {
		case <-capacityCheck.C:
		case <-sendContext.Done():
			return ErrQueueOverloaded
		}
	}
	if sendContext.Err() != nil {
		return ErrQueueOverloaded
	}

	// The producer lock reserves this capacity: consumers can only free more.
	for _, rawLog := range rawLogs {
		rawLogsQueue.channel <- rawLog
	}
	return nil
}
