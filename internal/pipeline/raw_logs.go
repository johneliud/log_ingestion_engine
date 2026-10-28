package pipeline

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/johneliud/log_ingestion_engine/internal/model"
)

var (
	ErrBatchTooLarge   = errors.New("batch exceeds raw logs queue capacity")
	ErrQueueOverloaded = errors.New("raw logs queue reservation timed out")
	ErrQueueClosed     = errors.New("raw logs queue is closed")
)

// The delay between two attempts to push into a queue that is at its capacity.
const capacityRetryDelay = 50 * time.Millisecond

// Holds the bounded raw logs buffer.
type RawLogsQueue struct {
	mutex       sync.Mutex
	condition   *sync.Cond
	buffer      []model.RawLog
	capacity    int
	sendTimeout time.Duration
	isClosed bool
}

// Returns a new raw logs queue with bounded capacity.
func NewRawLogsQueue(bufferSize int, sendTimeout time.Duration) *RawLogsQueue {
	rawLogsQueue := &RawLogsQueue{
		capacity:    bufferSize,
		sendTimeout: sendTimeout,
	}
	rawLogsQueue.condition = sync.NewCond(&rawLogsQueue.mutex)
	return rawLogsQueue
}

// Returns the number of logs currently waiting in the queue.
func (rawLogsQueue *RawLogsQueue) Len() int {
	rawLogsQueue.mutex.Lock()
	defer rawLogsQueue.mutex.Unlock()
	return len(rawLogsQueue.buffer)
}

// Blocks until one raw log is available, the context is cancelled, or the queue is closed and drained.
func (rawLogsQueue *RawLogsQueue) Receive(applicationContext context.Context) (model.RawLog, bool) {
	rawLogsQueue.mutex.Lock()
	defer rawLogsQueue.mutex.Unlock()

	for {
		if len(rawLogsQueue.buffer) > 0 {
			return rawLogsQueue.removeFront(), true
		}
		if rawLogsQueue.isClosed || applicationContext.Err() != nil {
			return model.RawLog{}, false
		}

		wakeOnCancellation := context.AfterFunc(applicationContext, func() {
			rawLogsQueue.mutex.Lock()
			defer rawLogsQueue.mutex.Unlock()
			rawLogsQueue.condition.Broadcast()
		})
		rawLogsQueue.condition.Wait()
		wakeOnCancellation()
	}
}

// Returns one waiting raw log without blocking.
func (rawLogsQueue *RawLogsQueue) TryReceive() (model.RawLog, bool) {
	rawLogsQueue.mutex.Lock()
	defer rawLogsQueue.mutex.Unlock()
	if len(rawLogsQueue.buffer) == 0 {
		return model.RawLog{}, false
	}
	return rawLogsQueue.removeFront(), true
}

// Pushes a batch of raw logs into the queue. If the whole batch cannot fit without blocking beyond the timeout, nothing is queued and the function reports failure.
func (rawLogsQueue *RawLogsQueue) PushBatch(rawLogs []model.RawLog) error {
	if len(rawLogs) == 0 {
		return nil
	}

	if len(rawLogs) > rawLogsQueue.capacity {
		return ErrBatchTooLarge
	}

	rawLogsQueue.mutex.Lock()
	defer rawLogsQueue.mutex.Unlock()

	deadline := time.Now().Add(rawLogsQueue.sendTimeout)
	for {
		if rawLogsQueue.isClosed {
			return ErrQueueClosed
		}
		if rawLogsQueue.capacity-len(rawLogsQueue.buffer) >= len(rawLogs) {
			rawLogsQueue.buffer = append(rawLogsQueue.buffer, rawLogs...)
			rawLogsQueue.condition.Broadcast()
			return nil
		}

		remainingTime := time.Until(deadline)
		if remainingTime <= 0 {
			return ErrQueueOverloaded
		}

		wakeAfterDeadline := time.AfterFunc(remainingTime, func() {
			rawLogsQueue.mutex.Lock()
			defer rawLogsQueue.mutex.Unlock()
			rawLogsQueue.condition.Broadcast()
		})
		rawLogsQueue.condition.Wait()
		wakeAfterDeadline.Stop()
	}
}

// Pushes the logs into the queue in pieces that fit the current capacity and waits for the pipeline to free capacity until the context is cancelled.
func (rawLogsQueue *RawLogsQueue) PushBatchWhenCapacityFrees(applicationContext context.Context, rawLogs []model.RawLog) error {
	if len(rawLogs) == 0 {
		return nil
	}

	rawLogsQueue.mutex.Lock()
	queueCapacity := rawLogsQueue.capacity
	rawLogsQueue.mutex.Unlock()
	if queueCapacity <= 0 {
		return ErrBatchTooLarge
	}

	remainingLogs := rawLogs
	for len(remainingLogs) > 0 {
		pieceSize := queueCapacity - rawLogsQueue.Len()
		if pieceSize <= 0 {
			pieceSize = 1
		}
		if pieceSize > len(remainingLogs) {
			pieceSize = len(remainingLogs)
		}

		pushError := rawLogsQueue.PushBatch(remainingLogs[:pieceSize])
		if pushError == nil {
			remainingLogs = remainingLogs[pieceSize:]
			continue
		}
		if errors.Is(pushError, ErrQueueClosed) {
			return pushError
		}

		select {
		case <-applicationContext.Done():
			return applicationContext.Err()
		case <-time.After(capacityRetryDelay):
		}
	}
	return nil
}

// Drains the queue for shutdown, blocks new producers, marks the queue closed and removes the logs still waiting so they are not lost.
func (rawLogsQueue *RawLogsQueue) DrainRemainingLogs() []model.RawLog {
	rawLogsQueue.mutex.Lock()
	defer rawLogsQueue.mutex.Unlock()

	rawLogsQueue.isClosed = true
	rawLogsQueue.condition.Broadcast()

	undrainedLogs := rawLogsQueue.buffer
	rawLogsQueue.buffer = nil
	return undrainedLogs
}

// Removes and returns the oldest waiting log. Callers must hold the mutex.
func (rawLogsQueue *RawLogsQueue) removeFront() model.RawLog {
	rawLog := rawLogsQueue.buffer[0]
	rawLogsQueue.buffer = rawLogsQueue.buffer[1:]
	if len(rawLogsQueue.buffer) == 0 {
		rawLogsQueue.buffer = nil
	}
	rawLogsQueue.condition.Broadcast()
	return rawLog
}
