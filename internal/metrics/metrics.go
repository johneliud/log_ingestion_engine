package metrics

import (
	"maps"
	"sync"
	"sync/atomic"
	"time"

	"github.com/johneliud/log_ingestion_engine/internal/model"
)

// Holds an immutable point-in-time view of every tracked metric.
type Snapshot struct {
	TotalLogsReceived   uint64            `json:"total_logs_received"`
	LogsByLevel         map[string]uint64 `json:"logs_by_level"`
	LogsByService       map[string]uint64 `json:"logs_by_service"`
	ErrorRate           float64           `json:"error_rate"`
	Throughput          float64           `json:"throughput"`
	QueueBacklog        int               `json:"queue_backlog"`
	RateLimitedRequests uint64            `json:"rate_limited_requests"`
	DeadLetteredLogs    uint64            `json:"dead_lettered_logs"`
	WriteFailures       uint64            `json:"write_failures"`
}

const (
	defaultThroughputWindow = time.Minute
	maximumThroughputWindow = time.Hour
	broadcastInterval       = 200 * time.Millisecond
)

// Owns every metric counter and the sliding throughput window used to compute logs per second.
type Metrics struct {
	mutex                 sync.RWMutex
	totalLogsReceived     uint64
	logsByLevel           map[model.LogLevel]uint64
	logsByService         map[string]uint64
	rateLimitedRequests   uint64
	deadLetteredLogs      uint64
	writeFailures         uint64
	queueBacklog          int
	fallbackQueueBacklog  int
	throughputWindow      time.Duration
	throughputWindowLogs  uint64
	lastThroughputTime    time.Time
	hasRecordedThroughput bool
	currentTimeProvider   func() time.Time

	subscribers    map[chan Snapshot]struct{}
	subscriberMu   sync.Mutex
	dirty          atomic.Bool
	changeNotifier chan struct{}
	broadcastDone  chan struct{}
}

// Returns a metrics registry whose throughput window matches the configured duration.
func NewMetrics(throughputWindow time.Duration, currentTimeProvider func() time.Time) *Metrics {
	if throughputWindow <= 0 {
		throughputWindow = defaultThroughputWindow
	}
	if throughputWindow > maximumThroughputWindow {
		throughputWindow = maximumThroughputWindow
	}
	if currentTimeProvider == nil {
		currentTimeProvider = time.Now
	}
	m := &Metrics{
		logsByLevel:           make(map[model.LogLevel]uint64),
		logsByService:         make(map[string]uint64),
		throughputWindow:      throughputWindow,
		throughputWindowLogs:  0,
		lastThroughputTime:    time.Time{},
		hasRecordedThroughput: false,
		currentTimeProvider:   currentTimeProvider,
		subscribers:           make(map[chan Snapshot]struct{}),
		changeNotifier:        make(chan struct{}, 1),
		broadcastDone:         make(chan struct{}),
	}

	go m.broadcastLoop()
	return m
}

// Records one accepted log, incrementing the total, level, service and throughput counters.
func (metrics *Metrics) RecordLogsReceived(logs []model.LogEntry) {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	for _, logEntry := range logs {
		metrics.totalLogsReceived++
		metrics.logsByLevel[logEntry.Level]++
		metrics.logsByService[logEntry.Service]++
	}

	metrics.advanceThroughputWindowTo(metrics.currentTimeProvider())
	metrics.throughputWindowLogs += uint64(len(logs))
	metrics.dirty.Store(true)
	metrics.notify()
}

// Records one request rejected by the rate limiter.
func (metrics *Metrics) RecordRateLimitedRequest() {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()
	metrics.rateLimitedRequests++
	metrics.dirty.Store(true)
	metrics.notify()
}

// Records one log that exhausted every storage retry and was dead-lettered.
func (metrics *Metrics) RecordDeadLetteredLog() {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()
	metrics.deadLetteredLogs++
	metrics.dirty.Store(true)
	metrics.notify()
}

// Records one failed durable write attempt.
func (metrics *Metrics) RecordWriteFailure() {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()
	metrics.writeFailures++
	metrics.dirty.Store(true)
	metrics.notify()
}

// Updates the queue backlog count sourced from the RabbitMQ management API.
func (metrics *Metrics) SetQueueBacklog(backlog int) {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()
	metrics.queueBacklog = backlog
	metrics.dirty.Store(true)
	metrics.notify()
}

// Records one message handed to the broker or the in-memory fallback, raising the in-flight backlog estimate.
func (metrics *Metrics) RecordQueueMessagePublished() {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()
	metrics.fallbackQueueBacklog++
	metrics.dirty.Store(true)
	metrics.notify()
}

// Records one message stored by a consumer, lowering the in-flight backlog estimate.
func (metrics *Metrics) RecordQueueMessageStored() {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()
	if metrics.fallbackQueueBacklog > 0 {
		metrics.fallbackQueueBacklog--
	}
	metrics.dirty.Store(true)
	metrics.notify()
}

// Records one message that left the pipeline without being stored, such as a dead-lettered log, lowering the in-flight backlog estimate.
func (metrics *Metrics) RecordQueueMessageReleased() {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()
	if metrics.fallbackQueueBacklog > 0 {
		metrics.fallbackQueueBacklog--
	}
	metrics.dirty.Store(true)
	metrics.notify()
}

// Returns the local backlog estimate reported when the management API cannot be reached.
func (metrics *Metrics) FallbackQueueBacklog() int {
	metrics.mutex.RLock()
	defer metrics.mutex.RUnlock()
	return metrics.fallbackQueueBacklog
}

// Returns an immutable copy of every metric with error rate and throughput already computed.
func (metrics *Metrics) Snapshot() Snapshot {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()
	metrics.advanceThroughputWindowTo(metrics.currentTimeProvider())

	logsByLevel := make(map[string]uint64, len(metrics.logsByLevel))
	for level, count := range metrics.logsByLevel {
		logsByLevel[string(level)] = count
	}

	logsByService := make(map[string]uint64, len(metrics.logsByService))
	maps.Copy(logsByService, metrics.logsByService)

	errorRate := float64(0)
	if metrics.totalLogsReceived > 0 {
		errorRate = float64(metrics.logsByLevel[model.LogLevelError]) / float64(metrics.totalLogsReceived)
	}

	throughput := float64(0)
	if metrics.hasRecordedThroughput {
		throughput = float64(metrics.throughputWindowLogs) / metrics.throughputWindow.Seconds()
	}

	return Snapshot{
		TotalLogsReceived:   metrics.totalLogsReceived,
		LogsByLevel:         logsByLevel,
		LogsByService:       logsByService,
		ErrorRate:           errorRate,
		Throughput:          throughput,
		QueueBacklog:        metrics.queueBacklog,
		RateLimitedRequests: metrics.rateLimitedRequests,
		DeadLetteredLogs:    metrics.deadLetteredLogs,
		WriteFailures:       metrics.writeFailures,
	}
}

// Returns a channel that receives a snapshot whenever any metric changes.
func (metrics *Metrics) Subscribe() chan Snapshot {
	ch := make(chan Snapshot, 1)
	metrics.subscriberMu.Lock()
	metrics.subscribers[ch] = struct{}{}
	metrics.subscriberMu.Unlock()
	return ch
}

// Removes a subscriber channel and closes it.
func (metrics *Metrics) Unsubscribe(ch chan Snapshot) {
	metrics.subscriberMu.Lock()
	delete(metrics.subscribers, ch)
	metrics.subscriberMu.Unlock()
	close(ch)
}

// Signals the broadcast loop that a metric changed. It is non-blocking.
func (metrics *Metrics) notify() {
	select {
	case metrics.changeNotifier <- struct{}{}:
	default:
	}
}

// Sends the current snapshot to every subscriber when at least one metric has changed since the last broadcast.
func (metrics *Metrics) broadcastIfDirty() {
	metrics.subscriberMu.Lock()
	if !metrics.dirty.Load() || len(metrics.subscribers) == 0 {
		metrics.dirty.Store(false)
		metrics.subscriberMu.Unlock()
		return
	}
	metrics.dirty.Store(false)
	subscribers := make([]chan Snapshot, 0, len(metrics.subscribers))
	for ch := range metrics.subscribers {
		subscribers = append(subscribers, ch)
	}
	metrics.subscriberMu.Unlock()

	snapshot := metrics.Snapshot()
	for _, ch := range subscribers {
		select {
		case ch <- snapshot:
		default:
		}
	}
}

// Ticks on an interval and on every change notification, sending the current snapshot to all subscribers.
func (metrics *Metrics) broadcastLoop() {
	defer close(metrics.broadcastDone)
	ticker := time.NewTicker(broadcastInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			metrics.broadcastIfDirty()
		case <-metrics.changeNotifier:
			metrics.broadcastIfDirty()
		}
	}
}

// Clears the throughput window once the configured duration has elapsed and starts a new accumulation.
func (metrics *Metrics) advanceThroughputWindowTo(currentTime time.Time) {
	if !metrics.hasRecordedThroughput {
		metrics.lastThroughputTime = currentTime
		metrics.hasRecordedThroughput = true
		return
	}

	if currentTime.Sub(metrics.lastThroughputTime) < metrics.throughputWindow {
		return
	}

	metrics.throughputWindowLogs = 0
	metrics.lastThroughputTime = currentTime
}
