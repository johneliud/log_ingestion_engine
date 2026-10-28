package metrics

import (
	"maps"
	"slices"
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
	LogsPerSecond       float64           `json:"logs_per_second"`
	ThroughputHistory   []HistoryPoint    `json:"throughput_history"`
	ErrorRateHistory    []HistoryPoint    `json:"error_rate_history"`
	QueueBacklog        int               `json:"queue_backlog"`
	RateLimitedRequests uint64            `json:"rate_limited_requests"`
	DeadLetteredLogs    uint64            `json:"dead_lettered_logs"`
	WriteFailures       uint64            `json:"write_failures"`
}

// Holds one timestamped sample of a derived metric for the dashboard history charts.
type HistoryPoint struct {
	Time  string  `json:"t"`
	Value float64 `json:"y"`
}

const (
	defaultThroughputWindow = time.Minute
	maximumThroughputWindow = time.Hour
	broadcastInterval       = 200 * time.Millisecond
	historySampleInterval   = 2 * time.Second
	maximumHistoryPoints    = 90
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

	throughputHistory     []HistoryPoint
	errorRateHistory      []HistoryPoint
	lastHistorySampleTime time.Time
	hasSampledHistory     bool

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

// Updates the queue backlog count sourced from the RabbitMQ management API, leaving the snapshot untouched when the value did not change.
func (metrics *Metrics) SetQueueBacklog(backlog int) {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()
	if metrics.queueBacklog == backlog {
		return
	}
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

	errorRate := metrics.currentErrorRate()
	throughput := metrics.currentThroughput()

	return Snapshot{
		TotalLogsReceived:   metrics.totalLogsReceived,
		LogsByLevel:         logsByLevel,
		LogsByService:       logsByService,
		ErrorRate:           errorRate,
		Throughput:          throughput,
		LogsPerSecond:       throughput,
		ThroughputHistory:   slices.Clone(metrics.throughputHistory),
		ErrorRateHistory:    slices.Clone(metrics.errorRateHistory),
		QueueBacklog:        metrics.queueBacklog,
		RateLimitedRequests: metrics.rateLimitedRequests,
		DeadLetteredLogs:    metrics.deadLetteredLogs,
		WriteFailures:       metrics.writeFailures,
	}
}

// Returns the share of received logs that were logged at the ERROR level.
func (metrics *Metrics) currentErrorRate() float64 {
	if metrics.totalLogsReceived == 0 {
		return 0
	}
	return float64(metrics.logsByLevel[model.LogLevelError]) / float64(metrics.totalLogsReceived)
}

// Returns the average logs per second over the configured throughput window.
func (metrics *Metrics) currentThroughput() float64 {
	if !metrics.hasRecordedThroughput {
		return 0
	}
	return float64(metrics.throughputWindowLogs) / metrics.throughputWindow.Seconds()
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
	defer metrics.subscriberMu.Unlock()
	delete(metrics.subscribers, ch)
	close(ch)
}

// Signals the broadcast loop that a metric changed. It is non-blocking.
func (metrics *Metrics) notify() {
	select {
	case metrics.changeNotifier <- struct{}{}:
	default:
	}
}

// Sends the current snapshot to every subscriber when at least one metric has changed since the last broadcast. The sends happen under the subscriber lock so a concurrent Unsubscribe can never close a channel mid-send; every send is non-blocking, so a slow subscriber never stalls the broadcast.
func (metrics *Metrics) broadcastIfDirty() {
	metrics.subscriberMu.Lock()
	defer metrics.subscriberMu.Unlock()
	if !metrics.dirty.Load() || len(metrics.subscribers) == 0 {
		metrics.dirty.Store(false)
		return
	}
	metrics.dirty.Store(false)

	snapshot := metrics.Snapshot()
	for ch := range metrics.subscribers {
		select {
		case ch <- snapshot:
		default:
			select {
			case <-ch:
			default:
			}
			select {
			case ch <- snapshot:
			default:
			}
		}
	}
}

// Samples and broadcasts on the fixed ticker interval.
func (metrics *Metrics) broadcastLoop() {
	defer close(metrics.broadcastDone)
	ticker := time.NewTicker(broadcastInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			metrics.sampleHistoryIfDue()
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

// Appends one timestamped sample of the throughput and the error rate to the history buffers, at most once per history sample interval, keeping only the most recent samples.
func (metrics *Metrics) sampleHistoryIfDue() {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	currentTime := metrics.currentTimeProvider()
	if metrics.hasSampledHistory && currentTime.Sub(metrics.lastHistorySampleTime) < historySampleInterval {
		return
	}
	metrics.lastHistorySampleTime = currentTime
	metrics.hasSampledHistory = true

	sampleTime := currentTime.UTC().Format(time.RFC3339)
	metrics.advanceThroughputWindowTo(currentTime)
	metrics.throughputHistory = appendHistoryPoint(metrics.throughputHistory, HistoryPoint{Time: sampleTime, Value: metrics.currentThroughput()})
	metrics.errorRateHistory = appendHistoryPoint(metrics.errorRateHistory, HistoryPoint{Time: sampleTime, Value: metrics.currentErrorRate()})
}

// Appends one history point, dropping the oldest point when the buffer is full.
func appendHistoryPoint(historyPoints []HistoryPoint, historyPoint HistoryPoint) []HistoryPoint {
	if len(historyPoints) >= maximumHistoryPoints {
		historyPoints = historyPoints[1:]
	}
	return append(historyPoints, historyPoint)
}
