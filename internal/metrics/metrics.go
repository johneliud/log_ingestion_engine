package metrics

import (
	"maps"
	"sync"
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
	return &Metrics{
		logsByLevel:           make(map[model.LogLevel]uint64),
		logsByService:         make(map[string]uint64),
		throughputWindow:      throughputWindow,
		throughputWindowLogs:  0,
		lastThroughputTime:    time.Time{},
		hasRecordedThroughput: false,
		currentTimeProvider:   currentTimeProvider,
	}
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
}

// Records one request rejected by the rate limiter.
func (metrics *Metrics) RecordRateLimitedRequest() {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()
	metrics.rateLimitedRequests++
}

// Records one log that exhausted every storage retry and was dead-lettered.
func (metrics *Metrics) RecordDeadLetteredLog() {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()
	metrics.deadLetteredLogs++
}

// Records one failed durable write attempt.
func (metrics *Metrics) RecordWriteFailure() {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()
	metrics.writeFailures++
}

// Updates the queue backlog count sourced from the RabbitMQ management API.
func (metrics *Metrics) SetQueueBacklog(backlog int) {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()
	metrics.queueBacklog = backlog
}

// Records one message handed to the broker or the in-memory fallback, raising the in-flight backlog estimate.
func (metrics *Metrics) RecordQueueMessagePublished() {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()
	metrics.fallbackQueueBacklog++
}

// Records one message stored by a consumer, lowering the in-flight backlog estimate.
func (metrics *Metrics) RecordQueueMessageStored() {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()
	if metrics.fallbackQueueBacklog > 0 {
		metrics.fallbackQueueBacklog--
	}
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
