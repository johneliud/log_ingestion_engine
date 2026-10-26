package metrics

import (
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

// Owns every metric counter and the sliding throughput window used to compute logs per second.
type Metrics struct {
	mutex                  sync.RWMutex
	totalLogsReceived      uint64
	logsByLevel            map[model.LogLevel]uint64
	logsByService          map[string]uint64
	rateLimitedRequests    uint64
	deadLetteredLogs       uint64
	writeFailures          uint64
	queueBacklog           int
	throughputWindow       time.Duration
	throughputWindowCount  int
	throughputWindowCounts []uint64
	lastThroughputSecond   int64
	hasRecordedThroughput  bool
	currentTimeProvider    func() time.Time
}

// Returns a metrics registry whose throughput window matches the configured duration.
func NewMetrics(throughputWindow time.Duration, currentTimeProvider func() time.Time) *Metrics {
	if throughputWindow <= 0 {
		throughputWindow = time.Minute
	}

	if currentTimeProvider == nil {
		currentTimeProvider = time.Now
	}

	throughputWindowCount := max(1, int(throughputWindow.Seconds()))
	return &Metrics{
		logsByLevel:            make(map[model.LogLevel]uint64),
		logsByService:          make(map[string]uint64),
		throughputWindow:       throughputWindow,
		throughputWindowCount:  throughputWindowCount,
		throughputWindowCounts: make([]uint64, throughputWindowCount),
		currentTimeProvider:    currentTimeProvider,
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

	currentSecond := metrics.currentTimeProvider().Unix()
	metrics.advanceThroughputWindowTo(currentSecond)
	metrics.throughputWindowCounts[currentSecond%int64(metrics.throughputWindowCount)] += uint64(len(logs))
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

